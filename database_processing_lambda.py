import os
import json
import logging
import ssl
from datetime import datetime, timezone
import boto3
import pg8000
from retry_utils import retry_with_backoff

logger = logging.getLogger()
logger.setLevel(logging.INFO)

# ---- ENVIRONMENT VARIABLES ----
DB_SECRET_ARN = os.environ["DB_SECRET_ARN"]
DB_HOST       = os.environ["DB_HOST"]
DB_PORT       = int(os.environ.get("DB_PORT", 5432))
DB_NAME       = os.environ["DB_NAME"]
AWS_REGION    = os.environ.get("AWS_REGION", "us-gov-west-1")
METRIC_NAMESPACE = os.environ["METRIC_NAMESPACE"]

RDS_CA_BUNDLE = "/opt/us-gov-west-1-bundle.pem"
SSL_CONTEXT   = ssl.create_default_context(cafile=RDS_CA_BUNDLE)

# ---- AWS CLIENTS ----
sm         = boto3.client("secretsmanager", region_name=AWS_REGION)
cloudwatch = boto3.client("cloudwatch",     region_name=AWS_REGION)
sqs        = boto3.client("sqs",            region_name=AWS_REGION)

METADATA_PROCESSING_QUEUE_NAME = os.environ.get("METADATA_PROCESSING_QUEUE_NAME")

# ---- GLOBAL CONNECTION ----
conn = None


# ---- CLOUDWATCH METRIC ----
def report_metric_publish_failure(metric_name, error):
    logger.error("METRIC_PUBLISH_FAILURE metric=%s error=%s", metric_name, str(error))


def put_metric(metric_name, value=1, unit="Count"):
    try:
        cloudwatch.put_metric_data(
            Namespace=METRIC_NAMESPACE,
            MetricData=[{
                "MetricName": metric_name,
                "Value": value,
                "Unit": unit,
                "Dimensions": [{
                    "Name": "FunctionName",
                    "Value": os.environ.get("AWS_LAMBDA_FUNCTION_NAME", "unknown"),
                }],
            }],
        )
    except Exception as e:
        report_metric_publish_failure(metric_name, e)


# ---- PARTNER CLOUDWATCH METRIC ----
def publish_partner_metric(partner_name, metric_name, value):
    try:
        cloudwatch.put_metric_data(
            Namespace=METRIC_NAMESPACE,
            MetricData=[
                {
                    "MetricName": metric_name,
                    "Value": value,
                    "Unit": "Count",
                    "Dimensions": [
                        {
                            "Name": "PartnerName",
                            "Value": partner_name
                        }
                    ]
                }
            ]
        )
    except Exception as e:
        report_metric_publish_failure(metric_name, e)

# ---- DASHBOARD METRICS ----
FUNCTION_NAME = os.environ.get("AWS_LAMBDA_FUNCTION_NAME", "unknown")


def put_dashboard_metric(metric_name, value=1, dimensions=None):
    try:
        metric = {"MetricName": metric_name, "Value": value, "Unit": "Count"}
        if dimensions:
            metric["Dimensions"] = dimensions
        cloudwatch.put_metric_data(Namespace=METRIC_NAMESPACE, MetricData=[metric])
    except Exception as metric_error:
        report_metric_publish_failure(metric_name, metric_error)


def put_crud_failure_metrics(failure_type, partner_id):
    """Publish CRUDFailureEvents with the PartnerId dimension the dashboard CRUD widgets
    query (the Partner dropdown value is the partner_id). Exactly two datapoints:
      1. PartnerId=<partner_id> (or "Unknown")  -> the partner-level datapoint
      2. PartnerId="All"                        -> an AGGREGATE copy for the "All" option
    The widgets filter on one exact PartnerId value, so each failure is counted once.
    Do not SUM across all PartnerId values: that would count each failure twice."""
    partner = str(partner_id) if partner_id not in (None, "") else "Unknown"  # "Unknown" fallback
    dimensions = [
        {"Name": "FunctionName", "Value": FUNCTION_NAME},
        {"Name": "FailureType", "Value": failure_type},
    ]
    for pid in (partner, "All"):                                               # only 2 copies: partner + All
        put_dashboard_metric(
            "CRUDFailureEvents",
            dimensions=dimensions + [{"Name": "PartnerId", "Value": pid}],
        )
    logger.info("Published CRUDFailureEvents FailureType=%s PartnerId=%s (+ All aggregate)", failure_type, partner)


# ---- CRUD OPERATION TRACKING ----
_last_crud_operation = None


def track_crud(operation):
    global _last_crud_operation
    _last_crud_operation = operation

_DB_ERROR_TYPES = tuple(
    t for t in (getattr(getattr(pg8000, "exceptions", None), "DatabaseError", None),) if t
)


# Only these CRUD FailureTypes are published (the ticket allows Create and Read only).
# A failed commit means the inserted rows were not saved, so it counts as Create.
CRUD_FAILURE_TYPES = {"Read": "Read", "Create": "Create", "Commit": "Create"}


def classify_failure(e, operation=None):
    """Return the CRUD FailureType ("Create" or "Read") for a failed write, or None.
      * connection error                         -> None (counted as RetryEvents instead)
      * database error during a Read step        -> "Read"
      * database error during a Create or Commit -> "Create"
      * anything else (bad input such as a missing partner_id, a non-database
        error, or a database error outside a tracked step) -> None (no CRUD metric;
        still logged and counted in ProcessingFailed / LambdaProcessingFailures)
    The step is read from the exception (e.crud_operation, set at the failure point)
    unless passed in."""
    if is_connection_error(e):
        return None
    if not isinstance(e, _DB_ERROR_TYPES):
        return None
    operation = operation or getattr(e, "crud_operation", None)
    return CRUD_FAILURE_TYPES.get(operation)


def reset_failure_tracking():
    global _last_crud_operation
    _last_crud_operation = None


class MessageProcessingError(Exception):
    def __init__(self, message, crud_failure_type=None):
        super().__init__(message)
        self.crud_failure_type = crud_failure_type

# ---- DATABASE CONNECTION FAILURES (dashboard "Failed Database Connections" + alarm) ----
class DatabaseConnectionError(Exception):
    """The database could not be reached (connect failed, or the connection was lost)."""


class ConnectionSetupError(DatabaseConnectionError):
    """Secrets Manager or the secret's JSON failed before connecting."""


_CONNECTION_ERROR_TYPES = tuple(
    t for t in (
        getattr(getattr(pg8000, "exceptions", None), "InterfaceError", None),  # pg8000 network / closed-connection errors
        ConnectionError,
        TimeoutError,
    ) if t
)


def is_connection_error(e):
    return isinstance(e, DatabaseConnectionError) or isinstance(e, _CONNECTION_ERROR_TYPES)


def publish_db_connection_failure():
    put_dashboard_metric(
        "RetryEvents",
        dimensions=[
            {"Name": "FunctionName", "Value": FUNCTION_NAME},
            {"Name": "FailureType", "Value": "DatabaseConnectionFailure"},
        ],
    )


# If connecting already failed in this invocation, later messages do not retry it again
# (that would only repeat the same failure and its metrics). Reset per invocation.
_conn_failed_this_invocation = False


# ---- DB CONNECTION WITH RETRY ----
def load_connection_settings():
    secret = sm.get_secret_value(SecretId=DB_SECRET_ARN)
    creds  = json.loads(secret["SecretString"])
    return creds


def get_conn():
    global _conn_failed_this_invocation
    try:
        creds = retry_with_backoff(
            operation=load_connection_settings,
            max_attempts=3,
            retry_delay_seconds=1,
            return_bool=False
        )
        username = creds["username"]
        password = creds["password"]
    except Exception as e:
        _conn_failed_this_invocation = True
        put_metric("DatabaseConnectionSetupFailures")
        logger.error("DB connection setup failed (Secrets Manager or secret JSON): %s", str(e))
        raise ConnectionSetupError("Failed to load database credentials") from e

    def connect():
        try:
            return pg8000.connect(
                host=DB_HOST,
                port=DB_PORT,
                database=DB_NAME,
                user=username,
                password=password,
                ssl_context=SSL_CONTEXT,
                timeout=5,
            )
        except Exception as e:
            publish_db_connection_failure()
            logger.warning("DB connection attempt failed: %s", str(e))
            raise

    try:
        return retry_with_backoff(
            operation=connect,
            max_attempts=3,
            retry_delay_seconds=1,
            return_bool=False
        )
    except Exception as e:
        _conn_failed_this_invocation = True
        put_metric("DBConnectionFailure")
        put_dashboard_metric("DatabaseConnectionFailures")
        logger.error("DB connection failed after all retries: %s", str(e))
        raise DatabaseConnectionError("Failed to connect to PostgreSQL after all retries") from e


def ensure_conn():
    """Reconnect if the connection was lost earlier in this invocation."""
    global conn
    if conn is not None:
        return
    if _conn_failed_this_invocation:
        raise DatabaseConnectionError("Database is unreachable (connection already failed in this invocation)")
    conn = get_conn()
    logger.info("DB connection re-established")


def drop_conn():
    global conn
    try:
        if conn is not None:
            conn.close()
    except Exception:
        pass
    conn = None


def log_ssl_status(connection):
    """Log proof that the DB connection uses SSL and the RDS certificate was verified."""
    try:
        cur = connection.cursor()
        cur.execute("SELECT ssl, version, cipher FROM pg_stat_ssl WHERE pid = pg_backend_pid()")
        ssl_on, tls_version, cipher = cur.fetchone()
        sock = getattr(connection, "_usock", None)
        cert = sock.getpeercert() if hasattr(sock, "getpeercert") else None
        issuer = dict(x[0] for x in cert.get("issuer", ())).get("commonName") if cert else None
        logger.info("DB SSL check: ssl=%s tls_version=%s cipher=%s certificate_verified=%s issuer=%s",
                    ssl_on, tls_version, cipher, bool(cert), issuer)
    except Exception as e:
        logger.warning("DB SSL check could not run: %s", str(e))
        try:
            connection.rollback()
        except Exception:
            pass


def handle_db_write_failure(e):
    """Called for every failed write attempt. A lost connection is reported as a
    database connection failure (and the connection is reopened on the next attempt);
    any other database error is recorded as a CRUD failure (Create / Read / Update)."""
    if is_connection_error(e):
        reset_failure_tracking()          # not a CRUD failure
        if not isinstance(e, DatabaseConnectionError):
            # connection was open and then lost; get_conn() already reports its own failures
            logger.warning("DB connection lost during write: %s", str(e))
            publish_db_connection_failure()
        drop_conn()
        return None
    failure_type = classify_failure(e)
    try:
        e.crud_failure_type = failure_type   # carry the result on the exception too
    except Exception:
        pass
    logger.warning("DB write attempt failed (%s): %s", failure_type or "not a CRUD failure", str(e))
    try:
        conn.rollback()
    except Exception:
        pass
    return failure_type


# ---- MESSAGE ROUTER WITH RETRY ----
def run_with_retries(message_type, write, message):
    """Write one message, retrying up to 3 times.

    COUNTING RULE: CRUDFailureEvents is counted PER MESSAGE, not per failed query. A
    message that fails after all retries publishes one CRUD failure, labelled with the
    most recent CRUD failure seen across its attempts (the same unit as ProcessingFailed
    and the DLQ). Every failed attempt is still logged.

    The failed step is carried ON THE EXCEPTION at the failure point, and kept here per
    message, so a later connection failure or a different exception raised by
    retry_with_backoff cannot lose an earlier CRUD failure."""
    last_failure = {"type": None}

    def run():
        reset_failure_tracking()
        try:
            ensure_conn()
            write(message)
        except Exception as e:
            # Carry the step that was running on the exception itself
            if getattr(e, "crud_operation", None) is None and _last_crud_operation:
                try:
                    e.crud_operation = _last_crud_operation
                except Exception:  # exception types that do not allow new attributes
                    pass
            failure_type = handle_db_write_failure(e)
            if failure_type:  # a later connection failure never clears an earlier CRUD failure
                last_failure["type"] = failure_type
            raise

    try:
        result = retry_with_backoff(
            operation=run,
            max_attempts=3,
            retry_delay_seconds=1,
            return_bool=True
        )
    except Exception as e:
        raise MessageProcessingError(
            f"Failed to process message_type: {message_type} after all retries",
            last_failure["type"] or getattr(e, "crud_failure_type", None),
        ) from e

    if result is False:
        raise MessageProcessingError(
            f"Failed to process message_type: {message_type} after all retries",
            last_failure["type"],
        )


def process_message(message):
    message_type = message.get("message_type")
    logger.info("Routing message_type=%s", message_type)

    if message_type in ("MANIFEST_METADATA_INSERT", "MANIFEST_METADATA_UPSERT"):
        run_with_retries(message_type, upsert_manifest_metadata, message)

    elif message_type == "PATIENT_REPORT_METADATA_UPSERT":
        run_with_retries(message_type, upsert_patient_report_metadata, message)

    else:
        raise ValueError(f"Unsupported message type: {message_type}")


# ---- DB FUNCTION: UPSERT MANIFEST METADATA ----
def upsert_manifest_metadata(message):
    source  = message.get("source", {})
    partner = message.get("partner", {})
    counts  = message.get("counts", {})
    files   = message.get("files", {})

    batch_id          = source.get("batch_id")
    partner_id        = partner.get("partner_id")
    partner_batch_key = partner.get("partner_batch_key")
    environment       = partner.get("environment")
    partner_name      = partner.get("partner_name")
    ingestion_method  = message.get("ingestion_method")
    file_name         = files.get("manifest", {}).get("file_name")

    if not partner_id:
        raise ValueError(f"partner_id missing from message for batch_id={batch_id}")

    expected    = counts.get("manifest_expected_count", 0)
    actual      = counts.get("zip_actual_count", 0)
    discrepancy = counts.get("count_discrepancy", abs((actual or 0) - (expected or 0)))
    now         = datetime.now(timezone.utc)

    logger.info("batch_id=%s expected=%s actual=%s discrepancy=%s",
                batch_id, expected, actual, discrepancy)

    cur = conn.cursor()

    # ---- CHECK IF manifest_batch ROW EXISTS ----
    track_crud("Read")
    cur.execute(
        """
        SELECT COUNT(*) FROM manifest_batch
        WHERE partner_id = %s
        AND batch_id = %s
        AND file_name = %s
        """,
        (partner_id, batch_id, file_name),
    )
    row = cur.fetchone()
    track_crud(None)

    if row[0] > 0:
        logger.info("manifest_batch row already exists for batch_id=%s file_name=%s skipping",
                    batch_id, file_name)
    else:
        track_crud("Create")
        cur.execute(
            """
            INSERT INTO manifest_batch (
                partner_id, batch_id, partner_batch_key,
                manifest_file_count, actual_file_count, count_discrepancy,
                submission_timestamp, created_at, updated_at,
                file_name, ingestion_method
            ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
            """,
            (partner_id, batch_id, str(partner_batch_key),
             expected, actual, discrepancy, now, now, now,
             file_name, ingestion_method),
        )
        track_crud(None)
        logger.info("Inserted manifest_batch row for batch_id=%s", batch_id)

    rows = message.get("patient_received_report", {}).get("rows", [])

    if rows:
        arrival  = datetime.now(timezone.utc)
        inserted = 0
        skipped  = 0
        for r in rows:
            try:
                edipi_value = int(r["edipi"])
            except (KeyError, TypeError, ValueError):
                continue

            # ---- CHECK IF patient_details ROW EXISTS ----
            track_crud("Read")
            cur.execute(
                """
                SELECT COUNT(*) FROM patient_details
                WHERE partner_id = %s
                AND batch_id = %s
                AND edipi = %s
                AND file_name = %s
                """,
                (partner_id, batch_id, edipi_value, file_name),
            )
            row = cur.fetchone()
            track_crud(None)

            if row[0] > 0:
                skipped += 1
                logger.info("Row already exists for partner_id=%s batch_id=%s edipi=%s file_name=%s skipping",
                            partner_id, batch_id, edipi_value, file_name)
            else:
                track_crud("Create")
                cur.execute(
                    """
                    INSERT INTO patient_details (
                        partner_id, batch_id, environment, file_name, edipi,
                        patient_last_name, patient_first_name,
                        date_of_receipt_utc, date_of_disclosure_utc,
                        file_arrival_time_utc, sending_organization,
                        purpose_of_use, purpose_of_use_code, user_role,
                        document_format_code, document_loinc_code, document_id,
                        repository_id, source_id, ccda_file_name,
                        receiving_organization, partner,
                        user_id, user_name, role, role_code, commonwell_indicator,ssn
                    ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                    """,
                    (partner_id, batch_id, environment, file_name, edipi_value,
                     r.get("last_name"), r.get("first_name"),
                     r.get("date_of_receipt"),
                     r.get("disclosure_date"),
                     arrival,
                     r.get("sending_org"),
                     r.get("purpose_of_use") or r.get("purpose"),
                     r.get("purpose_of_use_code") or r.get("purpose_code"),
                     r.get("user_role"),
                     r.get("document_format_code"),
                     r.get("document_loinc_code") or r.get("loinc_code"),
                     r.get("document_id"),
                     r.get("repository_id"),
                     r.get("source_id"),
                     r.get("ccda_file_name") or r.get("ccda_file"),
                     r.get("receiving_organization") or r.get("receiving_org"),
                     r.get("partner") or partner_name,
                     r.get("user_id"),
                     r.get("user_name"),
                     r.get("role"), r.get("role_code"),
                     r.get("commonwell_indicator"),
                     r.get("ssn")),
                )
                track_crud(None)
                inserted += 1

        logger.info("patient_details for batch_id=%s inserted=%d skipped=%d",
                    batch_id, inserted, skipped)
    else:
        logger.info("No patient rows in message for batch_id=%s skipping patient_details", batch_id)

    track_crud("Commit")
    conn.commit()
    track_crud(None)
    logger.info("Processed manifest_batch for batch_id=%s", batch_id)


# ---- DB FUNCTION: UPSERT PATIENT REPORT METADATA ----
def upsert_patient_report_metadata(message):
    source  = message.get("source", {})
    partner = message.get("partner", {})
    files   = message.get("files", {})
    direction = message.get("direction")

    batch_id     = source.get("batch_id")
    partner_id   = partner.get("partner_id")
    environment  = partner.get("environment")
    partner_name = partner.get("partner_name")
    file_name    = files.get("report", {}).get("file_name")

    if not partner_id:
        raise ValueError(f"partner_id missing from message for batch_id={batch_id}")

    rows = message.get("patient_received_report", {}).get("rows", [])

    if not rows:
        logger.info("No patient rows in message for batch_id=%s skipping", batch_id)
        return

    cur = conn.cursor()
    arrival  = datetime.now(timezone.utc)
    inserted = 0
    skipped  = 0

    for r in rows:
        try:
            edipi_value = int(r["edipi"])
        except (KeyError, TypeError, ValueError):
            logger.warning("Skipping row with invalid edipi for batch_id=%s", batch_id)
            continue

        # ---- CHECK IF patient_details ROW EXISTS ----
        track_crud("Read")
        cur.execute(
            """
            SELECT COUNT(*) FROM patient_details
            WHERE partner_id = %s
            AND batch_id = %s
            AND edipi = %s
            AND file_name = %s
            """,
            (partner_id, batch_id, edipi_value, file_name),
        )
        row = cur.fetchone()
        track_crud(None)

        if row[0] > 0:
            skipped += 1
            logger.info("Row already exists for partner_id=%s batch_id=%s edipi=%s file_name=%s skipping",
                        partner_id, batch_id, edipi_value, file_name)
        else:
            track_crud("Create")
            cur.execute(
                """
                INSERT INTO patient_details (
                    partner_id, batch_id, environment, file_name, edipi,
                    patient_last_name, patient_first_name,
                    date_of_receipt_utc, date_of_disclosure_utc,
                    file_arrival_time_utc, sending_organization,
                    purpose_of_use, purpose_of_use_code, user_role,
                    document_format_code, document_loinc_code, document_id,
                    repository_id, source_id, ccda_file_name,
                    receiving_organization, partner,
                    user_id, user_name, role, role_code, commonwell_indicator,direction
                ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                """,
                (partner_id, batch_id, environment, file_name, edipi_value,
                 r.get("last_name"), r.get("first_name"),
                 r.get("date_of_receipt"),
                 r.get("disclosure_date"),
                 arrival,
                 r.get("sending_org"),
                 r.get("purpose_of_use") or r.get("purpose"),
                 r.get("purpose_of_use_code") or r.get("purpose_code"),
                 r.get("user_role"),
                 r.get("document_format_code"),
                 r.get("document_loinc_code") or r.get("loinc_code"),
                 r.get("document_id"),
                 r.get("repository_id"),
                 r.get("source_id"),
                 r.get("ccda_file_name") or r.get("ccda_file"),
                 r.get("receiving_organization") or r.get("receiving_org"),
                 r.get("partner") or partner_name,
                 r.get("user_id"),
                 r.get("user_name"),
                 r.get("role"), r.get("role_code"),
                 r.get("commonwell_indicator"),
                 direction),         
            )
            track_crud(None)
            inserted += 1

    track_crud("Commit")
    conn.commit()
    track_crud(None)
    logger.info("patient_details for batch_id=%s inserted=%d skipped=%d",
                batch_id, inserted, skipped)


# ---- PROCESS ONE MESSAGE (shared by SNS and SQS paths) ----
def handle_one_message(message_id, raw_body):
    """Returns True on success, False on failure. Never raises."""
    message = None
    reset_failure_tracking()

    try:
        message = json.loads(raw_body)
        logger.info("msg %s type=%s batch=%s rows=%s",
                    message_id,
                    message.get("message_type"),
                    message.get("source", {}).get("batch_id"),
                    message.get("patient_received_report", {}).get("row_count"))

        process_message(message)

        partner_name = (
            (message.get("partner") or {}).get("partner_name")
            or "Unknown"
        )

        try:
            publish_partner_metric(
                partner_name,
                "ProcessingSuccess",
                1
            )
        except Exception as metric_error:
            logger.error(
                "Failed to publish ProcessingSuccess metric: %s",
                str(metric_error)
            )
        logger.info("Processed message ID: %s", message_id)
        return True

    except Exception as e:
        try:
            conn.rollback()
        except Exception:
            pass

        partner_name = (
            (message.get("partner") or {}).get("partner_name") or "Unknown"
            if isinstance(message, dict)
            else "Unknown"
        )

        try:
            publish_partner_metric(
                partner_name,
                "ProcessingFailed",
                1
            )
        except Exception as metric_error:
            logger.error(
                "Failed to publish ProcessingFailed metric: %s",
                str(metric_error)
            )
        logger.error("failed msg=%s: %s", message_id, str(e))
        put_metric("LambdaProcessingFailures")
        failure_type = getattr(e, "crud_failure_type", None)
        if failure_type:
            partner_id = (
                (message.get("partner") or {}).get("partner_id")
                if isinstance(message, dict)
                else None
            )
            put_crud_failure_metrics(failure_type, partner_id)
        return False


# ---- SNS PATH: PULL MESSAGES FROM SQS ----
def process_sqs_queue_messages():
    if not METADATA_PROCESSING_QUEUE_NAME:
        raise RuntimeError("METADATA_PROCESSING_QUEUE_NAME environment variable is not configured")

    queue_url = sqs.get_queue_url(QueueName=METADATA_PROCESSING_QUEUE_NAME)["QueueUrl"]
    processed = 0
    failed    = 0

    while True:
        response = sqs.receive_message(
            QueueUrl=queue_url,
            MaxNumberOfMessages=10,
            WaitTimeSeconds=2,
            VisibilityTimeout=300,
        )
        messages = response.get("Messages", [])

        if not messages:
            logger.info("No more SQS messages available.")
            break

        logger.info("Received %d message(s) from SQS queue", len(messages))

        for sqs_message in messages:
            message_id = sqs_message.get("MessageId")
            body       = sqs_message.get("Body")

            if handle_one_message(message_id, body):
                try:
                    sqs.delete_message(
                        QueueUrl=queue_url,
                        ReceiptHandle=sqs_message["ReceiptHandle"],
                    )
                    logger.info("Deleted processed SQS message_id=%s", message_id)
                except Exception:
                    logger.exception(
                        "Processed SQS message_id=%s but failed to delete it from the queue",
                        message_id,
                    )
                processed += 1
            else:
                # Not deleted -> becomes visible again after VisibilityTimeout,
                # and moves to the DLQ after maxReceiveCount.
                failed += 1

    return processed, failed


# ---- LAMBDA HANDLER ----
def lambda_handler(event, context):
    global conn, _conn_failed_this_invocation
    _conn_failed_this_invocation = False
    records = event.get("Records", [])
    logger.info("Lambda triggered - %d records", len(records))

    try:
        conn = get_conn()
        logger.info("DB connection established")
        log_ssl_status(conn)
    except Exception as e:
        logger.error("DB connection failed: %s", str(e))
        raise

    try:
        # ---- SNS TRIGGER (CloudWatch alarm) -> PULL FROM SQS QUEUE ----
        if any("Sns" in record for record in records):
            logger.info("Triggered by SNS. Reading metadata messages from SQS queue.")
            processed, failed = process_sqs_queue_messages()
            logger.info("SNS run complete: processed=%d failed=%d", processed, failed)
            return {
                "statusCode": 200,
                "processed": processed,
                "failed": failed,
            }

        # ---- DIRECT SQS TRIGGER -> MESSAGES ARE IN THE EVENT ----
        failures = []
        for record in records:
            message_id = record.get("messageId")
            if not handle_one_message(message_id, record.get("body")):
                failures.append({"itemIdentifier": message_id})
        return {"batchItemFailures": failures}

    finally:
        if conn is not None:
            try:
                conn.close()
                logger.info("DB connection closed")
            except Exception:
                pass
            conn = None
