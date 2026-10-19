import os
import json
import logging
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

# ---- AWS CLIENTS ----
sm         = boto3.client("secretsmanager", region_name=AWS_REGION)
cloudwatch = boto3.client("cloudwatch",     region_name=AWS_REGION)

# ---- GLOBAL CONNECTION ----
conn = None


# ---- CLOUDWATCH METRIC ----
def put_metric(metric_name, value=1, unit="Count"):
    cloudwatch.put_metric_data(
        Namespace="HIE/OperationalMonitoring",
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


# ---- PARTNER CLOUDWATCH METRIC ----
def publish_partner_metric(partner_name, metric_name, value):
    cloudwatch.put_metric_data(
        Namespace="HIE/OperationalMonitoring",
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

# ---- DASHBOARD METRICS ----
# Metrics the operational dashboard and alarms read. Publishing is wrapped so
# a CloudWatch error can never fail message processing.
FUNCTION_NAME = os.environ.get("AWS_LAMBDA_FUNCTION_NAME", "unknown")


def put_dashboard_metric(metric_name, value=1, unit="Count", dimensions=None,
                         namespace="HIE/OperationalMonitoring"):
    try:
        metric = {"MetricName": metric_name, "Value": value, "Unit": unit}
        if dimensions:
            metric["Dimensions"] = dimensions
        cloudwatch.put_metric_data(Namespace=namespace, MetricData=[metric])
    except Exception as metric_error:
        logger.warning("Failed to publish metric %s: %s", metric_name, str(metric_error))


def put_crud_failure_metrics(failure_type, partner_name):
    """CRUDFailureEvents per FunctionName + FailureType, also per partner and
    as "All" so the dashboard Partner dropdown can show them."""
    dimensions = [
        {"Name": "FunctionName", "Value": FUNCTION_NAME},
        {"Name": "FailureType", "Value": failure_type},
    ]
    put_dashboard_metric("CRUDFailureEvents", dimensions=dimensions)
    for pname in ([partner_name, "All"] if partner_name and partner_name != "All" else ["All"]):
        put_dashboard_metric(
            "CRUDFailureEvents",
            dimensions=dimensions + [{"Name": "PartnerName", "Value": pname}],
        )


# ---- CRUD OPERATION TRACKING ----
# Records which kind of SQL statement (Create / Read / Update / Delete) ran
# last, so a database error can be reported against that operation.
_last_crud_operation = None
_last_failure_type = None


def track_crud(operation):
    global _last_crud_operation
    _last_crud_operation = operation


def classify_failure(e):
    global _last_failure_type
    if isinstance(e, pg8000.Error) and _last_crud_operation:
        _last_failure_type = _last_crud_operation
    else:
        _last_failure_type = "OtherFailure"


def reset_failure_tracking():
    global _last_crud_operation, _last_failure_type
    _last_crud_operation = None
    _last_failure_type = None


# ---- DB CONNECTION WITH RETRY ----
def get_conn():
    secret = sm.get_secret_value(SecretId=DB_SECRET_ARN)
    creds  = json.loads(secret["SecretString"])

    def connect():
        try:
            return pg8000.connect(
                host=DB_HOST,
                port=DB_PORT,
                database=DB_NAME,
                user=creds["username"],
                password=creds["password"],
                ssl_context=True,
            )
        except Exception as e:
            put_metric("LambdaRetries")
            put_dashboard_metric(
                "RetryEvents",
                dimensions=[
                    {"Name": "FunctionName", "Value": FUNCTION_NAME},
                    {"Name": "FailureType", "Value": "DatabaseConnectionFailure"},
                ],
            )
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
        put_metric("DBConnectionFailure")
        logger.error("DB connection failed after all retries: %s", str(e))
        put_dashboard_metric("DatabaseConnectionFailures")
        if "password authentication failed" in str(e).lower():
            put_dashboard_metric("LoginFailures")
        put_crud_failure_metrics("DatabaseConnectionFailure", None)
        raise Exception("Failed to connect to PostgreSQL after all retries")


# ---- MESSAGE ROUTER WITH RETRY ----
def process_message(message):
    message_type = message.get("message_type")
    logger.info("Routing message_type=%s", message_type)

    if message_type in ("MANIFEST_METADATA_INSERT", "MANIFEST_METADATA_UPSERT"):

        def run():
            try:
                upsert_manifest_metadata(message)
            except Exception as e:
                put_metric("LambdaRetries")
                classify_failure(e)
                logger.warning("DB write attempt failed: %s", str(e))
                conn.rollback()
                raise

        result = retry_with_backoff(
            operation=run,
            max_attempts=3,
            retry_delay_seconds=1,
            return_bool=True
        )

        if result is False:
            raise Exception(f"Failed to process message_type: {message_type} after all retries")

    elif message_type == "PATIENT_REPORT_METADATA_UPSERT":

        def run():
            try:
                upsert_patient_report_metadata(message)
            except Exception as e:
                put_metric("LambdaRetries")
                classify_failure(e)
                logger.warning("DB write attempt failed: %s", str(e))
                conn.rollback()
                raise

        result = retry_with_backoff(
            operation=run,
            max_attempts=3,
            retry_delay_seconds=1,
            return_bool=True
        )

        if result is False:
            raise Exception(f"Failed to process message_type: {message_type} after all retries")

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

    if row[0] > 0:
        logger.info("manifest_batch row already exists for batch_id=%s file_name=%s skipping",
                    batch_id, file_name)
        put_dashboard_metric("DuplicateBatchSkipped")
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
                inserted += 1

        logger.info("patient_details for batch_id=%s inserted=%d skipped=%d",
                    batch_id, inserted, skipped)
    else:
        logger.info("No patient rows in message for batch_id=%s skipping patient_details", batch_id)

    conn.commit()
    logger.info("Processed manifest_batch for batch_id=%s", batch_id)

    if discrepancy:
        put_dashboard_metric("CountMismatchEvents")


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
            inserted += 1

    conn.commit()
    logger.info("patient_details for batch_id=%s inserted=%d skipped=%d",
                batch_id, inserted, skipped)
    put_dashboard_metric("ArchiveStatus")


# ---- LAMBDA HANDLER ----
def lambda_handler(event, context):
    global conn
    logger.info("Lambda triggered - %d records", len(event.get("Records", [])))

    try:
        conn = get_conn()
        logger.info("DB connection established")
    except Exception as e:
        logger.error("DB connection failed: %s", str(e))
        put_metric("DBConnectionFailure")
        raise

    failures = []

    try:
        for record in event.get("Records", []):
            message_id = record.get("messageId")
            raw_body   = record.get("body")
            message = None

            try:
                message = json.loads(raw_body)
                logger.info("msg %s type=%s batch=%s rows=%s",
                            message_id,
                            message.get("message_type"),
                            message.get("source", {}).get("batch_id"),
                            message.get("patient_received_report", {}).get("row_count"))

                reset_failure_tracking()
                processing_start = datetime.now(timezone.utc)

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

                put_dashboard_metric(
                    "ProcessingDelays",
                    value=(datetime.now(timezone.utc) - processing_start).total_seconds(),
                    unit="Seconds",
                )
                put_dashboard_metric(
                    "BytesIn",
                    value=(message.get("metrics") or {}).get("bytes_in", 0) or 0,
                    unit="Bytes",
                    dimensions=[{"Name": "PartnerName", "Value": partner_name}],
                    namespace="HIE/PartnerMonitoring",
                )
                if message.get("message_type") == "PATIENT_REPORT_METADATA_UPSERT":
                    put_dashboard_metric(
                        "FilesIn",
                        value=(message.get("counts") or {}).get("report_actual_count", 0) or 0,
                        dimensions=[{"Name": "PartnerName", "Value": partner_name}],
                        namespace="HIE/PartnerMonitoring",
                    )
                logger.info("Processed message ID: %s", message_id)

            except Exception as e:
                try:
                    conn.rollback()
                except Exception:
                    pass

                partner_name = (
                    message.get("partner", {}).get("partner_name", "Unknown")
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
                put_dashboard_metric("CRUDFailures")
                put_crud_failure_metrics(_last_failure_type or "OtherFailure", partner_name)
                put_dashboard_metric(
                    "Errors",
                    dimensions=[{"Name": "PartnerName", "Value": partner_name}],
                    namespace="HIE/PartnerMonitoring",
                )
                failures.append({"itemIdentifier": message_id})

    finally:
        if conn is not None:
            try:
                conn.close()
                logger.info("DB connection closed")
            except Exception:
                pass
            conn = None
    return {"batchItemFailures": failures}
