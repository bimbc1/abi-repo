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
DB_PORT       = os.environ.get("DB_PORT", 5432)
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
        MetricData=[{"MetricName": metric_name, "Value": value, "Unit": unit}],
    )


# ---- DB CONNECTION WITH RETRY ----
def get_conn():

    def connect():
        secret = sm.get_secret_value(SecretId=DB_SECRET_ARN)
        creds  = json.loads(secret["SecretString"])
        try:
            return pg8000.connect(
                host=DB_HOST,
                port=int(DB_PORT),
                database=DB_NAME,
                user=creds["username"],
                password=creds["password"],
                ssl_context=True,
            )
        except Exception as e:
            put_metric("LambdaRetries")
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
    status  = message.get("status", {})
    files   = message.get("files", {})

    batch_id          = source.get("batch_id")
    partner_id        = partner.get("partner_id")
    partner_batch_key = partner.get("partner_batch_key")
    environment       = partner.get("environment")
    partner_name      = partner.get("partner_name")
    file_name         = files.get("manifest", {}).get("file_name")
    # ← reads file_name from files.manifest.file_name

    if not partner_id:
        raise ValueError(f"partner_id missing from message for batch_id={batch_id}")

    expected    = counts.get("manifest_expected", {}).get("CCDA", 0)
    # ← changed from "PatientReceivedCCDA" to "CCDA"
    actual      = counts.get("zip_actual", 0)
    # ← changed from "zip_actual_count" to "zip_actual"
    discrepancy = counts.get("discrepancy", abs((actual or 0) - (expected or 0)))
    # ← changed from "count_discrepancy" to "discrepancy"
    now         = datetime.now(timezone.utc)

    cur = conn.cursor()
    cur.execute(
        """
        INSERT INTO manifest_batch (
            partner_id, batch_id, partner_batch_key,
            manifest_file_count, actual_file_count, count_discrepancy,
            submission_timestamp, created_at, updated_at,
            file_name
        ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
        """,
        (partner_id, batch_id, str(partner_batch_key),
         expected, actual, discrepancy, now, now, now,
         file_name),
    )

    if status.get("ready_for_db_insert") and counts.get("all_match"):
        # ← changed from "ready_for_database_insert" to "ready_for_db_insert"
        # ← changed from "count_match" to "all_match"
        rows = message.get("patient_received_report", {}).get("rows", [])
        cur.execute(
            "DELETE FROM patient_details WHERE partner_id = %s AND batch_id = %s",
            (partner_id, batch_id),
        )
        arrival  = datetime.now(timezone.utc)
        inserted = 0
        for r in rows:
            try:
                edipi_value = int(r["edipi"])
            except (KeyError, TypeError, ValueError):
                continue
            cur.execute(
                """
                INSERT INTO patient_details (
                    partner_id, batch_id, environment, edipi,
                    patient_last_name, patient_first_name,
                    date_of_receipt_utc, file_arrival_time_utc, sending_organization,
                    purpose_of_use, purpose_of_use_code, user_role,
                    document_format_code, document_loinc_code, document_id,
                    repository_id, source_id, ccda_file_name,
                    file_name, date_of_disclosure_utc,
                    receiving_organization, partner, user_id,
                    user_name, role, role_code, commonwell_indicator
                ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                """,
                (partner_id, batch_id, environment, edipi_value,
                 r.get("last_name"), r.get("first_name"),
                 r.get("receipt_date"), arrival, r.get("sending_org"),
                 r.get("purpose"), r.get("purpose_code"), r.get("role"),
                 r.get("format_code"), r.get("loinc_code"), r.get("document_id"),
                 r.get("repository_id"), r.get("source_id"), r.get("ccda_file"),
                 file_name, r.get("date_of_disclosure_utc"),
                 r.get("receiving_organization"), partner_name, r.get("user_id"),
                 r.get("user_name"), r.get("role"), r.get("role_code"), r.get("commonwell_indicator")),
            )
            inserted += 1
        logger.info("Inserted %d patient_details rows for batch_id=%s", inserted, batch_id)
    else:
        logger.info("Batch %s not ready / count mismatch; manifest_batch only", batch_id)

    conn.commit()
    logger.info("Upserted manifest_batch for batch_id=%s", batch_id)


# ---- DB FUNCTION: UPSERT PATIENT REPORT METADATA ----
def upsert_patient_report_metadata(message):
    source  = message.get("source", {})
    partner = message.get("partner", {})
    files   = message.get("files", {})

    batch_id     = source.get("batch_id")
    partner_id   = partner.get("partner_id")
    environment  = partner.get("environment")
    partner_name = partner.get("partner_name")
    file_name    = files.get("manifest", {}).get("file_name")
    # ← reads file_name from files.manifest.file_name

    if not partner_id:
        raise ValueError(f"partner_id missing from message for batch_id={batch_id}")

    rows = message.get("patient_received_report", {}).get("rows", [])

    if not rows:
        logger.info("No patient rows in message for batch_id=%s skipping", batch_id)
        return

    cur = conn.cursor()

    cur.execute(
        "DELETE FROM patient_details WHERE partner_id = %s AND batch_id = %s",
        (partner_id, batch_id),
    )

    arrival  = datetime.now(timezone.utc)
    inserted = 0

    for r in rows:
        try:
            edipi_value = int(r["edipi"])
        except (KeyError, TypeError, ValueError):
            logger.warning("Skipping row with invalid edipi for batch_id=%s", batch_id)
            continue

        cur.execute(
            """
            INSERT INTO patient_details (
                partner_id, batch_id, environment, edipi,
                patient_last_name, patient_first_name,
                date_of_receipt_utc, file_arrival_time_utc, sending_organization,
                purpose_of_use, purpose_of_use_code, user_role,
                document_format_code, document_loinc_code, document_id,
                repository_id, source_id, ccda_file_name,
                file_name, date_of_disclosure_utc,
                receiving_organization, partner, user_id,
                user_name, role, role_code, commonwell_indicator
            ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
            """,
            (partner_id, batch_id, environment, edipi_value,
             r.get("last_name"), r.get("first_name"),
             r.get("receipt_date"), arrival, r.get("sending_org"),
             r.get("purpose"), r.get("purpose_code"), r.get("role"),
             r.get("format_code"), r.get("loinc_code"), r.get("document_id"),
             r.get("repository_id"), r.get("source_id"), r.get("ccda_file"),
             file_name, r.get("date_of_disclosure_utc"),
             r.get("receiving_organization"), partner_name, r.get("user_id"),
             r.get("user_name"), r.get("role"), r.get("role_code"), r.get("commonwell_indicator")),
        )
        inserted += 1

    conn.commit()
    logger.info("Upserted %d patient_details rows for batch_id=%s", inserted, batch_id)


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
            # ← lowercase "body" for SQS ESM records

            try:
                message = json.loads(raw_body)
                logger.info("msg %s type=%s batch=%s rows=%s",
                            message_id,
                            message.get("message_type"),
                            message.get("source", {}).get("batch_id"),
                            message.get("patient_received_report", {}).get("row_count"))

                process_message(message)
                logger.info("Processed message ID: %s", message_id)

            except Exception as e:
                try:
                    conn.rollback()
                except Exception:
                    pass
                logger.error("failed msg=%s: %s", message_id, str(e))
                put_metric("LambdaProcessingFailures")
                failures.append({"itemIdentifier": message_id})

    finally:
        if conn is not None:
            conn.close()
            logger.info("DB connection closed")

    return {"batchItemFailures": failures}