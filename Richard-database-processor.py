import os
import json
import logging
from datetime import datetime, timezone
import boto3
import pg8000
logger = logging.getLogger()
logger.setLevel(logging.INFO)
# ---- ENVIRONMENT VARIABLES ----
DB_SECRET_ARN = os.environ["DB_SECRET_ARN"]
DB_HOST       = os.environ["DB_HOST"]
DB_PORT       = os.environ.get("DB_PORT", 5432)
DB_NAME       = os.environ["DB_NAME"]
AWS_REGION    = os.environ.get("AWS_REGION", "us-gov-west-1")
METADATA_PROCESSING_QUEUE_NAME = os.environ.get("METADATA_PROCESSING_QUEUE_NAME")
# ---- AWS CLIENTS ----
sm         = boto3.client("secretsmanager", region_name=AWS_REGION)
cloudwatch = boto3.client("cloudwatch",     region_name=AWS_REGION)
sqs        = boto3.client("sqs",            region_name=AWS_REGION)
# ---- GLOBAL CONNECTION ----
conn = None
# ---- CLOUDWATCH METRIC ----
def put_metric(metric_name, value=1, unit="Count"):
    cloudwatch.put_metric_data(
        Namespace="HIE/OperationalMonitoring",
        MetricData=[{"MetricName": metric_name, "Value": value, "Unit": unit}],
    )
# ---- DB CONNECTION ----
def get_conn():
    try:
        secret = sm.get_secret_value(SecretId=DB_SECRET_ARN)
        creds  = json.loads(secret["SecretString"])
        return pg8000.connect(
            host=DB_HOST,
            port=int(DB_PORT),
            database=DB_NAME,
            user=creds["username"],
            password=creds["password"],
            ssl_context=True,
            timeout=10,
        )
    except Exception as e:
        put_metric("LambdaRetries")
        logger.warning("DB connection attempt failed: %s", str(e))
        put_metric("DBConnectionFailure")
        logger.error("DB connection failed: %s", str(e))
        raise
# ---- MESSAGE ROUTER ----
def process_message(message):
    message_type = message.get("message_type")
    logger.info("Routing message_type=%s", message_type)
    if message_type in ("MANIFEST_METADATA_INSERT", "MANIFEST_METADATA_UPSERT"):
        try:
            upsert_manifest_metadata(message)
        except Exception as e:
            logger.warning("DB write attempt failed: %s", str(e))
            conn.rollback()
            raise
    elif message_type == "PATIENT_REPORT_METADATA_UPSERT":
        try:
            upsert_patient_report_metadata(message)
        except Exception as e:
            logger.warning("DB write attempt failed: %s", str(e))
            conn.rollback()
            raise
    else:
        raise ValueError(f"Unsupported message type: {message_type}")
def get_metadata_processing_queue_url():
    if not METADATA_PROCESSING_QUEUE_NAME:
        raise RuntimeError("METADATA_PROCESSING_QUEUE_NAME environment variable is not configured")
    response = sqs.get_queue_url(QueueName=METADATA_PROCESSING_QUEUE_NAME)
    return response["QueueUrl"]


def process_sqs_queue_messages():
    queue_url = get_metadata_processing_queue_url()
    processed_count = 0
    while True:
        response = sqs.receive_message(
            QueueUrl=queue_url,
            MaxNumberOfMessages=10,
            WaitTimeSeconds=2,
            VisibilityTimeout=300
        )
        messages = response.get("Messages", [])
        if not messages:
            logger.info("No more SQS messages available.")
            break
        logger.info("Received %d message(s) from SQS queue", len(messages))
        for sqs_message in messages:
            message_id = sqs_message.get("MessageId")
            receipt_handle = sqs_message["ReceiptHandle"]
            body = sqs_message.get("Body", "{}")
            logger.info("Processing SQS message_id=%s", message_id)
            try:
                message = json.loads(body)
            except Exception:
                logger.exception("Failed to parse SQS message body.")
                raise
            process_message(message)
            try:
                sqs.delete_message(QueueUrl=queue_url, ReceiptHandle=receipt_handle)
            except Exception:
                logger.exception(
                    "Processed SQS message_id=%s successfully but failed to delete it from the queue.",
                    message_id
                )
            processed_count += 1
            logger.info("Deleted processed SQS message_id=%s", message_id)
    return processed_count
def upsert_manifest_batch_row(cur, partner_id, batch_id, partner_batch_key, file_name, expected, actual, discrepancy, now, ingestion_method):
    cur.execute(
        """
        INSERT INTO manifest_batch (
            partner_id, batch_id, partner_batch_key,
            manifest_file_count, actual_file_count, count_discrepancy,
            submission_timestamp, created_at, updated_at,
            file_name, ingestion_method
        ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
        ON CONFLICT (partner_id, batch_id, file_name)
        DO UPDATE SET
            partner_batch_key    = EXCLUDED.partner_batch_key,
            manifest_file_count  = EXCLUDED.manifest_file_count,
            actual_file_count    = EXCLUDED.actual_file_count,
            count_discrepancy    = EXCLUDED.count_discrepancy,
            submission_timestamp = EXCLUDED.submission_timestamp,
            updated_at           = EXCLUDED.updated_at,
            ingestion_method     = EXCLUDED.ingestion_method
        """,
        (partner_id, batch_id,
         (str(partner_batch_key) if partner_batch_key is not None else None),
         expected, actual, discrepancy, now, now, now,
         file_name, ingestion_method),
    )
# ---- DB FUNCTION: UPSERT MANIFEST METADATA ----
def upsert_manifest_metadata(message):
    source  = message.get("source", {})
    partner = message.get("partner", {})
    counts  = message.get("counts", {})
    files   = message.get("files", {})
    report_type = message.get("report_type")
    ingestion_method  = message.get("ingestion_method")
    batch_id          = source.get("batch_id")
    partner_id        = partner.get("partner_id")
    partner_batch_key = partner.get("partner_batch_key")
    environment       = partner.get("environment")
    partner_name      = partner.get("partner_name")
    file_name         = files.get("zip", {}).get("file_name")
    if not partner_id:
        raise ValueError(f"partner_id missing from message for batch_id={batch_id}")
    expected    = counts.get("manifest_expected_count", 0)
    actual      = counts.get("zip_actual_count", 0)
    discrepancy = counts.get("count_discrepancy", abs((actual or 0) - (expected or 0)))
    now         = datetime.now(timezone.utc)
    logger.info("batch_id=%s expected=%s actual=%s discrepancy=%s",
                batch_id, expected, actual, discrepancy)
    cur = conn.cursor()
    upsert_manifest_batch_row(cur, partner_id, batch_id, partner_batch_key, file_name, expected, actual, discrepancy, now, ingestion_method)
    rows = message.get("patient_received_report", {}).get("rows", [])
    if rows:
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
            if report_type == "RECEIVED":
                user_role_value = r.get("role")
                role_value = None
            else:
                user_role_value = None
                role_value = r.get("role")
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
                    user_id, user_name, role, role_code, commonwell_indicator
                ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                """,
                (partner_id, batch_id, environment, file_name, edipi_value,
                 r.get("last_name"), r.get("first_name"),
                 r.get("receipt_date") or r.get("date_of_receipt"),
                 r.get("disclosure_date"),
                 arrival,
                 r.get("sending_org"),
                 r.get("purpose_of_use") or r.get("purpose"),
                 r.get("purpose_of_use_code") or r.get("purpose_code"),
                 user_role_value,
                 r.get("format_code") or r.get("document_format_code"),
                 r.get("document_loinc_code") or r.get("loinc_code"),
                 r.get("document_id"),
                 r.get("repository_id"),
                 r.get("source_id"),
                 r.get("ccda_file_name") or r.get("ccda_file"),
                 r.get("receiving_organization") or r.get("receiving_org"),
                 r.get("partner") or partner_name,
                 r.get("user_id"),
                 r.get("user_name"),
                 role_value, r.get("role_code"),
                 r.get("commonwell_indicator")),
            )
            inserted += 1
        logger.info("Inserted %d patient_details rows for batch_id=%s", inserted, batch_id)
    else:
        logger.info("No patient rows in message for batch_id=%s skipping patient_details", batch_id)
    conn.commit()
    logger.info("Upserted manifest_batch for batch_id=%s", batch_id)
# ---- DB FUNCTION: UPSERT PATIENT REPORT METADATA ----
def upsert_patient_report_metadata(message):
    source  = message.get("source", {})
    partner = message.get("partner", {})
    files   = message.get("files", {})
    counts  = message.get("counts", {})
    report_type       = message.get("report_type")
    ingestion_method  = message.get("ingestion_method")
    batch_id          = source.get("batch_id")
    partner_id        = partner.get("partner_id")
    partner_batch_key = partner.get("partner_batch_key")
    environment       = partner.get("environment")
    partner_name      = partner.get("partner_name")
    file_name         = files.get("report", {}).get("file_name")
    if not partner_id:
        raise ValueError(f"partner_id missing from message for batch_id={batch_id}")

    expected    = counts.get("manifest_expected_count", 0)
    actual      = counts.get("report_actual_count", 0)
    discrepancy = counts.get("count_discrepancy", abs((actual or 0) - (expected or 0)))
    now         = datetime.now(timezone.utc)
    cur = conn.cursor()
    upsert_manifest_batch_row(cur, partner_id, batch_id, partner_batch_key, file_name, expected, actual, discrepancy, now, ingestion_method)
    conn.commit()
    logger.info("Upserted manifest_batch (report) batch_id=%s file_name=%s expected=%s actual=%s",
                batch_id, file_name, expected, actual)

    rows = message.get("patient_received_report", {}).get("rows", [])
    if not rows:
        logger.info("No patient rows in message for batch_id=%s skipping", batch_id)
        return
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
        if report_type == "RECEIVED":
            user_role_value = r.get("role")
            role_value = None
        else:
            user_role_value = None
            role_value = r.get("role")
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
                user_id, user_name, role, role_code, commonwell_indicator
            ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
            """,
            (partner_id, batch_id, environment, file_name, edipi_value,
             r.get("last_name"), r.get("first_name"),
             r.get("receipt_date") or r.get("date_of_receipt"),
             r.get("disclosure_date"),
             arrival,
             r.get("sending_org"),
             r.get("purpose_of_use") or r.get("purpose"),
             r.get("purpose_of_use_code") or r.get("purpose_code"),
             user_role_value,
             r.get("format_code") or r.get("document_format_code"),
             r.get("document_loinc_code") or r.get("loinc_code"),
             r.get("document_id"),
             r.get("repository_id"),
             r.get("source_id"),
             r.get("ccda_file_name") or r.get("ccda_file"),
             r.get("receiving_organization") or r.get("receiving_org"),
             r.get("partner") or partner_name,
             r.get("user_id"),
             r.get("user_name"),
             role_value, r.get("role_code"),
             r.get("commonwell_indicator")),
        )
        inserted += 1
    conn.commit()
    logger.info("Upserted %d patient_details rows for batch_id=%s", inserted, batch_id)
# ---- LAMBDA HANDLER ----
def lambda_handler(event, context):
    global conn
    records = event.get("Records", [])
    logger.info("Lambda triggered - %d records", len(records))
    try:
        conn = get_conn()
        logger.info("DB connection established")
    except Exception as e:
        logger.error("DB connection failed: %s", str(e))
        put_metric("DBConnectionFailure")
        raise
    is_sns_trigger = any("Sns" in record for record in records)
    if is_sns_trigger:
        logger.info("Triggered by SNS / CloudWatch alarm. Reading metadata from SQS queue.")
        try:
            processed_count = process_sqs_queue_messages()
        finally:
            if conn is not None:
                conn.close()
                logger.info("DB connection closed")
        return {
            "statusCode": 200,
            "body": f"SNS trigger received. Processed {processed_count} SQS metadata message(s)."
        }
    failures = []
    try:
        for record in records:
            message_id = record.get("messageId")
            raw_body   = record.get("body")
            if raw_body is None:
                logger.warning("Skipping record because no SQS body was found: %s", json.dumps(record))
                continue
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
