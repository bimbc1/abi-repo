import os
import json
import logging
from datetime import datetime, timezone

import boto3
import psycopg2

logger = logging.getLogger()
logger.setLevel(logging.INFO)

DB_SECRET_ARN = os.environ["DB_SECRET_ARN"]
DB_HOST = os.environ["DB_HOST"]
DB_PORT = int(os.environ.get("DB_PORT", "5432"))
DB_NAME = os.environ.get("DB_NAME", "ccda01")
AWS_REGION = os.environ.get("AWS_REGION", "us-gov-west-1")
SNS_TOPIC_ARN = os.environ.get("SNS_TOPIC_ARN")
PROCESSING_FAILURE_MESSAGE_TEMPLATE = os.environ.get(
    "PROCESSING_FAILURE_MESSAGE_TEMPLATE",
    "Metadata database insertion failed. Bucket: {bucket} Batch ID: {batch_id} Partner: {partner_name} Error: {error}"
)

sm = boto3.client("secretsmanager", region_name=AWS_REGION)
sns = boto3.client("sns", region_name=AWS_REGION)
cloudwatch = boto3.client("cloudwatch", region_name=AWS_REGION)
sqs = boto3.client("sqs", region_name=AWS_REGION)
METADATA_PROCESSING_QUEUE_NAME = os.environ.get("METADATA_PROCESSING_QUEUE_NAME")

# NOTE: this Lambda no longer talks to S3 at all. The producer Lambda now
# embeds everything this Lambda needs directly in the SQS message body --
# file metadata (size/timestamp), expected vs. actual counts, and (for the
# report message) the already-parsed patient rows. So there's no more
# independent S3 read/parse happening here, and this Lambda's IAM role no
# longer needs s3:GetObject / s3:HeadObject permissions.

# NOTE: this Lambda now only writes to manifest_batch and patient_details.
# partner_transmission_state and partner_schedule (breach-flag) writes are
# owned entirely by the manifest metadata (producer) Lambda now, so
# update_partner_state / clear_breach_flag / send_recovery_sns and the
# RESUMED_MESSAGE_TEMPLATE / extract_batch_id helpers they used have been
# removed from this file -- they'd be dead code duplicating logic that
# already runs, and already commits, upstream.


# ============================================================
# COMMON HELPERS
# ============================================================

def put_metric(namespace, metric_name, value, unit="Count", dimensions=None):
    try:
        metric = {
            "MetricName": metric_name,
            "Value": value,
            "Unit": unit
        }

        if dimensions:
            metric["Dimensions"] = dimensions

        cloudwatch.put_metric_data(
            Namespace=namespace,
            MetricData=[metric]
        )

    except Exception as e:
        logger.warning("Failed to publish metric %s: %s", metric_name, e)


def get_conn():
    try:
        secret = sm.get_secret_value(SecretId=DB_SECRET_ARN)
        creds = json.loads(secret["SecretString"])

        return psycopg2.connect(
            host=DB_HOST,
            port=DB_PORT,
            dbname=DB_NAME,
            user=creds["username"],
            password=creds["password"],
            sslmode="require",
            connect_timeout=10,
        )

    except Exception as e:
        put_metric(
            namespace="HIE/OperationalMonitoring",
            metric_name="DatabaseConnectionFailures",
            value=1
        )

        if "password authentication failed" in str(e).lower():
            put_metric(
                namespace="HIE/OperationalMonitoring",
                metric_name="LoginFailures",
                value=1
            )

        logger.exception("Database connection failure")
        raise


# ============================================================
# PARTNER LOOKUP
# ============================================================

def get_partner_info(cur, bucket_name):
    bucket_arn = f"arn:aws-us-gov:s3:::{bucket_name}"

    cur.execute(
        """
        SELECT partner_id, partner_batch_key, environment
        FROM partner_registry
        WHERE s3_bucket_arn = %s
        """,
        (bucket_arn,),
    )

    row = cur.fetchone()

    if not row:
        raise RuntimeError(f"No partner registered for bucket ARN {bucket_arn}")

    return row[0], row[1], row[2]


def get_partner_name(cur, partner_id):
    cur.execute(
        """
        SELECT partner_name
        FROM partner_contact_details
        WHERE partner_id = %s
        LIMIT 1
        """,
        (partner_id,),
    )

    row = cur.fetchone()
    return row[0] if row else f"Partner {partner_id}"


# ============================================================
# DATABASE HELPERS
# ============================================================

def get_existing_batch_counts(cur, partner_id, batch_id, batch_name):
    """Look up the previously recorded file/row counts for this exact
    batch (partner_id, batch_id, batch_name), if any. Used to detect a
    duplicate re-delivery of a batch that was already fully processed --
    same files, same row counts -- so the caller can skip re-writing the
    database. Returns (manifest_file_count, actual_file_count), or None
    if no manifest_batch row exists yet for this batch."""
    cur.execute(
        """
        SELECT manifest_file_count, actual_file_count
        FROM manifest_batch
        WHERE partner_id = %s AND batch_id = %s AND batch_name = %s
        """,
        (partner_id, batch_id, batch_name),
    )

    return cur.fetchone()


def get_existing_patient_rows(cur, partner_id, batch_id):
    """Pull the patient_details rows currently stored for this batch, in
    a fixed field order, so they can be compared against the incoming
    message's rows to detect a content-level change (e.g. a corrected
     name) even when the row count hasn't changed."""
    cur.execute(
        """
        SELECT EDIPI, Patient_Last_Name, Patient_First_Name, Date_of_Receipt_UTC,
               Sending_Organization, Purpose_of_Use, Purpose_of_Use_Code, User_Role,
               Document_Format_Code, Document_LOINC_Code, Document_ID, Repository_ID,
               Source_ID, CCDA_File_Name
        FROM patient_details
        WHERE partner_id = %s AND batch_id = %s
        """,
        (partner_id, batch_id),
    )

    return sorted(
        tuple(None if v is None else str(v) for v in row)
        for row in cur.fetchall()
    )


def build_incoming_patient_rows(rows):
    """Build the same fixed-order row tuples from the incoming message's
    rows as get_existing_patient_rows builds from the database, using the
    same field set (and invalid-EDIPI skip) as insert_patient_details_rows,
    so the two can be compared directly."""
    built = []

    for row in rows:
        try:
            edipi_value = int(row.get("edipi"))
        except (TypeError, ValueError):
            continue

        built.append(tuple(None if v is None else str(v) for v in (
            edipi_value,
            row.get("last_name"),
            row.get("first_name"),
            row.get("receipt_date"),
            row.get("sending_org"),
            row.get("purpose"),
            row.get("purpose_code"),
            row.get("role"),
            row.get("format_code"),
            row.get("loinc_code"),
            row.get("document_id"),
            row.get("repository_id"),
            row.get("source_id"),
            row.get("ccda_file"),
        )))

    return sorted(built)


def derive_batch_name(file_name, batch_id, fallback):
    """Derive a short, human-readable batch_name from the actual uploaded
    file_name by stripping the leading "{batch_id}_" prefix and the file
    extension, e.g.:
        "1234_12121_outbound_ccda_manifest.txt" -> "outbound_ccda_manifest"
        "1234_12121_PatientReceivedReport.txt"  -> "PatientReceivedReport"
    Falls back to `fallback` if file_name is missing/empty, or if
    stripping leaves nothing behind."""
    if not file_name:
        return fallback

    name = file_name
    prefix = f"{batch_id}_" if batch_id else None
    if prefix and name.startswith(prefix):
        name = name[len(prefix):]

    name, _ext = os.path.splitext(name)
    return name or fallback


def upsert_manifest_row(cur, partner_id, partner_batch_key, batch_id, batch_name, expected_count, actual_count):
    # NOTE: batch_name now carries a short, derived name from the actual
    # uploaded file_name -- see derive_batch_name() -- e.g.
    # "PatientDisclosureReport" or "outbound_ccda_manifest", not a
    # generic "CCDA"/"REPORT" label and not the raw filename (batch_id
    # prefix and extension are stripped). This is a deliberate
    # no-schema-change choice: manifest_batch has no dedicated filename
    # column, and the caller was told not to ALTER TABLE, so the derived
    # name goes into the one existing free-text column that's already
    # unique per (partner_id, batch_id, batch_name).
    # Trade-off: you can no longer do `WHERE batch_name = 'CCDA'` to group
    # rows by file type across batches -- each row's batch_name is still
    # a one-off value specific to that upload's file type.
    now = datetime.now(timezone.utc)

    cur.execute(
        """
        INSERT INTO manifest_batch(
            partner_id,
            batch_id,
            partner_batch_key,
            batch_name,
            manifest_file_count,
            actual_file_count,
            count_discrepancy,
            submission_timestamp,
            archived_date,
            created_at,
            updated_at
        )
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
        ON CONFLICT (partner_id, batch_id, batch_name)
        DO UPDATE SET
            partner_batch_key = EXCLUDED.partner_batch_key,
            manifest_file_count = EXCLUDED.manifest_file_count,
            actual_file_count = EXCLUDED.actual_file_count,
            count_discrepancy = EXCLUDED.count_discrepancy,
            submission_timestamp = EXCLUDED.submission_timestamp,
            archived_date = EXCLUDED.archived_date,
            updated_at = EXCLUDED.updated_at
        """,
        (
            partner_id,
            batch_id,
            partner_batch_key,
            batch_name,
            expected_count,
            actual_count,
            abs(actual_count - expected_count),
            now,
            now,
            now,
            now,
        ),
    )


def send_processing_failure_sns(error, bucket, batch_id, partner_name=None):
    if not SNS_TOPIC_ARN:
        logger.warning("SNS_TOPIC_ARN is not configured. Skipping processing failure SNS.")
        return

    try:
        message = PROCESSING_FAILURE_MESSAGE_TEMPLATE.format(
            bucket=bucket,
            batch_id=batch_id,
            partner_name=partner_name or "Unknown",
            error=str(error)
        )

        sns.publish(
            TopicArn=SNS_TOPIC_ARN,
            Subject="Metadata Database Insert Failed",
            Message=message,
        )

        put_metric(
            namespace="HIE/OperationalMonitoring",
            metric_name="ProcessingFailureNotificationsSent",
            value=1
        )

        logger.info(
            "Processing failure SNS notification sent for bucket=%s batch_id=%s",
            bucket,
            batch_id
        )

    except Exception:
        logger.exception("Failed to send processing failure SNS notification")


def delete_patient_rows_for_batch(cur, partner_id, batch_id):
    cur.execute(
        """
        DELETE FROM patient_details
        WHERE partner_id = %s AND batch_id = %s
        """,
        (partner_id, batch_id),
    )


def insert_patient_details_rows(cur, partner_id, batch_id, environment, batch_name, rows):
    """Insert patient_details rows straight from the parsed rows already
    embedded in the PATIENT_REPORT_METADATA_UPSERT message. The report
    file itself is not re-read from S3 -- the producer already parsed it.

    batch_name is the same derived value (see derive_batch_name()) that
    upsert_manifest_row() writes to manifest_batch for this same batch --
    passed through here so patient_details carries it too, in its
    file_name column (patient_details' column was renamed from
    batch_name to file_name; manifest_batch's column is still
    batch_name -- only the patient_details write below changed)."""
    arrival_time = datetime.now(timezone.utc)

    delete_patient_rows_for_batch(cur, partner_id, batch_id)

    inserted = 0
    skipped_invalid_edipi = 0

    for row in rows:
        try:
            edipi_value = int(row.get("edipi"))
        except (TypeError, ValueError):
            skipped_invalid_edipi += 1
            logger.warning(
                "Skipping row with invalid EDIPI for batch_id=%s: %s",
                batch_id, row.get("edipi")
            )
            continue

        cur.execute(
            """
            INSERT INTO patient_details(
                partner_id,
                batch_id,
                environment,
                file_name,
                EDIPI,
                Patient_Last_Name,
                Patient_First_Name,
                Date_of_Receipt_UTC,
                File_Arrival_Time_UTC,
                Sending_Organization,
                Purpose_of_Use,
                Purpose_of_Use_Code,
                User_Role,
                Document_Format_Code,
                Document_LOINC_Code,
                Document_ID,
                Repository_ID,
                Source_ID,
                CCDA_File_Name
            )
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
            """,
            (
                partner_id,
                batch_id,
                environment,
                batch_name,
                edipi_value,
                row.get("last_name"),
                row.get("first_name"),
                row.get("receipt_date"),
                arrival_time,
                row.get("sending_org"),
                row.get("purpose"),
                row.get("purpose_code"),
                row.get("role"),
                row.get("format_code"),
                row.get("loinc_code"),
                row.get("document_id"),
                row.get("repository_id"),
                row.get("source_id"),
                row.get("ccda_file"),
            ),
        )

        inserted += 1

    logger.info(
        "Inserted patient_details for batch_id=%s batch_name=%s inserted=%d skipped_invalid_edipi=%d",
        batch_id,
        batch_name,
        inserted,
        skipped_invalid_edipi,
    )

    return inserted


# ============================================================
# SQS MESSAGE PROCESSING -- MANIFEST_METADATA_UPSERT
# ============================================================

def process_manifest_metadata_message(message):
    """Handle a MANIFEST_METADATA_UPSERT message: manifest + CCDA zip
    count validation, written only to manifest_batch. No patient-level
    data, partner_transmission_state, or partner_schedule is touched
    here -- those are owned by the producer Lambda now."""
    processing_start = datetime.now(timezone.utc)

    source = message.get("source", {})
    files = message.get("files", {})
    counts = message.get("counts", {})
    status = message.get("status", {})

    bucket = source.get("bucket")
    batch_id = source.get("batch_id")
    manifest_file = files.get("manifest") or {}
    zip_file = files.get("zip") or {}
    manifest_key = manifest_file.get("key")
    zip_key = zip_file.get("key")

    if not bucket:
        raise ValueError("Missing source.bucket in MANIFEST_METADATA_UPSERT message")
    if not batch_id:
        raise ValueError("Missing source.batch_id in MANIFEST_METADATA_UPSERT message")
    if not manifest_key:
        raise ValueError("Missing files.manifest.key in MANIFEST_METADATA_UPSERT message")
    if not zip_key:
        raise ValueError("Missing files.zip.key in MANIFEST_METADATA_UPSERT message")

    validation_status = status.get("validation_status")

    logger.info(
        "Starting DB processing (MANIFEST_METADATA_UPSERT) for bucket=%s batch_id=%s validation_status=%s",
        bucket,
        batch_id,
        validation_status
    )

    conn = None
    partner_name = None
    partner_id = None
    bytes_in = message.get("metrics", {}).get("bytes_in", 0)

    try:
        conn = get_conn()

        with conn.cursor() as cur:
            partner_id, partner_batch_key, environment = get_partner_info(cur, bucket)
            partner_name = get_partner_name(cur, partner_id)

            zip_expected_count = counts.get("manifest_expected_count", 0)
            zip_actual_count = counts.get("zip_actual_count", 0)

            # batch_name is now a short name derived from the real
            # uploaded zip filename (e.g. "outbound_ccda" rather than the
            # raw "1234_12121_outbound_ccda.zip") -- see derive_batch_name()
            # and the note in upsert_manifest_row().
            batch_name = derive_batch_name(zip_file.get("file_name"), batch_id, fallback="CCDA")

            # Idempotency check: if this exact batch (same partner_id,
            # batch_id, batch_name) was already recorded with the same
            # expected/actual file counts, treat it as a re-delivery of a
            # batch we've already processed and skip all database writes
            # for it. If the counts differ (files added/removed since
            # last time), fall through and update normally.
            existing_counts = get_existing_batch_counts(cur, partner_id, batch_id, batch_name)
            is_duplicate_batch = (
                existing_counts is not None
                and existing_counts[0] == zip_expected_count
                and existing_counts[1] == zip_actual_count
            )

            if is_duplicate_batch:
                logger.info(
                    "Duplicate batch detected for partner_id=%s batch_id=%s batch_name=%s "
                    "(file counts unchanged: expected=%d actual=%d). Skipping database update.",
                    partner_id,
                    batch_id,
                    batch_name,
                    zip_expected_count,
                    zip_actual_count,
                )

                put_metric(
                    namespace="HIE/OperationalMonitoring",
                    metric_name="DuplicateBatchSkipped",
                    value=1
                )
            else:
                upsert_manifest_row(
                    cur=cur,
                    partner_id=partner_id,
                    partner_batch_key=partner_batch_key,
                    batch_id=batch_id,
                    batch_name=batch_name,
                    expected_count=zip_expected_count,
                    actual_count=zip_actual_count,
                )

            if validation_status == "COUNT_MISMATCH":
                logger.warning(
                    "CCDA zip count mismatch for partner_id=%s batch_id=%s expected=%d actual=%d",
                    partner_id,
                    batch_id,
                    zip_expected_count,
                    zip_actual_count,
                )

                put_metric(
                    namespace="HIE/OperationalMonitoring",
                    metric_name="CountMismatchEvents",
                    value=1
                )

            conn.commit()

            logger.info(
                "ProcessedSummary(manifest) partner_name=%s batch_id=%s ZipExpected=%d ZipActual=%d",
                partner_name.replace(" ", "_"),
                batch_id,
                zip_expected_count,
                zip_actual_count,
            )

            put_metric(
                namespace="HIE/PartnerMonitoring",
                metric_name="BytesIn",
                value=bytes_in,
                unit="Bytes",
                dimensions=[{"Name": "PartnerName", "Value": partner_name}]
            )

    except Exception as e:
        if conn:
            conn.rollback()

        put_metric(
            namespace="HIE/OperationalMonitoring",
            metric_name="CRUDFailures",
            value=1
        )

        put_metric(
            namespace="HIE/PartnerMonitoring",
            metric_name="Errors",
            value=1,
            unit="Count",
            dimensions=[{"Name": "PartnerName", "Value": partner_name or "Unknown"}]
        )

        logger.exception(
            "Manifest metadata processing failed for bucket=%s batch_id=%s. Raising error so SQS can retry.",
            bucket,
            batch_id
        )

        send_processing_failure_sns(
            error=e,
            bucket=bucket,
            batch_id=batch_id,
            partner_name=partner_name
        )

        raise

    finally:
        if conn:
            conn.close()

    processing_delay = (datetime.now(timezone.utc) - processing_start).total_seconds()

    put_metric(
        namespace="HIE/OperationalMonitoring",
        metric_name="ProcessingDelays",
        value=processing_delay,
        unit="Seconds"
    )

    logger.info(
        "Manifest metadata processing completed successfully for bucket=%s batch_id=%s",
        bucket,
        batch_id
    )


# ============================================================
# SQS MESSAGE PROCESSING -- PATIENT_REPORT_METADATA_UPSERT
# ============================================================

def process_patient_report_metadata_message(message):
    """Handle a PATIENT_REPORT_METADATA_UPSERT message: report count
    validation plus patient_details insert/replace, written only to
    manifest_batch and patient_details. Patient rows come straight from
    the message; the report file is not re-read here."""
    processing_start = datetime.now(timezone.utc)

    source = message.get("source", {})
    files = message.get("files", {})
    counts = message.get("counts", {})
    status = message.get("status", {})
    patient_report = message.get("patient_received_report", {})

    bucket = source.get("bucket")
    batch_id = source.get("batch_id")
    report_file = files.get("report") or {}
    report_key = report_file.get("key")

    if not bucket:
        raise ValueError("Missing source.bucket in PATIENT_REPORT_METADATA_UPSERT message")
    if not batch_id:
        raise ValueError("Missing source.batch_id in PATIENT_REPORT_METADATA_UPSERT message")
    if not report_key:
        raise ValueError("Missing files.report.key in PATIENT_REPORT_METADATA_UPSERT message")

    validation_status = status.get("validation_status")
    ready_for_database_insert = status.get("ready_for_database_insert", False)
    report_actual_count = counts.get("report_actual_count", 0)
    report_expected_count = counts.get("manifest_expected_count", 0)
    rows = patient_report.get("rows", [])

    logger.info(
        "Starting DB processing (PATIENT_REPORT_METADATA_UPSERT) for bucket=%s batch_id=%s "
        "validation_status=%s rows=%d",
        bucket,
        batch_id,
        validation_status,
        len(rows)
    )

    conn = None
    partner_name = None
    partner_id = None
    bytes_in = message.get("metrics", {}).get("bytes_in", 0)

    try:
        conn = get_conn()

        with conn.cursor() as cur:
            partner_id, partner_batch_key, environment = get_partner_info(cur, bucket)
            partner_name = get_partner_name(cur, partner_id)

            # batch_name is now a short name derived from the real
            # uploaded report filename (e.g. "PatientDisclosureReport"
            # rather than the raw "1234_12121_PatientDisclosureReport.txt")
            # -- see derive_batch_name() and the note in upsert_manifest_row().
            batch_name = derive_batch_name(report_file.get("file_name"), batch_id, fallback="REPORT")

            # Idempotency check: same batch (same partner_id, batch_id,
            # batch_name), same expected/actual row counts, AND the same
            # row content already sitting in patient_details -- treat it
            # as a re-delivery of a batch we've already processed and
            # skip all database writes for it. Counts alone aren't
            # enough: a file can be edited (e.g. a corrected name)
            # without changing the row count, so the actual row content
            # is compared too. If either the counts or the row content
            # differ, fall through and update normally.
            existing_counts = get_existing_batch_counts(cur, partner_id, batch_id, batch_name)
            counts_unchanged = (
                existing_counts is not None
                and existing_counts[0] == report_expected_count
                and existing_counts[1] == report_actual_count
            )

            is_duplicate_batch = False
            if counts_unchanged:
                is_duplicate_batch = (
                    build_incoming_patient_rows(rows) == get_existing_patient_rows(cur, partner_id, batch_id)
                )

            if is_duplicate_batch:
                logger.info(
                    "Duplicate batch detected for partner_id=%s batch_id=%s batch_name=%s "
                    "(row counts and row content unchanged: expected=%d actual=%d). "
                    "Skipping database update.",
                    partner_id,
                    batch_id,
                    batch_name,
                    report_expected_count,
                    report_actual_count,
                )

                put_metric(
                    namespace="HIE/OperationalMonitoring",
                    metric_name="DuplicateBatchSkipped",
                    value=1
                )

                inserted = 0
            else:
                upsert_manifest_row(
                    cur=cur,
                    partner_id=partner_id,
                    partner_batch_key=partner_batch_key,
                    batch_id=batch_id,
                    batch_name=batch_name,
                    expected_count=report_expected_count,
                    actual_count=report_actual_count,
                )

                # A count mismatch is recorded (warning log + CountMismatchEvents
                # metric, and manifest_batch.count_discrepancy already shows it)
                # but no longer blocks the patient_details insert below --
                # whatever rows actually arrived in the report still get written.
                if not ready_for_database_insert:
                    logger.warning(
                        "Report count mismatch for partner_id=%s batch_id=%s expected=%d actual=%d. "
                        "Inserting patient_details anyway (count mismatch no longer blocks insert).",
                        partner_id,
                        batch_id,
                        report_expected_count,
                        report_actual_count,
                    )

                    put_metric(
                        namespace="HIE/OperationalMonitoring",
                        metric_name="CountMismatchEvents",
                        value=1
                    )

                inserted = insert_patient_details_rows(
                    cur=cur,
                    partner_id=partner_id,
                    batch_id=batch_id,
                    environment=environment,
                    batch_name=batch_name,
                    rows=rows,
                )

                put_metric(
                    namespace="HIE/OperationalMonitoring",
                    metric_name="ArchiveStatus",
                    value=1
                )

            conn.commit()

            logger.info(
                "ProcessedSummary(report) partner_name=%s batch_id=%s FilesIn=%d Inserted=%d BytesIn=%d",
                partner_name.replace(" ", "_"),
                batch_id,
                report_actual_count,
                inserted,
                bytes_in,
            )

            put_metric(
                namespace="HIE/PartnerMonitoring",
                metric_name="FilesIn",
                value=report_actual_count,
                unit="Count",
                dimensions=[{"Name": "PartnerName", "Value": partner_name}]
            )

            put_metric(
                namespace="HIE/PartnerMonitoring",
                metric_name="BytesIn",
                value=bytes_in,
                unit="Bytes",
                dimensions=[{"Name": "PartnerName", "Value": partner_name}]
            )

    except Exception as e:
        if conn:
            conn.rollback()

        put_metric(
            namespace="HIE/OperationalMonitoring",
            metric_name="CRUDFailures",
            value=1
        )

        put_metric(
            namespace="HIE/PartnerMonitoring",
            metric_name="Errors",
            value=1,
            unit="Count",
            dimensions=[{"Name": "PartnerName", "Value": partner_name or "Unknown"}]
        )

        logger.exception(
            "Patient report metadata processing failed for bucket=%s batch_id=%s. Raising error so SQS can retry.",
            bucket,
            batch_id
        )

        send_processing_failure_sns(
            error=e,
            bucket=bucket,
            batch_id=batch_id,
            partner_name=partner_name
        )

        raise

    finally:
        if conn:
            conn.close()

    processing_delay = (datetime.now(timezone.utc) - processing_start).total_seconds()

    put_metric(
        namespace="HIE/OperationalMonitoring",
        metric_name="ProcessingDelays",
        value=processing_delay,
        unit="Seconds"
    )

    logger.info(
        "Patient report metadata processing completed successfully for bucket=%s batch_id=%s",
        bucket,
        batch_id
    )


# ============================================================
# MESSAGE TYPE DISPATCH
# ============================================================

MESSAGE_TYPE_HANDLERS = {
    "MANIFEST_METADATA_UPSERT": process_manifest_metadata_message,
    "PATIENT_REPORT_METADATA_UPSERT": process_patient_report_metadata_message,
}


def process_metadata_message(message):
    """Route an incoming SQS message to the handler for its message_type."""
    message_type = message.get("message_type")
    handler = MESSAGE_TYPE_HANDLERS.get(message_type)

    if handler is None:
        raise ValueError(f"Unknown or missing message_type in SQS message: {message_type!r}")

    handler(message)


# ============================================================
# LAMBDA ENTRY FOR SNS
# ============================================================

def get_metadata_processing_queue_url():
    if not METADATA_PROCESSING_QUEUE_NAME:
        raise RuntimeError("METADATA_PROCESSING_QUEUE_NAME environment variable is not configured")

    response = sqs.get_queue_url(
        QueueName=METADATA_PROCESSING_QUEUE_NAME
    )

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

            process_metadata_message(message)

            # FIX: actually delete the message from the queue after it has
            # been successfully processed. The previous version only logged
            # "Deleted processed SQS message_id=%s" without ever calling
            # sqs.delete_message() -- receipt_handle was captured but never
            # used. That meant processed messages sat in the queue until the
            # 5-minute VisibilityTimeout expired, then reappeared and were
            # reprocessed (duplicate DB writes / duplicate SNS) on a loop.
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


def lambda_handler(event, context):
    records = event.get("Records", [])

    logger.info("Lambda triggered. Event record count=%d", len(records))
    logger.info("Incoming event=%s", json.dumps(event))

    is_sns_trigger = any("Sns" in record for record in records)

    if is_sns_trigger:
        logger.info("Triggered by SNS / CloudWatch alarm. Reading metadata from SQS queue.")

        processed_count = process_sqs_queue_messages()

        return {
            "statusCode": 200,
            "body": f"SNS trigger received. Processed {processed_count} SQS metadata message(s)."
        }

    if not records:
        logger.info("No records found in event.")
        return {
            "statusCode": 200,
            "body": "No records to process."
        }

    logger.info("Processing direct SQS event with %d record(s)", len(records))

    for record in records:
        message_id = record.get("messageId")
        body = record.get("body")

        if body is None:
            logger.warning("Skipping record because no SQS body was found: %s", json.dumps(record))
            continue

        logger.info("Processing SQS message_id=%s", message_id)

        try:
            message = json.loads(body)
        except Exception:
            logger.exception("Failed to parse SQS message body. Raising error so SQS can retry.")
            raise

        process_metadata_message(message)

    return {
        "statusCode": 200,
        "body": "SQS metadata processing completed successfully."
    }
