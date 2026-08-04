import os
import json
import io
import re
import zipfile
import logging
from datetime import datetime, timezone
from urllib.parse import unquote_plus

import boto3

logger = logging.getLogger()
logger.setLevel(os.getenv("LOG_LEVEL", "INFO").upper())

AWS_REGION = os.environ.get("AWS_REGION", "us-gov-west-1")
METADATA_QUEUE_URL = os.environ["METADATA_QUEUE_URL"]

s3 = boto3.client("s3")
sqs = boto3.client("sqs", region_name=AWS_REGION)
cloudwatch = boto3.client("cloudwatch", region_name=AWS_REGION)


# ============================================================
# COMMON HELPERS
# ============================================================

def utc_now_iso():
    # Returns current UTC time as an ISO string.
    return datetime.now(timezone.utc).isoformat()


def extract_batch_id(filename):
    # Batch ID is everything before the first underscore.
    # NOTE: unaffected by the naming changes below -- the batch id always
    # sits in front of the first "_", regardless of what the rest of the
    # filename looks like.
    return filename.split("_")[0]


# ============================================================
# FILE NAME MATCHING
# ============================================================
# The "first name" portion of these files can change per partner/feed
# (PatientReceived..., PatientDiscloser..., inbound_/outbound_...), but the
# *ending* of each filename is stable. So instead of matching one exact
# suffix per file, match on the stable ending only (case-insensitive).
#
#   ..._outbound_ccda_manifest.txt   -> ends with "manifest.txt"
#   ..._inbound_ccda_manifest.txt    -> ends with "manifest.txt"
#   ..._PatientReceivedReport.txt    -> ends with "report.txt"
#   ..._PatientDisclouserReport.txt  -> ends with "report.txt"
#   ..._PatientReceivedCCDA.zip      -> ends with "ccda.zip"
#   ..._PatientDisclouserCCDA.zip    -> ends with "ccda.zip"

def is_manifest_file(filename):
    return filename.lower().endswith("manifest.txt")


def is_report_file(filename):
    return filename.lower().endswith("report.txt")


def is_zip_file(filename):
    return filename.lower().endswith("ccda.zip")


def is_target_file(filename):
    """Check if file is one of the 3 target files."""
    return is_manifest_file(filename) or is_report_file(filename) or is_zip_file(filename)


# There are two distinct, non-interchangeable batch pairs that share the
# same 3 stable suffixes above:
#
#   RECEIVED    -- inbound_ccda_manifest.txt + PatientReceivedReport.txt
#                  + PatientReceivedCCDA.zip
#   DISCLOSURE  -- outbound_ccda_manifest.txt + PatientDisclosureReport.txt
#                  + PatientDisclosureCCDA.zip
#
# Matching purely on the stable suffix (as find_available_keys used to)
# means a manifest from one pair could get matched up with a report/zip
# from the OTHER pair -- e.g. an inbound manifest sitting next to a
# PatientDisclosure report/zip under the same batch_id. classify_batch_type()
# reads the "first name" portion each file still carries (outbound/inbound
# on the manifest, Received/Disclosure on the report and zip) so that
# find_available_keys() can restrict itself to files from the SAME pair
# only, and a mismatched combination simply never completes into a
# "ready" batch.

def classify_batch_type(filename):
    """Classify a target file (manifest/report/zip) as belonging to the
    RECEIVED pair or the DISCLOSURE pair, based on the type marker still
    present in its filename. Returns None if the filename is a target
    file but doesn't carry a recognizable marker for its kind."""
    lower = filename.lower()

    if is_manifest_file(filename):
        if "inbound" in lower:
            return "RECEIVED"
        if "outbound" in lower:
            return "DISCLOSURE"
        return None

    if is_report_file(filename) or is_zip_file(filename):
        if "received" in lower:
            return "RECEIVED"
        # Tolerates "disclosure", "discloser", and the "disclouser" typo
        # already anticipated in the comment block above -- all three
        # share the "disclo" prefix.
        if "disclo" in lower:
            return "DISCLOSURE"
        return None

    return None


def list_batch_keys(bucket, batch_id):
    """List all files in S3 with the given batch ID prefix."""
    keys = []
    token = None

    while True:
        kwargs = {"Bucket": bucket, "Prefix": f"{batch_id}_"}
        if token:
            kwargs["ContinuationToken"] = token

        # Page through S3 listing in case there are >1000 objects.
        resp = s3.list_objects_v2(**kwargs)

        for obj in resp.get("Contents", []):
            keys.append(obj["Key"])

        if resp.get("IsTruncated"):
            token = resp.get("NextContinuationToken")
        else:
            break

    return keys


def find_available_keys(bucket, batch_id, batch_type):
    """Find the manifest, report, and zip S3 keys for this batch that
    belong to the given batch_type ("RECEIVED" or "DISCLOSURE") only.

    A file that matches one of the 3 target suffixes but belongs to the
    OTHER type (or to no recognizable type at all) is deliberately never
    matched here -- it's ignored for the purposes of this batch_type, so
    an accidental mixed upload (e.g. an inbound manifest next to a
    PatientDisclosure report/zip) can never complete into a "ready" batch;
    it just sits as permanently incomplete for both types.
    """
    keys = list_batch_keys(bucket, batch_id)

    manifest_key = None
    report_key = None
    zip_key = None
    ignored_files = []

    for k in keys:
        fname = os.path.basename(k)
        file_type = classify_batch_type(fname)

        if is_manifest_file(fname):
            if file_type == batch_type and manifest_key is None:
                manifest_key = k
            elif file_type != batch_type:
                ignored_files.append(fname)
        elif is_report_file(fname):
            if file_type == batch_type and report_key is None:
                report_key = k
            elif file_type != batch_type:
                ignored_files.append(fname)
        elif is_zip_file(fname):
            if file_type == batch_type and zip_key is None:
                zip_key = k
            elif file_type != batch_type:
                ignored_files.append(fname)

    if ignored_files:
        logger.info(
            "Batch %s | type=%s | ignoring %d file(s) that belong to a "
            "different batch type or have no recognizable type marker: %s",
            batch_id,
            batch_type,
            len(ignored_files),
            ignored_files,
        )

    return manifest_key, report_key, zip_key


def get_object_metadata(bucket, key):
    """Get metadata for an S3 object."""
    if not key:
        return None

    # HEAD request avoids downloading the object body.
    head = s3.head_object(Bucket=bucket, Key=key)

    return {
        "key": key,
        "file_name": os.path.basename(key),
        "size_bytes": head.get("ContentLength", 0),
        "last_modified_utc": head["LastModified"].astimezone(timezone.utc).isoformat(),
        "etag": head.get("ETag", "").replace('"', "")
    }


PROCESSED_TAG_KEY = "metadata-sent"
PROCESSED_TAG_VALUE = "true"


def is_batch_already_sent(bucket, manifest_key):
    """Check an S3 object tag on the manifest to see if this batch
    was already published to SQS. Tagging the manifest is cheap
    and survives across separate Lambda invocations."""
    try:
        tags = s3.get_object_tagging(Bucket=bucket, Key=manifest_key)
        tag_set = {t["Key"]: t["Value"] for t in tags.get("TagSet", [])}
        return tag_set.get(PROCESSED_TAG_KEY) == PROCESSED_TAG_VALUE
    except Exception as e:
        logger.warning("Could not read tags on %s: %s", manifest_key, e)
        return False


def mark_batch_as_sent(bucket, manifest_key):
    """Tag the manifest object so future invocations for the same
    batch know metadata was already sent."""
    try:
        s3.put_object_tagging(
            Bucket=bucket,
            Key=manifest_key,
            Tagging={"TagSet": [{"Key": PROCESSED_TAG_KEY, "Value": PROCESSED_TAG_VALUE}]}
        )
    except Exception as e:
        logger.warning("Could not tag %s as sent: %s", manifest_key, e)


# ============================================================
# FILE VALIDATION AND COUNTING
# ============================================================

def read_manifest_expected_counts(bucket, key):
    """Read the manifest file and extract expected counts.

    Real manifest lines look like this (tab-separated, confirmed from a
    live upload):

        12345_PatientDisclosureReport\t- number of rows/files:\t5\tsha256 Checksum:\t...
        12345_PatientDisclosureCCDA\t- number of rows/files:\t5\tsha256 Checksum:\t...

    The "Report"/"CCDA" keyword is fused directly onto the end of the
    variable prefix with no separator (e.g. "...DisclosureReport"), so
    there's no word boundary in front of it -- only require the boundary
    *after* the keyword (so it doesn't accidentally match inside a longer
    word like "Reporting"), not before it.
    """
    obj = s3.get_object(Bucket=bucket, Key=key)
    content = obj["Body"].read().decode("utf-8")

    counts = {}

    for line in content.splitlines():
        match = re.search(
            r"(?i)(report|ccda)\b.*?number of rows/files:\s*(\d+)",
            line,
        )

        if match:
            # Normalize to a fixed key ("REPORT" / "CCDA") regardless of
            # what varying prefix or casing appeared in front of it.
            label = match.group(1).upper()
            counts[label] = int(match.group(2))

    if not counts:
        raise ValueError(f"Manifest contains no counts: {key}")

    return counts


def count_zip(bucket, key):
    """Count the number of files inside a ZIP archive."""
    obj = s3.get_object(Bucket=bucket, Key=key)

    # Loads whole zip into memory, then counts entries that aren't folders.
    with zipfile.ZipFile(io.BytesIO(obj["Body"].read())) as z:
        return len([f for f in z.namelist() if not f.endswith("/")])


def count_report_rows(bucket, key):
    """Count the number of data rows in the report file."""
    obj = s3.get_object(Bucket=bucket, Key=key)
    content = obj["Body"].read().decode("utf-8")
    lines = content.splitlines()

    if not lines:
        return 0

    # Skip header row, count remaining non-blank lines.
    return sum(1 for line in lines[1:] if line.strip())


def extract_patient_received_report_rows(bucket, key):
    obj = s3.get_object(Bucket=bucket, Key=key)
    content = obj["Body"].read().decode("utf-8")
    lines = content.splitlines()

    if not lines:
        return []

    rows = []

    for line in lines[1:]:
        if not line.strip():
            continue

        parts = line.split("|")

        if len(parts) < 14:
            logger.warning("Skipping malformed report row: %s", line)
            continue

        rows.append({
            "edipi": parts[0],
            "last_name": parts[1],
            "first_name": parts[2],
            "receipt_date": parts[3],
            "sending_org": parts[4],
            "purpose": parts[5],
            "purpose_code": parts[6],
            "role": parts[7],
            "format_code": parts[8],
            "loinc_code": parts[9],
            "document_id": parts[10],
            "repository_id": parts[11],
            "source_id": parts[12],
            "ccda_file": parts[13],
        })

    return rows


# ============================================================
# SQS MESSAGE BUILDERS
# ============================================================

def build_manifest_metadata_message(bucket, trigger_key, batch_id, manifest_key, zip_key, manifest_expected_counts):
    """Build the SQS message that carries manifest + CCDA zip
    validation info. No patient-level data goes in this message."""

    zip_actual_count = count_zip(bucket, zip_key)
    zip_expected_count = manifest_expected_counts.get("CCDA", 0)
    zip_matches_manifest = zip_actual_count == zip_expected_count

    file_metadata = {
        "manifest": get_object_metadata(bucket, manifest_key),
        "zip": get_object_metadata(bucket, zip_key),
    }

    bytes_in = sum(item.get("size_bytes", 0) for item in file_metadata.values() if item)

    validation_status = "READY_FOR_DATABASE_SQS_INSERT" if zip_matches_manifest else "COUNT_MISMATCH"

    return {
        "message_type": "MANIFEST_METADATA_UPSERT",
        "schema_version": "1.0",
        "created_at_utc": utc_now_iso(),
        "source": {
            "bucket": bucket,
            "bucket_arn": f"arn:aws-us-gov:s3:::{bucket}",
            "trigger_key": trigger_key,
            "batch_id": batch_id
        },
        "files": file_metadata,
        "counts": {
            "manifest_expected_count": zip_expected_count,
            "zip_actual_count": zip_actual_count,
            "zip_matches_manifest": zip_matches_manifest,
            "count_discrepancy": abs(zip_actual_count - zip_expected_count)
        },
        "status": {
            "validation_status": validation_status,
            "ready_for_database_insert": zip_matches_manifest
        },
        "database_processor": {
            "action": "UPSERT_MANIFEST_METADATA",
            "idempotency_key": f"{bucket}:{batch_id}:manifest"
        },
        "metrics": {
            "bytes_in": bytes_in
        }
    }


def build_patient_report_metadata_message(bucket, trigger_key, batch_id, report_key, manifest_expected_counts):
    """Build the SQS message that carries the PatientReceivedReport
    validation info and the patient-level rows."""

    report_actual_count = count_report_rows(bucket, report_key)
    patient_report_rows = extract_patient_received_report_rows(bucket, report_key)
    report_expected_count = manifest_expected_counts.get("REPORT", 0)
    report_matches_manifest = report_actual_count == report_expected_count

    file_metadata = {
        "report": get_object_metadata(bucket, report_key),
    }

    bytes_in = sum(item.get("size_bytes", 0) for item in file_metadata.values() if item)

    validation_status = "READY_FOR_DATABASE_SQS_INSERT" if report_matches_manifest else "COUNT_MISMATCH"

    return {
        "message_type": "PATIENT_REPORT_METADATA_UPSERT",
        "schema_version": "1.0",
        "created_at_utc": utc_now_iso(),
        "source": {
            "bucket": bucket,
            "bucket_arn": f"arn:aws-us-gov:s3:::{bucket}",
            "trigger_key": trigger_key,
            "batch_id": batch_id
        },
        "files": file_metadata,
        "counts": {
            "manifest_expected_count": report_expected_count,
            "report_actual_count": report_actual_count,
            "report_matches_manifest": report_matches_manifest,
            "count_discrepancy": abs(report_actual_count - report_expected_count)
        },
        "status": {
            "validation_status": validation_status,
            "ready_for_database_insert": report_matches_manifest
        },
        "patient_received_report": {
            "row_count": len(patient_report_rows),
            "rows": patient_report_rows
        },
        "database_processor": {
            "action": "UPSERT_PATIENT_REPORT_METADATA_AND_PATIENT_DETAILS",
            "idempotency_key": f"{bucket}:{batch_id}:report"
        },
        "metrics": {
            "bytes_in": bytes_in,
            "files_in": report_actual_count
        }
    }


def build_metadata_messages(bucket, trigger_key):
    """Build both metadata messages (manifest/zip validation and
    patient report) for a batch. Returns None if the triggering file
    isn't a recognized batch type, the batch isn't ready yet (missing
    a same-type file), or the batch was already sent."""
    filename = os.path.basename(trigger_key)
    batch_id = extract_batch_id(filename)

    # Which of the two pairs (RECEIVED or DISCLOSURE) does the file that
    # just landed belong to? Everything below only ever looks for OTHER
    # files from this same pair -- a file from the other pair is never
    # eligible to complete this batch, no matter how long it sits there.
    batch_type = classify_batch_type(filename)

    if batch_type is None:
        logger.warning(
            "Batch %s | file %s does not carry a recognizable batch-type "
            "marker (expected 'outbound'/'inbound' on the manifest, or "
            "'Received'/'Disclosure' on the report/zip). Skipping -- this "
            "file will not be paired with anything.",
            batch_id,
            filename
        )
        put_metric(
            namespace="HIE/OperationalMonitoring",
            metric_name="UnrecognizedBatchTypeEvents",
            value=1
        )
        return None

    manifest_key, report_key, zip_key = find_available_keys(bucket, batch_id, batch_type)

    logger.info(
        "Batch %s | type=%s | availability: manifest=%s report=%s zip=%s",
        batch_id,
        batch_type,
        bool(manifest_key),
        bool(report_key),
        bool(zip_key)
    )

    # Bail out until all 3 files for THIS batch type exist. If the files
    # actually present are a mismatched combination (e.g. an inbound
    # manifest next to a PatientDisclosure report/zip), this stays
    # permanently true for both types -- it never silently pairs them.
    if not manifest_key or not report_key or not zip_key:
        missing_files = []
        if not manifest_key:
            missing_files.append("manifest")
        if not report_key:
            missing_files.append("report")
        if not zip_key:
            missing_files.append("zip")

        logger.info(
            "Batch %s | type=%s is not ready yet. Missing files: %s",
            batch_id,
            batch_type,
            ", ".join(missing_files)
        )
        return None

    # stop here if this batch was already sent by a prior invocation.
    if is_batch_already_sent(bucket, manifest_key):
        logger.info("Batch %s | type=%s already sent to SQS, skipping duplicate.", batch_id, batch_type)
        return None

    # Read expected counts once, share across both messages.
    manifest_expected_counts = read_manifest_expected_counts(bucket, manifest_key)

    logger.info(
        "Batch %s | type=%s | Manifest expected counts | CCDA=%d | REPORT=%d",
        batch_id,
        batch_type,
        manifest_expected_counts.get("CCDA", 0),
        manifest_expected_counts.get("REPORT", 0),
    )

    manifest_message = build_manifest_metadata_message(
        bucket, trigger_key, batch_id, manifest_key, zip_key, manifest_expected_counts
    )
    report_message = build_patient_report_metadata_message(
        bucket, trigger_key, batch_id, report_key, manifest_expected_counts
    )

    return manifest_message, report_message, manifest_key


# ============================================================
# SQS SENDER
# ============================================================

def send_metadata_to_sqs(message):
    """Send a metadata message to SQS."""
    message_body = json.dumps(message, default=str)

    send_args = {
        "QueueUrl": METADATA_QUEUE_URL,
        "MessageBody": message_body,
        "MessageAttributes": {
            "MessageType": {
                "DataType": "String",
                "StringValue": message.get("message_type", "UNKNOWN_METADATA_TYPE")
            },
            "BatchId": {
                "DataType": "String",
                "StringValue": message["source"]["batch_id"]
            },
            "Bucket": {
                "DataType": "String",
                "StringValue": message["source"]["bucket"]
            },
            "ValidationStatus": {
                "DataType": "String",
                "StringValue": message["status"]["validation_status"]
            }
        }
    }

    try:
        # Actually publish the message to the queue.
        response = sqs.send_message(**send_args)

        logger.info(
            "Batch %s | %s | SQS message sent = true",
            message["source"]["batch_id"],
            message["message_type"]
        )

    except Exception:
        logger.exception(
            "Batch %s | %s | SQS message sent = false",
            message["source"]["batch_id"],
            message["message_type"]
        )
        raise

    # Emit a per-message-type breakdown for finer-grained monitoring.
    # NOTE: this is deliberately a *different* metric name from
    # "MetadataMessagesSentToSQS" -- that one is emitted once per
    # completed BATCH (see process_object) so existing CloudWatch
    # alarms built on it (e.g. "fire after N batches") keep working
    # unchanged now that each batch sends 2 messages instead of 1.
    put_metric(
        namespace="HIE/OperationalMonitoring",
        metric_name="MetadataMessageTypesSentToSQS",
        value=1,
        dimensions=[{"Name": "MessageType", "Value": message["message_type"]}]
    )

    put_metric(
        namespace="HIE/PartnerMonitoring",
        metric_name="BytesIn",
        value=message["metrics"]["bytes_in"],
        unit="Bytes"
    )

    return response


# ============================================================
# CLOUDWATCH METRICS
# ============================================================

def put_metric(namespace, metric_name, value, unit="Count", dimensions=None):
    """Send CloudWatch metric."""
    try:
        metric = {"MetricName": metric_name, "Value": value, "Unit": unit}
        if dimensions:
            metric["Dimensions"] = dimensions

        # Metrics are best-effort: never let a metric failure break the Lambda.
        cloudwatch.put_metric_data(
            Namespace=namespace,
            MetricData=[metric]
        )
    except Exception as e:
        logger.warning("Failed to publish CloudWatch metric %s: %s", metric_name, e)


# ============================================================
# MAIN OBJECT PROCESSOR
# ============================================================

def process_object(bucket, key, record):
    """Process a single S3 object."""
    filename = os.path.basename(key)
    event_time = datetime.fromisoformat(
        record["eventTime"].replace("Z", "+00:00")
    )

    processing_start = datetime.now(timezone.utc)

    # How long between S3 event and Lambda actually running.
    processing_delay = (processing_start - event_time).total_seconds()
    put_metric(
        namespace="HIE/OperationalMonitoring",
        metric_name="ProcessingDelaySeconds",
        value=processing_delay,
        unit="Seconds"
    )

    logger.info("Processing delay=%s seconds", processing_delay)

    # Track every S3 event that reaches the Lambda.
    put_metric(
        namespace="HIE/OperationalMonitoring",
        metric_name="S3ObjectArrivals",
        value=1
    )

    # Ignore any file that isn't one of the 3 expected types.
    if not is_target_file(filename):
        logger.info("Skipping non-target file: %s", filename)
        return {"processed": False, "reason": "NON_TARGET_FILE"}

    # Try to assemble both batch messages (returns None if not ready/already sent).
    result = build_metadata_messages(bucket, key)

    if result is None:
        return {"processed": False, "reason": "BATCH_NOT_READY_OR_ALREADY_SENT"}

    manifest_message, report_message, manifest_key = result

    # Push both messages onto the queue. If the second send fails after the
    # first succeeds, we deliberately do NOT tag the batch as sent below --
    # the manifest message may be re-delivered on retry (at-least-once),
    # which the consumer's UPSERT + idempotency_key should absorb.
    send_metadata_to_sqs(manifest_message)
    send_metadata_to_sqs(report_message)

    # FIX: tag the manifest so a later, near-simultaneous invocation
    # for the same batch won't send duplicate messages.
    mark_batch_as_sent(bucket, manifest_key)

    # RESTORED: emit exactly one "batch completed" datapoint here, not one
    # per message in send_metadata_to_sqs(). A batch now sends 2 messages
    # instead of 1, so emitting this per-message would double the rate and
    # break any CloudWatch alarm tuned to a batch count (e.g. "fire the
    # SNS -> DB processor Lambda chain after N batches / N*3 files").
    # Emitting it once here, after both sends succeed, keeps "+1 per batch"
    # semantics identical to before the message-splitting change.
    put_metric(
        namespace="HIE/OperationalMonitoring",
        metric_name="MetadataMessagesSentToSQS",
        value=1
    )

    # Record a per-bucket success metric.
    put_metric(
        namespace="HIE/OperationalMonitoring",
        metric_name="ArchiveStatus",
        value=1,
        dimensions=[
            {"Name": "Bucket", "Value": bucket},
            {"Name": "Status", "Value": "Success"}
        ]
    )

    return {
        "processed": True,
        "reason": "METADATA_SENT_TO_SQS",
        "batch_id": manifest_message["source"]["batch_id"]
    }


# ============================================================
# LAMBDA ENTRY
# ============================================================

def lambda_handler(event, context):
    """Main Lambda handler."""
    records = event.get("Records", [])

    if not records:
        logger.info("No records found in event.")
        return {"statusCode": 200, "body": "No records to process."}

    results = []

    for record in records:
        # FIX: pull bucket name out before the try block so it's always
        # defined, even if something fails while reading the record itself.
        bucket = record.get("s3", {}).get("bucket", {}).get("name", "unknown")

        try:
            key = unquote_plus(record["s3"]["object"]["key"])

            # Skip "folder" placeholder keys (no real file content).
            if key.endswith("/"):
                logger.info("Skipping folder key: %s", key)
                continue

            result = process_object(bucket, key, record)
            results.append(result)

        except Exception as e:
            error_message = str(e)

            # Flag permission problems specifically.
            if "AccessDenied" in error_message or "access denied" in error_message.lower():
                put_metric(
                    namespace="HIE/OperationalMonitoring",
                    metric_name="AccessDeniedEvents",
                    value=1
                )

            # General failure counter.
            put_metric(
                namespace="HIE/OperationalMonitoring",
                metric_name="MetadataLambdaFailures",
                value=1
            )

            # Per-bucket failure status.
            put_metric(
                namespace="HIE/OperationalMonitoring",
                metric_name="ArchiveStatus",
                value=0,
                dimensions=[
                    {"Name": "Bucket", "Value": bucket},
                    {"Name": "Status", "Value": "Failure"}
                ]
            )

            # Re-raise so the Lambda invocation shows as failed (e.g. for
            # retry/DLQ behavior on the underlying S3 event source).
            raise

    return {
        "statusCode": 200,
        "body": json.dumps({
            "message": "Metadata Lambda completed successfully.",
            "results": results
        })
    }
