import os
import json
import io
import re
import zipfile
import logging
from datetime import datetime, timezone
from urllib.parse import unquote_plus
import boto3
import psycopg2
import retry_utils

logger = logging.getLogger()
logger.setLevel(os.getenv("LOG_LEVEL", "INFO").upper())

AWS_REGION = os.environ.get("AWS_REGION", "us-gov-west-1")

SNS_TOPIC_ARN = os.environ.get("SNS_TOPIC_ARN")
SNS_SUBJECT_TEMPLATE = os.environ.get(
    "SNS_SUBJECT_TEMPLATE",
    "Notification of $PARTNER_NAME File Delivery Resumption in the $ENVIRONMENT Environment"
)
RESUMED_MESSAGE_TEMPLATE = os.environ.get(
    "RESUMED_MESSAGE_TEMPLATE",
    "{partner_name} file delivery has resumed and files are being received successfully in the {environment} environment."
)
WARNING_INFO = os.environ.get("WARNING_INFO", "")

# Variables for database connection and SQS queue.

ALERT_COOLDOWN_SECONDS = int(
	os.environ.get("ALERT_COOLDOWN_SECONDS", "900")
)
_last_alert_times = {}


sns = boto3.client("sns", region_name=AWS_REGION)

METADATA_QUEUE_URL = os.environ["METADATA_QUEUE_URL"]

DB_SECRET_ARN = os.environ["DB_SECRET_ARN"]
DB_HOST = os.environ["DB_HOST"]
DB_PORT = int(os.environ.get("DB_PORT", "5432"))
DB_NAME = os.environ.get("DB_NAME", "ccda01")

try:
    INGESTION_METHOD_ARN_MARKERS = json.loads(os.environ.get("INGESTION_METHOD_ARN_MARKERS", "{}"))
except (TypeError, ValueError):
    logger.warning("INGESTION_METHOD_ARN_MARKERS is not valid JSON; ignoring it.")
    INGESTION_METHOD_ARN_MARKERS = {}
INGESTION_METHOD_DEFAULT = os.environ.get("INGESTION_METHOD_DEFAULT", "DIRECT_S3")

s3 = boto3.client("s3")
sqs = boto3.client("sqs", region_name=AWS_REGION)
cloudwatch = boto3.client("cloudwatch", region_name=AWS_REGION)
sm = boto3.client("secretsmanager", region_name=AWS_REGION)

def invoke_retry_handler(error, event):
    """Replaces the old send_to_dlq() call site. retry_utils is
    imported at the top of this file from the Lambda Layer (same
    pattern as psycopg2) -- no S3 download needed at runtime, since the
    layer is already mounted at /opt by the time this code runs.

    ASSUMPTION -- confirm this matches your actual layer's contents:
    it's expected to expose a function `handle_retry(error, event)`.
    Adjust the call below if your layer's function name differs.
    """
    try:
        retry_utils.handle_retry(error, event)
        logger.info("Retry handler invoked successfully")
        return True

    except Exception as e:
        logger.error(f"Retry handler invocation failed: {e}")
        put_metric(
            namespace="HIE/OperationalMonitoring",
            metric_name="RetryHandlerInvocationFailures",
            value=1
        )
        return False


# COMMON HELPERS
def utc_now_iso():
    return datetime.now(timezone.utc).isoformat()


def extract_batch_id(filename):
    parts = filename.split("_")
    return "_".join(parts[:2])


def detect_ingestion_method(record):
    """Classify how this object arrived in S3, using the S3 event
    record's userIdentity.arn (the IAM principal that performed the
    PutObject). Transfer Family writes as its configured access role;
    another API/script would show up as whatever role/user it assumed.
    INGESTION_METHOD_ARN_MARKERS maps a substring of that ARN to a
    label -- the first match wins. Falls back to
    INGESTION_METHOD_DEFAULT when nothing matches or the event has no
    userIdentity.arn."""
    arn = (record.get("userIdentity") or {}).get("arn") or ""
    for marker, label in INGESTION_METHOD_ARN_MARKERS.items():
        if marker and marker in arn:
            return label
    if not arn:
        logger.warning(
            "S3 event record has no userIdentity.arn; defaulting ingestion_method to %s",
            INGESTION_METHOD_DEFAULT
        )
    return INGESTION_METHOD_DEFAULT


# FILE NAME MATCHING
def is_manifest_file(filename):
    return filename.lower().endswith("manifest.txt")


def is_report_file(filename):
    return filename.lower().endswith("report.txt")


def is_zip_file(filename):
    return filename.lower().endswith("ccda.zip")


def is_target_file(filename):
    """Check if file is one of the 3 target files."""
    return is_manifest_file(filename) or is_report_file(filename) or is_zip_file(filename)


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
    belong to the given batch_type ("RECEIVED" or "DISCLOSURE") only."""
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
    head = s3.head_object(Bucket=bucket, Key=key)
    return {
        "key": key,
        "file_name": os.path.basename(key),
        "size_bytes": head.get("ContentLength", 0),
        "last_modified_utc": head["LastModified"].astimezone(timezone.utc).isoformat(),
        "etag": head.get("ETag", "").replace('"', "")
    }


PROCESSED_TAG_VALUE = "true"
MANIFEST_SENT_TAG_KEY = "manifest-metadata-sent"
REPORT_SENT_TAG_KEY = "report-metadata-sent"


def _get_object_tags(bucket, key):
    """Read all tags on an S3 object as a dict. Fails safe toward an
    empty dict (logging a warning) so callers treat an unreadable tag
    set as "not sent yet" rather than raising."""
    try:
        tags = s3.get_object_tagging(Bucket=bucket, Key=key)
        return {t["Key"]: t["Value"] for t in tags.get("TagSet", [])}
    except Exception as e:
        logger.warning("Could not read tags on %s: %s", key, e)
        return {}


def is_message_already_sent(bucket, key, tag_key):
    """Check a single per-message sent-status tag (manifest vs. report
    are tracked independently under MANIFEST_SENT_TAG_KEY /
    REPORT_SENT_TAG_KEY), so a retry after a partial failure only
    re-sends whichever message didn't actually make it to SQS, instead
    of re-sending both."""
    return _get_object_tags(bucket, key).get(tag_key) == PROCESSED_TAG_VALUE


def mark_message_as_sent(bucket, key, tag_key):
    """Tag the object with a single message's sent-status. Reads the
    existing tag set first and merges in, since put_object_tagging
    replaces the whole tag set and would otherwise clobber the other
    message's tag."""
    try:
        tag_set = _get_object_tags(bucket, key)
        tag_set[tag_key] = PROCESSED_TAG_VALUE
        s3.put_object_tagging(
            Bucket=bucket,
            Key=key,
            Tagging={"TagSet": [{"Key": k, "Value": v} for k, v in tag_set.items()]}
        )
    except Exception as e:
        logger.warning("Could not tag %s as sent (%s): %s", key, tag_key, e)


def is_batch_already_sent(bucket, manifest_key):
    """Check whether BOTH the manifest and report messages for this
    batch have already been published to SQS. This is also the gate
    that keeps the partner-state update / breach-flag clear (added
    below) from firing more than once for the same batch on
    retries/re-invocations."""
    tag_set = _get_object_tags(bucket, manifest_key)
    return (
        tag_set.get(MANIFEST_SENT_TAG_KEY) == PROCESSED_TAG_VALUE
        and tag_set.get(REPORT_SENT_TAG_KEY) == PROCESSED_TAG_VALUE
    )


class DatabaseConnectionError(RuntimeError):
	"""Raised when there is an error connecting to the database."""

class SQSPublishError(RuntimeError):
	"""Raised when there is an error publishing to the SQS queue."""


def _should_publish_alert(failure_category):
    """Determine whether an alert may be published for the category."""
    now = datetime.now(timezone.utc)
    last_alert_time = _last_alert_times.get(failure_category)

    if last_alert_time is not None:
        elapsed_seconds = (now - last_alert_time).total_seconds()
        if elapsed_seconds < ALERT_COOLDOWN_SECONDS:
            logging.info(
                "Suppressing repeated alert for category '%s'. Last alert was sent %d seconds ago.",
                failure_category,
                ALERT_COOLDOWN_SECONDS - elapsed_seconds,
            )
            return False
    return True

def _record_alert_time(failed_category):
	""" Record the time when an alert was sent for a specific category. """
	_last_alert_times[failed_category] = datetime.now(timezone.utc)

def _publish_operational_alert(subject, message, failure_category):
    """Publish an alert to the SNS topic if the cooldown has passed."""
    if not SNS_TOPIC_ARN:
        logging.error(
            "SNS_TOPIC_ARN is not configured. Cannot publish alert for category '%s'.",
            failure_category,
        )
        return False

    if not _should_publish_alert(failure_category):
        return False

    try:
        sns.publish(
            TopicArn=SNS_TOPIC_ARN,
            Subject=subject[:100],
            Message=message,
            MessageAttributes={
                "failed_category": {
                    "StringValue": failure_category,
                    "DataType": "String",
                }
            },
        )
        _record_alert_time(failure_category)
        logger.info(
            "Published alert to SNS topic '%s' for category '%s'.",
            SNS_TOPIC_ARN,
            failure_category,
        )
        return True

    except Exception:
        logging.exception("Unable to publish metadata message")
        return False

    except Exception:
        logging.error(
            "Failed to publish alert to SNS topic '%s' for category '%s'.",
            SNS_TOPIC_ARN,
            failure_category,
            exc_info=True,
        )
        return False

def notify_database_connection_failure(failure_category, error_message):
    """Notify about a database connection failure."""
    request_id = (
        getattr(globals().get("context"), "aws_request_id", None)
        if globals().get("context")
        else None
    )
    message = (
        f"CRITICAL: manifest processing failed for category '{failure_category}' due to database connection error.\n"
        f"Failure category: {failure_category}\n"
        f"Environment: {os.environ.get('ENVIRONMENT', 'Unknown')}\n"
        f"AWS Region: {AWS_REGION}\n"
        f"Lambda request ID: {request_id}\n"
        f"Database host: {DB_HOST}\n"
        f"Error type: {type(error_message).__name__}\n"
        f"Error message: {str(error_message)}\n"
        "Requested action: Check Aurora database connectivity and credentials.\n"
        "security groups, VPC settings, and database availability.\n"
    )
    return _publish_operational_alert(
        subject=f"CRITICAL: Database Connection Failure for category '{failure_category}'",
        message=message,
        failure_category="Database Connection Failure",
    )

def notify_sqs_publish_failure(error, context=None):
    """Notify about an SQS publish failure without exposing message metadata."""
    request_id = (
        getattr(context, "aws_request_id", None)
        if context
        else None
    )
    message = (
        "CRITICAL: Manifest processing failed due to SQS publish error.\n"
        "Failure category: SQS Publish Failure\n"
        f"Environment: {os.environ.get('ENVIRONMENT', 'Unknown')}\n"
        f"AWS Region: {AWS_REGION}\n"
        f"Lambda request ID: {request_id}\n"
        f"Database host: {DB_HOST}\n"
        f"Error type: {type(error).__name__}\n"
        f"Error message: {str(error)}\n"
        "Requested action: Check Aurora database connectivity and credentials.\n"
        "security groups, VPC settings, and database availability.\n"
    )
    return _publish_operational_alert(
        subject="CRITICAL: SQS Publish Failure",
        message=message,
        failure_category="SQS Publish Failure",
    )

def route_critical_failure_notification(error, context=None):
    """Route critical failure notifications based on the error type."""
    if isinstance(error, DatabaseConnectionError):
        return notify_database_connection_failure("Database Connection Failure", error)
    if isinstance(error, SQSPublishError):
        return notify_sqs_publish_failure(error, context)

    logging.debug(
        "Error message does not match any known critical failure types. No alert will be sent. "
        "Error type: %s",
        type(error).__name__,
    )
    return False

def get_conn():
	""" Establish a connection to the Aurora database using psycopg2. """
	try:
		secret = sm.get_secret_value(SecretId=DB_SECRET_ARN)
		creds = json.loads(secret['SecretString'])

		return psycopg2.connect(
			host=DB_HOST,
			port=DB_PORT,
			dbname=DB_NAME,
			user=creds['username'],
			password=creds['password'],
			connect_timeout=10,
		)
	except Exception as error:
		logging.exception("Unable to connect to the database: %s", str(error))
		raise DatabaseConnectionError("Manifest processing failed due to database connection error."
  		) from error


# SQS MESSAGE HELPERS

def publish_metadata_message(message_body, message_attributes=None):
    """ Publish a message to the SQS queue. """
    try:
        if not METADATA_QUEUE_URL:
            raise ValueError("METADATA_QUEUE_URL is not configured.")
        request = {
            "QueueUrl": METADATA_QUEUE_URL,
            "MessageBody": (
                message_body if isinstance(message_body, str) else json.dumps(message_body)
            )
        }
        if message_attributes:
            request["MessageAttributes"] = message_attributes

        return sqs.send_message(
            message_body=request["MessageBody"],
            message_attributes=request.get("MessageAttributes")
        )
    except Exception as error:
        logging.exception("Unable to publish metadata message: %s", str(error))

        raise SQSPublishError("Manifest processing failed due to SQS publish error.") from error
# =====================================================================

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


def update_partner_state(cur, partner_id, object_key):
    now = datetime.now(timezone.utc)
    filename = os.path.basename(object_key)
    batch_id = extract_batch_id(filename)
    batch_prefix = f"{batch_id}_"
    clean_filename = filename[len(batch_prefix):] if filename.startswith(batch_prefix) else filename
    cur.execute(
        """
        SELECT 1
        FROM partner_transmission_state
        WHERE partner_id = %s
        """,
        (partner_id,),
    )
    if cur.fetchone():
        cur.execute(
            """
            UPDATE partner_transmission_state
            SET last_seen_at = %s,
                batch_id = %s,
                last_object_key = %s,
                updated_at = %s
            WHERE partner_id = %s
            """,
            (now, batch_id, clean_filename, now, partner_id),
        )
    else:
        cur.execute(
            """
            INSERT INTO partner_transmission_state(
                partner_id,
                last_seen_at,
                batch_id,
                last_object_key,
                updated_at
            )
            VALUES (%s,%s,%s,%s,%s)
            """,
            (partner_id, now, batch_id, clean_filename, now),
        )


def clear_breach_flag(cur, partner_id, expected_interval_seconds):
    """Clear the breach flag for this partner, creating their
    partner_schedule row if one doesn't exist yet.

    - No row exists for this partner (their first-ever successful batch,
      or the row was otherwise never created): INSERT one now, already
      in a non-breached state. expected_interval_seconds is NOT NULL on
      this table, so the caller measures how long this Lambda took to
      process the batch's files (wall-clock, from the start of
      process_object up to this point) and passes that in as the seed
      value for a brand-new row. This is NOT a "recovery" -- there was
      no breach to recover from -- so last_recovery_email_sent_at is
      left NULL and the caller should not send a recovery SNS for it.
    - A row exists with breach_flag TRUE: UPDATE it to FALSE and stamp
      last_recovery_email_sent_at. This IS a recovery.
      expected_interval_seconds is left untouched on this path -- once a
      partner has a real row, this Lambda doesn't overwrite their
      configured SLA value.
    - A row exists with breach_flag already FALSE: this is a normal,
      on-time upload -- NOT a no-op. breach_flag is re-affirmed FALSE and
      updated_at is refreshed to NOW(), so the row always reflects the
      most recent successful batch even when nothing "changed" status-
      wise. last_recovery_email_sent_at is left untouched (this isn't a
      recovery), so no recovery SNS fires for this path.

    Returns True only for the middle case (an actual breach->recovered
    transition), so the caller knows whether to send the recovery SNS.
    """
    cur.execute(
        """
        SELECT breach_flag
        FROM partner_schedule
        WHERE partner_id = %s
        """,
        (partner_id,),
    )
    row = cur.fetchone()

    if row is None:
        cur.execute(
            """
            INSERT INTO partner_schedule(
                partner_id,
                expected_interval_seconds,
                breach_flag,
                updated_at
            )
            VALUES (%s, %s, FALSE, NOW())
            """,
            (partner_id, expected_interval_seconds),
        )
        logger.info(
            "No partner_schedule row existed for partner_id=%s -- created one "
            "(first successful batch) with expected_interval_seconds=%s.",
            partner_id,
            expected_interval_seconds
        )
        return False

    if row[0]:
        cur.execute(
            """
            UPDATE partner_schedule
            SET breach_flag = FALSE,
                last_recovery_email_sent_at = NOW(),
                updated_at = NOW()
            WHERE partner_id = %s
            """,
            (partner_id,),
        )
        logger.info("Recovery detected. Breach cleared for partner_id=%s", partner_id)
        return True

    cur.execute(
        """
        UPDATE partner_schedule
        SET breach_flag = FALSE,
            updated_at = NOW()
        WHERE partner_id = %s
        """,
        (partner_id,),
    )
    logger.info(
        "No breach detected for partner_id=%s -- refreshed partner_schedule.updated_at "
        "for this on-time upload.",
        partner_id
    )
    return False


def render_sns_template(template, partner_name, environment):
    """Fill a subject/message template with partner_name and environment,
    supporting both placeholder styles the CloudFormation template's env
    vars use: $PARTNER_NAME / $ENVIRONMENT (SNS_SUBJECT_TEMPLATE) and
    {partner_name} / {environment} (RESUMED_MESSAGE_TEMPLATE)."""
    rendered = template.replace("$PARTNER_NAME", str(partner_name)).replace(
        "$ENVIRONMENT", str(environment)
    )
    try:
        rendered = rendered.format(partner_name=partner_name, environment=environment)
    except (KeyError, IndexError):
        pass
    return rendered


def send_recovery_sns(partner_name, partner_id, environment):
    """Send the recovery notification. Called only when clear_breach_flag()
    reports an actual breach_flag TRUE -> FALSE transition (see its
    docstring and sync_partner_state_and_breach_flag) -- never for a
    brand-new partner_schedule row and never for an already-FALSE ->
    FALSE on-time upload."""
    if not SNS_TOPIC_ARN:
        logger.warning("SNS_TOPIC_ARN is not configured. Skipping recovery SNS.")
        return
    try:
        subject = render_sns_template(SNS_SUBJECT_TEMPLATE, partner_name, environment)
        message = render_sns_template(RESUMED_MESSAGE_TEMPLATE, partner_name, environment)
        if WARNING_INFO:
            message = f"{message}\n\n{WARNING_INFO}"
        sns.publish(
            TopicArn=SNS_TOPIC_ARN,
            Subject=subject,
            Message=message,
        )
        put_metric(
            namespace="HIE/OperationalMonitoring",
            metric_name="SNSNotificationsSent",
            value=1
        )
        logger.info("SNS notification sent for partner_id=%s", partner_id)
    except Exception:
        logger.exception(
            "SNS notification failed for partner_id=%s, but database changes were already committed.",
            partner_id,
        )


# FILE VALIDATION AND COUNTING
def read_manifest_expected_counts(bucket, key):
    """Read the manifest file and extract expected counts."""
    obj = s3.get_object(Bucket=bucket, Key=key)
    content = obj["Body"].read().decode("utf-8")
    counts = {}
    for line in content.splitlines():
        match = re.search(
            r"(?i)(report|ccda)\b.*?number of rows/files:\s*(\d+)",
            line,
        )
        if match:
            label = match.group(1).upper()
            counts[label] = int(match.group(2))
    if not counts:
        raise ValueError(f"Manifest contains no counts: {key}")
    return counts


class _S3RangeReader:
    """Minimal seekable, read-only file-like object that fetches only
    the byte ranges zipfile actually needs from S3 via ranged
    GetObject calls, so count_zip() never has to pull the whole
    archive into Lambda memory."""

    def __init__(self, bucket, key, size):
        self._bucket = bucket
        self._key = key
        self._size = size
        self._pos = 0

    def read(self, size=-1):
        if size is None or size < 0:
            end = self._size - 1
        else:
            end = min(self._pos + size, self._size) - 1
        if self._size == 0 or self._pos > end:
            return b""
        resp = s3.get_object(
            Bucket=self._bucket,
            Key=self._key,
            Range=f"bytes={self._pos}-{end}"
        )
        data = resp["Body"].read()
        self._pos += len(data)
        return data

    def seek(self, offset, whence=0):
        if whence == 0:
            self._pos = offset
        elif whence == 1:
            self._pos += offset
        elif whence == 2:
            self._pos = self._size + offset
        else:
            raise ValueError(f"Unsupported whence: {whence}")
        return self._pos

    def tell(self):
        return self._pos

    def seekable(self):
        return True


def count_zip(bucket, key):
    """Count the number of files inside a ZIP archive."""
    size = s3.head_object(Bucket=bucket, Key=key)["ContentLength"]
    with zipfile.ZipFile(_S3RangeReader(bucket, key, size)) as z:
        return len([f for f in z.namelist() if not f.endswith("/")])


def count_report_rows(bucket, key):
    """Count the number of data rows in the report file."""
    obj = s3.get_object(Bucket=bucket, Key=key)
    content = obj["Body"].read().decode("utf-8")
    lines = content.splitlines()
    if not lines:
        return 0
    return sum(1 for line in lines[1:] if line.strip())


def extract_received_report_rows(bucket, key):
    """Parse a PatientReceivedReport.txt file. Confirmed real header:

        EDIPI|Patient_Last_Name|Patient_First_Name|SSN|Date_of_Receipt_(UTC)|
        Sending_Organization|Purpose_of_Use|Purpose_of_Use_Code|User_Role|
        Document_Format_Code|Document_LOINC_Code|Document_ID|Repository_ID|
        Source_ID|CCDA_File_Name

    -- 15 pipe-delimited fields. SSN (index 3) is intentionally parsed and
    then dropped here: it is not included in the returned rows, so the real

    SSN is never persisted downstream.

    """
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
        if len(parts) < 15:
            logger.warning("Skipping malformed received-report row: %s", line)
            continue
        rows.append({
            "edipi": parts[0],
            "last_name": parts[1],
            "first_name": parts[2],
            "receipt_date": parts[4],
            "sending_org": parts[5],
            "purpose": parts[6],
            "purpose_code": parts[7],
            "role": parts[8],
            "format_code": parts[9],
            "loinc_code": parts[10],
            "document_id": parts[11],
            "repository_id": parts[12],
            "source_id": parts[13],
            "ccda_file": parts[14],
        })
    return rows


def extract_disclosure_report_rows(bucket, key):
    """Parse a PatientDisclosureReport.txt file. Confirmed real header:

        EDIPI|Patient_Last_Name|Patient_First_Name|SSN|Date_of_Disclosure_(UTC)|
        Receiving_Organization_ID|Receiving_Organization|Partner|User_ID|
        User_Name|Role|Role_Code|Purpose_of_Use|Purpose_of_Use_Code|
        Document_LOINC_Code|Document_ID|CCDA_File_Name|CommonWell_Indicator

    -- 18 pipe-delimited fields. This is NOT just a shifted version of the
    received-report layout -- past the shared EDIPI/name/SSN prefix, the
    columns are completely different (no Sending_Organization, Repository_ID,
    Source_ID, or Document_Format_Code; adds Receiving_Organization(_ID),
    Partner, User_ID/Name, Role_Code, CommonWell_Indicator). SSN (index 3)
    is intentionally parsed and dropped, same as extract_received_report_rows.
    """
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
        if len(parts) < 18:
            logger.warning("Skipping malformed disclosure-report row: %s", line)
            continue
        rows.append({
            "edipi": parts[0],
            "last_name": parts[1],
            "first_name": parts[2],
            "disclosure_date": parts[4],
            "receiving_org_id": parts[5],
            "receiving_org": parts[6],
            "partner": parts[7],
            "user_id": parts[8],
            "user_name": parts[9],
            "role": parts[10],
            "role_code": parts[11],
            "purpose": parts[12],
            "purpose_code": parts[13],
            "loinc_code": parts[14],
            "document_id": parts[15],
            "ccda_file": parts[16],
            "commonwell_indicator": parts[17],
        })
    return rows


# SQS MESSAGE BUILDERS
def build_manifest_metadata_message(bucket, trigger_key, batch_id, manifest_meta, zip_meta,
                                     manifest_expected_counts, partner_context, ingestion_method):
    """Build the SQS message that carries manifest + CCDA zip
    validation info. No patient-level data goes in this message."""
    zip_actual_count = count_zip(bucket, zip_meta["key"])
    zip_expected_count = manifest_expected_counts.get("CCDA", 0)
    zip_matches_manifest = zip_actual_count == zip_expected_count
    file_metadata = {
        "manifest": manifest_meta,
        "zip": zip_meta,
    }
    bytes_in = sum(item.get("size_bytes", 0) for item in file_metadata.values() if item)
    validation_status = "READY_FOR_DATABASE_SQS_INSERT" if zip_matches_manifest else "COUNT_MISMATCH"

    if not zip_matches_manifest:
        put_metric(
            namespace="HIE/OperationalMonitoring",
            metric_name="CountMismatchEvents",
            value=1
        )

    return {
        "message_type": "MANIFEST_METADATA_UPSERT",
        "schema_version": "2.0",
        "created_at_utc": utc_now_iso(),
        "source": {
            "bucket": bucket,
            "bucket_arn": f"arn:aws-us-gov:s3:::{bucket}",
            "trigger_key": trigger_key,
            "batch_id": batch_id
        },
        "partner_id": partner_context.get("partner_id"),
        "ingestion_method": ingestion_method,
        "partner": partner_context,
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


def build_patient_report_metadata_message(bucket, trigger_key, batch_id, batch_type, manifest_meta, report_meta,
                                           manifest_expected_counts, partner_context, ingestion_method):
    """Build the SQS message that carries the patient report validation
    info and the patient-level rows.

    The report's column layout depends on batch_type -- RECEIVED and
    DISCLOSURE reports share only an EDIPI/name/SSN prefix and diverge
    completely after that (see extract_received_report_rows /
    extract_disclosure_report_rows), so the row shape in this message
    differs depending on report_type below. The database consumer needs
    to branch on report_type to know which set of keys to expect."""
    report_key = report_meta["key"]
    report_actual_count = count_report_rows(bucket, report_key)
    if batch_type == "RECEIVED":
        patient_report_rows = extract_received_report_rows(bucket, report_key)
    else:
        patient_report_rows = extract_disclosure_report_rows(bucket, report_key)
    report_expected_count = manifest_expected_counts.get("REPORT", 0)
    report_matches_manifest = report_actual_count == report_expected_count
    file_metadata = {
        "report": report_meta,
    }
    bytes_in = sum(item.get("size_bytes", 0) for item in file_metadata.values() if item)
    validation_status = "READY_FOR_DATABASE_SQS_INSERT" if report_matches_manifest else "COUNT_MISMATCH"

    if not report_matches_manifest:
        put_metric(
            namespace="HIE/OperationalMonitoring",
            metric_name="CountMismatchEvents",
            value=1
        )

    return {
        "message_type": "PATIENT_REPORT_METADATA_UPSERT",
        "schema_version": "2.0",
        "created_at_utc": utc_now_iso(),
        "source": {
            "bucket": bucket,
            "bucket_arn": f"arn:aws-us-gov:s3:::{bucket}",
            "trigger_key": trigger_key,
            "batch_id": batch_id
        },
        "partner_id": partner_context.get("partner_id"),
        "ingestion_method": ingestion_method,
        "direction": "inbound" if batch_type == "RECEIVED" else "outbound",
        "partner": partner_context,
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
        "report_type": batch_type,
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


def sync_partner_state_and_breach_flag(bucket, batch_id, manifest_meta, report_meta, zip_meta, processing_start):
    """Resolve the partner for this bucket, update partner_transmission_state,
    and clear the breach flag.

    This is called exactly once per batch, only after find_available_keys()
    has confirmed the full matching trio (manifest+report+zip) is present
    AND is_batch_already_sent() has confirmed this batch hasn't been
    processed before -- i.e. this is the moment "the correct pair of files
    was uploaded" for the first time. Gating on the same S3-tag check that
    protects the SQS sends means a re-invocation for an already-processed
    batch won't re-clear the flag or re-fire a recovery notification;
    clear_breach_flag() is also naturally idempotent on its own (its
    RETURNING clause is empty, and breach_cleared comes back False, on any
    call after the first), which additionally protects a retry that
    happens after this step committed but before the SQS sends completed.

    processing_start is the timestamp captured at the very top of
    process_object(), before any S3/DB work for this record began. It's
    used, if a brand-new partner_schedule row needs to be created, to seed
    expected_interval_seconds with how long this Lambda has taken (in
    seconds) to process this batch's files up to that point.
    """
    conn = None
    partner_id = partner_batch_key = environment = partner_name = None
    breach_cleared = False
    try:
        conn = get_conn()
        with conn.cursor() as cur:
            partner_id, partner_batch_key, environment = get_partner_info(cur, bucket)
            partner_name = get_partner_name(cur, partner_id)

            candidates = [m for m in (manifest_meta, report_meta, zip_meta) if m.get("last_modified_utc")]
            latest_meta = max(candidates, key=lambda m: datetime.fromisoformat(m["last_modified_utc"])) if candidates else manifest_meta
            update_partner_state(cur, partner_id, latest_meta["key"])

            elapsed_seconds = (datetime.now(timezone.utc) - processing_start).total_seconds()
            expected_interval_seconds = max(1, int(round(elapsed_seconds)))

            breach_cleared = clear_breach_flag(cur, partner_id, expected_interval_seconds)
        conn.commit()
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
            "Partner-state/breach-flag sync failed for bucket=%s batch_id=%s",
            bucket,
            batch_id
        )
        if not isinstance(e, DatabaseConnectionError):
            send_processing_failure_sns(error=e, bucket=bucket, batch_id=batch_id, partner_name=partner_name)
        raise
    finally:
        if conn:
            conn.close()

    if breach_cleared:
        send_recovery_sns(partner_name, partner_id, environment)

    put_metric(
        namespace="HIE/PartnerMonitoring",
        metric_name="PartnerRecovered",
        value=1 if breach_cleared else 0,
        unit="Count",
        dimensions=[{"Name": "PartnerName", "Value": partner_name}]
    )

    return {
        "partner_id": partner_id,
        "partner_batch_key": partner_batch_key,
        "environment": environment,
        "partner_name": partner_name,
    }


def build_metadata_messages(bucket, trigger_key, processing_start, ingestion_method):
    """Build both metadata messages (manifest/zip validation and
    patient report) for a batch. Returns None if the triggering file
    isn't a recognized batch type, the batch isn't ready yet (missing
    a same-type file), or the batch was already sent.

    processing_start is passed straight through to
    sync_partner_state_and_breach_flag() -- see its docstring.
    ingestion_method is passed straight through to both SQS message
    builders -- see detect_ingestion_method()."""
    filename = os.path.basename(trigger_key)
    batch_id = extract_batch_id(filename)

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

    if is_batch_already_sent(bucket, manifest_key):
        logger.info("Batch %s | type=%s already sent to SQS, skipping duplicate.", batch_id, batch_type)
        return None

    manifest_meta = get_object_metadata(bucket, manifest_key)
    report_meta = get_object_metadata(bucket, report_key)
    zip_meta = get_object_metadata(bucket, zip_key)

    manifest_expected_counts = read_manifest_expected_counts(bucket, manifest_key)
    logger.info(
        "Batch %s | type=%s | Manifest expected counts | CCDA=%d | REPORT=%d",
        batch_id,
        batch_type,
        manifest_expected_counts.get("CCDA", 0),
        manifest_expected_counts.get("REPORT", 0),
    )

    partner_context = sync_partner_state_and_breach_flag(
        bucket, batch_id, manifest_meta, report_meta, zip_meta, processing_start
    )

    manifest_message = build_manifest_metadata_message(
        bucket, trigger_key, batch_id, manifest_meta, zip_meta, manifest_expected_counts, partner_context, ingestion_method
    )
    report_message = build_patient_report_metadata_message(
        bucket, trigger_key, batch_id, batch_type, manifest_meta, report_meta, manifest_expected_counts, partner_context, ingestion_method
    )
    return manifest_message, report_message, manifest_key


# SQS SENDER
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
        response = sqs.send_message(**send_args)
        logger.info(
            "Batch %s | %s | SQS message sent = true",
            message["source"]["batch_id"],
            message["message_type"]
        )
    except Exception as error:
        logger.exception(
            "Batch %s | %s | SQS message sent = false",
            message["source"]["batch_id"],
            message["message_type"]
        )
        raise SQSPublishError("Manifest processing failed due to SQS publish error.") from error

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
    partner_name = message.get("partner", {}).get("partner_name")
    if partner_name:
        put_metric(
            namespace="HIE/PartnerMonitoring",
            metric_name="BytesIn",
            value=message["metrics"]["bytes_in"],
            unit="Bytes",
            dimensions=[{"Name": "PartnerName", "Value": partner_name}]
        )
        if message["message_type"] == "PATIENT_REPORT_METADATA_UPSERT":
            put_metric(
                namespace="HIE/PartnerMonitoring",
                metric_name="FilesIn",
                value=message["metrics"].get("files_in", 0),
                unit="Count",
                dimensions=[{"Name": "PartnerName", "Value": partner_name}]
            )

    return response


# CLOUDWATCH METRICS
def put_metric(namespace, metric_name, value, unit="Count", dimensions=None):
    """Send CloudWatch metric."""
    try:
        metric = {"MetricName": metric_name, "Value": value, "Unit": unit}
        if dimensions:
            metric["Dimensions"] = dimensions
        cloudwatch.put_metric_data(
            Namespace=namespace,
            MetricData=[metric]
        )
    except Exception as e:
        logger.warning("Failed to publish CloudWatch metric %s: %s", metric_name, e)


# MAIN OBJECT PROCESSOR
def process_object(bucket, key, record):
    """Process a single S3 object."""
    filename = os.path.basename(key)
    event_time = datetime.fromisoformat(
        record["eventTime"].replace("Z", "+00:00")
    )
    processing_start = datetime.now(timezone.utc)
    processing_delay = (processing_start - event_time).total_seconds()
    put_metric(
        namespace="HIE/OperationalMonitoring",
        metric_name="ProcessingDelaySeconds",
        value=processing_delay,
        unit="Seconds"
    )
    logger.info("Processing delay=%s seconds", processing_delay)

    put_metric(
        namespace="HIE/OperationalMonitoring",
        metric_name="S3ObjectArrivals",
        value=1
    )

    if not is_target_file(filename):
        logger.info("Skipping non-target file: %s", filename)
        return {"processed": False, "reason": "NON_TARGET_FILE"}

    ingestion_method = detect_ingestion_method(record)
    logger.info("Detected ingestion_method=%s for key=%s", ingestion_method, key)

    result = build_metadata_messages(bucket, key, processing_start, ingestion_method)
    if result is None:
        return {"processed": False, "reason": "BATCH_NOT_READY_OR_ALREADY_SENT"}
    manifest_message, report_message, manifest_key = result

    batch_id = manifest_message["source"]["batch_id"]

    if not is_message_already_sent(bucket, manifest_key, MANIFEST_SENT_TAG_KEY):
        send_metadata_to_sqs(manifest_message)
        mark_message_as_sent(bucket, manifest_key, MANIFEST_SENT_TAG_KEY)
    else:
        logger.info("Batch %s | manifest message already sent, skipping duplicate send.", batch_id)

    if not is_message_already_sent(bucket, manifest_key, REPORT_SENT_TAG_KEY):
        send_metadata_to_sqs(report_message)
        mark_message_as_sent(bucket, manifest_key, REPORT_SENT_TAG_KEY)
    else:
        logger.info("Batch %s | report message already sent, skipping duplicate send.", batch_id)

    put_metric(
        namespace="HIE/OperationalMonitoring",
        metric_name="MetadataMessagesSentToSQS",
        value=1
    )

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


# LAMBDA ENTRY
def lambda_handler(event, context):
    """Main Lambda handler."""
    records = event.get("Records", [])
    if not records:
        logger.info("No records found in event.")
        return {"statusCode": 200, "body": "No records to process."}
    results = []
    had_failure = False
    retry_handler_failed = False
    for record in records:
        bucket = record.get("s3", {}).get("bucket", {}).get("name", "unknown")
        try:
            key = unquote_plus(record["s3"]["object"]["key"])
            if key.endswith("/"):
                logger.info("Skipping folder key: %s", key)
                continue
            result = process_object(bucket, key, record)
            results.append(result)
        except Exception as e:
            had_failure = True
            error_message = str(e)
            if "AccessDenied" in error_message or "access denied" in error_message.lower():
                put_metric(
                    namespace="HIE/OperationalMonitoring",
                    metric_name="AccessDeniedEvents",
                    value=1
                )
            put_metric(
                namespace="HIE/OperationalMonitoring",
                metric_name="MetadataLambdaFailures",
                value=1
            )
            put_metric(
                namespace="HIE/OperationalMonitoring",
                metric_name="ArchiveStatus",
                value=0,
                dimensions=[
                    {"Name": "Bucket", "Value": bucket},
                    {"Name": "Status", "Value": "Failure"}
                ]
            )
            if not invoke_retry_handler(e, event):
                retry_handler_failed = True

            route_critical_failure_notification(e, record)

            results.append({
                "processed": False,
                "reason": "PROCESSING_ERROR",
                "error": error_message
            })
    if retry_handler_failed:
        return {
            "statusCode": 500,
            "body": json.dumps({
                "message": "Metadata Lambda processing failed and the retry handler also failed; no retry is guaranteed.",
                "results": results
            })
        }
    if had_failure:
        return {
            "statusCode": 202,
            "body": json.dumps({
                "message": "Metadata Lambda processing failed for one or more records; retry handler invoked.",
                "results": results
            })
        }
    return {
        "statusCode": 200,
        "body": json.dumps({
            "message": "Metadata Lambda completed successfully.",
            "results": results
        })
    }
