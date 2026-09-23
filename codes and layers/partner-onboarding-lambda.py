import os
import json
import logging
import time
import boto3
import psycopg2
from urllib.parse import unquote_plus
    # ---RETRY UTILITY IMPORT WITH FALLBACK---
try:
    import retry_utils
    if not hasattr(retry_utils, "handle_retry"):
        raise ImportError("retry_utils module does not export handle_retry")
except ImportError as e:
    logging.getLogger().error(
        "retry_utils layer missing or failed to import: %s", e
    )
    class _RetryUtilsFallback:
        @staticmethod
        def handle_retry(error, event):
            logging.getLogger().error(
                "Fallback handle_retry invoked -- retry_utils layer not "
                "available. Error: %s | Event: %s", error, event
            )
            return False
    retry_utils = _RetryUtilsFallback()
    # ---SETTING UP LOGGER FOR WRITING LOGS TO CLOUDWATCH---
logger = logging.getLogger()
logger.setLevel(logging.INFO)
# ---ENVIRONMENT VARIABLES---
DB_SECRET_ARN           = os.environ["DB_SECRET_ARN"]
DB_HOST                 = os.environ["DB_HOST"]
DB_PORT                 = int(os.environ.get("DB_PORT", "5432"))
DB_NAME                 = os.environ.get("DB_NAME", "ccda01")
AWS_REGION              = os.environ.get("AWS_REGION", "us-gov-west-1")
ENVIRONMENT             = os.environ.get("ENVIRONMENT")
METADATA_LAMBDA_ARN     = os.environ.get("MANIFEST_LAMBDA_FUNCTION")
    # ---AWS CLIENTS---
s3 = boto3.client("s3", region_name=AWS_REGION)
lambda_client = boto3.client("lambda", region_name=AWS_REGION)
sm = boto3.client("secretsmanager", region_name=AWS_REGION)
cloudwatch = boto3.client("cloudwatch", region_name=AWS_REGION)
    # ---CREDENTIALS CACHING---
_cached_creds = None
_cached_creds_expiry = 0.0
_CREDS_TTL_SECONDS = 900
    # ---SETTING UP RETRY HANDLER FUNCTION---
def invoke_retry_handler(error: Exception, event: dict) -> bool:
    try:
        result = retry_utils.handle_retry(error, event)
        return bool(result)
    except Exception as retry_err:
        logger.exception(f"Retry handler itself failed: {retry_err}")
        return False
    # ---GETTING DATABASE CREDENTIALS FROM SECRETS MANAGER---
def get_db_credentials():
    global _cached_creds, _cached_creds_expiry
    now = time.time()
    if _cached_creds and now < _cached_creds_expiry:
        return _cached_creds
    secret_value = sm.get_secret_value(SecretId=DB_SECRET_ARN)
    creds = json.loads(secret_value["SecretString"])
    creds["host"] = DB_HOST
    creds.setdefault("port", DB_PORT)
    creds.setdefault("dbname", DB_NAME)
    creds.setdefault("sslmode", "require")
        # ---CACHE THE CREDENTIALS WITH AN EXPIRY TIME---
    _cached_creds = creds
    _cached_creds_expiry = now + _CREDS_TTL_SECONDS
    return _cached_creds
    # ---DATABASE CONNECTION FUNCTION---
def get_conn():
    try:
        creds = get_db_credentials()
        return psycopg2.connect(
            host=creds["host"],
            port=creds["port"],
            dbname=creds["dbname"],
            user=creds["username"],
            password=creds["password"],
            sslmode=creds["sslmode"],
        )
        # ---HANDLE DATABASE CONNECTION ERRORS---
    except Exception as e:
        if isinstance(e, psycopg2.OperationalError) and "authentication failed" in str(e).lower():
            global _cached_creds, _cached_creds_expiry
            _cached_creds = None
            _cached_creds_expiry = 0.0  # force re-fetch on next call
            # ---LOGGING DATABASE CONNECTION FAILURES TO CLOUDWATCH---
        cloudwatch.put_metric_data(
            Namespace='HIE/OperationalMonitoring',
            MetricData=[
                {
                    'MetricName': 'DatabaseConnectionFailures',
                    'Value': 1,
                    'Unit': 'Count'
                }
            ]
        )
        logger.exception("Database connection failure")
        raise
    # ---REQUIRED FIELDS IN THE UPLOADED PARTNER JSON FILE---
REQUIRED_PARTNER_FIELDS = (
    "partner_name",
    "contact_email",
    "contact_person",
    "contact_phone_number",
    "bucket_name",
)
    # ---READING AND VALIDATING THE UPLOADED PARTNER JSON FILE---
def parse_partner_json(bucket, key):
    obj = s3.get_object(Bucket=bucket, Key=key)
    content = obj["Body"].read().decode("utf-8")
    data = json.loads(content)
    missing = [f for f in REQUIRED_PARTNER_FIELDS if not data.get(f)]
    if missing:
        raise ValueError(
            f"Partner JSON {key} is missing required field(s): {', '.join(missing)}"
        )
    return data
    # ---CHECKING IF THIS PARTNER/BUCKET ALREADY EXISTS---
def find_partner_registry_match(cur, partner_name, bucket_arn):
    """Look at partner_registry for rows matching this partner_name and/or
    this bucket_arn.

    Returns (exact_match, arn_match):
      - exact_match is set only when BOTH partner_name and s3_bucket_arn
        already match the same row -- this is a true duplicate, skip it.
      - arn_match is set when the bucket_arn already belongs to a
        DIFFERENT partner_name -- s3_bucket_arn is UNIQUE in this table,
        so we can never insert a second row for the same bucket. This is
        flagged as a conflict rather than silently creating anything.
    A partner_name match on its own (different bucket) is allowed through
    as a new row -- the same partner can have more than one bucket.
    """
    cur.execute(
        """
        SELECT partner_id, partner_batch_key, s3_bucket_arn, partner_name
        FROM partner_registry
        WHERE partner_name = %s OR s3_bucket_arn = %s
        """,
        (partner_name, bucket_arn),
    )
    exact_match = None
    arn_match = None
    for row in cur.fetchall():
        _, _, row_arn, row_name = row
        if row_arn == bucket_arn:
            arn_match = row
            if row_name == partner_name:
                exact_match = row
    return exact_match, arn_match
    # ---CREATING THE NEW PARTNER'S S3 BUCKET---
def ensure_partner_bucket(bucket_name):
    create_kwargs = {"Bucket": bucket_name}
    if AWS_REGION != "us-east-1":
        create_kwargs["CreateBucketConfiguration"] = {"LocationConstraint": AWS_REGION}
    try:
        s3.create_bucket(**create_kwargs)
        logger.info("Created S3 bucket: %s", bucket_name)
    except s3.exceptions.BucketAlreadyOwnedByYou:
        logger.info(
            "Bucket %s already exists and is owned by this account -- reusing it.",
            bucket_name
        )
    except s3.exceptions.BucketAlreadyExists:
        logger.error(
            "Bucket %s already exists under a different AWS account -- cannot use it.",
            bucket_name
        )
        raise
    # ---WIRING THE NEW BUCKET TO TRIGGER THE METADATA-PROCESSING LAMBDA---
def wire_bucket_notification(bucket_name):
    if not METADATA_LAMBDA_ARN:
        logger.warning(
            "MANIFEST_LAMBDA_FUNCTION is not configured; skipping automatic S3 "
            "event notification wiring for bucket %s. Set this up manually "
            "if the new partner's uploads should trigger metadata processing.",
            bucket_name
        )
        return
    bucket_arn = f"arn:aws-us-gov:s3:::{bucket_name}"
    statement_id = f"AllowS3Invoke-{bucket_name}"
    try:
        lambda_client.add_permission(
            FunctionName=METADATA_LAMBDA_ARN,
            StatementId=statement_id,
            Action="lambda:InvokeFunction",
            Principal="s3.amazonaws.com",
            SourceArn=bucket_arn,
        )
        logger.info(
            "Granted bucket %s permission to invoke %s.",
            bucket_name, METADATA_LAMBDA_ARN
        )
    except lambda_client.exceptions.ResourceConflictException:
        logger.info(
            "Invoke permission for bucket %s on %s already exists.",
            bucket_name, METADATA_LAMBDA_ARN
        )
    s3.put_bucket_notification_configuration(
        Bucket=bucket_name,
        NotificationConfiguration={
            "LambdaFunctionConfigurations": [
                {
                    "LambdaFunctionArn": METADATA_LAMBDA_ARN,
                    "Events": ["s3:ObjectCreated:*"],
                }
            ]
        },
    )
    logger.info(
        "Wired S3 event notifications on %s to %s.",
        bucket_name, METADATA_LAMBDA_ARN
    )
    # ---LAMBDA HANDLER FUNCTION---
def lambda_handler(event, context):
    logger.info("Running partner onboarding lambda (S3-triggered)")
    records = event.get("Records", [])
    if not records:
        logger.info("No records found in event.")
        return {"statusCode": 200, "body": json.dumps("No records to process.")}
        # ---TRY TO ESTABLISH DATABASE CONNECTION---
    conn = None
    try:
        conn = get_conn()
    except Exception as e:
        logger.exception("Database connection failed")
        if not invoke_retry_handler(e, event):
            raise
        return {
            "statusCode": 202,
            "body": json.dumps("Database connection failed; retry handler invoked.")
        }
    inserted = 0
    skipped = 0
    errors = []
    try:
        with conn.cursor() as cur:
            for record in records:
                if record.get("eventSource") != "aws:s3":
                    logger.info("Skipping non-S3 record: %s", record.get("eventSource"))
                    continue
                source_bucket = record["s3"]["bucket"]["name"]
                source_key = unquote_plus(record["s3"]["object"]["key"])
                    # ---READING THE UPLOADED PARTNER JSON FILE---
                try:
                    partner_data = parse_partner_json(source_bucket, source_key)
                except Exception as e:
                    logger.exception(
                        "Failed to read/parse partner JSON %s/%s", source_bucket, source_key
                    )
                    errors.append(f"{source_key}: {e}")
                    continue
                partner_name = partner_data["partner_name"]
                contact_email = partner_data["contact_email"]
                contact_person = partner_data["contact_person"]
                contact_phone_number = partner_data["contact_phone_number"]
                bucket_name = partner_data["bucket_name"]
                bucket_arn = f"arn:aws-us-gov:s3:::{bucket_name}"
                    # ---CHECKING FOR AN EXISTING MATCH BEFORE CREATING ANYTHING---
                exact_match, arn_match = find_partner_registry_match(cur, partner_name, bucket_arn)
                if exact_match:
                    logger.info(
                        "Partner '%s' with bucket %s is already fully registered -- skipping.",
                        partner_name, bucket_name
                    )
                    skipped += 1
                    continue
                if arn_match:
                    # bucket_arn is UNIQUE in partner_registry -- this bucket is
                    # already registered to a different partner_name, so a new
                    # row for it can't be created. Flag it instead of failing.
                    logger.error(
                        "Bucket %s is already registered under a different "
                        "partner_name ('%s'); cannot register it again for "
                        "'%s'. Skipping %s -- check the uploaded JSON.",
                        bucket_name, arn_match[3], partner_name, source_key
                    )
                    skipped += 1
                    errors.append(
                        f"{source_key}: bucket {bucket_name} already registered "
                        f"to '{arn_match[3]}'"
                    )
                    continue
                    # ---CREATING THE BUCKET AND WIRING ITS TRIGGER---
                try:
                    ensure_partner_bucket(bucket_name)
                    wire_bucket_notification(bucket_name)
                except Exception as e:
                    logger.exception(
                        "Failed to create/wire bucket %s for partner %s",
                        bucket_name, partner_name
                    )
                    errors.append(f"{source_key}: bucket setup failed -- {e}")
                    continue
                    # ---INSERTING THE NEW PARTNER REGISTRY RECORD---
                cur.execute(
                    """
                    INSERT INTO partner_registry (
                        s3_bucket_arn,
                        environment,
                        partner_name
                    )
                    VALUES (%s, %s, %s)
                    RETURNING partner_id, partner_batch_key;
                    """,
                    (bucket_arn, ENVIRONMENT, partner_name),
                )
                partner_id, partner_batch_key = cur.fetchone()
                    # ---INSERTING THE NEW PARTNER CONTACT DETAILS RECORD---
                cur.execute(
                    """
                    INSERT INTO partner_contact_details (
                        partner_id,
                        partner_batch_key,
                        partner_name,
                        contact_person,
                        contact_email,
                        contact_phone_number
                    )
                    VALUES (%s, %s, %s, %s, %s, %s);
                    """,
                    (
                        partner_id,
                        partner_batch_key,
                        partner_name,
                        contact_person,
                        contact_email,
                        contact_phone_number,
                    ),
                )
                    # ---INSERTING THE NEW PARTNER SCHEDULE RECORD---
                cur.execute(
                    """
                    INSERT INTO partner_schedule (
                        partner_id,
                        expected_interval_seconds,
                        last_alert_at
                    )
                    VALUES (%s, %s, NOW());
                    """,
                    (partner_id, 1800),
                )
                logger.info(
                    "Onboarded new partner '%s' -- bucket=%s partner_id=%s",
                    partner_name, bucket_name, partner_id
                )
                inserted += 1
        conn.commit()
            # ---RETURNING SUCCESS RESPONSE---
        return {
            "statusCode": 200,
            "body": json.dumps({
                "message": "Partner onboarding lambda executed successfully",
                "inserted": inserted,
                "skipped": skipped,
                "errors": errors,
            })
        }
        # ---SENDING ERROR RESPONSE AND INVOKING RETRY HANDLER ON EXCEPTION---
    except Exception as e:
        conn.rollback()
        logger.exception("Error onboarding partner(s)")
        if not invoke_retry_handler(e, event):
            raise
        return {
            "statusCode": 500,
            "body": json.dumps(str(e))
        }
    finally:
        if conn:
            conn.close()
