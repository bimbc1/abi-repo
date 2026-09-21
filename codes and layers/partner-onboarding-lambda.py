import os
import json
import logging
import time
import boto3
import psycopg2
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
PARTNER_1_BUCKET_ARN    = os.environ["PARTNER_1_BUCKET"]
    # ---AWS CLIENTS---
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
    # ---LAMBDA HANDLER FUNCTION---
def lambda_handler(event, context):
    logger.info("Running partner contact insert lambda (final + schedule)")
    conn = None
        # ---TRY TO ESTABLISH DATABASE CONNECTION---
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
    try:
        with conn.cursor() as cur:
            bucket_arns = [
                PARTNER_1_BUCKET_ARN
            ]
            environment = os.environ.get("ENVIRONMENT")
            for arn in bucket_arns:
                cur.execute("""
                    SELECT partner_id, partner_batch_key
                    FROM partner_registry
                    WHERE s3_bucket_arn = %s
                    LIMIT 1;
                """, (arn,))
                existing_row = cur.fetchone()
                if existing_row:
                    pid, pbatch = existing_row
                    logger.info(
                        f"partner_registry already exists → {arn}: {pid}, {pbatch}"
                    )
                else:
                        # ---INSERTING NEW PARTNER REGISTRY RECORD---
                    cur.execute("""
                        INSERT INTO partner_registry (
                            s3_bucket_arn,
                            environment
                        )
                        VALUES (%s, %s)
                        RETURNING partner_id, partner_batch_key;
                    """, (arn, environment))
                    pid, pbatch = cur.fetchone()
                    logger.info(
                        f"Created partner_registry → {arn}: {pid}, {pbatch}"
                    )
            partners = [
                {
                    "s3_bucket_arn": PARTNER_1_BUCKET_ARN,
                    "partner_name": "Oracle Health",
                    "contact_person": "Abim",
                    "contact_email": "abim@oracle.com",
                    "contact_phone": "1234567890",
                },
            ]
            inserted = 0
            skipped = 0
            for p in partners:
                    # ---GETTING DATA FROM PARTNER REGISTRY BASED ON S3 BUCKET ARN---
                cur.execute("""
                    SELECT partner_id, partner_batch_key, environment
                    FROM partner_registry
                    WHERE s3_bucket_arn = %s
                    ORDER BY created_at DESC
                    LIMIT 1;
                """, (p["s3_bucket_arn"],))
                row = cur.fetchone()
                if not row:
                    logger.warning(f"Skipping (ARN not found): {p['s3_bucket_arn']}")
                    skipped += 1
                    continue
                partner_id, partner_batch_key, environment = row
                cur.execute("""
                    SELECT 1
                    FROM partner_contact_details
                    WHERE partner_id = %s
                    AND partner_batch_key = %s
                    AND partner_name = %s
                    LIMIT 1;
                """, (partner_id, partner_batch_key, p["partner_name"]))
                if cur.fetchone():
                    logger.info(f"Skipping duplicate partner: {p['partner_name']}")
                    skipped += 1
                else:
                        # ---INSERTING NEW PARTNER CONTACT DETAILS RECORD---
                    cur.execute("""
                        INSERT INTO partner_contact_details (
                            partner_id,
                            partner_batch_key,
                            partner_name,
                            contact_person,
                            contact_email,
                            contact_phone_number
                        )
                        VALUES (%s, %s, %s, %s, %s, %s);
                    """, (
                        partner_id,
                        partner_batch_key,
                        p["partner_name"],
                        p["contact_person"],
                        p["contact_email"],
                        p["contact_phone"]
                    ))
                    inserted += 1
                cur.execute("""
                    SELECT 1
                    FROM partner_schedule
                    WHERE partner_id = %s
                    LIMIT 1;
                """, (partner_id,))
                if not cur.fetchone():
                        # ---INSERTING NEW PARTNER SCHEDULE RECORD---
                    cur.execute("""
                        INSERT INTO partner_schedule (
                            partner_id,
                            expected_interval_seconds,
                            last_alert_at
                        )
                        VALUES (%s, %s, NOW());
                    """, (partner_id, 1800))
        conn.commit()
            # ---RETURNING SUCCESS RESPONSE---
        return {
            "statusCode": 200,
            "body": json.dumps({
                "message": "Lambda executed successfully",
                "inserted": inserted,
                "skipped": skipped
            })
        }
        # ---SENDING ERROR RESPONSE AND INVOKING RETRY HANDLER ON EXCEPTION---
    except Exception as e:
        conn.rollback()
        logger.exception("Error inserting partner contacts")
        if not invoke_retry_handler(e, event):
            raise
        return {
            "statusCode": 500,
            "body": json.dumps(str(e))
        }
    finally:
        if conn:
            conn.close()
