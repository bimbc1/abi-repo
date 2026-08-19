import os
import json
import logging
import boto3
import psycopg2
import retry_utils

logger = logging.getLogger()
logger.setLevel(logging.INFO)

# ENV VARIABLES
DB_SECRET_ARN = os.environ["DB_SECRET_ARN"]
DB_HOST = os.environ["DB_HOST"]
DB_PORT = int(os.environ.get("DB_PORT", "5432"))
DB_NAME = os.environ.get("DB_NAME", "ccda02")
AWS_REGION = os.environ.get("AWS_REGION") or os.environ.get("AWS_DEFAULT_REGION")

sm = boto3.client("secretsmanager", region_name=AWS_REGION)

cloudwatch = boto3.client("cloudwatch", region_name=AWS_REGION)

_cached_creds = None


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

    except Exception as e:
        logger.error(f"Retry handler invocation failed: {e}")


# DB CONNECTION
def get_db_credentials():
    global _cached_creds
    if _cached_creds is not None:
        return _cached_creds

    secret_value = sm.get_secret_value(SecretId=DB_SECRET_ARN)
    creds = json.loads(secret_value["SecretString"])

    creds["host"] = DB_HOST
    creds.setdefault("port", DB_PORT)
    creds.setdefault("dbname", DB_NAME)
    creds.setdefault("sslmode", "require")

    _cached_creds = creds
    return creds


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

    except Exception as e:

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

# TABLES
def ensure_tables(cur):
    logger.info("Creating tables...")

    cur.execute("CREATE EXTENSION IF NOT EXISTS pgcrypto;")

    cur.execute("""
        CREATE TABLE IF NOT EXISTS partner_registry (
            partner_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
            partner_batch_key UUID NOT NULL DEFAULT gen_random_uuid(),
            created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            s3_bucket_arn VARCHAR(255) NOT NULL UNIQUE,
            environment VARCHAR(10) NOT NULL,
            updated_at TIMESTAMPTZ DEFAULT NOW()
        );
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS partner_contact_details (
            contact_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
            partner_id UUID NOT NULL,
            partner_batch_key UUID NOT NULL,
            partner_name VARCHAR(100) NOT NULL,
            contact_email VARCHAR(100) NOT NULL,
            contact_phone_number VARCHAR(20) NOT NULL,
            contact_person VARCHAR(100) NOT NULL,
            created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            updated_at TIMESTAMPTZ DEFAULT NOW()
        );
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS patient_details (
            partner_id UUID NOT NULL,
            batch_id TEXT NOT NULL,
            environment VARCHAR(20) NOT NULL,
            file_name TEXT,
            manifest_companion_mapping TEXT,
            edipi BIGINT NOT NULL,
            SSN VARCHAR(11),
            patient_last_name VARCHAR(100) NOT NULL,
            patient_first_name VARCHAR(100) NOT NULL,
            date_of_receipt_utc TIMESTAMPTZ,
            date_of_disclosure_utc TIMESTAMPTZ,
            file_arrival_time_utc TIMESTAMPTZ NOT NULL,
            sending_organization VARCHAR(255),
            receiving_organization_id TEXT,
            receiving_organization VARCHAR(255),
            partner VARCHAR(150),
            user_id TEXT,
            user_name VARCHAR(150),
            user_role VARCHAR(150),
            role VARCHAR(50),
            role_code VARCHAR(50),
            purpose_of_use VARCHAR(255),
            purpose_of_use_code VARCHAR(50),
            document_format_code VARCHAR(255),
            document_loinc_code VARCHAR(50),
            document_id TEXT NOT NULL,
            repository_id TEXT,
            source_id TEXT,
            ccda_file_name TEXT NOT NULL,
            commonwell_indicator VARCHAR(20)
        );
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS manifest_batch (
            partner_id UUID NOT NULL,
            batch_id TEXT NOT NULL,
            partner_batch_key TEXT NOT NULL,
            file_name TEXT,
            manifest_file_count INTEGER,
            actual_file_count INTEGER,
            count_discrepancy INTEGER,
            ingestion_method TEXT,
            submission_timestamp TIMESTAMPTZ,
            created_at TIMESTAMPTZ DEFAULT NOW(),
            updated_at TIMESTAMPTZ DEFAULT NOW(),
            CONSTRAINT unique_batch UNIQUE (partner_id, batch_id, file_name)
        );
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS partner_schedule (
            schedule_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
            partner_id UUID NOT NULL,
            expected_interval_seconds INTEGER NOT NULL,
            breach_flag BOOLEAN DEFAULT FALSE,
            last_breach_email_sent_at TIMESTAMPTZ,
            last_recovery_email_sent_at TIMESTAMPTZ,
            created_at TIMESTAMPTZ DEFAULT NOW(),
            updated_at TIMESTAMPTZ DEFAULT NOW()
        );
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS partner_transmission_state (
            state_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
            partner_id UUID NOT NULL,
            last_seen_at TIMESTAMPTZ NOT NULL,
            batch_id TEXT NOT NULL,
            last_object_key TEXT,
            updated_at TIMESTAMPTZ DEFAULT NOW()
        );
    """)

    logger.info("Tables created successfully.")


# LAMBDA HANDLER
def lambda_handler(event, context):
    logger.info("Running lambda_tables_generator")

    conn = None

    try:
        conn = get_conn()
    except Exception as e:
        logger.exception("Database connection failed")
        invoke_retry_handler(e, event)
        return {
            "statusCode": 202,
            "body": json.dumps("Database connection failed; retry handler invoked.")
        }

    try:
        with conn.cursor() as cur:
            ensure_tables(cur)

        conn.commit()

    except Exception:
        conn.rollback()
        global _cached_creds
        _cached_creds = None
        logger.exception("Failed creating tables")
        raise

    finally:
        if conn:
            conn.close()

    return {
        "statusCode": 200,
        "body": json.dumps("Tables created successfully.")
    }
