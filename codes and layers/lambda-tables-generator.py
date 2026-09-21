import os
import json
import logging
import boto3
import psycopg2
    # ---RETRY UTILITY IMPORT WITH FALLBACK---
try:
    import retry_utils
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
DB_SECRET_ARN   = os.environ["DB_SECRET_ARN"]
DB_HOST         = os.environ["DB_HOST"]
DB_PORT         = int(os.environ.get("DB_PORT", "5432"))
DB_NAME         = os.environ.get("DB_NAME", "ccda01")
AWS_REGION      = os.environ.get("AWS_REGION") or os.environ.get("AWS_DEFAULT_REGION")
    # ---AWS CLIENTS---
sm = boto3.client("secretsmanager", region_name=AWS_REGION)
cloudwatch = boto3.client("cloudwatch", region_name=AWS_REGION)
    # ---CREDENTIALS CACHING---
_cached_creds = None
    # ---RETRY HANDLER---
def invoke_retry_handler(error, event) -> bool:
    try:
        result = retry_utils.handle_retry(error, event)
        logger.info("Retry handler invoked successfully")
        return bool(result)
    except Exception as e:
        logger.error(f"Retry handler invocation failed: {e}")
        return False
    # ---GETTING DATABASE CREDENTIALS FROM SECRETS MANAGER---
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
    # ---GETTING DATABASE CONNECTION--- 
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
        # ---IF CONNECTION FAILS, LOG TO CLOUDWATCH AND RAISE EXCEPTION---
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
    # ---CREATING TABLES---
def ensure_tables(cur):
    logger.info("Creating/updating tables...")
    cur.execute("CREATE EXTENSION IF NOT EXISTS pgcrypto;")
        # ---CREATING PARTNER_REGISTRY TABLE---
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
        # ---CREATING PARTNER_CONTACT_DETAILS TABLE---
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
        # ---CREATING PATIENT_DETAILS TABLE---
    cur.execute("""
        CREATE TABLE IF NOT EXISTS patient_details (
            partner_id UUID NOT NULL,
            batch_id TEXT NOT NULL,
            environment VARCHAR(20) NOT NULL,
            file_name VARCHAR(255),
            direction VARCHAR(20) NOT NULL,
            edipi BIGINT NOT NULL,
            ssn VARCHAR(11),
            CONSTRAINT ssn_must_be_null CHECK (ssn IS NULL),
            patient_last_name VARCHAR(100) NOT NULL,
            patient_first_name VARCHAR(100) NOT NULL,
            date_of_receipt_utc TIMESTAMPTZ,
            date_of_disclosure_utc TIMESTAMP,
            file_arrival_time_utc TIMESTAMPTZ NOT NULL,
            sending_organization VARCHAR(255),
            receiving_organization_id VARCHAR(64),
            receiving_organization VARCHAR(255),
            receiving_organization_name VARCHAR(255),
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
            commonwell_indicator BOOLEAN
        );
    """)
        # ---ADDING NEW COLUMNS TO PATIENT_DETAILS TABLE IF THEY DO NOT EXIST---
    cur.execute("""
        ALTER TABLE patient_details
        ADD COLUMN IF NOT EXISTS direction VARCHAR(20);
        ALTER TABLE patient_details
        ADD COLUMN IF NOT EXISTS file_name VARCHAR(255);
        ALTER TABLE patient_details
        ADD COLUMN IF NOT EXISTS date_of_disclosure_utc TIMESTAMP;
        ALTER TABLE patient_details
        ADD COLUMN IF NOT EXISTS receiving_organization_name VARCHAR(255);
        ALTER TABLE patient_details
        ADD COLUMN IF NOT EXISTS receiving_organization_id VARCHAR(64);
        ALTER TABLE patient_details
        ADD COLUMN IF NOT EXISTS commonwell_indicator BOOLEAN;
    """)
        # ---ENSURING THE CHECK CONSTRAINT ON SSN IS PRESENT---
    cur.execute("""
        DO $$
        BEGIN
            IF NOT EXISTS (
                SELECT 1
                FROM pg_constraint
                WHERE conname = 'ssn_must_be_null'
                  AND conrelid = 'patient_details'::regclass
            ) THEN
                ALTER TABLE patient_details
                ADD CONSTRAINT ssn_must_be_null
                CHECK (ssn IS NULL);
            END IF;
        END
        $$;
    """)
        # ---CREATING MANIFEST_BATCH TABLE---
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
            CONSTRAINT unique_batch UNIQUE (file_name)
        );
    """)
        # ---ENSURING THE UNIQUE CONSTRAINT ON FILE_NAME IS PRESENT---
    cur.execute("""
        ALTER TABLE manifest_batch
        DROP CONSTRAINT IF EXISTS unique_batch;
        ALTER TABLE manifest_batch
        ADD CONSTRAINT unique_batch UNIQUE (file_name);
    """)
        # ---CREATING PARTNER_SCHEDULE TABLE---
    cur.execute("""
        CREATE TABLE IF NOT EXISTS partner_schedule (
            schedule_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
            partner_id UUID NOT NULL,
            failure_type TEXT,
            last_alert_at TIMESTAMPTZ NOT NULL,
            expected_interval_seconds INTEGER NOT NULL,
            breach_flag BOOLEAN DEFAULT FALSE,
            last_breach_email_sent_at TIMESTAMPTZ,
            last_recovery_email_sent_at TIMESTAMPTZ,
            created_at TIMESTAMPTZ DEFAULT NOW(),
            updated_at TIMESTAMPTZ DEFAULT NOW()
        );
    """)
        # ---DROPPING TIMEZONE COLUMN FROM PARTNER_SCHEDULE TABLE IF IT EXISTS---
    cur.execute("""
        ALTER TABLE partner_schedule
        DROP COLUMN IF EXISTS timezone;
    """)
    # ---CREATING PARTNER_TRANSMISSION_STATE TABLE---
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
        # ---SENDING SUCCESS LOG AFTER TABLE CREATION/UPDATE---
    logger.info("Tables created/updated successfully.")

    # ---LAMBDA HANDLER---
def lambda_handler(event, context):
    logger.info("Running lambda_tables_generator")
    conn = None
        # ---TRYING TO ESTABLISH DATABASE CONNECTION---
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
        # ---TRYING TO CREATE/UPDATE TABLES---
    try:
        with conn.cursor() as cur:
            ensure_tables(cur)
        conn.commit()
        # ---IF TABLE CREATION/UPDATE FAILS, ROLLBACK AND LOG EXCEPTION---
    except Exception:
        conn.rollback()
        global _cached_creds
        _cached_creds = None
        logger.exception("Failed creating tables")
        raise
        # ---SENDING SUCCESS LOG AFTER TABLE CREATION/UPDATE---
    finally:
        if conn:
            conn.close()
        # ---RETURNING SUCCESS RESPONSE---
    return {
        "statusCode": 200,
        "body": json.dumps("Tables created successfully.")
    }
