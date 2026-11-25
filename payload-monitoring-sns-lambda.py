import os
import json
import logging
import time
import boto3
import pg8000
from datetime import datetime, timezone

logger = logging.getLogger()
logger.setLevel(logging.INFO)

# =========================
# Environment variables
# =========================
DB_HOST = os.environ["DB_HOST"]
DB_NAME = os.environ["DB_NAME"]
DB_PORT = int(os.environ.get("DB_PORT", "5432"))
DB_SECRET_ARN = os.environ["DB_SECRET_ARN"]

SNS_TOPIC_ARN = os.environ["SNS_TOPIC_ARN"]
SNS_SUBJECT_TEMPLATE = os.environ["SNS_SUBJECT_TEMPLATE"]
SNS_MESSAGE_TEMPLATE = os.environ["SNS_MESSAGE_TEMPLATE"]
WARNING_INFO = os.environ["WARNING_INFO"]

# 43200 = 12 hours
EMAIL_RESEND_INTERVAL_SECONDS = int(
    os.environ["EMAIL_RESEND_INTERVAL_SECONDS"]
)

# AWS clients
sm = boto3.client("secretsmanager", region_name="us-gov-west-1")
sns = boto3.client("sns")
cloudwatch = boto3.client("cloudwatch", region_name="us-gov-west-1")


def get_db_credentials():
    """
    Read database credentials from Secrets Manager and
    combine them with host/port/dbname from Lambda env vars.
    """
    secret_value = sm.get_secret_value(SecretId=DB_SECRET_ARN)
    creds = json.loads(secret_value["SecretString"])

    return {
        "host": DB_HOST,
        "port": DB_PORT,
        "database": DB_NAME,
        "user": creds["username"],
        "password": creds["password"],
        "ssl_context": True
    }


def get_conn():
    """
    Create a PostgreSQL connection using pg8000, retrying up to 3 times.

    Each attempt loads the credentials from Secrets Manager AND connects, so a
    Secrets Manager (or secret JSON) failure is retried just like a connect failure.

    RetryEvents (FailureType=DatabaseConnectionFailure) is published only when a
    retry follows (attempt < max_attempts), so 3 failed attempts give 2 RetryEvents.
    The final failure is not a retry: it is re-raised and reported once as
    DatabaseConnectionFailures in lambda_handler.
    """
    max_attempts = 3

    for attempt in range(1, max_attempts + 1):
        try:
            creds = get_db_credentials()
            return pg8000.connect(
                host=creds["host"],
                port=creds["port"],
                database=creds["database"],
                user=creds["user"],
                password=creds["password"],
                ssl_context=creds["ssl_context"]
            )
        except Exception as e:
            logger.warning("DB connection attempt %s/%s failed: %s",
                           attempt, max_attempts, str(e))
            if attempt == max_attempts:
                raise  # final failure: no retry follows, so no RetryEvents
            put_dashboard_metric(
                "RetryEvents",
                dimensions=[
                    {"Name": "FunctionName", "Value": FUNCTION_NAME},
                    {"Name": "FailureType", "Value": "DatabaseConnectionFailure"},
                ],
            )
            time.sleep(1)


def format_last_seen_duration(elapsed_seconds):
    """
    Convert elapsed seconds into a readable duration.
    Examples:
      7 minutes
      1 hour
      2 hours 14 minutes
    """
    hours = int(elapsed_seconds // 3600)
    minutes = int((elapsed_seconds % 3600) // 60)

    if hours > 0 and minutes > 0:
        hour_label = "hour" if hours == 1 else "hours"
        minute_label = "minute" if minutes == 1 else "minutes"
        return f"{hours} {hour_label} {minutes} {minute_label}"

    elif hours > 0:
        hour_label = "hour" if hours == 1 else "hours"
        return f"{hours} {hour_label}"

    else:
        minute_label = "minute" if minutes == 1 else "minutes"
        return f"{minutes} {minute_label}"


def publish_partner_metric(partner_name, metric_name, value):
    partner_name_clean = str(partner_name).replace(" ", "_")

    cloudwatch.put_metric_data(
        Namespace="HIE/PartnerMonitoring",
        MetricData=[
            {
                "MetricName": metric_name,
                "Dimensions": [
                    {
                        "Name": "PartnerName",
                        "Value": partner_name_clean
                    }
                ],
                "Value": value,
                "Unit": "Count"
            }
        ]
    )


# ---- DASHBOARD METRICS ----
FUNCTION_NAME = os.environ.get("AWS_LAMBDA_FUNCTION_NAME", "unknown")


def put_dashboard_metric(metric_name, value=1, dimensions=None,
                         namespace="HIE/OperationalMonitoring"):
    try:
        metric = {"MetricName": metric_name, "Value": value, "Unit": "Count"}
        if dimensions:
            metric["Dimensions"] = dimensions
        cloudwatch.put_metric_data(Namespace=namespace, MetricData=[metric])
    except Exception as metric_error:
        logger.warning("Failed to publish metric %s: %s", metric_name, str(metric_error))


def put_crud_failure_metrics(failure_type, partner_id):
    """Publish CRUDFailureEvents keyed by PartnerId, the same dimension the other Lambdas
    use and the dashboard CRUD widgets query (the Partner dropdown value is the partner_id).

    Exactly two datapoints per failure, with no partner-less datapoint:
      1. PartnerId=<partner_id>, or "Unknown" when no partner is being processed
         (e.g. the Step 1 query that reads ALL partners)
      2. PartnerId="All" -> an AGGREGATE copy, used only when "All" is selected in the
         dashboard Partner dropdown. Do not SUM across all PartnerId values: that
         would count each failure twice."""
    partner = str(partner_id).strip() if partner_id not in (None, "") else ""
    partner = partner or "Unknown"
    dimensions = [
        {"Name": "FunctionName", "Value": FUNCTION_NAME},
        {"Name": "FailureType", "Value": failure_type},
    ]
    for pid in (partner, "All"):  # partner-level datapoint + "All" aggregate
        put_dashboard_metric(
            "CRUDFailureEvents",
            dimensions=dimensions + [{"Name": "PartnerId", "Value": pid}],
        )
    logger.info("Published CRUDFailureEvents FailureType=%s PartnerId=%s (+ All aggregate)",
                failure_type, partner)


def put_partner_breach_metrics(partner_id, partner_name, in_breach):
    dimensions = [
        {"Name": "PartnerId", "Value": str(partner_id)},
        {"Name": "PartnerName", "Value": str(partner_name)},
    ]
    put_dashboard_metric("PartnerNotInBreach", 0 if in_breach else 1,
                         dimensions=dimensions, namespace="HIE/PartnerMonitoring")


def put_breach_event_metric(partner_id):
    """BreachEvents per PartnerId and as "All", matching the dashboard Breach Status widget."""
    for pid in (str(partner_id), "All"):
        put_dashboard_metric(
            "BreachEvents",
            1,
            dimensions=[{"Name": "PartnerId", "Value": pid}],
            namespace="HIE/PartnerMonitoring",
        )


# ---- CRUD OPERATION TRACKING ----
# _last_crud_operation is set just BEFORE a database step and cleared (None) right
# AFTER it succeeds. So if an error happens, it names the step that actually failed,
# and an error outside a tracked step is never blamed on an earlier Read or Update.
# It is also reset at the end of every invocation (finally in lambda_handler).
_last_crud_operation = None


def track_crud(operation):
    global _last_crud_operation
    _last_crud_operation = operation


# Errors that count as a failed database step (CRUDFailureEvents) when they happen
# during a tracked Read / Update / Commit:
#   * pg8000.Error          - all pg8000 errors, incl. DatabaseError and InterfaceError
#   * pg8000.InterfaceError - listed explicitly for clarity (it is a pg8000.Error)
#   * OSError               - network / socket errors raised by a query, incl.
#                             BrokenPipeError, ConnectionResetError and socket timeouts
CRUD_ERROR_TYPES = (pg8000.Error, pg8000.InterfaceError, OSError)


def commit_tracked(conn):
    """Commit as its own tracked step, so a commit error is reported as "Commit"."""
    track_crud("Commit")
    conn.commit()
    track_crud(None)


def publish_db_connection_exhausted():
    """The database could not be reached after all retries. Published with the same
    FunctionName + FailureType=DatabaseConnectionFailure dimensions as RetryEvents,
    so dashboards and alarms can filter it the same way."""
    put_dashboard_metric(
        "DatabaseConnectionFailures",
        dimensions=[
            {"Name": "FunctionName", "Value": FUNCTION_NAME},
            {"Name": "FailureType", "Value": "DatabaseConnectionFailure"},
        ],
    )


def lambda_handler(event, context):
    conn = None
    cur = None
    # Partner context for CRUD failure metrics. Set to the partner_id BEFORE each
    # partner's queries. It stays None for the Step 1 query, which reads all partners
    # (no single partner), so that failure is published as PartnerId="Unknown" + "All".
    current_partner_id = None
    # Set ONLY when get_conn() itself fails. "conn is None" is not used for this,
    # because conn is also None for failures that happen before get_conn() runs.
    connection_failed = False
    track_crud(None)

    try:
        current_time = datetime.now(timezone.utc)

        try:
            conn = get_conn()
        except Exception:
            connection_failed = True
            raise
        cur = conn.cursor()

        logger.info("Connected to database successfully")

        # =========================
        # Step 1: Get all partners
        # =========================
        track_crud("Read")
        cur.execute("""
            SELECT
                ps.partner_id,
                ps.expected_interval_seconds,
                ps.breach_flag,
                ps.last_breach_email_sent_at,
                pcd.partner_name
            FROM partner_schedule ps
            LEFT JOIN partner_contact_details pcd
                ON ps.partner_id = pcd.partner_id
        """)

        schedule_rows = cur.fetchall()
        track_crud(None)  # Read succeeded
        logger.info("Found %s partner schedule rows", len(schedule_rows))

        # =========================
        # Step 2: Loop through each partner
        # =========================
        for (
            partner_id,
            expected_interval_seconds,
            breach_flag,
            last_breach_email_sent_at,
            partner_name
        ) in schedule_rows:

            # Set the partner context first, before any query for this partner
            current_partner_id = partner_id

            logger.info(
                "Processing partner_id=%s partner_name=%s",
                partner_id,
                partner_name
            )

            if partner_name is None:
                partner_name = partner_id

            # =========================
            # Step 3: Get last payload timestamp
            # =========================
            track_crud("Read")
            cur.execute("""
                SELECT last_seen_at
                FROM partner_transmission_state
                WHERE partner_id = %s
            """, (partner_id,))

            transmission_row = cur.fetchone()
            track_crud(None)  # Read succeeded

            if not transmission_row:
                logger.info("No transmission row found for partner_id=%s", partner_id)
                continue

            last_seen_at = transmission_row[0]

            if last_seen_at is None:
                logger.info("last_seen_at is null for partner_id=%s", partner_id)
                continue

            # Ensure timezone
            if last_seen_at.tzinfo is None:
                last_seen_at = last_seen_at.replace(tzinfo=timezone.utc)

            if last_breach_email_sent_at and last_breach_email_sent_at.tzinfo is None:
                last_breach_email_sent_at = last_breach_email_sent_at.replace(
                    tzinfo=timezone.utc
                )

            # =========================
            # Step 4: Calculate elapsed time
            # =========================
            elapsed_seconds = (current_time - last_seen_at).total_seconds()
            last_seen_duration = format_last_seen_duration(elapsed_seconds)

            logger.info(
                "partner_id=%s elapsed_seconds=%s last_seen_duration=%s expected_interval_seconds=%s",
                partner_id,
                elapsed_seconds,
                last_seen_duration,
                expected_interval_seconds
            )

            # =========================
            # Step 5: BREACH LOGIC
            # =========================
            if elapsed_seconds > expected_interval_seconds:
                logger.info(
                    "MonitoringSummary partner_name=%s FilesIn=%d BreachEvents=%d PartnerSubmissionStatus=%s BreachFlag=%s",
                    str(partner_name).replace(" ", "_"),
                    0,
                    1,
                    "INBREACH",
                    "TRUE"
                )

                publish_partner_metric(partner_name, "PartnerInBreach", 1)
                publish_partner_metric(partner_name, "BreachEvents", 1)
                put_partner_breach_metrics(partner_id, partner_name, True)
                put_breach_event_metric(partner_id)

                should_send_email = False

                # First breach -> send immediately
                if last_breach_email_sent_at is None:
                    should_send_email = True
                    logger.info("First breach detected for partner_id=%s", partner_id)
                else:
                    resend_elapsed_seconds = (
                        current_time - last_breach_email_sent_at
                    ).total_seconds()

                    logger.info(
                        "partner_id=%s resend_elapsed_seconds=%s EMAIL_RESEND_INTERVAL_SECONDS=%s",
                        partner_id,
                        resend_elapsed_seconds,
                        EMAIL_RESEND_INTERVAL_SECONDS
                    )

                    # Only resend after configured interval
                    if resend_elapsed_seconds >= EMAIL_RESEND_INTERVAL_SECONDS:
                        should_send_email = True

                if should_send_email:
                    subject = SNS_SUBJECT_TEMPLATE.replace(
                        "$PARTNER_NAME",
                        str(partner_name)
                    ).replace(
                        "$ENVIRONMENT",
                        os.environ["ENVIRONMENT"]
                    )

                    message = SNS_MESSAGE_TEMPLATE.replace(
                        "$PARTNER_NAME",
                        str(partner_name)
                    ).replace(
                        "$LAST_SEEN_TIME",
                        last_seen_duration
                    ) + "\n\n" + WARNING_INFO

                    logger.info(
                        "Sending SNS for partner_id=%s subject=%s",
                        partner_id,
                        subject
                    )

                    sns.publish(
                        TopicArn=SNS_TOPIC_ARN,
                        Subject=subject,
                        Message=message
                    )

                    track_crud("Update")
                    cur.execute("""
                        UPDATE partner_schedule
                        SET breach_flag = TRUE,
                            last_breach_email_sent_at = %s
                        WHERE partner_id = %s
                    """, (current_time, partner_id))
                    track_crud(None)  # Update succeeded
                    commit_tracked(conn)

                    logger.info(
                        "Updated breach_flag=TRUE and last_breach_email_sent_at for partner_id=%s",
                        partner_id
                    )

                else:
                    # Keep breach_flag true even if not sending email again yet
                    if not breach_flag:
                        track_crud("Update")
                        cur.execute("""
                            UPDATE partner_schedule
                            SET breach_flag = TRUE
                            WHERE partner_id = %s
                        """, (partner_id,))
                        track_crud(None)  # Update succeeded
                        commit_tracked(conn)

                        logger.info(
                            "Updated breach_flag=TRUE without SNS for partner_id=%s",
                            partner_id
                        )

            # =========================
            # Step 6: RECOVERY LOGIC
            # =========================
            else:
                publish_partner_metric(partner_name, "PartnerInBreach", 0)
                put_partner_breach_metrics(partner_id, partner_name, False)

                if breach_flag:
                    track_crud("Update")
                    cur.execute("""
                        UPDATE partner_schedule
                        SET breach_flag = FALSE
                        WHERE partner_id = %s
                    """, (partner_id,))
                    track_crud(None)  # Update succeeded
                    commit_tracked(conn)

                    logger.info(
                        "Recovered partner_id=%s and reset breach_flag=FALSE",
                        partner_id
                    )

                    publish_partner_metric(partner_name, "PartnerRecovered", 1)

        return {
            "statusCode": 200,
            "body": json.dumps("Lambda execution completed")
        }

    except Exception as e:
        logger.exception("Lambda execution failed")

        if connection_failed:
            publish_db_connection_exhausted()
        elif isinstance(e, CRUD_ERROR_TYPES) and _last_crud_operation:
            put_crud_failure_metrics(_last_crud_operation, current_partner_id)

        if conn:
            try:
                conn.rollback()  # not tracked: a rollback error is never reported as a CRUD step
            except Exception as rollback_error:
                logger.warning("Rollback failed: %s", str(rollback_error))

        return {
            "statusCode": 500,
            "body": json.dumps(f"Lambda execution failed: {str(e)}")
        }

    finally:
        track_crud(None)  # reset for the next invocation (warm Lambda reuses globals)
        if cur:
            cur.close()
        if conn:
            conn.close()
            
