import importlib.util
import json
import os
import sys
import types
import unittest

# ---- Fake modules (must exist before the Lambda file is imported) ----
os.environ.setdefault("DB_SECRET_ARN", "test-secret")
os.environ.setdefault("DB_HOST", "test-host")
os.environ.setdefault("DB_NAME", "test-db")
os.environ.setdefault("METRIC_NAMESPACE", "HIE/OperationalMonitoring")
os.environ.setdefault("AWS_LAMBDA_FUNCTION_NAME", "database-processor-test")

PUBLISHED = []  # every metric the Lambda sends: (MetricName, {dimension: value})


class FakeCloudWatch:
    def put_metric_data(self, Namespace, MetricData):
        for m in MetricData:
            dims = {d["Name"]: d["Value"] for d in m.get("Dimensions", [])}
            PUBLISHED.append((m["MetricName"], dims))


fake_boto3 = types.ModuleType("boto3")
fake_boto3.client = lambda name, **kwargs: FakeCloudWatch() if name == "cloudwatch" else object()
sys.modules["boto3"] = fake_boto3


class FakeDatabaseError(Exception):
    """Stands in for pg8000.exceptions.DatabaseError (a failed query)."""


class FakeInterfaceError(Exception):
    """Stands in for pg8000.exceptions.InterfaceError (a lost connection)."""


fake_pg8000 = types.ModuleType("pg8000")
fake_pg8000.exceptions = types.SimpleNamespace(
    DatabaseError=FakeDatabaseError, InterfaceError=FakeInterfaceError
)
sys.modules["pg8000"] = fake_pg8000


def fake_retry_with_backoff(operation, max_attempts, retry_delay_seconds, return_bool):
    last_error = None
    for _ in range(max_attempts):
        try:
            result = operation()
            return True if return_bool else result
        except Exception as e:  # noqa: BLE001 - mirrors a generic retry helper
            last_error = e
    raise last_error


fake_retry_utils = types.ModuleType("retry_utils")
fake_retry_utils.retry_with_backoff = fake_retry_with_backoff
sys.modules["retry_utils"] = fake_retry_utils

# ---- Load database_processing_lambda.py from the same folder as this test ----
_HERE = os.path.dirname(os.path.abspath(__file__))
_spec = importlib.util.spec_from_file_location(
    "database_processing_lambda", os.path.join(_HERE, "database_processing_lambda.py")
)
dpl = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(dpl)


class ClassifyFailureTests(unittest.TestCase):
    """One test per case in the classify_failure() rules."""

    def test_read_step_is_labelled_read(self):
        self.assertEqual(dpl.classify_failure(FakeDatabaseError("select failed"), "Read"), "Read")

    def test_create_step_is_labelled_create(self):
        self.assertEqual(dpl.classify_failure(FakeDatabaseError("insert failed"), "Create"), "Create")

    def test_commit_step_is_labelled_commit(self):
        self.assertEqual(dpl.classify_failure(FakeDatabaseError("commit failed"), "Commit"), "Commit")

    def test_step_carried_on_the_exception_is_used(self):
        error = FakeDatabaseError("insert failed")
        error.crud_operation = "Create"
        self.assertEqual(dpl.classify_failure(error), "Create")

    def test_value_error_with_no_step_is_validation(self):
        self.assertEqual(dpl.classify_failure(ValueError("partner_id missing")), "Validation")

    def test_value_error_during_a_step_keeps_the_step(self):
        self.assertEqual(dpl.classify_failure(ValueError("bad value"), "Create"), "Create")

    def test_database_error_with_no_step_is_other(self):
        self.assertEqual(dpl.classify_failure(FakeDatabaseError("db error")), "Other")

    def test_any_other_error_is_other(self):
        self.assertEqual(dpl.classify_failure(KeyError("missing key")), "Other")

    def test_connection_error_is_not_a_crud_failure(self):
        self.assertIsNone(dpl.classify_failure(FakeInterfaceError("connection lost")))
        self.assertIsNone(dpl.classify_failure(dpl.DatabaseConnectionError("db unreachable")))


class FakeCursor:
    def __init__(self, fail_on=None):
        self.fail_on = fail_on  # "SELECT" or "INSERT"

    def execute(self, sql, params=None):
        if self.fail_on and sql.strip().upper().startswith(self.fail_on):
            raise FakeDatabaseError(f"{self.fail_on} failed")

    def fetchone(self):
        return (0,)


class FakeConnection:
    def __init__(self, fail_on=None, commit_fails=False):
        self.fail_on = fail_on
        self.commit_fails = commit_fails

    def cursor(self):
        return FakeCursor(self.fail_on)

    def commit(self):
        if self.commit_fails:
            raise FakeDatabaseError("commit failed")

    def rollback(self):
        pass

    def close(self):
        pass


MANIFEST_MESSAGE = {
    "message_type": "MANIFEST_METADATA_UPSERT",
    "partner": {"partner_id": 7, "partner_name": "Test Partner"},
    "source": {"batch_id": "B1"},
    "files": {"manifest": {"file_name": "B1_manifest.txt"}},
    "counts": {},
}


class PublishedCrudFailureTests(unittest.TestCase):
    """End to end: a failed message publishes exactly one labelled CRUDFailureEvents (+ "All")."""

    def setUp(self):
        PUBLISHED.clear()
        dpl._conn_failed_this_invocation = False

    def crud_events(self):
        return [(d["FailureType"], d["PartnerId"]) for name, d in PUBLISHED if name == "CRUDFailureEvents"]

    def run_message(self, message, connection):
        dpl.conn = connection
        return dpl.handle_one_message("msg-1", json.dumps(message))

    def test_missing_partner_id_publishes_validation(self):
        message = dict(MANIFEST_MESSAGE, partner={})
        self.assertFalse(self.run_message(message, FakeConnection()))
        self.assertEqual(self.crud_events(), [("Validation", "Unknown"), ("Validation", "All")])

    def test_failed_read_publishes_read(self):
        self.assertFalse(self.run_message(MANIFEST_MESSAGE, FakeConnection(fail_on="SELECT")))
        self.assertEqual(self.crud_events(), [("Read", "7"), ("Read", "All")])

    def test_failed_insert_publishes_create(self):
        self.assertFalse(self.run_message(MANIFEST_MESSAGE, FakeConnection(fail_on="INSERT")))
        self.assertEqual(self.crud_events(), [("Create", "7"), ("Create", "All")])

    def test_failed_commit_publishes_commit(self):
        self.assertFalse(self.run_message(MANIFEST_MESSAGE, FakeConnection(commit_fails=True)))
        self.assertEqual(self.crud_events(), [("Commit", "7"), ("Commit", "All")])

    def test_unsupported_message_type_is_not_a_crud_failure(self):
        message = dict(MANIFEST_MESSAGE, message_type="SOMETHING_NEW")
        self.assertFalse(self.run_message(message, FakeConnection()))
        self.assertEqual(self.crud_events(), [])

    def test_success_publishes_no_crud_failure(self):
        self.assertTrue(self.run_message(MANIFEST_MESSAGE, FakeConnection()))
        self.assertEqual(self.crud_events(), [])


if __name__ == "__main__":
    unittest.main()