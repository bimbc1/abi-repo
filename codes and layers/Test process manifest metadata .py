"""Unit tests for the Transfer Family user lookup in process_manifest_metadata.py
(is_close_line_for_path, transfer_user_from_text_line, transfer_user_from_stream,
find_transfer_family_user). Covers both Transfer Family log formats (JSON and text).

No AWS access is needed: boto3, psycopg2 and retry_utils are replaced with fakes.
Run from the folder that contains process_manifest_metadata.py:
    python test_process_manifest_metadata.py -v
"""
import importlib.util
import json
import os
import sys
import types
import unittest
from unittest import mock

# ---- Fake modules and environment, so the Lambda file can be imported locally ----
fake_boto3 = types.ModuleType("boto3")
fake_boto3.client = lambda *args, **kwargs: mock.MagicMock()
sys.modules["boto3"] = fake_boto3

fake_psycopg2 = types.ModuleType("psycopg2")
fake_psycopg2.Error = type("Error", (Exception,), {})
fake_psycopg2.connect = mock.MagicMock()
sys.modules["psycopg2"] = fake_psycopg2

fake_retry_utils = types.ModuleType("retry_utils")
fake_retry_utils.handle_retry = lambda error, event: True
sys.modules["retry_utils"] = fake_retry_utils

os.environ.setdefault("METADATA_QUEUE_URL", "https://sqs.example/queue")
os.environ.setdefault("DB_SECRET_ARN", "arn:aws-us-gov:secretsmanager:us-gov-west-1:123:secret:test")
os.environ.setdefault("DB_HOST", "db.example")

HERE = os.path.dirname(os.path.abspath(__file__))
spec = importlib.util.spec_from_file_location(
    "process_manifest_metadata", os.path.join(HERE, "process_manifest_metadata.py")
)
pmm = importlib.util.module_from_spec(spec)
spec.loader.exec_module(pmm)

BUCKET = "partner-bucket"
KEY = "BATCH01_Received_Report.txt"
PATH = f"/{BUCKET}/{KEY}"
STREAM = "Oracle.a1b2c3d4e5f6a7b8"


def json_line(activity, path, user="Oracle"):
    return json.dumps({"activity-type": activity, "path": path, "user": user, "bytes-in": 100})


def text_line(activity, path, user="Oracle"):
    return f"{user}.a1b2c3d4e5f6a7b8 {activity} Path={path} BytesIn=100"


class IsCloseLineForPathTests(unittest.TestCase):
    # ---- JSON format ----
    def test_json_close_exact_path_matches(self):
        self.assertTrue(pmm.is_close_line_for_path(json_line("CLOSE", PATH), PATH))

    def test_json_close_other_path_does_not_match(self):
        self.assertFalse(pmm.is_close_line_for_path(json_line("CLOSE", PATH + ".bak"), PATH))

    def test_json_partial_close_does_not_match(self):
        self.assertFalse(pmm.is_close_line_for_path(json_line("PARTIAL_CLOSE", PATH), PATH))

    def test_json_open_does_not_match(self):
        self.assertFalse(pmm.is_close_line_for_path(json_line("OPEN", PATH), PATH))

    # ---- text format ----
    def test_text_close_exact_path_matches(self):
        self.assertTrue(pmm.is_close_line_for_path(text_line("CLOSE", PATH), PATH))

    def test_text_close_at_end_of_line_matches(self):
        line = f"Oracle.a1b2c3d4e5f6a7b8 CLOSE Path={PATH}"
        self.assertTrue(pmm.is_close_line_for_path(line, PATH))

    def test_text_close_path_with_space_matches(self):
        path = f"/{BUCKET}/my file.txt"
        self.assertTrue(pmm.is_close_line_for_path(text_line("CLOSE", path), path))

    def test_text_close_longer_path_does_not_match(self):
        # Same start, longer name: must not be treated as the same file
        self.assertFalse(pmm.is_close_line_for_path(text_line("CLOSE", PATH + ".bak"), PATH))

    def test_text_close_other_bucket_does_not_match(self):
        self.assertFalse(pmm.is_close_line_for_path(text_line("CLOSE", "/other-bucket/" + KEY), PATH))

    def test_text_partial_close_does_not_match(self):
        self.assertFalse(pmm.is_close_line_for_path(text_line("PARTIAL_CLOSE", PATH), PATH))

    def test_text_close_word_not_near_start_does_not_match(self):
        # "CLOSE" only inside the path, not as the activity
        line = f"Oracle.a1b2c3d4e5f6a7b8 OPEN Mode=READ Path=/{BUCKET}/CLOSE {PATH}"
        self.assertFalse(pmm.is_close_line_for_path(line, PATH))

    def test_empty_or_none_message(self):
        self.assertFalse(pmm.is_close_line_for_path("", PATH))
        self.assertFalse(pmm.is_close_line_for_path(None, PATH))


class TransferUserHelperTests(unittest.TestCase):
    def test_user_from_stream_name(self):
        self.assertEqual(pmm.transfer_user_from_stream(STREAM), "Oracle")

    def test_user_from_stream_name_with_prefix(self):
        self.assertEqual(pmm.transfer_user_from_stream("ERRORS/" + STREAM), "Oracle")

    def test_user_from_text_line(self):
        self.assertEqual(pmm.transfer_user_from_text_line(text_line("CLOSE", PATH)), "Oracle")

    def test_no_user_from_json_line(self):
        self.assertIsNone(pmm.transfer_user_from_text_line(json_line("CLOSE", PATH)))

    def test_no_user_from_empty_line(self):
        self.assertIsNone(pmm.transfer_user_from_text_line(""))


class FindTransferFamilyUserTests(unittest.TestCase):
    def setUp(self):
        self.logs = mock.MagicMock()
        self.patches = [
            mock.patch.object(pmm, "logs", self.logs),
            mock.patch.object(pmm, "_transfer_log_groups", ["/aws/transfer/s-123"]),
            mock.patch.object(pmm.time, "sleep"),
        ]
        for p in self.patches:
            p.start()
        self.sleep = pmm.time.sleep

    def tearDown(self):
        for p in reversed(self.patches):
            p.stop()

    def test_filter_pattern_is_plain_text_close_and_path(self):
        self.logs.filter_log_events.return_value = {"events": []}
        pmm.find_transfer_family_user(BUCKET, KEY)
        kwargs = self.logs.filter_log_events.call_args.kwargs
        self.assertEqual(kwargs["filterPattern"], f'"CLOSE" "{PATH}"')
        self.assertEqual(kwargs["logGroupName"], "/aws/transfer/s-123")

    def test_json_event_returns_user_from_stream(self):
        self.logs.filter_log_events.return_value = {
            "events": [{"logStreamName": STREAM, "message": json_line("CLOSE", PATH)}]
        }
        self.assertEqual(pmm.find_transfer_family_user(BUCKET, KEY), "Oracle")

    def test_text_event_returns_user(self):
        self.logs.filter_log_events.return_value = {
            "events": [{"logStreamName": STREAM, "message": text_line("CLOSE", PATH)}]
        }
        self.assertEqual(pmm.find_transfer_family_user(BUCKET, KEY), "Oracle")

    def test_text_event_without_stream_name_uses_line(self):
        self.logs.filter_log_events.return_value = {
            "events": [{"message": text_line("CLOSE", PATH, user="Cerner")}]
        }
        self.assertEqual(pmm.find_transfer_family_user(BUCKET, KEY), "Cerner")

    def test_wrong_events_are_skipped_and_right_one_is_used(self):
        self.logs.filter_log_events.return_value = {"events": [
            {"logStreamName": "Wrong1.a1b2c3d4e5f6a7b8", "message": text_line("PARTIAL_CLOSE", PATH, "Wrong1")},
            {"logStreamName": "Wrong2.a1b2c3d4e5f6a7b8", "message": text_line("CLOSE", PATH + ".bak", "Wrong2")},
            {"logStreamName": STREAM, "message": json_line("CLOSE", PATH)},
        ]}
        self.assertEqual(pmm.find_transfer_family_user(BUCKET, KEY), "Oracle")

    def test_match_on_second_page(self):
        self.logs.filter_log_events.side_effect = [
            {"events": [], "nextToken": "page2"},
            {"events": [{"logStreamName": STREAM, "message": text_line("CLOSE", PATH)}]},
        ]
        self.assertEqual(pmm.find_transfer_family_user(BUCKET, KEY), "Oracle")
        self.assertEqual(self.logs.filter_log_events.call_args.kwargs["nextToken"], "page2")

    def test_not_found_retries_then_returns_none(self):
        self.logs.filter_log_events.return_value = {"events": []}
        self.assertIsNone(pmm.find_transfer_family_user(BUCKET, KEY))
        self.assertEqual(self.logs.filter_log_events.call_count, pmm.TRANSFER_LOG_ATTEMPTS)
        self.assertEqual(self.sleep.call_count, pmm.TRANSFER_LOG_ATTEMPTS - 1)

    def test_log_line_arrives_on_second_attempt(self):
        self.logs.filter_log_events.side_effect = [
            {"events": []},
            {"events": [{"logStreamName": STREAM, "message": json_line("CLOSE", PATH)}]},
        ]
        self.assertEqual(pmm.find_transfer_family_user(BUCKET, KEY), "Oracle")
        self.assertEqual(self.sleep.call_count, 1)

    def test_only_wrong_events_returns_none(self):
        self.logs.filter_log_events.return_value = {"events": [
            {"logStreamName": STREAM, "message": json_line("CLOSE", "/other-bucket/" + KEY)},
        ]}
        self.assertIsNone(pmm.find_transfer_family_user(BUCKET, KEY))


if __name__ == "__main__":
    unittest.main()