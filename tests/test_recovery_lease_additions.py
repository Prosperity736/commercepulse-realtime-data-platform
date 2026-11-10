"""
Additional tests for lease ownership behaviour.

Keep the existing 19 regression tests. Add these tests to the existing
tests/test_producer_recovery_worker.py file or merge the cases into the
corresponding test classes.
"""

import unittest
from unittest.mock import patch
from uuid import uuid4

from src.producer.producer_recovery_worker import (
    ClaimHeartbeat,
    claim_failed_events,
    mark_event_resolved,
    mark_event_rejected,
    release_claims,
    renew_claims,
    schedule_retry,
)


class TestRecoveryLeaseOwnership(unittest.TestCase):

    @patch("src.producer.producer_recovery_worker.get_db_connection")
    def test_claim_query_assigns_token_and_lease(self, mock_connection):
        token = uuid4()
        cursor = mock_connection.return_value.__enter__.return_value.cursor.return_value.__enter__.return_value
        cursor.description = [
            type("Column", (), {"name": "event_id"})(),
            type("Column", (), {"name": "event_type"})(),
            type("Column", (), {"name": "raw_payload"})(),
            type("Column", (), {"name": "retry_count"})(),
            type("Column", (), {"name": "claim_token"})(),
        ]
        cursor.fetchall.return_value = []

        result = claim_failed_events(batch_size=10, claim_token=token)

        self.assertEqual(result, [])
        sql = cursor.execute.call_args.args[0]
        self.assertIn("claim_expires_at", sql)
        self.assertIn("FOR UPDATE SKIP LOCKED", sql)
        self.assertIn("claim_token = %s", sql)
        self.assertEqual(cursor.execute.call_args.args[1], (10, token, 5))

    @patch("src.producer.producer_recovery_worker.get_db_connection")
    def test_renewal_is_scoped_to_unexpired_owned_claim(self, mock_connection):
        token = uuid4()
        cursor = mock_connection.return_value.__enter__.return_value.cursor.return_value.__enter__.return_value
        cursor.fetchall.return_value = [("event-1",)]

        self.assertEqual(renew_claims(token), 1)
        sql = cursor.execute.call_args.args[0]
        self.assertIn("claim_token = %s", sql)
        self.assertIn("claim_expires_at > NOW()", sql)

    @patch("src.producer.producer_recovery_worker.get_db_connection")
    def test_resolve_clears_lease_and_requires_owner(self, mock_connection):
        token = uuid4()
        cursor = mock_connection.return_value.__enter__.return_value.cursor.return_value.__enter__.return_value
        cursor.fetchone.return_value = ("event-1",)

        mark_event_resolved(uuid4(), token)
        sql = cursor.execute.call_args.args[0]
        self.assertIn("claim_token = NULL", sql)
        self.assertIn("claim_expires_at = NULL", sql)
        self.assertIn("claim_token = %s", sql)
        self.assertIn("claim_expires_at > NOW()", sql)

    @patch("src.producer.producer_recovery_worker.get_db_connection")
    def test_rejected_event_clears_lease(self, mock_connection):
        token = uuid4()
        cursor = mock_connection.return_value.__enter__.return_value.cursor.return_value.__enter__.return_value
        cursor.fetchone.return_value = ("event-1",)

        mark_event_rejected(uuid4(), "invalid payload", token)
        sql = cursor.execute.call_args.args[0]
        self.assertIn("claim_token = NULL", sql)
        self.assertIn("claim_expires_at = NULL", sql)

    @patch("src.producer.producer_recovery_worker.get_db_connection")
    def test_retry_clears_lease_and_increments_retry_count(self, mock_connection):
        token = uuid4()
        cursor = mock_connection.return_value.__enter__.return_value.cursor.return_value.__enter__.return_value
        cursor.fetchone.return_value = ("event-1",)

        schedule_retry(uuid4(), 0, "Kafka unavailable", token)
        sql = cursor.execute.call_args.args[0]
        self.assertIn("claim_token = NULL", sql)
        self.assertIn("claim_expires_at = NULL", sql)
        self.assertIn("retry_count = %s", sql)

    @patch("src.producer.producer_recovery_worker.get_db_connection")
    def test_release_only_affects_events_owned_by_token(self, mock_connection):
        token = uuid4()
        cursor = mock_connection.return_value.__enter__.return_value.cursor.return_value.__enter__.return_value
        cursor.fetchall.return_value = [("event-1",)]

        self.assertEqual(release_claims(token, "startup failed"), 1)
        sql = cursor.execute.call_args.args[0]
        self.assertIn("claim_token = %s", sql)
        self.assertIn("resolution_status = 'retrying'", sql)
        self.assertIn("claim_token = NULL", sql)

    def test_heartbeat_rejects_nonpositive_interval(self):
        with self.assertRaises(ValueError):
            ClaimHeartbeat(uuid4(), interval_seconds=0)


if __name__ == "__main__":
    unittest.main()
