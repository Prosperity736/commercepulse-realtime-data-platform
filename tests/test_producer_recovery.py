"""
Unit tests for CommercePulse producer-side recovery.

All PostgreSQL interactions are mocked.
No real database records are created or modified.
"""

import json
import unittest
from pathlib import Path
from unittest.mock import MagicMock, patch
from uuid import UUID

from src.producer.producer_recovery import (
    preserve_failed_producer_event,
)


class TestProducerRecovery(unittest.TestCase):

    def setUp(self):

        self.event = {
            "event_id": "123e4567-e89b-12d3-a456-426614174000",
            "event_type": "payment",
            "event_time": None,
            "event_time_source": "unavailable",
            "processing_time": "2026-10-31T10:00:00+00:00",
            "data": {
                "order_id": "ORDER-001",
                "payment_sequential": 1,
                "payment_type": "credit_card",
                "payment_value": 150.75,
            },
        }

        # Mock PostgreSQL connection.
        self.connection = MagicMock()

        # Simulate entering the connection context manager.
        self.connection.__enter__.return_value = self.connection

        # Mock PostgreSQL cursor.
        self.cursor = MagicMock()

        # Simulate entering the cursor context manager.
        self.connection.cursor.return_value.__enter__.return_value = (
            self.cursor
        )

        # Simulate a successful database INSERT/UPDATE.
        self.cursor.fetchone.return_value = (
            UUID(self.event["event_id"]),
        )

    # ==========================================
    # 1. Successful persistence
    # ==========================================

    @patch(
        "src.producer.producer_recovery.get_db_connection"
    )
    def test_preserves_failed_event_successfully(
        self,
        mock_connection,
    ):

        mock_connection.return_value = self.connection

        result = preserve_failed_producer_event(
            event=self.event,
            source_file="olist_order_payments_dataset.csv",
            failure_reason="Kafka delivery failed",
        )

        self.assertTrue(result)

        self.cursor.execute.assert_called_once()

        sql, parameters = self.cursor.execute.call_args.args

        self.assertIn(
            "INSERT INTO producer_failed_events",
            sql,
        )

        self.assertEqual(
            parameters[0],
            UUID(self.event["event_id"]),
        )

        self.assertEqual(
            parameters[1],
            "olist_order_payments_dataset.csv",
        )

        self.assertEqual(
            parameters[2],
            "payment",
        )

        self.assertEqual(
            parameters[3],
            "Kafka delivery failed",
        )

        stored_payload = json.loads(parameters[4])

        self.assertEqual(
            stored_payload["event_id"],
            self.event["event_id"],
        )

    # ==========================================
    # 2. Idempotency and terminal statuses
    # ==========================================

    @patch(
        "src.producer.producer_recovery.get_db_connection"
    )
    def test_duplicate_unresolved_event_can_be_updated(
        self,
        mock_connection,
    ):

        mock_connection.return_value = self.connection

        result = preserve_failed_producer_event(
            event=self.event,
            source_file="payments.csv",
            failure_reason="Delivery timeout",
        )

        self.assertTrue(result)

        sql = self.cursor.execute.call_args.args[0]

        self.assertIn(
            "ON CONFLICT (event_id)",
            sql,
        )

        self.assertIn(
            "WHERE producer_failed_events.resolution_status",
            sql,
        )

    @patch(
        "src.producer.producer_recovery.get_db_connection"
    )
    def test_terminal_event_is_not_reopened(
        self,
        mock_connection,
    ):

        mock_connection.return_value = self.connection

        # No returned row means the database rejected the
        # update because the existing record is terminal.
        self.cursor.fetchone.return_value = None

        result = preserve_failed_producer_event(
            event=self.event,
            source_file="payments.csv",
            failure_reason="Delivery timeout",
        )

        self.assertFalse(result)

        self.cursor.execute.assert_called_once()

    # ==========================================
    # 3. Input validation
    # ==========================================

    def test_non_dictionary_event_is_rejected(self):

        with self.assertRaises(TypeError):

            preserve_failed_producer_event(
                event="invalid",
                source_file="payments.csv",
                failure_reason="Delivery failed",
            )

    def test_missing_event_id_is_rejected(self):

        event = self.event.copy()
        event.pop("event_id")

        with self.assertRaises(ValueError):

            preserve_failed_producer_event(
                event=event,
                source_file="payments.csv",
                failure_reason="Delivery failed",
            )

    def test_invalid_event_id_is_rejected(self):

        event = self.event.copy()
        event["event_id"] = "not-a-uuid"

        with self.assertRaises(ValueError):

            preserve_failed_producer_event(
                event=event,
                source_file="payments.csv",
                failure_reason="Delivery failed",
            )

    def test_invalid_event_type_is_rejected(self):

        event = self.event.copy()
        event["event_type"] = "customer"

        with self.assertRaises(ValueError):

            preserve_failed_producer_event(
                event=event,
                source_file="payments.csv",
                failure_reason="Delivery failed",
            )

    def test_empty_failure_reason_is_rejected(self):

        with self.assertRaises(ValueError):

            preserve_failed_producer_event(
                event=self.event,
                source_file="payments.csv",
                failure_reason=" ",
            )

    def test_empty_source_file_is_rejected(self):

        with self.assertRaises(ValueError):

            preserve_failed_producer_event(
                event=self.event,
                source_file="",
                failure_reason="Delivery failed",
            )

    def test_non_json_serializable_event_is_rejected(self):

        event = self.event.copy()
        event["invalid_value"] = float("nan")

        with self.assertRaises(ValueError):

            preserve_failed_producer_event(
                event=event,
                source_file="payments.csv",
                failure_reason="Delivery failed",
            )

    # ==========================================
    # 4. Source filename handling
    # ==========================================

    @patch(
        "src.producer.producer_recovery.get_db_connection"
    )
    def test_only_source_filename_is_stored(
        self,
        mock_connection,
    ):

        mock_connection.return_value = self.connection

        preserve_failed_producer_event(
            event=self.event,
            source_file=Path(
                "data/raw/olist_order_payments_dataset.csv"
            ),
            failure_reason="Kafka delivery failed",
        )

        parameters = self.cursor.execute.call_args.args[1]

        self.assertEqual(
            parameters[1],
            "olist_order_payments_dataset.csv",
        )

if __name__ == "__main__":
    unittest.main(verbosity=2)