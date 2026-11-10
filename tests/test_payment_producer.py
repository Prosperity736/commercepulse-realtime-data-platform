"""
Unit tests for the CommercePulse payment producer.

Kafka publishing is mocked.
No real Kafka messages are produced during these tests.
"""

import tempfile
import unittest
from pathlib import Path
from unittest.mock import MagicMock, patch

import pandas as pd

from src.common.schemas import EventValidationError
from src.producer.payment_producer import (
    load_payments,
    load_orders,
    enrich_payments,
    create_payment_event,
    publish_payments,
)


class TestPaymentProducer(unittest.TestCase):

    def setUp(self):

        self.temp_dir = tempfile.TemporaryDirectory()
        self.temp_path = Path(self.temp_dir.name)

        self.payment_data = {
            "order_id": "ORDER-001",
            "payment_sequential": 1,
            "payment_type": "credit_card",
            "payment_installments": 2,
            "payment_value": 150.75,
        }

        self.order_data = {
            "order_id": "ORDER-001",
            "order_purchase_timestamp": "2017-11-18 19:28:06",
        }

        self.payments_df = pd.DataFrame([
            self.payment_data,
            {
                "order_id": "ORDER-002",
                "payment_sequential": 1,
                "payment_type": "boleto",
                "payment_installments": 1,
                "payment_value": 200.00,
            },
        ])

        self.orders_df = pd.DataFrame([
            self.order_data,
        ])

    def tearDown(self):
        self.temp_dir.cleanup()

    # ==========================================
    # 1. Dataset loading tests
    # ==========================================

    def test_load_payments_successfully(self):

        file_path = self.temp_path / "payments.csv"

        self.payments_df.to_csv(
            file_path,
            index=False,
        )

        result = load_payments(file_path)

        self.assertEqual(len(result), 2)
        self.assertIn("order_id", result.columns)

    def test_load_payments_missing_file(self):

        file_path = self.temp_path / "missing.csv"

        with self.assertRaises(FileNotFoundError):
            load_payments(file_path)

    def test_load_payments_missing_columns(self):

        file_path = self.temp_path / "invalid_payments.csv"

        pd.DataFrame({
            "order_id": ["ORDER-001"],
        }).to_csv(file_path, index=False)

        with self.assertRaises(ValueError):
            load_payments(file_path)

    def test_load_orders_successfully(self):

        file_path = self.temp_path / "orders.csv"

        self.orders_df.to_csv(
            file_path,
            index=False,
        )

        result = load_orders(file_path)

        self.assertEqual(len(result), 1)
        self.assertIn(
            "order_purchase_timestamp",
            result.columns,
        )

    def test_load_orders_missing_file(self):

        file_path = self.temp_path / "missing_orders.csv"

        with self.assertRaises(FileNotFoundError):
            load_orders(file_path)

    def test_load_orders_duplicate_ids_are_rejected(self):

        file_path = self.temp_path / "duplicate_orders.csv"

        duplicate_orders = pd.DataFrame([
            self.order_data,
            self.order_data,
        ])

        duplicate_orders.to_csv(
            file_path,
            index=False,
        )

        with self.assertRaises(ValueError):
            load_orders(file_path)

    # ==========================================
    # 2. Payment enrichment tests
    # ==========================================

    def test_enrich_payments_preserves_unmatched_records(self):

        result = enrich_payments(
            self.payments_df,
            self.orders_df,
        )

        self.assertEqual(
            len(result),
            len(self.payments_df),
        )

        self.assertEqual(
            result["order_match_status"].tolist(),
            ["matched", "unmatched"],
        )

    def test_enrich_payments_identifies_missing_timestamps(self):

        orders = pd.DataFrame([
            {
                "order_id": "ORDER-001",
                "order_purchase_timestamp": None,
            },
        ])

        result = enrich_payments(
            self.payments_df.iloc[[0]],
            orders,
        )

        self.assertTrue(
            pd.isna(
                result.iloc[0]["order_purchase_timestamp"]
            )
        )

        self.assertEqual(
            result.iloc[0]["order_match_status"],
            "matched",
        )

    def test_enrich_payments_rejects_duplicate_order_lookup(self):

        duplicate_orders = pd.DataFrame([
            self.order_data,
            self.order_data,
        ])

        with self.assertRaises(ValueError):
            enrich_payments(
                self.payments_df,
                duplicate_orders,
            )

    # ==========================================
    # 3. Payment event creation tests
    # ==========================================

    def test_create_valid_payment_event(self):

        payment = {
            **self.payment_data,
            **self.order_data,
            "order_match_status": "matched",
        }

        event = create_payment_event(payment)

        self.assertEqual(
            event["event_type"],
            "payment",
        )

        self.assertEqual(
            event["data"]["order_id"],
            "ORDER-001",
        )

        self.assertEqual(
            event["event_time_source"],
            "order_purchase_timestamp_proxy",
        )

        self.assertIsNotNone(event["event_id"])

    def test_payment_event_id_is_deterministic(self):

        payment = {
            **self.payment_data,
            **self.order_data,
            "order_match_status": "matched",
        }

        first = create_payment_event(payment)
        second = create_payment_event(payment)

        self.assertEqual(
            first["event_id"],
            second["event_id"],
        )

    def test_unmatched_payment_event_is_preserved(self):

        payment = {
            **self.payment_data,
            "order_purchase_timestamp": None,
            "order_match_status": "unmatched",
        }

        event = create_payment_event(payment)

        self.assertIsNone(
            event["event_time"],
        )

        self.assertEqual(
            event["event_time_source"],
            "unavailable",
        )

        self.assertEqual(
            event["data"]["order_match_status"],
            "unmatched",
        )

    def test_matched_order_without_timestamp_is_preserved(self):

        payment = {
            **self.payment_data,
            "order_purchase_timestamp": None,
            "order_match_status": "matched",
        }

        event = create_payment_event(payment)

        self.assertIsNone(
            event["event_time"],
        )

        self.assertEqual(
            event["event_time_source"],
            "unavailable",
        )

    def test_invalid_payment_sequence_is_rejected(self):

        payment = {
            **self.payment_data,
            **self.order_data,
            "order_match_status": "matched",
        }

        payment["payment_sequential"] = 0

        with self.assertRaises(EventValidationError):
            create_payment_event(payment)

    def test_invalid_payment_value_is_rejected(self):

        payment = {
            **self.payment_data,
            **self.order_data,
            "order_match_status": "matched",
        }

        payment["payment_value"] = -100

        with self.assertRaises(EventValidationError):
            create_payment_event(payment)

    def test_missing_order_id_is_rejected(self):

        payment = {
            **self.payment_data,
            **self.order_data,
            "order_match_status": "matched",
        }

        payment["order_id"] = None

        with self.assertRaises(EventValidationError):
            create_payment_event(payment)

    def test_fractional_payment_sequence_is_rejected(self):

        payment = {
            **self.payment_data,
            **self.order_data,
            "order_match_status": "matched",
        }

        payment["payment_sequential"] = 1.5

        with self.assertRaises(EventValidationError):
            create_payment_event(payment)

    # ==========================================
    # 4. Publishing tests
    # ==========================================

    @patch("src.producer.payment_producer.load_orders")
    @patch("src.producer.payment_producer.load_payments")
    @patch("src.producer.payment_producer.CommercePulseKafkaProducer")
    @patch("src.producer.payment_producer.get_settings")
    def test_successful_payment_publishing(
        self,
        mock_settings,
        mock_producer_class,
        mock_load_payments,
        mock_load_orders,
    ):

        mock_settings.return_value.payments_topic = "olist-payments"

        mock_load_payments.return_value = self.payments_df
        mock_load_orders.return_value = self.orders_df

        mock_producer = MagicMock()
        mock_producer.publish.return_value = True

        mock_producer_class.return_value = mock_producer

        summary = publish_payments(
            delay_seconds=0,
            progress_interval=100,
        )

        self.assertEqual(
            summary["source_records"],
            2,
        )

        self.assertEqual(
            summary["published"],
            2,
        )

        self.assertEqual(
            summary["unmatched_orders"],
            1,
        )

        self.assertEqual(
            summary["failed"],
            0,
        )

        self.assertEqual(
            summary["invalid"],
            0,
        )

        self.assertEqual(
            mock_producer.publish.call_count,
            2,
        )

        mock_producer.close.assert_called_once()

    @patch("src.producer.payment_producer.load_orders")
    @patch("src.producer.payment_producer.load_payments")
    @patch("src.producer.payment_producer.CommercePulseKafkaProducer")
    @patch("src.producer.payment_producer.get_settings")
    def test_failed_payment_delivery_is_counted(
        self,
        mock_settings,
        mock_producer_class,
        mock_load_payments,
        mock_load_orders,
    ):

        mock_settings.return_value.payments_topic = "olist-payments"

        mock_load_payments.return_value = self.payments_df
        mock_load_orders.return_value = self.orders_df

        mock_producer = MagicMock()
        mock_producer.publish.return_value = False

        mock_producer_class.return_value = mock_producer

        summary = publish_payments()

        self.assertEqual(
            summary["failed"],
            2,
        )

        self.assertEqual(
            summary["published"],
            0,
        )

        mock_producer.close.assert_called_once()

    @patch("src.producer.payment_producer.load_orders")
    @patch("src.producer.payment_producer.load_payments")
    @patch("src.producer.payment_producer.CommercePulseKafkaProducer")
    @patch("src.producer.payment_producer.get_settings")
    def test_invalid_payment_is_counted_separately(
        self,
        mock_settings,
        mock_producer_class,
        mock_load_payments,
        mock_load_orders,
    ):

        invalid_payments = self.payments_df.copy()

        invalid_payments.loc[
            0,
            "payment_value",
        ] = -100

        mock_settings.return_value.payments_topic = "olist-payments"

        mock_load_payments.return_value = invalid_payments
        mock_load_orders.return_value = self.orders_df

        mock_producer = MagicMock()
        mock_producer.publish.return_value = True

        mock_producer_class.return_value = mock_producer

        summary = publish_payments()

        self.assertEqual(
            summary["invalid"],
            1,
        )

        self.assertEqual(
            summary["published"],
            1,
        )

        self.assertEqual(
            summary["source_records"],
            2,
        )

    # ==========================================
    # 5. Configuration validation
    # ==========================================

    def test_negative_delay_is_rejected(self):

        with self.assertRaises(ValueError):
            publish_payments(delay_seconds=-1)

    def test_zero_progress_interval_is_rejected(self):

        with self.assertRaises(ValueError):
            publish_payments(progress_interval=0)

if __name__ == "__main__":
    unittest.main(verbosity=2)