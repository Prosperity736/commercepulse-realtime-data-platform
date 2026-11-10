"""
Unit tests for the CommercePulse Orders Producer.

These tests do not publish messages to Kafka.
"""

import tempfile
import unittest

from pathlib import Path
from unittest.mock import MagicMock, patch

import pandas as pd

from src.common.schemas import EventValidationError
from src.producer.orders_producer import (
    create_order_event,
    load_orders,
    publish_orders,
)


class TestOrdersProducer(unittest.TestCase):

    def setUp(self):
        self.order = {
            "order_id": "TEST-ORDER-001",
            "customer_id": "TEST-CUSTOMER-001",
            "order_status": "delivered",
            "order_purchase_timestamp": "2017-11-18 19:28:06",
        }

    # ----------------------------------------
    # 1. Order event transformation
    # ----------------------------------------

    def test_valid_order_event_creation(self):

        event = create_order_event(self.order)

        self.assertEqual(event["event_type"], "order")
        self.assertEqual(
            event["data"]["order_id"],
            "TEST-ORDER-001",
        )
        self.assertEqual(
            event["data"]["customer_id"],
            "TEST-CUSTOMER-001",
        )

        self.assertIn("event_id", event)

    def test_order_event_id_is_deterministic(self):

        first = create_order_event(self.order)
        second = create_order_event(self.order)

        self.assertEqual(
            first["event_id"],
            second["event_id"],
        )

    def test_invalid_order_timestamp_is_rejected(self):

        invalid_order = self.order.copy()

        invalid_order["order_purchase_timestamp"] = "invalid-date"

        with self.assertRaises(EventValidationError):
            create_order_event(invalid_order)

    def test_missing_order_id_is_rejected(self):

        invalid_order = self.order.copy()
        invalid_order["order_id"] = None

        with self.assertRaises(EventValidationError):
            create_order_event(invalid_order)

    # ----------------------------------------
    # 2. Source dataset loading
    # ----------------------------------------

    def test_missing_source_file_is_rejected(self):

        with self.assertRaises(FileNotFoundError):
            load_orders(
                Path("nonexistent_orders_dataset.csv")
            )

    def test_missing_required_columns_are_rejected(self):

        with tempfile.TemporaryDirectory() as temp_dir:

            file_path = Path(temp_dir) / "orders.csv"

            pd.DataFrame({
                "order_id": ["ORDER-001"],
                "customer_id": ["CUSTOMER-001"],
            }).to_csv(file_path, index=False)

            with self.assertRaises(ValueError):
                load_orders(file_path)

    def test_valid_source_file_loads(self):

        with tempfile.TemporaryDirectory() as temp_dir:

            file_path = Path(temp_dir) / "orders.csv"

            pd.DataFrame({
                "order_id": ["ORDER-001"],
                "customer_id": ["CUSTOMER-001"],
                "order_purchase_timestamp": [
                    "2017-11-18 19:28:06"
                ],
            }).to_csv(file_path, index=False)

            result = load_orders(file_path)

            self.assertEqual(len(result), 1)
            self.assertIn("order_id", result.columns)

    # ----------------------------------------
    # 3. Publishing behaviour
    # ----------------------------------------

    @patch("src.producer.orders_producer.CommercePulseKafkaProducer")
    @patch("src.producer.orders_producer.load_orders")
    @patch("src.producer.orders_producer.get_settings")
    def test_successful_order_publishing(
        self,
        mock_settings,
        mock_load_orders,
        mock_producer_class,
    ):

        mock_settings.return_value.orders_topic = "olist-orders"

        mock_load_orders.return_value = pd.DataFrame([
            self.order
        ])

        mock_producer = mock_producer_class.return_value

        mock_producer.publish.return_value = True

        result = publish_orders(
            delay_seconds=0,
            progress_interval=100,
        )

        self.assertEqual(result["published"], 1)
        self.assertEqual(result["failed"], 0)
        self.assertEqual(result["skipped"], 0)

        mock_producer.publish.assert_called_once()

        mock_producer.close.assert_called_once()

    @patch("src.producer.orders_producer.CommercePulseKafkaProducer")
    @patch("src.producer.orders_producer.load_orders")
    @patch("src.producer.orders_producer.get_settings")
    def test_failed_kafka_delivery_is_counted(
        self,
        mock_settings,
        mock_load_orders,
        mock_producer_class,
    ):

        mock_settings.return_value.orders_topic = "olist-orders"

        mock_load_orders.return_value = pd.DataFrame([
            self.order
        ])

        mock_producer = mock_producer_class.return_value

        mock_producer.publish.return_value = False

        result = publish_orders(
            delay_seconds=0,
            progress_interval=100,
        )

        self.assertEqual(result["published"], 0)
        self.assertEqual(result["failed"], 1)

    @patch("src.producer.orders_producer.CommercePulseKafkaProducer")
    @patch("src.producer.orders_producer.load_orders")
    @patch("src.producer.orders_producer.get_settings")
    def test_invalid_order_is_counted_as_skipped(
        self,
        mock_settings,
        mock_load_orders,
        mock_producer_class,
    ):

        mock_settings.return_value.orders_topic = "olist-orders"

        invalid_order = self.order.copy()
        invalid_order["order_purchase_timestamp"] = "invalid-date"

        mock_load_orders.return_value = pd.DataFrame([
            invalid_order
        ])

        result = publish_orders(
            delay_seconds=0,
            progress_interval=100,
        )

        self.assertEqual(result["published"], 0)
        self.assertEqual(result["failed"], 0)
        self.assertEqual(result["skipped"], 1)

        mock_producer_class.return_value.publish.assert_not_called()

    def test_negative_delay_is_rejected(self):

        with self.assertRaises(ValueError):
            publish_orders(delay_seconds=-1)

    def test_invalid_progress_interval_is_rejected(self):

        with self.assertRaises(ValueError):
            publish_orders(progress_interval=0)


if __name__ == "__main__":
    unittest.main(verbosity=2)