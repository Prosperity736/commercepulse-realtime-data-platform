"""
Unit tests for CommercePulse shared Kafka producer.
"""

import unittest
from unittest.mock import MagicMock, patch
from types import SimpleNamespace

from src.producer.kafka_producer import CommercePulseKafkaProducer


class TestCommercePulseKafkaProducer(unittest.TestCase):

    def setUp(self):
        self.settings = SimpleNamespace(
            kafka_bootstrap_servers="localhost:19092"
        )

        self.kafka_patcher = patch(
            "src.producer.kafka_producer.KafkaProducer"
        )

        self.settings_patcher = patch(
            "src.producer.kafka_producer.get_settings",
            return_value=self.settings,
        )

        self.mock_kafka_class = self.kafka_patcher.start()
        self.settings_patcher.start()

        self.addCleanup(self.kafka_patcher.stop)
        self.addCleanup(self.settings_patcher.stop)

        self.mock_producer = self.mock_kafka_class.return_value

        self.producer = CommercePulseKafkaProducer()

        self.sample_event = {
            "event_id": "test-event-001",
            "event_type": "order",
            "event_time": "2017-11-18T19:28:06",
            "processing_time": "2026-10-31T12:00:00+00:00",
            "data": {
                "order_id": "TEST-ORDER-001",
                "customer_id": "TEST-CUSTOMER-001",
            },
        }

    def test_producer_initializes_successfully(self):

        self.mock_kafka_class.assert_called_once()

        self.assertIsNotNone(self.producer.producer)

    def test_successful_event_delivery(self):

        metadata = SimpleNamespace(
            topic="olist-orders",
            partition=0,
            offset=10,
        )

        future = MagicMock()
        future.get.return_value = metadata

        self.mock_producer.send.return_value = future

        result = self.producer.publish(
            topic="olist-orders",
            event=self.sample_event,
            message_key="TEST-ORDER-001",
        )

        self.assertTrue(result)

        self.mock_producer.send.assert_called_once_with(
            topic="olist-orders",
            key="TEST-ORDER-001",
            value=self.sample_event,
        )

    def test_failed_event_delivery(self):

        future = MagicMock()
        future.get.side_effect = TimeoutError(
            "Simulated delivery timeout"
        )

        self.mock_producer.send.return_value = future

        result = self.producer.publish(
            topic="olist-orders",
            event=self.sample_event,
            message_key="TEST-ORDER-001",
        )

        self.assertFalse(result)

    def test_empty_topic_is_rejected(self):

        with self.assertRaises(ValueError):

            self.producer.publish(
                topic="",
                event=self.sample_event,
                message_key="TEST-ORDER-001",
            )

    def test_empty_message_key_is_rejected(self):

        with self.assertRaises(ValueError):

            self.producer.publish(
                topic="olist-orders",
                event=self.sample_event,
                message_key="",
            )

    def test_producer_closes_successfully(self):

        self.producer.close()

        self.mock_producer.flush.assert_called_once()
        self.mock_producer.close.assert_called_once()


if __name__ == "__main__":
    unittest.main(verbosity=2)