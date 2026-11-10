"""
Unit tests for the CommercePulse Producer Recovery Worker.

All Kafka and PostgreSQL interactions are mocked.
"""

import unittest
from types import SimpleNamespace
from unittest.mock import Mock, patch
from uuid import UUID

from src.common.schemas import (
    EventType,
    EventValidationError,
    create_event_envelope,
)

from src.producer.producer_recovery_worker import (
    calculate_retry_delay,
    validate_recovery_event,
    verify_deterministic_event_id,
    process_recovery_event,
    run_recovery,
)


class TestRetryDelay(unittest.TestCase):

    def test_first_retry_is_one_minute(self):
        self.assertEqual(calculate_retry_delay(1), 1)

    def test_second_retry_is_two_minutes(self):
        self.assertEqual(calculate_retry_delay(2), 2)

    def test_third_retry_is_four_minutes(self):
        self.assertEqual(calculate_retry_delay(3), 4)

    def test_retry_delay_is_capped_at_sixty_minutes(self):
        self.assertEqual(calculate_retry_delay(7), 60)
        self.assertEqual(calculate_retry_delay(10), 60)

    def test_zero_retry_count_is_rejected(self):
        with self.assertRaises(ValueError):
            calculate_retry_delay(0)


class TestRecoveryEventValidation(unittest.TestCase):

    def setUp(self):

        self.order_event = create_event_envelope(
            event_type=EventType.ORDER,
            event_time="2017-11-18T19:28:06",
            data={
                "order_id": "TEST-ORDER-001",
                "customer_id": "TEST-CUSTOMER-001",
            },
        )

        self.payment_event = create_event_envelope(
            event_type=EventType.PAYMENT,
            event_time=None,
            event_time_source="unavailable",
            data={
                "order_id": "TEST-ORDER-002",
                "payment_sequential": 1,
                "payment_type": "credit_card",
                "payment_installments": 1,
                "payment_value": 100.0,
                "order_match_status": "unmatched",
            },
        )

    def test_valid_order_event_is_accepted(self):

        result = validate_recovery_event(
            self.order_event
        )

        self.assertEqual(
            result["event_type"],
            "order",
        )

    def test_valid_unmatched_payment_is_accepted(self):

        result = validate_recovery_event(
            self.payment_event
        )

        self.assertEqual(
            result["event_type"],
            "payment",
        )

        self.assertIsNone(result["event_time"])

        self.assertEqual(
            result["event_time_source"],
            "unavailable",
        )

    def test_json_string_payload_is_accepted(self):

        import json

        result = validate_recovery_event(
            json.dumps(self.order_event)
        )

        self.assertEqual(
            result["event_id"],
            self.order_event["event_id"],
        )

    def test_non_dictionary_payload_is_rejected(self):

        with self.assertRaises(EventValidationError):
            validate_recovery_event("invalid payload")

    def test_invalid_event_payload_is_rejected(self):

        invalid_event = self.order_event.copy()

        invalid_event["data"] = {
            "order_id": "",
            "customer_id": "TEST-CUSTOMER-001",
        }

        with self.assertRaises(EventValidationError):
            validate_recovery_event(invalid_event)

    def test_malformed_event_id_is_rejected(self):

        invalid_event = self.order_event.copy()
        invalid_event["event_id"] = "not-a-uuid"

        with self.assertRaises(EventValidationError):
            validate_recovery_event(invalid_event)

    def test_incorrect_deterministic_event_id_is_rejected(self):

        invalid_event = self.order_event.copy()

        invalid_event["event_id"] = str(
            UUID("123e4567-e89b-12d3-a456-426614174000")
        )

        with self.assertRaises(EventValidationError):
            verify_deterministic_event_id(invalid_event)


class TestProcessRecoveryEvent(unittest.TestCase):

    def setUp(self):

        self.producer = Mock()

        self.order_event = create_event_envelope(
            event_type=EventType.ORDER,
            event_time="2017-11-18T19:28:06",
            data={
                "order_id": "TEST-ORDER-001",
                "customer_id": "TEST-CUSTOMER-001",
            },
        )

        self.payment_event = create_event_envelope(
            event_type=EventType.PAYMENT,
            event_time=None,
            event_time_source="unavailable",
            data={
                "order_id": "TEST-ORDER-002",
                "payment_sequential": 1,
                "payment_type": "credit_card",
                "payment_installments": 1,
                "payment_value": 100.0,
                "order_match_status": "unmatched",
            },
        )

    def create_record(self, event):
     return {
        "event_id": UUID(event["event_id"]),
        "event_type": event["event_type"],
        "raw_payload": event,
        "retry_count": 0,
        "claim_token": UUID("123e4567-e89b-12d3-a456-426614174000"),
    }

    @patch(
        "src.producer.producer_recovery_worker.mark_event_resolved"
    )
    @patch(
        "src.producer.producer_recovery_worker.get_settings"
    )
    def test_successful_order_recovery(
        self,
        mock_settings,
        mock_resolved,
    ):

        mock_settings.return_value = SimpleNamespace(
            orders_topic="olist-orders",
            payments_topic="olist-payments",
        )

        self.producer.publish.return_value = True

        result = process_recovery_event(
            self.create_record(self.order_event),
            self.producer,
        )

        self.assertEqual(result, "resolved")

        self.producer.publish.assert_called_once_with(
            topic="olist-orders",
            event=self.order_event,
            message_key="TEST-ORDER-001",
        )

        mock_resolved.assert_called_once()

    @patch(
        "src.producer.producer_recovery_worker.mark_event_resolved"
    )
    @patch(
        "src.producer.producer_recovery_worker.get_settings"
    )
    def test_successful_payment_recovery(
        self,
        mock_settings,
        mock_resolved,
    ):

        mock_settings.return_value = SimpleNamespace(
            orders_topic="olist-orders",
            payments_topic="olist-payments",
        )

        self.producer.publish.return_value = True

        result = process_recovery_event(
            self.create_record(self.payment_event),
            self.producer,
        )

        self.assertEqual(result, "resolved")

        self.producer.publish.assert_called_once_with(
            topic="olist-payments",
            event=self.payment_event,
            message_key="TEST-ORDER-002",
        )

        mock_resolved.assert_called_once()

    @patch(
        "src.producer.producer_recovery_worker.schedule_retry"
    )
    @patch(
        "src.producer.producer_recovery_worker.get_settings"
    )
    def test_failed_kafka_delivery_schedules_retry(
        self,
        mock_settings,
        mock_schedule_retry,
    ):

        mock_settings.return_value = SimpleNamespace(
            orders_topic="olist-orders",
            payments_topic="olist-payments",
        )

        self.producer.publish.return_value = False

        result = process_recovery_event(
            self.create_record(self.order_event),
            self.producer,
        )

        self.assertEqual(result, "retried")

        mock_schedule_retry.assert_called_once()

    @patch(
        "src.producer.producer_recovery_worker.mark_event_rejected"
    )
    def test_invalid_event_is_rejected(
        self,
        mock_rejected,
    ):

        invalid_event = self.order_event.copy()

        invalid_event["data"] = {
            "order_id": "",
            "customer_id": "TEST-CUSTOMER-001",
        }

        result = process_recovery_event(
            self.create_record(invalid_event),
            self.producer,
        )

        self.assertEqual(result, "rejected")

        mock_rejected.assert_called_once()

        self.producer.publish.assert_not_called()


class TestRunRecovery(unittest.TestCase):

    def setUp(self):

        self.records = [
            {
                "event_id": UUID(
                    "123e4567-e89b-12d3-a456-426614174001"
                ),
                "event_type": "order",
                "raw_payload": {},
                "retry_count": 0,
            },
            {
                "event_id": UUID(
                    "123e4567-e89b-12d3-a456-426614174002"
                ),
                "event_type": "order",
                "raw_payload": {},
                "retry_count": 0,
            },
            {
                "event_id": UUID(
                    "123e4567-e89b-12d3-a456-426614174003"
                ),
                "event_type": "payment",
                "raw_payload": {},
                "retry_count": 0,
            },
        ]

    @patch(
        "src.producer.producer_recovery_worker.CommercePulseKafkaProducer"
    )
    @patch(
        "src.producer.producer_recovery_worker.process_recovery_event"
    )
    @patch(
        "src.producer.producer_recovery_worker.claim_failed_events"
    )
    def test_unexpected_exception_does_not_stop_batch(
        self,
        mock_claim,
        mock_process,
        mock_producer_class,
    ):

        mock_claim.return_value = self.records

        mock_producer = Mock()
        mock_producer_class.return_value = mock_producer

        mock_process.side_effect = [
            RuntimeError("Unexpected processing failure"),
            "resolved",
            "retried",
        ]

        result = run_recovery(batch_size=3)

        # All three events must be attempted.
        self.assertEqual(
            mock_process.call_count,
            3,
        )

        # Verify that all three events were claimed.
        mock_claim.assert_called_once()

        self.assertEqual(
            mock_claim.call_args.kwargs["batch_size"],
            3,
        )

        self.assertIsInstance(
            mock_claim.call_args.kwargs["claim_token"],
            UUID,
       )

        # Verify the recovery summary.
        self.assertEqual(
            result,
            {
                "claimed": 3,
                "resolved": 1,
                "retried": 1,
                "rejected": 0,
                "errors": 1,
            },
        )

        # Verify Kafka producer is closed.
        mock_producer.close.assert_called_once()

    @patch(
        "src.producer.producer_recovery_worker.claim_failed_events"
    )
    def test_empty_batch_does_not_initialize_kafka_producer(
        self,
        mock_claim,
    ):

        mock_claim.return_value = []

        with patch(
            "src.producer.producer_recovery_worker.CommercePulseKafkaProducer"
        ) as mock_producer_class:

            result = run_recovery(batch_size=100)

        self.assertEqual(
            result,
            {
                "claimed": 0,
                "resolved": 0,
                "retried": 0,
                "rejected": 0,
                "errors": 0,
            },
        )

        mock_producer_class.assert_not_called()

    def test_zero_batch_size_is_rejected(self):

        with self.assertRaises(ValueError):
            run_recovery(batch_size=0)


if __name__ == "__main__":
    unittest.main()