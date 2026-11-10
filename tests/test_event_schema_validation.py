"""
Dedicated validation tests for CommercePulse event schemas.
"""

import unittest

from src.common.schemas import (
    EventType,
    EventValidationError,
    create_event_envelope,
    validate_event,
)


class TestEventSchemaValidation(unittest.TestCase):

    def setUp(self):
        self.order_data = {
            "order_id": "TEST-ORDER-001",
            "customer_id": "TEST-CUSTOMER-001",
            "order_status": "delivered",
        }

        self.payment_data = {
            "order_id": "TEST-ORDER-001",
            "payment_sequential": 1,
            "payment_type": "credit_card",
            "payment_installments": 2,
            "payment_value": 150.75,
        }

        self.event_time = "2017-11-18T19:28:06"

        self.payment_timestamp_source = (
            "order_purchase_timestamp_proxy"
        )

    # ==========================================
    # 1. Event type validation
    # ==========================================

    def test_invalid_event_type_is_rejected(self):
        with self.assertRaises(EventValidationError):
            create_event_envelope(
                event_type=None,
                event_time=self.event_time,
                data=self.order_data,
            )

    # ==========================================
    # 2. Payment field validation
    # ==========================================

    def test_invalid_payment_sequence_is_rejected(self):

        for invalid_value in [0, -1, 1.5, True, "1"]:

            with self.subTest(value=invalid_value):

                data = self.payment_data.copy()
                data["payment_sequential"] = invalid_value

                with self.assertRaises(EventValidationError):
                    create_event_envelope(
                        EventType.PAYMENT,
                        self.event_time,
                        data,
                        event_time_source=self.payment_timestamp_source,
                    )

    def test_invalid_installments_are_rejected(self):

        for invalid_value in [-1, 1.5, True, "2"]:

            with self.subTest(value=invalid_value):

                data = self.payment_data.copy()
                data["payment_installments"] = invalid_value

                with self.assertRaises(EventValidationError):
                    create_event_envelope(
                        EventType.PAYMENT,
                        self.event_time,
                        data,
                        event_time_source=self.payment_timestamp_source,
                    )

    def test_zero_installments_are_accepted(self):

        data = self.payment_data.copy()
        data["payment_installments"] = 0

        event = create_event_envelope(
            EventType.PAYMENT,
            self.event_time,
            data,
            event_time_source=self.payment_timestamp_source,
        )

        self.assertEqual(
            event["data"]["payment_installments"],
            0,
        )

    def test_invalid_payment_values_are_rejected(self):

        invalid_values = [
            -10,
            float("nan"),
            float("inf"),
            float("-inf"),
            True,
            "150.75",
            None,
        ]

        for invalid_value in invalid_values:

            with self.subTest(value=invalid_value):

                data = self.payment_data.copy()
                data["payment_value"] = invalid_value

                with self.assertRaises(EventValidationError):
                    create_event_envelope(
                        EventType.PAYMENT,
                        self.event_time,
                        data,
                        event_time_source=self.payment_timestamp_source,
                    )

    def test_valid_payment_value_is_accepted(self):

        data = self.payment_data.copy()
        data["payment_value"] = 150.75

        event = create_event_envelope(
            EventType.PAYMENT,
            self.event_time,
            data,
            event_time_source=self.payment_timestamp_source,
        )

        self.assertEqual(
            event["data"]["payment_value"],
            150.75,
        )

    # ==========================================
    # 3. Deterministic event IDs
    # ==========================================

    def test_event_id_is_stable_for_same_order(self):

        first = create_event_envelope(
            EventType.ORDER,
            self.event_time,
            self.order_data,
        )

        second = create_event_envelope(
            EventType.ORDER,
            self.event_time,
            self.order_data,
        )

        self.assertEqual(
            first["event_id"],
            second["event_id"],
        )

    def test_event_id_is_stable_for_same_payment(self):

        first = create_event_envelope(
            EventType.PAYMENT,
            self.event_time,
            self.payment_data,
            event_time_source=self.payment_timestamp_source,
        )

        second = create_event_envelope(
            EventType.PAYMENT,
            self.event_time,
            self.payment_data,
            event_time_source=self.payment_timestamp_source,
        )

        self.assertEqual(
            first["event_id"],
            second["event_id"],
        )

    def test_different_payments_have_different_event_ids(self):

        first = create_event_envelope(
            EventType.PAYMENT,
            self.event_time,
            self.payment_data,
            event_time_source=self.payment_timestamp_source,
        )

        different_payment = self.payment_data.copy()
        different_payment["payment_sequential"] = 2

        second = create_event_envelope(
            EventType.PAYMENT,
            self.event_time,
            different_payment,
            event_time_source=self.payment_timestamp_source,
        )

        self.assertNotEqual(
            first["event_id"],
            second["event_id"],
        )

    # ==========================================
    # 4. Event envelope validation
    # ==========================================

    def test_missing_event_id_is_rejected(self):

        event = create_event_envelope(
            EventType.ORDER,
            self.event_time,
            self.order_data,
        )

        event.pop("event_id")

        with self.assertRaises(EventValidationError):
            validate_event(event)

    def test_invalid_timestamp_is_rejected(self):

        with self.assertRaises(EventValidationError):
            create_event_envelope(
                EventType.ORDER,
                "not-a-valid-timestamp",
                self.order_data,
            )

    # ==========================================
    # 5. Payment timestamp source validation
    # ==========================================

    def test_payment_without_order_timestamp_is_accepted(self):

        event = create_event_envelope(
            EventType.PAYMENT,
            None,
            self.payment_data,
            event_time_source="unavailable",
        )

        self.assertIsNone(event["event_time"])

        self.assertEqual(
            event["event_time_source"],
            "unavailable",
        )

        self.assertIsNotNone(
            event["processing_time"]
        )

    def test_payment_with_order_timestamp_proxy_is_accepted(self):

        event = create_event_envelope(
            EventType.PAYMENT,
            self.event_time,
            self.payment_data,
            event_time_source="order_purchase_timestamp_proxy",
        )

        self.assertEqual(
            event["event_time"],
            self.event_time,
        )

        self.assertEqual(
            event["event_time_source"],
            "order_purchase_timestamp_proxy",
        )

    def test_payment_without_timestamp_and_wrong_source_is_rejected(self):

        with self.assertRaises(EventValidationError):

            create_event_envelope(
                EventType.PAYMENT,
                None,
                self.payment_data,
                event_time_source="order_purchase_timestamp_proxy",
            )

    def test_payment_with_timestamp_and_unavailable_source_is_rejected(self):

        with self.assertRaises(EventValidationError):

            create_event_envelope(
                EventType.PAYMENT,
                self.event_time,
                self.payment_data,
                event_time_source="unavailable",
            )

    def test_payment_without_timestamp_source_is_rejected(self):

        with self.assertRaises(EventValidationError):

            create_event_envelope(
                EventType.PAYMENT,
                None,
                self.payment_data,
            )

if __name__ == "__main__":
    unittest.main(verbosity=2)
