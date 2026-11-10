"""
Integration tests for CommercePulse shared components.
"""

from src.common.config import get_settings
from src.common.logging import get_logger
from src.common.schemas import (
    EventType,
    EventValidationError,
    create_event_envelope,
    validate_event,
)


def main():
    print("\n=== CommercePulse Foundation Tests ===")

    # 1. Configuration
    settings = get_settings()

    assert settings.db_host
    assert settings.db_name
    assert settings.kafka_bootstrap_servers

    print("[PASS] Configuration loaded successfully.")

    # 2. Logging
    logger = get_logger(__name__)

    logger.info("Foundation test logging initialized.")

    print("[PASS] Logging initialized successfully.")

    # 3. Valid order event
    order = create_event_envelope(
        event_type=EventType.ORDER,
        event_time="2017-11-18T19:28:06",
        data={
            "order_id": "TEST-ORDER-001",
            "customer_id": "TEST-CUSTOMER-001",
            "order_status": "delivered",
        },
    )

    validate_event(order)

    print("[PASS] Valid order event accepted.")

    # 4. Valid payment event
    payment = create_event_envelope(
        event_type=EventType.PAYMENT,
        event_time="2017-11-18T19:28:06",
        data={
            "order_id": "TEST-ORDER-001",
            "payment_sequential": 1,
            "payment_type": "credit_card",
            "payment_installments": 2,
            "payment_value": 150.75,
        },
    )

    validate_event(payment)

    print("[PASS] Valid payment event accepted.")

    # 5. Invalid event rejection
    invalid_order = {
        "event_type": "order",
        "event_time": "invalid-date",
        "processing_time": "2026-10-31T08:30:00+00:00",
        "data": {
            "order_id": "TEST-ORDER-002",
            "customer_id": "TEST-CUSTOMER-002",
        },
    }

    try:
        validate_event(invalid_order)
    except EventValidationError:
        print("[PASS] Invalid event correctly rejected.")
    else:
        raise AssertionError("Invalid event was accepted.")

    print("\nAll foundation tests passed!")


if __name__ == "__main__":
    main()