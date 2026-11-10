"""
Shared event schemas and validation for CommercePulse.

This module defines the event contract used by:
- Kafka producers
- PySpark Structured Streaming
- Data quality validation
- Recovery and replay processes
"""

from datetime import datetime, timezone
from enum import Enum
import math
from uuid import NAMESPACE_URL, uuid5


class EventType(str, Enum):
    """Supported CommercePulse event types."""

    ORDER = "order"
    PAYMENT = "payment"


class EventValidationError(ValueError):
    """Raised when an event does not satisfy the event contract."""


def _validate_timestamp(value: object, field_name: str) -> None:
    """Validate an ISO 8601 timestamp."""

    if not isinstance(value, str) or not value.strip():
        raise EventValidationError(
            f"{field_name} must be a non-empty timestamp string."
        )

    try:
        datetime.fromisoformat(
            value.strip().replace("Z", "+00:00")
        )
    except ValueError as exc:
        raise EventValidationError(
            f"{field_name} must be a valid ISO 8601 timestamp."
        ) from exc


def _require_string(data: dict, field_name: str) -> str:
    """Require a non-empty string field."""

    value = data.get(field_name)

    if not isinstance(value, str) or not value.strip():
        raise EventValidationError(
            f"'{field_name}' must be a non-empty string."
        )

    return value.strip()


def _require_integer(
    data: dict,
    field_name: str,
    minimum: int,
) -> int:
    """Require a genuine integer, excluding booleans."""

    value = data.get(field_name)

    if isinstance(value, bool) or not isinstance(value, int):
        raise EventValidationError(
            f"'{field_name}' must be an integer."
        )

    if value < minimum:
        raise EventValidationError(
            f"'{field_name}' must be at least {minimum}."
        )

    return value


def _validate_order(data: dict) -> None:
    """Validate an order event payload."""

    _require_string(data, "order_id")
    _require_string(data, "customer_id")


def _validate_payment(data: dict) -> None:
    """Validate a payment event payload."""

    _require_string(data, "order_id")
    _require_string(data, "payment_type")

    _require_integer(
        data,
        field_name="payment_sequential",
        minimum=1,
    )

    _require_integer(
        data,
        field_name="payment_installments",
        minimum=0,
    )

    payment_value = data.get("payment_value")

    if (
        isinstance(payment_value, bool)
        or not isinstance(payment_value, (int, float))
    ):
        raise EventValidationError(
            "'payment_value' must be a numeric value."
        )

    if not math.isfinite(payment_value):
        raise EventValidationError(
            "'payment_value' must be finite."
        )

    if payment_value < 0:
        raise EventValidationError(
            "'payment_value' cannot be negative."
        )


def _generate_event_id(
    event_type: EventType,
    data: dict,
) -> str:
    """Generate a deterministic event ID."""

    if event_type == EventType.ORDER:
        business_key = _require_string(data, "order_id")

    elif event_type == EventType.PAYMENT:
        order_id = _require_string(data, "order_id")

        payment_sequential = _require_integer(
            data,
            field_name="payment_sequential",
            minimum=1,
        )

        business_key = f"{order_id}:{payment_sequential}"

    else:
        raise EventValidationError(
            f"Unsupported event type: {event_type}"
        )

    identity = f"commercepulse:{event_type.value}:{business_key}"

    return str(uuid5(NAMESPACE_URL, identity))


def validate_event(event: dict) -> None:
    """Validate a complete CommercePulse event envelope."""

    if not isinstance(event, dict):
        raise EventValidationError(
            "Event must be a dictionary."
        )

    try:
        event_type = EventType(event.get("event_type"))
    except (TypeError, ValueError) as exc:
        raise EventValidationError(
            f"Unsupported event type: {event.get('event_type')}"
        ) from exc

    _require_string(event, "event_id")

    event_time = event.get("event_time")

    if event_type == EventType.ORDER:
        _validate_timestamp(event_time, "event_time")

    elif event_type == EventType.PAYMENT:
        if event_time is not None:
            _validate_timestamp(event_time, "event_time")

        event_time_source = event.get("event_time_source")

        if event_time_source not in (
            "order_purchase_timestamp_proxy",
            "unavailable",
        ):
            raise EventValidationError(
                "Payment event_time_source must be "
                "'order_purchase_timestamp_proxy' or 'unavailable'."
            )

        if event_time is None and event_time_source != "unavailable":
            raise EventValidationError(
                "Missing payment event_time must have "
                "event_time_source='unavailable'."
            )

        if (
            event_time is not None
            and event_time_source != "order_purchase_timestamp_proxy"
        ):
            raise EventValidationError(
                "A payment with an event_time must identify "
                "its source as 'order_purchase_timestamp_proxy'."
            )

    _validate_timestamp(
        event.get("processing_time"),
        "processing_time",
    )

    data = event.get("data")

    if not isinstance(data, dict):
        raise EventValidationError(
            "'data' must be a dictionary."
        )

    if event_type == EventType.ORDER:
        _validate_order(data)

    elif event_type == EventType.PAYMENT:
        _validate_payment(data)


def create_event_envelope(
    event_type: EventType,
    event_time: str | None,
    data: dict,
    event_time_source: str | None = None,
) -> dict:
    """Create and validate a complete event envelope."""

    try:
        event_type = EventType(event_type)
    except (TypeError, ValueError) as exc:
        raise EventValidationError(
            f"Unsupported event type: {event_type}"
        ) from exc

    if not isinstance(data, dict):
        raise EventValidationError(
            "'data' must be a dictionary."
        )

    if event_type == EventType.ORDER:
        _validate_order(data)

        if event_time_source is not None:
            raise EventValidationError(
                "event_time_source is not currently supported for orders."
            )

    elif event_type == EventType.PAYMENT:
        _validate_payment(data)

        if event_time_source is None:
            raise EventValidationError(
                "Payment events require event_time_source."
            )

    event = {
        "event_id": _generate_event_id(event_type, data),
        "event_type": event_type.value,
        "event_time": event_time,
        "processing_time": datetime.now(
            timezone.utc
        ).isoformat(),
        "data": data,
    }

    if event_type == EventType.PAYMENT:
        event["event_time_source"] = event_time_source

    validate_event(event)

    return event