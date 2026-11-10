"""
CommercePulse Payment Event Producer.

Loads Olist payment and order datasets, enriches payment
records with order timestamps, validates events and publishes
them to Kafka.

Failed deliveries are preserved in PostgreSQL for recovery.
The publishing ledger prevents unnecessary republication.
"""

import time
from pathlib import Path

import pandas as pd

from src.common.config import get_settings
from src.common.logging import get_logger
from src.common.schemas import (
    EventType,
    EventValidationError,
    create_event_envelope,
)

from src.producer.kafka_producer import CommercePulseKafkaProducer

from src.producer.producer_recovery import (
    preserve_failed_producer_event,
)

from src.producer.publishing_ledger import (
    get_published_event_ids,
    mark_event_pending,
    mark_event_published,
    mark_event_failed,
)


logger = get_logger(__name__)

PROJECT_ROOT = Path(__file__).resolve().parents[2]

PAYMENTS_FILE = (
    PROJECT_ROOT
    / "data"
    / "raw"
    / "olist_order_payments_dataset.csv"
)

ORDERS_FILE = (
    PROJECT_ROOT
    / "data"
    / "raw"
    / "olist_orders_dataset.csv"
)

PROGRESS_INTERVAL = 1000


# ==========================================
# 1. Load payment dataset
# ==========================================

def load_payments(file_path: Path = PAYMENTS_FILE) -> pd.DataFrame:
    """Load and validate the Olist payment dataset."""

    if not file_path.exists():
        raise FileNotFoundError(
            f"Payment dataset not found: {file_path}"
        )

    payments = pd.read_csv(
        file_path,
        dtype={"order_id": "string"},
    )

    required_columns = {
        "order_id",
        "payment_sequential",
        "payment_type",
        "payment_installments",
        "payment_value",
    }

    missing_columns = required_columns - set(payments.columns)

    if missing_columns:
        raise ValueError(
            "Payment dataset is missing required columns: "
            f"{sorted(missing_columns)}"
        )

    logger.info(
        "Payment dataset loaded | Records: %s",
        f"{len(payments):,}",
    )

    return payments


# ==========================================
# 2. Load order lookup dataset
# ==========================================

def load_orders(file_path: Path = ORDERS_FILE) -> pd.DataFrame:
    """Load the order dataset for timestamp enrichment."""

    if not file_path.exists():
        raise FileNotFoundError(
            f"Orders dataset not found: {file_path}"
        )

    orders = pd.read_csv(
        file_path,
        dtype={"order_id": "string"},
    )

    required_columns = {
        "order_id",
        "order_purchase_timestamp",
    }

    missing_columns = required_columns - set(orders.columns)

    if missing_columns:
        raise ValueError(
            "Orders dataset is missing required columns: "
            f"{sorted(missing_columns)}"
        )

    orders_lookup = orders[
        [
            "order_id",
            "order_purchase_timestamp",
        ]
    ].copy()

    if orders_lookup["order_id"].duplicated().any():
        raise ValueError(
            "Orders dataset contains duplicate order IDs. "
            "Cannot safely perform many-to-one enrichment."
        )

    logger.info(
        "Order lookup loaded | Records: %s",
        f"{len(orders_lookup):,}",
    )

    return orders_lookup


# ==========================================
# 3. Enrich payment records
# ==========================================

def enrich_payments(
    payments: pd.DataFrame,
    orders: pd.DataFrame,
) -> pd.DataFrame:
    """
    Enrich payments with order purchase timestamps.

    LEFT JOIN ensures unmatched payment records are preserved.
    """

    original_count = len(payments)

    payments_enriched = payments.merge(
        orders,
        on="order_id",
        how="left",
        validate="many_to_one",
        indicator=True,
    )

    payments_enriched["order_match_status"] = (
        payments_enriched["_merge"]
        .map({
            "both": "matched",
            "left_only": "unmatched",
        })
    )

    payments_enriched.drop(
        columns=["_merge"],
        inplace=True,
    )

    if len(payments_enriched) != original_count:
        raise RuntimeError(
            "Payment record count changed during enrichment. "
            "Potential payment record loss detected."
        )

    unmatched_count = (
        payments_enriched["order_match_status"]
        .eq("unmatched")
        .sum()
    )

    missing_timestamp_count = (
        payments_enriched["order_purchase_timestamp"]
        .isna()
        .sum()
    )

    logger.info(
        "Payment enrichment completed | "
        "Source records: %s | "
        "Unmatched orders: %s | "
        "Missing order timestamps: %s",
        f"{original_count:,}",
        f"{unmatched_count:,}",
        f"{missing_timestamp_count:,}",
    )

    return payments_enriched


# ==========================================
# 4. Create payment event
# ==========================================

def _convert_integer(
    value: object,
    field_name: str,
) -> int:
    """Convert a valid integer-like CSV value into an integer."""

    if pd.isna(value):
        raise EventValidationError(
            f"'{field_name}' cannot be missing."
        )

    try:
        numeric_value = float(value)

    except (TypeError, ValueError) as exc:
        raise EventValidationError(
            f"'{field_name}' must be numeric."
        ) from exc

    if not numeric_value.is_integer():
        raise EventValidationError(
            f"'{field_name}' must be a whole number."
        )

    return int(numeric_value)


def create_payment_event(payment: dict) -> dict:
    """
    Transform one enriched payment record into a
    validated CommercePulse payment event.

    Matched payments use order_purchase_timestamp as a proxy.

    Unmatched payments are preserved with event_time=None.
    """

    order_id = payment.get("order_id")

    if pd.isna(order_id):
        raise EventValidationError(
            "Payment order_id cannot be missing."
        )

    order_id = str(order_id).strip()

    if not order_id:
        raise EventValidationError(
            "Payment order_id cannot be empty."
        )

    payment_sequential = _convert_integer(
        payment.get("payment_sequential"),
        "payment_sequential",
    )

    payment_installments = _convert_integer(
        payment.get("payment_installments"),
        "payment_installments",
    )

    try:
        payment_value = float(payment.get("payment_value"))

    except (TypeError, ValueError) as exc:
        raise EventValidationError(
            "'payment_value' must be numeric."
        ) from exc

    payment_type = payment.get("payment_type")

    if pd.isna(payment_type):
        raise EventValidationError(
            "'payment_type' cannot be missing."
        )

    payment_type = str(payment_type).strip()

    order_match_status = payment.get(
        "order_match_status",
        "unmatched",
    )

    if order_match_status not in ("matched", "unmatched"):
        raise EventValidationError(
            "Invalid order_match_status."
        )

    order_timestamp = payment.get(
        "order_purchase_timestamp"
    )

    event_time = None
    event_time_source = "unavailable"

    if not pd.isna(order_timestamp):

        parsed_timestamp = pd.to_datetime(
            order_timestamp,
            errors="coerce",
        )

        if pd.isna(parsed_timestamp):
            raise EventValidationError(
                f"Invalid order purchase timestamp for "
                f"payment {order_id}."
            )

        event_time = parsed_timestamp.isoformat()

        event_time_source = (
            "order_purchase_timestamp_proxy"
        )

    payment_data = {
        "order_id": order_id,
        "payment_sequential": payment_sequential,
        "payment_type": payment_type,
        "payment_installments": payment_installments,
        "payment_value": payment_value,
        "order_match_status": order_match_status,
    }

    return create_event_envelope(
        event_type=EventType.PAYMENT,
        event_time=event_time,
        data=payment_data,
        event_time_source=event_time_source,
    )


# ==========================================
# 5. Preserve failed producer event
# ==========================================

def _preserve_failed_payment(
    payment_event: dict,
    failure_reason: str,
) -> str:
    """
    Persist a failed Kafka payment event in PostgreSQL.

    Returns:
        saved
        not_updated
        storage_failed
    """

    try:
        recovery_saved = preserve_failed_producer_event(
            event=payment_event,
            source_file=PAYMENTS_FILE,
            failure_reason=failure_reason,
        )

        if recovery_saved:
            return "saved"

        return "not_updated"

    except Exception:

        logger.critical(
            "PAYMENT RECOVERY STORAGE FAILURE | "
            "Event ID: %s | Order: %s. "
            "Kafka delivery failed and PostgreSQL "
            "could not preserve the event.",
            payment_event.get("event_id"),
            payment_event.get("data", {}).get("order_id"),
            exc_info=True,
        )

        return "storage_failed"


def _update_recovery_counters(
    recovery_result: str,
    summary: dict,
) -> None:
    """Update the appropriate recovery statistics."""

    if recovery_result == "saved":
        summary["recovery_saved"] += 1

    elif recovery_result == "not_updated":
        summary["recovery_not_updated"] += 1

    elif recovery_result == "storage_failed":
        summary["recovery_storage_failed"] += 1


# ==========================================
# 6. Publish payment events
# ==========================================

def publish_payments(
    delay_seconds: float = 0,
    progress_interval: int = PROGRESS_INTERVAL,
) -> dict:
    """
    Publish payment events to Kafka.

    Previously published events are skipped.

    Successful Kafka acknowledgements are recorded
    in the PostgreSQL publishing ledger.

    Failed deliveries are preserved for recovery.
    """

    if delay_seconds < 0:
        raise ValueError(
            "delay_seconds cannot be negative."
        )

    if progress_interval <= 0:
        raise ValueError(
            "progress_interval must be greater than zero."
        )

    settings = get_settings()

    payments = load_payments()
    orders = load_orders()

    payments_enriched = enrich_payments(
        payments,
        orders,
    )

    total_records = len(payments_enriched)

    # Retrieve previously published payment IDs once.
    logger.info(
        "Checking publishing ledger for previously published payments."
    )

    published_event_ids = get_published_event_ids(
        EventType.PAYMENT.value
    )

    logger.info(
        "Previously published payments found: %s",
        f"{len(published_event_ids):,}",
    )

    unmatched_count = int(
        payments_enriched["order_match_status"]
        .eq("unmatched")
        .sum()
    )

    timestamp_unavailable_count = 0

    summary = {
        "source_records": total_records,
        "published": 0,
        "failed": 0,
        "already_published": 0,
        "recovery_saved": 0,
        "recovery_not_updated": 0,
        "recovery_storage_failed": 0,
        "invalid": 0,
        "unmatched_orders": unmatched_count,
        "timestamp_unavailable": 0,
        "topic": settings.payments_topic,
    }

    producer = None

    try:

        producer = CommercePulseKafkaProducer()

        for row_number, payment in enumerate(
            payments_enriched.to_dict(orient="records"),
            start=1,
        ):

            payment_event = None
            ledger_pending = False

            try:

                payment_event = create_payment_event(
                    payment
                )

                if payment_event["event_time"] is None:
                    timestamp_unavailable_count += 1

                event_id = payment_event["event_id"]

                message_key = payment_event["data"]["order_id"]

                payment_sequential = (
                    payment_event["data"]["payment_sequential"]
                )

                business_key = (
                    f"{message_key}:{payment_sequential}"
                )

                # Skip previously acknowledged payments.
                if event_id in published_event_ids:

                    summary["already_published"] += 1

                else:

                    # Record the event before Kafka publication.
                    mark_event_pending(
                        event_id=event_id,
                        event_type=EventType.PAYMENT.value,
                        business_key=business_key,
                        topic=settings.payments_topic,
                    )

                    ledger_pending = True

                    delivered = producer.publish(
                        topic=settings.payments_topic,
                        event=payment_event,
                        message_key=message_key,
                    )

                    if delivered:

                        # Record success only after Kafka acknowledgement.
                        mark_event_published(event_id)

                        published_event_ids.add(event_id)

                        summary["published"] += 1

                    else:

                        mark_event_failed(event_id)

                        ledger_pending = False

                        summary["failed"] += 1

                        recovery_result = _preserve_failed_payment(
                            payment_event=payment_event,
                            failure_reason=(
                                "Kafka producer returned False "
                                "for payment event delivery."
                            ),
                        )

                        _update_recovery_counters(
                            recovery_result,
                            summary,
                        )

            except EventValidationError as exc:

                summary["invalid"] += 1

                logger.error(
                    "Invalid payment record | "
                    "Row: %s | Order: %s | Error: %s",
                    row_number,
                    payment.get("order_id"),
                    exc,
                )

            except Exception as exc:

                summary["failed"] += 1

                logger.exception(
                    "Unexpected payment processing or "
                    "publishing failure | "
                    "Row: %s | Order: %s",
                    row_number,
                    payment.get("order_id"),
                )

                # If the ledger was marked PENDING,
                # attempt to record the failure.
                if ledger_pending and payment_event is not None:

                    try:
                        mark_event_failed(
                            payment_event["event_id"]
                        )

                    except Exception:
                        logger.exception(
                            "Could not update payment publishing ledger "
                            "| Event ID: %s",
                            payment_event["event_id"],
                        )

                # Preserve only successfully created events.
                if payment_event is not None:

                    recovery_result = _preserve_failed_payment(
                        payment_event=payment_event,
                        failure_reason=(
                            f"Unexpected producer failure: "
                            f"{type(exc).__name__}: {exc}"
                        ),
                    )

                    _update_recovery_counters(
                        recovery_result,
                        summary,
                    )

            # Progress reporting.
            if row_number % progress_interval == 0:

                logger.info(
                    "Payment producer progress | "
                    "Processed: %s/%s | "
                    "Published: %s | "
                    "Failed: %s | "
                    "Already published: %s | "
                    "Recovery saved: %s | "
                    "Recovery not updated: %s | "
                    "Recovery storage failures: %s | "
                    "Invalid: %s",
                    f"{row_number:,}",
                    f"{total_records:,}",
                    f"{summary['published']:,}",
                    f"{summary['failed']:,}",
                    f"{summary['already_published']:,}",
                    f"{summary['recovery_saved']:,}",
                    f"{summary['recovery_not_updated']:,}",
                    f"{summary['recovery_storage_failed']:,}",
                    f"{summary['invalid']:,}",
                )

            if delay_seconds > 0:
                time.sleep(delay_seconds)

    except KeyboardInterrupt:

        logger.warning(
            "Payment publishing interrupted by user."
        )

    finally:

        if producer is not None:
            producer.close()

    summary["timestamp_unavailable"] = (
        timestamp_unavailable_count
    )

    logger.info(
        "PAYMENT PRODUCER SUMMARY | %s",
        summary,
    )

    return summary


# ==========================================
# 7. Application entry point
# ==========================================

if __name__ == "__main__":

    publish_payments(
        delay_seconds=0,
        progress_interval=PROGRESS_INTERVAL,
    )
