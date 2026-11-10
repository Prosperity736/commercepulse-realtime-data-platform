"""
CommercePulse Orders Producer.

Reads historical Olist order records, validates them,
creates standardized event envelopes and publishes them
to the configured Kafka orders topic.

Failed producer events are preserved in PostgreSQL
for independent recovery.

The publishing ledger prevents unnecessary republication
of events already acknowledged by Kafka.
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
from src.producer.producer_recovery import preserve_failed_producer_event

from src.producer.publishing_ledger import (
    get_published_event_ids,
    mark_event_pending,
    mark_event_published,
    mark_event_failed,
)


logger = get_logger(__name__)


PROJECT_ROOT = Path(__file__).resolve().parents[2]

ORDERS_FILE = (
    PROJECT_ROOT
    / "data"
    / "raw"
    / "olist_orders_dataset.csv"
)

REQUIRED_COLUMNS = {
    "order_id",
    "customer_id",
    "order_purchase_timestamp",
}


def load_orders(file_path: Path) -> pd.DataFrame:
    """Load and validate the source orders dataset."""

    if not file_path.exists():
        raise FileNotFoundError(
            f"Orders dataset not found: {file_path}"
        )

    logger.info("Loading orders dataset: %s", file_path.name)

    orders = pd.read_csv(
        file_path,
        dtype={
            "order_id": "string",
            "customer_id": "string",
        },
    )

    missing_columns = REQUIRED_COLUMNS - set(orders.columns)

    if missing_columns:
        raise ValueError(
            f"Orders dataset is missing required columns: "
            f"{sorted(missing_columns)}"
        )

    logger.info(
        "Orders dataset loaded | Records: %s",
        f"{len(orders):,}",
    )

    return orders


def create_order_event(order: dict) -> dict:
    """Transform a source order into a validated event."""

    order_id = order.get("order_id")

    event_time = pd.to_datetime(
        order.get("order_purchase_timestamp"),
        errors="coerce",
    )

    if pd.isna(event_time):
        raise EventValidationError(
            f"Invalid purchase timestamp for order: {order_id}"
        )

    event_time = event_time.isoformat()

    cleaned_data = {
        key: None if pd.isna(value) else value
        for key, value in order.items()
    }

    return create_event_envelope(
        event_type=EventType.ORDER,
        event_time=event_time,
        data=cleaned_data,
    )


def _preserve_failed_order(
    event: dict,
    source_file: str,
    failure_reason: str,
    summary: dict,
) -> None:
    """Attempt to preserve a failed order for recovery."""

    try:
        saved = preserve_failed_producer_event(
            event=event,
            source_file=source_file,
            failure_reason=failure_reason,
        )

        if saved:
            summary["recovery_saved"] += 1

        else:
            summary["recovery_not_updated"] += 1

            logger.warning(
                "Order recovery record was not updated | Order ID: %s",
                event.get("data", {}).get("order_id"),
            )

    except Exception:
        summary["recovery_storage_failed"] += 1

        logger.exception(
            "Could not preserve failed order | Order ID: %s",
            event.get("data", {}).get("order_id"),
        )


def publish_orders(
    delay_seconds: float = 0,
    progress_interval: int = 1000,
) -> dict:
    """
    Publish validated orders to Kafka.

    Previously published events are skipped.

    Successful Kafka deliveries are recorded in the
    PostgreSQL publishing ledger.

    Failed Kafka deliveries are preserved in PostgreSQL
    for subsequent recovery by the recovery worker.
    """

    if delay_seconds < 0:
        raise ValueError("Delay cannot be negative.")

    if progress_interval < 1:
        raise ValueError("Progress interval must be positive.")

    settings = get_settings()

    orders = load_orders(ORDERS_FILE)

    logger.info(
        "Checking publishing ledger for previously published orders."
    )

    published_event_ids = get_published_event_ids(
        EventType.ORDER.value
    )

    logger.info(
        "Previously published orders found: %s",
        f"{len(published_event_ids):,}",
    )

    producer = CommercePulseKafkaProducer()

    summary = {
        "source_records": len(orders),
        "published": 0,
        "failed": 0,
        "skipped": 0,
        "already_published": 0,
        "recovery_saved": 0,
        "recovery_not_updated": 0,
        "recovery_storage_failed": 0,
    }

    try:

        for row in orders.itertuples(index=False):

            order = row._asdict()
            event = None

            try:

                event = create_order_event(order)

                event_id = event["event_id"]
                order_id = event["data"]["order_id"]

                # Skip events already acknowledged by Kafka.
                if event_id in published_event_ids:

                    summary["already_published"] += 1

                    continue

                # Record the event before attempting publication.
                mark_event_pending(
                    event_id=event_id,
                    event_type=EventType.ORDER.value,
                    business_key=order_id,
                    topic=settings.orders_topic,
                )

                delivered = producer.publish(
                    topic=settings.orders_topic,
                    event=event,
                    message_key=order_id,
                )

                if delivered:

                    # Mark as published only after Kafka acknowledgement.
                    mark_event_published(event_id)

                    published_event_ids.add(event_id)

                    summary["published"] += 1

                else:

                    mark_event_failed(event_id)

                    summary["failed"] += 1

                    _preserve_failed_order(
                        event=event,
                        source_file=ORDERS_FILE.name,
                        failure_reason=(
                            "Kafka delivery was not acknowledged."
                        ),
                        summary=summary,
                    )

            except EventValidationError as exc:

                summary["skipped"] += 1

                logger.error(
                    "Order rejected | Order ID: %s | Reason: %s",
                    order.get("order_id"),
                    exc,
                )

            except Exception as exc:

                summary["failed"] += 1

                logger.exception(
                    "Unexpected order processing failure | Order ID: %s",
                    order.get("order_id"),
                )

                if event is not None:

                    try:
                        mark_event_failed(event["event_id"])

                    except Exception:
                        logger.exception(
                            "Could not update publishing ledger | Event ID: %s",
                            event.get("event_id"),
                        )

                    _preserve_failed_order(
                        event=event,
                        source_file=ORDERS_FILE.name,
                        failure_reason=(
                            f"Unexpected order processing failure: {exc}"
                        ),
                        summary=summary,
                    )

            processed = (
                summary["published"]
                + summary["failed"]
                + summary["skipped"]
                + summary["already_published"]
            )

            if processed % progress_interval == 0:

                logger.info(
                    "Order producer progress | Processed: %s/%s "
                    "| Published: %s | Failed: %s | Skipped: %s "
                    "| Already published: %s | Recovery saved: %s",
                    f"{processed:,}",
                    f"{summary['source_records']:,}",
                    f"{summary['published']:,}",
                    f"{summary['failed']:,}",
                    f"{summary['skipped']:,}",
                    f"{summary['already_published']:,}",
                    f"{summary['recovery_saved']:,}",
                )

            if delay_seconds > 0:
                time.sleep(delay_seconds)

    finally:

        producer.close()

    logger.info(
        "ORDER PRODUCER COMPLETED | Source: %s | Published: %s "
        "| Failed: %s | Skipped: %s | Already published: %s "
        "| Recovery saved: %s | Recovery not updated: %s "
        "| Recovery storage failures: %s",
        f"{summary['source_records']:,}",
        f"{summary['published']:,}",
        f"{summary['failed']:,}",
        f"{summary['skipped']:,}",
        f"{summary['already_published']:,}",
        f"{summary['recovery_saved']:,}",
        f"{summary['recovery_not_updated']:,}",
        f"{summary['recovery_storage_failed']:,}",
    )

    return summary


if __name__ == "__main__":

    # Historical replay mode: no artificial delay.
    result = publish_orders(
        delay_seconds=0,
        progress_interval=1000,
    )

    print("\nORDER PRODUCER SUMMARY")

    for key, value in result.items():
        print(f"{key.replace('_', ' ').title():30}: {value:,}")
