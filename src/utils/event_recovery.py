import json
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import psycopg
from dotenv import load_dotenv
from kafka import KafkaProducer


# --------------------------------------------------
# 1. Load project configuration
# --------------------------------------------------

PROJECT_ROOT = Path(__file__).resolve().parents[2]

load_dotenv(PROJECT_ROOT / ".env", override=True)

KAFKA_SERVER = os.getenv(
    "KAFKA_BOOTSTRAP_SERVERS",
    "localhost:19092",
)

DEAD_LETTER_TOPIC = "commercepulse-dead-letter"


# --------------------------------------------------
# 2. PostgreSQL connection
# --------------------------------------------------

def get_db_connection():
    """Create a connection to the CommercePulse database."""

    return psycopg.connect(
        host=os.getenv("DB_HOST"),
        port=os.getenv("DB_PORT", "5432"),
        dbname=os.getenv("DB_NAME"),
        user=os.getenv("DB_USER"),
        password=os.getenv("DB_PASSWORD"),
    )


# --------------------------------------------------
# 3. Create a Kafka producer
# --------------------------------------------------

def create_dead_letter_producer():
    """Create a producer for the dead-letter topic."""

    return KafkaProducer(
        bootstrap_servers=KAFKA_SERVER,
        key_serializer=lambda key: (
            str(key).encode("utf-8") if key is not None else None
        ),
        value_serializer=lambda value: json.dumps(
            value,
            default=str,
        ).encode("utf-8"),
        acks="all",
        retries=5,
    )


# --------------------------------------------------
# 4. Preserve an event requiring recovery
# --------------------------------------------------

def preserve_failed_event(
    source_topic: str,
    source_partition: int,
    source_offset: int,
    event: dict[str, Any],
    failure_reason: str,
) -> None:
    """
    Record a failed event in PostgreSQL and publish it
    to the CommercePulse dead-letter topic.

    The Kafka source coordinates make database writes
    idempotent for repeated handling of the same message.
    """

    if not isinstance(event, dict):
        raise TypeError("event must be a dictionary")

    if not failure_reason.strip():
        raise ValueError("failure_reason cannot be empty")

    event_type = event.get("event_type")
    event_data = event.get("data") or {}

    order_id = event_data.get("order_id")

    event_time = event.get("event_time")

    # Parse the event timestamp if it is valid.
    parsed_event_time = None

    if event_time:
        try:
            parsed_event_time = datetime.fromisoformat(
                event_time.replace("Z", "+00:00")
            )
        except (ValueError, AttributeError):
            parsed_event_time = None

    dead_letter_event = {
        "source_topic": source_topic,
        "source_partition": source_partition,
        "source_offset": source_offset,
        "event_type": event_type,
        "order_id": order_id,
        "event_time": event_time,
        "failure_reason": failure_reason,
        "original_event": event,
        "failed_at": datetime.now(timezone.utc).isoformat(),
    }

    # First, preserve the event in PostgreSQL.
    # This is an idempotent upsert by source coordinates.
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO unprocessed_events (
                    source_topic,
                    source_partition,
                    source_offset,
                    event_type,
                    order_id,
                    event_time,
                    failure_reason,
                    raw_payload,
                    resolution_status
                )
                VALUES (
                    %s, %s, %s, %s, %s, %s, %s, %s::jsonb, 'pending'
                )
                ON CONFLICT (
                    source_topic,
                    source_partition,
                    source_offset
                )
                DO NOTHING;
                """,
                (
                    source_topic,
                    source_partition,
                    source_offset,
                    event_type,
                    order_id,
                    parsed_event_time,
                    failure_reason,
                    json.dumps(event, default=str),
                ),
            )

    # Then publish the event to Kafka.
    # If publishing fails, the PostgreSQL record remains available.
    producer = create_dead_letter_producer()

    try:
        producer.send(
            DEAD_LETTER_TOPIC,
            key=f"{source_topic}:{source_partition}:{source_offset}",
            value=dead_letter_event,
        ).get(timeout=60)

    finally:
        producer.flush()
        producer.close()

    print(
        "Event preserved for recovery:",
        f"topic={source_topic}",
        f"partition={source_partition}",
        f"offset={source_offset}",
        f"reason={failure_reason}",
    )

