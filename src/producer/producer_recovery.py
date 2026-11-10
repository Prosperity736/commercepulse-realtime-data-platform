"""
Producer-side recovery for CommercePulse.

Persists failed Kafka producer events in PostgreSQL
so they can be recovered without re-reading source files.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any
from uuid import UUID

import psycopg

from src.common.config import get_settings
from src.common.logging import get_logger


logger = get_logger(__name__)


VALID_EVENT_TYPES = {"order", "payment"}


def get_db_connection():
    """
    Create a PostgreSQL connection using centralized settings.
    """

    settings = get_settings()

    return psycopg.connect(
        host=settings.db_host,
        port=settings.db_port,
        dbname=settings.db_name,
        user=settings.db_user,
        password=settings.db_password,
        connect_timeout=settings.db_connect_timeout,
    )


def preserve_failed_producer_event(
    event: dict[str, Any],
    source_file: str | Path,
    failure_reason: str,
) -> bool:
    """
    Persist a failed producer event in PostgreSQL.

    Deterministic event UUIDs prevent duplicate recovery records.

    Returns:
        True: Event was inserted or safely updated.
        False: Event already has a terminal or active retry status.

    Raises:
        TypeError: Event is not a dictionary.
        ValueError: Event metadata is invalid.
        Exception: PostgreSQL persistence fails.
    """

    if not isinstance(event, dict):
        raise TypeError("event must be a dictionary.")

    if not failure_reason or not failure_reason.strip():
        raise ValueError("failure_reason cannot be empty.")

    if not source_file:
        raise ValueError("source_file cannot be empty.")

    event_id = event.get("event_id")
    event_type = event.get("event_type")

    if not event_id:
        raise ValueError("Event is missing event_id.")

    if event_type not in VALID_EVENT_TYPES:
        raise ValueError(
            f"Unsupported event_type: {event_type}"
        )

    try:
        event_uuid = UUID(str(event_id))
    except (ValueError, TypeError, AttributeError) as exc:
        raise ValueError(
            "event_id must be a valid UUID."
        ) from exc

    source_filename = Path(source_file).name

    if not source_filename:
        raise ValueError("Invalid source filename.")

    try:
        raw_payload = json.dumps(
            event,
            ensure_ascii=False,
            allow_nan=False,
        )
    except (TypeError, ValueError) as exc:
        raise ValueError(
            "Event cannot be serialized as valid JSON."
        ) from exc

    query = """
        INSERT INTO producer_failed_events (
            event_id,
            source_file,
            event_type,
            failure_reason,
            raw_payload,
            resolution_status,
            retry_count,
            next_retry_at
        )
        VALUES (
            %s, %s, %s, %s, %s::jsonb, 'pending', 0, NULL
        )

        ON CONFLICT (event_id)
        DO UPDATE SET
            source_file = EXCLUDED.source_file,
            failure_reason = EXCLUDED.failure_reason,
            raw_payload = EXCLUDED.raw_payload,
            next_retry_at = NULL,
            updated_at = NOW()

        WHERE producer_failed_events.resolution_status = 'pending'

        RETURNING event_id;
    """

    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                query,
                (
                    event_uuid,
                    source_filename,
                    event_type,
                    failure_reason.strip(),
                    raw_payload,
                ),
            )

            result = cur.fetchone()

    if result is None:
        logger.warning(
            "Producer recovery record was not updated because "
            "it is already resolved, rejected, or being retried | "
            "Event ID: %s",
            event_uuid,
        )

        return False

    logger.warning(
        "Producer event preserved for recovery | "
        "Event ID: %s | Type: %s | Source: %s",
        event_uuid,
        event_type,
        source_filename,
    )

    return True