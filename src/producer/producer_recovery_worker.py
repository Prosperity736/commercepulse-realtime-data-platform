"""
CommercePulse Producer Recovery Worker.

Uses PostgreSQL-backed leases and batch ownership tokens to safely
coordinate concurrent recovery workers.

Delivery semantics: at-least-once. Kafka publishing and PostgreSQL
status updates are not one atomic transaction, so downstream consumers
must remain idempotent.
"""

from __future__ import annotations

import json
import threading
import time
from typing import Any
from uuid import NAMESPACE_URL, UUID, uuid4, uuid5

from src.common.config import get_settings
from src.common.logging import get_logger
from src.common.schemas import EventValidationError, validate_event
from src.producer.kafka_producer import CommercePulseKafkaProducer
from src.producer.producer_recovery import get_db_connection
from src.producer.publishing_ledger import mark_event_published

logger = get_logger(__name__)

DEFAULT_BATCH_SIZE = 100
LEASE_MINUTES = 5
HEARTBEAT_SECONDS = 60
MAX_RETRY_DELAY_MINUTES = 60


def calculate_retry_delay(retry_count: int) -> int:
    """Return capped exponential retry delay in minutes."""
    if retry_count < 1:
        raise ValueError("retry_count must be at least 1.")
    return min(2 ** (retry_count - 1), MAX_RETRY_DELAY_MINUTES)


def verify_deterministic_event_id(event: dict[str, Any]) -> None:
    """Verify event_id against the event's business identity."""
    event_type = event["event_type"]
    data = event["data"]
    order_id = data["order_id"]

    if event_type == "order":
        business_key = order_id
    elif event_type == "payment":
        business_key = f'{order_id}:{data["payment_sequential"]}'
    else:
        raise EventValidationError(f"Unsupported event type: {event_type}")

    identity = f"commercepulse:{event_type}:{business_key}"
    expected = str(uuid5(NAMESPACE_URL, identity))

    try:
        actual = str(UUID(event["event_id"]))
    except (ValueError, TypeError, KeyError, AttributeError) as exc:
        raise EventValidationError("Event ID must be a valid UUID.") from exc

    if actual != expected:
        raise EventValidationError(
            "Event ID does not match its deterministic business identity."
        )


def validate_recovery_event(event: Any) -> dict:
    """Decode and validate a persisted recovery payload."""
    if isinstance(event, str):
        try:
            event = json.loads(event)
        except json.JSONDecodeError as exc:
            raise EventValidationError("Stored payload is not valid JSON.") from exc

    if not isinstance(event, dict):
        raise EventValidationError("Stored event payload must be a dictionary.")

    validate_event(event)
    verify_deterministic_event_id(event)
    return event


def claim_failed_events(
    batch_size: int = DEFAULT_BATCH_SIZE,
    stale_claim_minutes: int = LEASE_MINUTES,
    claim_token: UUID | None = None,
) -> list[dict]:
    """
    Atomically claim due pending events and events with expired leases.

    A NULL lease on a legacy retrying row is treated as expired.
    """
    if batch_size < 1:
        raise ValueError("batch_size must be positive.")
    if stale_claim_minutes < 1:
        raise ValueError("stale_claim_minutes must be positive.")

    token = claim_token or uuid4()
    query = """
        WITH candidates AS (
            SELECT event_id
            FROM producer_failed_events
            WHERE
                (
                    resolution_status = 'pending'
                    AND (next_retry_at IS NULL OR next_retry_at <= NOW())
                )
                OR
                (
                    resolution_status = 'retrying'
                    AND (
                        claim_expires_at IS NULL
                        OR claim_expires_at <= NOW()
                    )
                )
            ORDER BY created_at, event_id
            LIMIT %s
            FOR UPDATE SKIP LOCKED
        )
        UPDATE producer_failed_events AS failed
        SET
            resolution_status = 'retrying',
            claim_token = %s,
            claim_expires_at = NOW() + (%s * INTERVAL '1 minute'),
            last_attempt_at = NOW(),
            updated_at = NOW()
        FROM candidates
        WHERE failed.event_id = candidates.event_id
        RETURNING
            failed.event_id,
            failed.event_type,
            failed.raw_payload,
            failed.retry_count,
            failed.claim_token;
    """

    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(query, (batch_size, token, stale_claim_minutes))
            columns = [d.name for d in cur.description]
            rows = cur.fetchall()

    records = [dict(zip(columns, row)) for row in rows]
    logger.info(
        "Recovery events claimed | Count: %s | Claim token: %s",
        len(records), token,
    )
    return records


def renew_claims(claim_token: UUID, lease_minutes: int = LEASE_MINUTES) -> int:
    """Extend only unexpired leases still owned by this worker."""
    query = """
        UPDATE producer_failed_events
        SET
            claim_expires_at = NOW() + (%s * INTERVAL '1 minute'),
            updated_at = NOW()
        WHERE
            resolution_status = 'retrying'
            AND claim_token = %s
            AND claim_expires_at > NOW()
        RETURNING event_id;
    """
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(query, (lease_minutes, claim_token))
            return len(cur.fetchall())


def _owned_update(query: str, params: tuple, event_id: UUID, claim_token: UUID) -> None:
    """Execute a state transition guarded by event and ownership token."""
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(query, (*params, event_id, claim_token))
            if cur.fetchone() is None:
                raise RuntimeError(
                    f"Claim ownership lost; refusing to update event {event_id}."
                )


def mark_event_resolved(event_id: UUID, claim_token: UUID) -> None:
    query = """
        UPDATE producer_failed_events
        SET
            resolution_status = 'resolved',
            resolved_at = NOW(),
            next_retry_at = NULL,
            claim_token = NULL,
            claim_expires_at = NULL,
            updated_at = NOW()
        WHERE event_id = %s
          AND claim_token = %s
          AND resolution_status = 'retrying'
          AND claim_expires_at > NOW()
        RETURNING event_id;
    """
    _owned_update(query, (), event_id, claim_token)
    logger.info("Recovery event resolved | Event ID: %s", event_id)


def schedule_retry(
    event_id: UUID,
    current_retry_count: int,
    failure_reason: str,
    claim_token: UUID,
) -> None:
    next_count = current_retry_count + 1
    delay = calculate_retry_delay(next_count)
    query = """
        UPDATE producer_failed_events
        SET
            resolution_status = 'pending',
            retry_count = %s,
            failure_reason = %s,
            next_retry_at = NOW() + (%s * INTERVAL '1 minute'),
            claim_token = NULL,
            claim_expires_at = NULL,
            updated_at = NOW()
        WHERE event_id = %s
          AND claim_token = %s
          AND resolution_status = 'retrying'
          AND claim_expires_at > NOW()
        RETURNING event_id;
    """
    _owned_update(query, (next_count, failure_reason, delay), event_id, claim_token)
    logger.warning(
        "Recovery retry scheduled | Event ID: %s | Retry: %s | Delay: %s minutes",
        event_id, next_count, delay,
    )


def mark_event_rejected(
    event_id: UUID,
    failure_reason: str,
    claim_token: UUID,
) -> None:
    query = """
        UPDATE producer_failed_events
        SET
            resolution_status = 'rejected',
            failure_reason = %s,
            next_retry_at = NULL,
            claim_token = NULL,
            claim_expires_at = NULL,
            updated_at = NOW()
        WHERE event_id = %s
          AND claim_token = %s
          AND resolution_status = 'retrying'
          AND claim_expires_at > NOW()
        RETURNING event_id;
    """
    _owned_update(query, (failure_reason,), event_id, claim_token)
    logger.error(
        "Recovery event rejected | Event ID: %s | Reason: %s",
        event_id, failure_reason,
    )


class ClaimHeartbeat:
    """Background lease renewal for all unresolved events in one batch."""

    def __init__(
        self,
        claim_token: UUID,
        lease_minutes: int = LEASE_MINUTES,
        interval_seconds: int = HEARTBEAT_SECONDS,
    ) -> None:
        if interval_seconds <= 0:
            raise ValueError("interval_seconds must be positive.")
        self.claim_token = claim_token
        self.lease_minutes = lease_minutes
        self.interval_seconds = interval_seconds
        self._stop = threading.Event()
        self._thread = threading.Thread(
            target=self._run,
            name="commercepulse-recovery-heartbeat",
            daemon=True,
        )
        self.last_error: Exception | None = None

    def _run(self) -> None:
        while not self._stop.wait(self.interval_seconds):
            try:
                renewed = renew_claims(self.claim_token, self.lease_minutes)
                if renewed == 0:
                    logger.info(
                        "No active leases remain for recovery claim %s",
                        self.claim_token,
                    )
                    return
                logger.debug(
                    "Recovery leases renewed | Count: %s | Claim: %s",
                    renewed, self.claim_token,
                )
            except Exception as exc:
                self.last_error = exc
                logger.exception(
                    "Recovery lease heartbeat failed | Claim: %s",
                    self.claim_token,
                )

    def start(self) -> None:
        self._thread.start()

    def stop(self) -> None:
        self._stop.set()
        if self._thread.is_alive():
            self._thread.join(timeout=self.interval_seconds + 5)


def process_recovery_event(
    record: dict,
    producer: CommercePulseKafkaProducer,
    claim_token: UUID | None = None,
) -> str:
    """Validate and republish one claimed event."""
    event_id = record["event_id"]
    event_type = record["event_type"]
    retry_count = record["retry_count"]
    token = claim_token or record.get("claim_token")

    if token is None:
        raise ValueError("A claim token is required to process a recovery event.")

    try:
        event = validate_recovery_event(record["raw_payload"])
        if event["event_type"] != event_type:
            raise EventValidationError(
                "Stored event_type does not match payload event_type."
            )
    except EventValidationError as exc:
        try:
            mark_event_rejected(event_id, str(exc), token)
            return "rejected"
        except Exception:
            logger.exception(
                "Failed to reject invalid recovery event | Event ID: %s",
                event_id,
            )
            return "error"

    settings = get_settings()
    if event_type == "order":
        topic = settings.orders_topic
    elif event_type == "payment":
        topic = settings.payments_topic
    else:
        try:
            mark_event_rejected(
                event_id, f"Unsupported event type: {event_type}", token
            )
            return "rejected"
        except Exception:
            logger.exception("Could not reject unsupported event %s", event_id)
            return "error"

    delivered = producer.publish(
        topic=topic,
        event=event,
        message_key=event["data"]["order_id"],
    )

    if delivered:
     try:
        mark_event_published(str(event_id))
        mark_event_resolved(event_id, token)
        return "resolved"
     except Exception:
        logger.exception(
            "Kafka acknowledged event but ledger or resolution update failed | %s",
            event_id,
        )
        return "error"

    try:
        schedule_retry(
            event_id=event_id,
            current_retry_count=retry_count,
            failure_reason="Kafka delivery failed during recovery.",
            claim_token=token,
        )
        return "retried"
    except Exception:
        logger.exception("Could not schedule recovery retry | %s", event_id)
        return "error"


def release_claims(claim_token: UUID, reason: str) -> int:
    """
    Return still-owned events to pending immediately.

    Used when the worker cannot start or complete its Kafka processing.
    Only events still owned by this token are affected.
    """
    query = """
        UPDATE producer_failed_events
        SET
            resolution_status = 'pending',
            next_retry_at = NOW(),
            failure_reason = %s,
            claim_token = NULL,
            claim_expires_at = NULL,
            updated_at = NOW()
        WHERE
            resolution_status = 'retrying'
            AND claim_token = %s
        RETURNING event_id;
    """
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(query, (reason, claim_token))
            return len(cur.fetchall())


def run_recovery(batch_size: int = DEFAULT_BATCH_SIZE) -> dict:
    """Execute one resilient recovery batch."""
    if batch_size < 1:
        raise ValueError("batch_size must be positive.")

    summary = {
        "claimed": 0,
        "resolved": 0,
        "retried": 0,
        "rejected": 0,
        "errors": 0,
    }
    claim_token = uuid4()

    records = claim_failed_events(
        batch_size=batch_size,
        claim_token=claim_token,
    )
    summary["claimed"] = len(records)

    if not records:
        logger.info("No eligible producer recovery events found.")
        return summary

    producer = None
    heartbeat = None
    try:
        # Initialize before heartbeat. If initialization fails, release claims.
        producer = CommercePulseKafkaProducer()
        heartbeat = ClaimHeartbeat(claim_token)
        heartbeat.start()

        result_counters = {
            "resolved": "resolved",
            "retried": "retried",
            "rejected": "rejected",
            "error": "errors",
        }

        for record in records:
            event_id = record.get("event_id", "unknown")
            try:
                result = process_recovery_event(
                    record=record,
                    producer=producer,
                    claim_token=claim_token,
                )
                if result not in result_counters:
                    raise ValueError(f"Unexpected recovery result: {result}")
                summary[result_counters[result]] += 1
            except Exception:
                summary["errors"] += 1
                logger.exception(
                    "Unexpected recovery processing failure | Event ID: %s. "
                    "Continuing with next event.",
                    event_id,
                )

    except Exception:
        # If startup fails, or batch-level processing fails, release only
        # records that remain owned by this run. Already transitioned rows
        # are not touched by this ownership-scoped update.
        summary["errors"] += 1
        logger.exception("Recovery batch initialization/execution failed.")
        try:
            released = release_claims(
                claim_token,
                "Recovery worker could not complete this batch.",
            )
            logger.warning("Released unfinished recovery claims | Count: %s", released)
        except Exception:
            logger.exception("Could not release unfinished recovery claims.")

    finally:
        if heartbeat is not None:
            heartbeat.stop()
        if producer is not None:
            try:
                producer.close()
            except Exception:
                summary["errors"] += 1
                logger.exception("Failed to close Kafka producer.")

    logger.info(
        "PRODUCER RECOVERY COMPLETED | Claimed: %s | Resolved: %s "
        "| Retried: %s | Rejected: %s | Errors: %s",
        summary["claimed"],
        summary["resolved"],
        summary["retried"],
        summary["rejected"],
        summary["errors"],
    )
    return summary


if __name__ == "__main__":
    result = run_recovery(batch_size=DEFAULT_BATCH_SIZE)
    print("\nPRODUCER RECOVERY SUMMARY")
    for key, value in result.items():
        print(f"{key.replace('_', ' ').title():20}: {value:,}")
