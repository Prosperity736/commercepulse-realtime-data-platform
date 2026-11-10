from src.producer.producer_recovery import get_db_connection


def get_published_event_ids(event_type: str) -> set[str]:
    """
    Retrieve event IDs already successfully published to Kafka.
    """

    query = """
        SELECT event_id
        FROM public.kafka_publish_ledger
        WHERE event_type = %s
          AND publication_status = 'PUBLISHED';
    """

    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(query, (event_type,))
            rows = cur.fetchall()

    return {str(row[0]) for row in rows}


def mark_event_pending(
    event_id: str,
    event_type: str,
    business_key: str,
    topic: str,
) -> None:
    """
    Register an event before Kafka publication.

    Existing FAILED or PENDING events can be retried.
    Existing PUBLISHED events are never downgraded.
    """

    query = """
        INSERT INTO public.kafka_publish_ledger (
            event_id,
            event_type,
            business_key,
            topic,
            publication_status
        )
        VALUES (%s, %s, %s, %s, 'PENDING')

        ON CONFLICT (event_id)
        DO UPDATE SET
            publication_status = CASE
                WHEN public.kafka_publish_ledger.publication_status = 'PUBLISHED'
                THEN 'PUBLISHED'
                ELSE 'PENDING'
            END,
            updated_at = NOW()

        WHERE public.kafka_publish_ledger.event_type = EXCLUDED.event_type
          AND public.kafka_publish_ledger.business_key = EXCLUDED.business_key
          AND public.kafka_publish_ledger.topic = EXCLUDED.topic;
    """

    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                query,
                (event_id, event_type, business_key, topic),
            )

            if cur.rowcount != 1:
                raise RuntimeError(
                    f"Publishing ledger identity conflict: {event_id}"
                )


def mark_event_published(event_id: str) -> None:
    """Mark an event as successfully acknowledged by Kafka."""

    query = """
        UPDATE public.kafka_publish_ledger
        SET publication_status = 'PUBLISHED',
            published_at = NOW(),
            updated_at = NOW()
        WHERE event_id = %s;
    """

    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(query, (event_id,))

            if cur.rowcount != 1:
                raise RuntimeError(
                    f"Could not mark event as published: {event_id}"
                )


def mark_event_failed(event_id: str) -> None:
    """Mark an unsuccessful publication attempt as failed."""

    query = """
        UPDATE public.kafka_publish_ledger
        SET publication_status = 'FAILED',
            updated_at = NOW()
        WHERE event_id = %s
          AND publication_status <> 'PUBLISHED';
    """

    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(query, (event_id,))

            if cur.rowcount != 1:
                raise RuntimeError(
                    f"Could not mark event as failed: {event_id}"
                )