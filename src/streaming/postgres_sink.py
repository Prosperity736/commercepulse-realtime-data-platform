import json
import os
from pathlib import Path

import psycopg
from dotenv import load_dotenv


# ==================================================
# 1. ENVIRONMENT CONFIGURATION
# ==================================================

PROJECT_ROOT = Path(__file__).resolve().parents[2]

load_dotenv(PROJECT_ROOT / ".env", override=True)

DB_CONFIG = {
    "host": os.getenv("DB_HOST"),
    "port": os.getenv("DB_PORT", "5432"),
    "dbname": os.getenv("DB_NAME"),
    "user": os.getenv("DB_USER"),
    "password": os.getenv("DB_PASSWORD"),
}

required_values = [
    DB_CONFIG["host"],
    DB_CONFIG["dbname"],
    DB_CONFIG["user"],
    DB_CONFIG["password"],
]

if not all(required_values):
    raise ValueError(
        "Missing PostgreSQL configuration. "
        "Check DB_HOST, DB_NAME, DB_USER and DB_PASSWORD in .env."
    )


# ==================================================
# 2. DATABASE CONNECTION
# ==================================================

def get_connection():
    return psycopg.connect(**DB_CONFIG)


# ==================================================
# 3. MAIN ANALYTICAL TABLE INSERT
# ==================================================

INSERT_SQL = """
INSERT INTO order_payment_events (
    order_id,
    customer_id,
    order_status,
    order_event_time,
    payment_sequential,
    payment_type,
    payment_installments,
    payment_value,
    payment_event_time
)
VALUES (
    %(order_id)s,
    %(customer_id)s,
    %(order_status)s,
    %(order_event_time)s,
    %(payment_sequential)s,
    %(payment_type)s,
    %(payment_installments)s,
    %(payment_value)s,
    %(payment_event_time)s
)
ON CONFLICT (order_id, payment_sequential) DO NOTHING;
"""


# ==================================================
# 4. DATA QUALITY EXCEPTION INSERT
# ==================================================

INSERT_EXCEPTION_SQL = """
INSERT INTO data_quality_exceptions (
    source_topic,
    source_partition,
    source_offset,
    event_type,
    order_id,
    raw_event,
    error_category,
    error_message
)
VALUES (
    %(source_topic)s,
    %(source_partition)s,
    %(source_offset)s,
    %(event_type)s,
    %(order_id)s,
    %(raw_event)s::jsonb,
    %(error_category)s,
    %(error_message)s
)
ON CONFLICT (
    source_topic,
    source_partition,
    source_offset
)
DO NOTHING;
"""


# ==================================================
# 5. PREPARE RAW EVENT FOR JSONB
# ==================================================

def prepare_raw_event(raw_event):

    if not isinstance(raw_event, str):
        return json.dumps(raw_event)

    try:
        parsed_event = json.loads(raw_event)

        return json.dumps(parsed_event)

    except (json.JSONDecodeError, TypeError):

        return json.dumps({
            "raw": raw_event
        })


# ==================================================
# 6. WRITE VALID EVENTS TO ANALYTICAL TABLE
# ==================================================

def write_batch(batch_df, batch_id):

    print(
        f"\nProcessing PostgreSQL micro-batch: {batch_id}"
    )

    processed_count = 0
    inserted_count = 0

    with get_connection() as conn:

        with conn.cursor() as cur:

            for row in batch_df.toLocalIterator():

                record = row.asDict(recursive=True)

                cur.execute(
                    INSERT_SQL,
                    record
                )

                processed_count += 1

                if cur.rowcount > 0:
                    inserted_count += 1

    print(
        f"Rows received: {processed_count}"
    )

    print(
        f"New analytical records inserted: {inserted_count}"
    )

    print(
        f"PostgreSQL batch {batch_id} completed."
    )

    return inserted_count


# ==================================================
# 7. WRITE DATA QUALITY EXCEPTIONS
# ==================================================

def write_exceptions(exception_records):

    if not exception_records:
        return 0

    inserted_count = 0

    with get_connection() as conn:

        with conn.cursor() as cur:

            for record in exception_records:

                record = dict(record)

                record["raw_event"] = prepare_raw_event(
                    record.get("raw_event")
                )

                cur.execute(
                    INSERT_EXCEPTION_SQL,
                    record
                )

                if cur.rowcount > 0:
                    inserted_count += 1

    print(
        f"Data quality exceptions inserted: {inserted_count}"
    )

    return inserted_count


# ==================================================
# 8. ORDER STAGING INSERT
# ==================================================

ORDER_STAGING_SQL = """
INSERT INTO order_event_staging (
    source_topic,
    source_partition,
    source_offset,
    order_id,
    customer_id,
    order_status,
    event_time,
    raw_event
)
VALUES (
    %(source_topic)s,
    %(source_partition)s,
    %(source_offset)s,
    %(order_id)s,
    %(customer_id)s,
    %(order_status)s,
    %(event_time)s,
    %(raw_event)s::jsonb
)
ON CONFLICT (
    source_topic,
    source_partition,
    source_offset
)
DO NOTHING;
"""


# ==================================================
# 9. WRITE ORDERS TO STAGING
# ==================================================

def write_order_staging(batch_df, batch_id):

    print(
        f"\nProcessing order staging batch: {batch_id}"
    )

    processed_count = 0
    inserted_count = 0

    with get_connection() as conn:

        with conn.cursor() as cur:

            for row in batch_df.toLocalIterator():

                record = row.asDict(recursive=True)

                record["raw_event"] = prepare_raw_event(
                    record["raw_event"]
                )

                cur.execute(
                    ORDER_STAGING_SQL,
                    record
                )

                processed_count += 1

                if cur.rowcount > 0:
                    inserted_count += 1

    print(
        f"Orders processed: {processed_count}"
    )

    print(
        f"Orders staged: {inserted_count}"
    )


# ==================================================
# 10. PAYMENT STAGING INSERT
# ==================================================

PAYMENT_STAGING_SQL = """
INSERT INTO payment_event_staging (
    source_topic,
    source_partition,
    source_offset,
    order_id,
    payment_sequential,
    payment_type,
    payment_installments,
    payment_value,
    event_time,
    raw_event
)
VALUES (
    %(source_topic)s,
    %(source_partition)s,
    %(source_offset)s,
    %(order_id)s,
    %(payment_sequential)s,
    %(payment_type)s,
    %(payment_installments)s,
    %(payment_value)s,
    %(event_time)s,
    %(raw_event)s::jsonb
)
ON CONFLICT (
    source_topic,
    source_partition,
    source_offset
)
DO NOTHING;
"""


# ==================================================
# 11. WRITE PAYMENTS TO STAGING
# ==================================================

def write_payment_staging(batch_df, batch_id):

    print(
        f"\nProcessing payment staging batch: {batch_id}"
    )

    processed_count = 0
    inserted_count = 0

    with get_connection() as conn:

        with conn.cursor() as cur:

            for row in batch_df.toLocalIterator():

                record = row.asDict(recursive=True)

                record["raw_event"] = prepare_raw_event(
                    record["raw_event"]
                )

                cur.execute(
                    PAYMENT_STAGING_SQL,
                    record
                )

                processed_count += 1

                if cur.rowcount > 0:
                    inserted_count += 1

    print(
        f"Payments processed: {processed_count}"
    )

    print(
        f"Payments staged: {inserted_count}"
    )