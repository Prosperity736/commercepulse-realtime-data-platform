import os
from pathlib import Path

import psycopg
from dotenv import load_dotenv


# --------------------------------------------------
# 1. Load database configuration
# --------------------------------------------------

PROJECT_ROOT = Path(__file__).resolve().parents[2]

load_dotenv(PROJECT_ROOT / ".env", override=True)

DB_CONFIG = {
    "host": os.getenv("DB_HOST"),
    "port": os.getenv("DB_PORT", "5432"),
    "dbname": os.getenv("DB_NAME"),
    "user": os.getenv("DB_USER"),
    "password": os.getenv("DB_PASSWORD"),
}

REQUIRED_SETTINGS = ("host", "dbname", "user", "password")

missing_settings = [
    key for key in REQUIRED_SETTINGS
    if not DB_CONFIG.get(key)
]

if missing_settings:
    raise ValueError(
        f"Missing database configuration: {missing_settings}"
    )


# --------------------------------------------------
# 2. Insert SQL
# --------------------------------------------------

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
    ON CONFLICT (order_id, payment_sequential)
    DO NOTHING;
"""


# --------------------------------------------------
# 3. Persist one Spark micro-batch
# --------------------------------------------------

def write_batch(batch_df, batch_id):
    """
    Persist a Spark micro-batch into PostgreSQL.

    Existing order-payment combinations are left unchanged.
    The database transaction commits only if the batch succeeds.
    """

    print(f"Processing Spark micro-batch: {batch_id}")

    columns = [
        "order_id",
        "customer_id",
        "order_status",
        "order_event_time",
        "payment_sequential",
        "payment_type",
        "payment_installments",
        "payment_value",
        "payment_event_time",
    ]

    # Collect only the columns required by PostgreSQL.
    # Appropriate for small test batches; large batches should
    # use distributed partition-based database writes.
    records = (
        batch_df
        .select(*columns)
        .toLocalIterator()
    )

    inserted_count = 0

    with psycopg.connect(**DB_CONFIG) as conn:
        with conn.cursor() as cur:
            for row in records:
                record = row.asDict(recursive=True)

                cur.execute(INSERT_SQL, record)

                if cur.rowcount == 1:
                    inserted_count += 1

    print(
        f"Micro-batch {batch_id} completed. "
        f"New records inserted: {inserted_count}"
    )
