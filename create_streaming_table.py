import os
from pathlib import Path

import psycopg
from dotenv import load_dotenv

# Load project configuration
PROJECT_ROOT = Path(__file__).resolve().parent
load_dotenv(PROJECT_ROOT / ".env", override=True)

# Connect to PostgreSQL
with psycopg.connect(
    host=os.getenv("DB_HOST"),
    port=os.getenv("DB_PORT", "5432"),
    dbname=os.getenv("DB_NAME"),
    user=os.getenv("DB_USER"),
    password=os.getenv("DB_PASSWORD"),
) as conn:
    with conn.cursor() as cur:
        cur.execute("""
            CREATE TABLE IF NOT EXISTS order_payment_events (
                event_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
                order_id TEXT NOT NULL,
                customer_id TEXT,
                order_status TEXT,
                order_event_time TIMESTAMP,
                payment_sequential INTEGER NOT NULL,
                payment_type TEXT,
                payment_installments INTEGER,
                payment_value NUMERIC(12, 2),
                payment_event_time TIMESTAMP,
                stored_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),

                CONSTRAINT uq_order_payment_event
                    UNIQUE (order_id, payment_sequential)
            );
        """)

    conn.commit()

print("SUCCESS: order_payment_events table is ready.")
