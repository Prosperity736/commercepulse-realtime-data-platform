import os
from pathlib import Path

import psycopg
from dotenv import load_dotenv


# --------------------------------------------------
# 1. Load project configuration
# --------------------------------------------------

PROJECT_ROOT = Path(__file__).resolve().parent

load_dotenv(PROJECT_ROOT / ".env", override=True)


# --------------------------------------------------
# 2. Connect to PostgreSQL
# --------------------------------------------------

with psycopg.connect(
    host=os.getenv("DB_HOST"),
    port=os.getenv("DB_PORT", "5432"),
    dbname=os.getenv("DB_NAME"),
    user=os.getenv("DB_USER"),
    password=os.getenv("DB_PASSWORD"),
) as conn:

    with conn.cursor() as cur:

        # ------------------------------------------
        # 3. Create the event recovery table
        # ------------------------------------------

        cur.execute("""
            CREATE TABLE IF NOT EXISTS unprocessed_events (
                event_id BIGINT GENERATED ALWAYS AS IDENTITY
                    PRIMARY KEY,

                source_topic TEXT NOT NULL,
                source_partition INTEGER,
                source_offset BIGINT,

                event_type TEXT,
                order_id TEXT,
                event_time TIMESTAMPTZ,

                failure_reason TEXT NOT NULL,
                raw_payload JSONB NOT NULL,

                received_at TIMESTAMPTZ NOT NULL
                    DEFAULT NOW(),

                resolution_status TEXT NOT NULL
                    DEFAULT 'pending',

                retry_count INTEGER NOT NULL
                    DEFAULT 0,

                resolved_at TIMESTAMPTZ,

                CONSTRAINT uq_unprocessed_event_source
                    UNIQUE (
                        source_topic,
                        source_partition,
                        source_offset
                    ),

                CONSTRAINT chk_unprocessed_event_status
                    CHECK (
                        resolution_status IN (
                            'pending',
                            'retrying',
                            'resolved',
                            'rejected'
                        )
                    )
            );
        """)

        # Indexes for efficient investigation and recovery.
        cur.execute("""
            CREATE INDEX IF NOT EXISTS
                idx_unprocessed_events_status
            ON unprocessed_events (resolution_status);
        """)

        cur.execute("""
            CREATE INDEX IF NOT EXISTS
                idx_unprocessed_events_order_id
            ON unprocessed_events (order_id);
        """)

    conn.commit()


print("SUCCESS: unprocessed_events table is ready.")

