import os
from pathlib import Path

import psycopg
from dotenv import load_dotenv


# ==================================================
# 1. CONFIGURATION
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

required = [
    DB_CONFIG["host"],
    DB_CONFIG["dbname"],
    DB_CONFIG["user"],
    DB_CONFIG["password"],
]

if not all(required):
    raise ValueError(
        "Missing PostgreSQL configuration in .env"
    )


# ==================================================
# 2. DATABASE CONNECTION
# ==================================================

def get_connection():
    return psycopg.connect(**DB_CONFIG)


# ==================================================
# 3. FIND MATCHING ORDER-PAYMENT PAIRS
# ==================================================

MATCH_SQL = """
SELECT DISTINCT ON (
    p.source_topic,
    p.source_partition,
    p.source_offset
)

    o.staging_id AS order_staging_id,
    p.staging_id AS payment_staging_id,

    o.source_topic AS order_topic,
    o.source_partition AS order_partition,
    o.source_offset AS order_offset,

    p.source_topic AS payment_topic,
    p.source_partition AS payment_partition,
    p.source_offset AS payment_offset,

    o.order_id,
    o.customer_id,
    o.order_status,
    o.event_time AS order_event_time,

    p.payment_sequential,
    p.payment_type,
    p.payment_installments,
    p.payment_value,
    p.event_time AS payment_event_time

FROM order_event_staging o

INNER JOIN payment_event_staging p
    ON o.order_id = p.order_id

WHERE o.match_status = 'PENDING'
  AND p.match_status = 'PENDING'

  AND p.event_time >= o.event_time

  AND p.event_time <= (
      o.event_time + INTERVAL '10 minutes'
  )

ORDER BY
    p.source_topic,
    p.source_partition,
    p.source_offset,
    o.event_time DESC,
    o.staging_id DESC;
"""


# ==================================================
# 4. INSERT MATCHED RECORD
# ==================================================

INSERT_ANALYTICAL_SQL = """
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
    %s, %s, %s, %s, %s, %s, %s, %s, %s
)
ON CONFLICT (
    order_id,
    payment_sequential
)
DO NOTHING;
"""


# ==================================================
# 5. UPDATE PAYMENT STAGING STATUS
# ==================================================

UPDATE_PAYMENT_SQL = """
UPDATE payment_event_staging
SET match_status = 'MATCHED'
WHERE staging_id = %s
  AND match_status = 'PENDING';
"""


# ==================================================
# 6. RUN RECONCILIATION
# ==================================================

def run_reconciliation():

    print("\n======================================")
    print("COMMERCEPULSE EVENT RECONCILIATION")
    print("======================================")

    matched_count = 0
    duplicate_count = 0

    with get_connection() as conn:

        with conn.cursor() as cur:

            # --------------------------------------
            # FIND MATCHING EVENTS
            # --------------------------------------

            cur.execute(MATCH_SQL)

            matches = cur.fetchall()

            print(
                f"\nPotential unique payment matches: {len(matches)}"
            )

            # --------------------------------------
            # PROCESS MATCHES
            # --------------------------------------

            for record in matches:

                (
                    order_staging_id,
                    payment_staging_id,

                    order_topic,
                    order_partition,
                    order_offset,

                    payment_topic,
                    payment_partition,
                    payment_offset,

                    order_id,
                    customer_id,
                    order_status,
                    order_event_time,

                    payment_sequential,
                    payment_type,
                    payment_installments,
                    payment_value,
                    payment_event_time

                ) = record

                cur.execute(
                    INSERT_ANALYTICAL_SQL,
                    (
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
                )

                if cur.rowcount == 1:

                    matched_count += 1

                    print(
                        f"MATCHED: {order_id} | "
                        f"Payment sequence: {payment_sequential}"
                    )

                else:

                    duplicate_count += 1

                    print(
                        f"EXISTING ANALYTICAL RECORD: {order_id} | "
                        f"Payment sequence: {payment_sequential}"
                    )

                # ----------------------------------
                # UPDATE PAYMENT STATUS
                # ----------------------------------

                cur.execute(
                    UPDATE_PAYMENT_SQL,
                    (payment_staging_id,)
                )

                # IMPORTANT:
                # Do not mark the order staging record
                # as MATCHED here.
                #
                # One order can have multiple payment
                # sequences. Keeping the order PENDING
                # allows subsequent payments to match.


    # ==================================================
    # 7. SUMMARY
    # ==================================================

    print("\n======================================")
    print("RECONCILIATION SUMMARY")
    print("======================================")

    print(
        f"New analytical records: {matched_count}"
    )

    print(
        f"Existing analytical records: {duplicate_count}"
    )

    print(
        f"Total reconciled payment events: "
        f"{matched_count + duplicate_count}"
    )

    print("\nReconciliation completed successfully.")


# ==================================================
# 8. ENTRY POINT
# ==================================================

if __name__ == "__main__":

    run_reconciliation()
