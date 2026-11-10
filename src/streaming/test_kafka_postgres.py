import json
import os
import sys
import time
import uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path

import psycopg
from dotenv import load_dotenv
from kafka import KafkaProducer


# ============================================================
# 1. CONFIGURATION
# ============================================================

PROJECT_ROOT = Path(__file__).resolve().parents[2]

load_dotenv(PROJECT_ROOT / ".env", override=True)

KAFKA_SERVER = "localhost:19092"
ORDERS_TOPIC = "olist-orders"
PAYMENTS_TOPIC = "olist-payments"

DB_CONFIG = {
    "host": os.getenv("DB_HOST"),
    "port": os.getenv("DB_PORT", "5432"),
    "dbname": os.getenv("DB_NAME"),
    "user": os.getenv("DB_USER"),
    "password": os.getenv("DB_PASSWORD"),
}

REQUIRED_SETTINGS = ("host", "dbname", "user", "password")

missing_settings = [
    setting for setting in REQUIRED_SETTINGS
    if not DB_CONFIG.get(setting)
]

if missing_settings:
    raise ValueError(
        f"Missing database configuration: {missing_settings}"
    )

READY_FILE = (
    PROJECT_ROOT
    / "checkpoints"
    / "e2e_test_ready.txt"
)

READY_TIMEOUT_SECONDS = 120
RECORD_TIMEOUT_SECONDS = 300
POLL_INTERVAL_SECONDS = 2

# Bound individual database operations so a single poll cannot
# block verification indefinitely.
DB_CONNECT_TIMEOUT_SECONDS = 5
DB_STATEMENT_TIMEOUT_MS = 3000

TEST_RUN_ID = uuid.uuid4().hex[:12]
TEST_ORDER_ID = f"commercepulse_e2e_{TEST_RUN_ID}"
TEST_CUSTOMER_ID = "commercepulse_test_customer"

BASE_EVENT_TIME = (
    datetime.now(timezone.utc)
    .replace(tzinfo=None, microsecond=0)
)

print("=" * 65)
print("COMMERCEPULSE KAFKA-TO-POSTGRESQL INTEGRATION TEST")
print("=" * 65)
print(f"Test order ID: {TEST_ORDER_ID}")
print(f"Base event time: {BASE_EVENT_TIME.isoformat()}")
print(f"Kafka broker: {KAFKA_SERVER}")
print(f"Orders topic: {ORDERS_TOPIC}")
print(f"Payments topic: {PAYMENTS_TOPIC}")
print(f"PostgreSQL database: {DB_CONFIG['dbname']}")


# ============================================================
# 2. DATABASE HELPERS
# ============================================================

def connect_to_database():
    """Open a bounded PostgreSQL connection for test queries."""
    return psycopg.connect(
        **DB_CONFIG,
        connect_timeout=DB_CONNECT_TIMEOUT_SECONDS,
        options=f"-c statement_timeout={DB_STATEMENT_TIMEOUT_MS}",
    )


# ============================================================
# 3. VERIFY POSTGRESQL CONNECTION
# ============================================================

with connect_to_database() as conn:
    with conn.cursor() as cur:
        cur.execute("SELECT COUNT(*) FROM order_payment_events")
        baseline_count = cur.fetchone()[0]

print(f"Baseline PostgreSQL records: {baseline_count}")


# ============================================================
# 4. EVENT BUILDERS
# ============================================================

def build_order_event(order_id, event_time):
    timestamp = event_time.isoformat()

    return {
        "event_type": "order",
        "event_time": timestamp,
        "processing_time": datetime.now(timezone.utc).isoformat(),
        "data": {
            "order_id": order_id,
            "customer_id": TEST_CUSTOMER_ID,
            "order_status": "test",
            "order_purchase_timestamp": timestamp,
            "order_approved_at": None,
            "order_delivered_carrier_date": None,
            "order_delivered_customer_date": None,
            "order_estimated_delivery_date": None,
        },
    }


def build_payment_event(order_id, event_time):
    return {
        "event_type": "payment",
        "event_time": event_time.isoformat(),
        "processing_time": datetime.now(timezone.utc).isoformat(),
        "data": {
            "order_id": order_id,
            "payment_sequential": 1,
            "payment_type": "test",
            "payment_installments": 1,
            "payment_value": 1.23,
        },
    }


def publish_event(producer, topic, event):
    producer.send(topic, value=event).get(timeout=30)


# ============================================================
# 5. WAIT FOR SPARK READINESS
# ============================================================

print("\nWaiting for the Spark streaming query...")

readiness_deadline = time.monotonic() + READY_TIMEOUT_SECONDS

while time.monotonic() < readiness_deadline:
    if READY_FILE.is_file():
        print("Spark readiness signal received.")
        break

    time.sleep(1)
else:
    raise TimeoutError(
        f"Spark readiness signal was not received within "
        f"{READY_TIMEOUT_SECONDS} seconds. "
        "Check the existing stream_to_postgres.py terminal."
    )


# ============================================================
# 6. PUBLISH THE MATCHING EVENTS
# ============================================================

producer = KafkaProducer(
    bootstrap_servers=KAFKA_SERVER,
    value_serializer=lambda value: json.dumps(value).encode("utf-8"),
    acks="all",
    retries=5,
)

try:
    print("\nPublishing the matching test order and payment...")

    publish_event(
        producer,
        ORDERS_TOPIC,
        build_order_event(TEST_ORDER_ID, BASE_EVENT_TIME),
    )

    publish_event(
        producer,
        PAYMENTS_TOPIC,
        build_payment_event(TEST_ORDER_ID, BASE_EVENT_TIME),
    )

    producer.flush()
    print("Matching events acknowledged by Kafka.")

    print("\nPublishing watermark-advancement markers...")

    for marker_number, minutes_ahead in enumerate((20, 40), start=1):
        marker_time = BASE_EVENT_TIME + timedelta(minutes=minutes_ahead)

        marker_order_id = (
            f"commercepulse_marker_{TEST_RUN_ID}_{marker_number}"
        )

        publish_event(
            producer,
            ORDERS_TOPIC,
            build_order_event(marker_order_id, marker_time),
        )

        publish_event(
            producer,
            PAYMENTS_TOPIC,
            build_payment_event(
                f"{marker_order_id}_payment_only",
                marker_time,
            ),
        )

    producer.flush()
    print("Marker events acknowledged by Kafka.")

finally:
    producer.close()


# ============================================================
# 7. VERIFY THE DATABASE INSERT
# ============================================================

print("\nWaiting for the joined test record in PostgreSQL...")
print(f"Verification timeout: {RECORD_TIMEOUT_SECONDS} seconds")
print(f"Polling interval: {POLL_INTERVAL_SECONDS} seconds")
print(
    "Per-query limits: "
    f"{DB_CONNECT_TIMEOUT_SECONDS}s connection, "
    f"{DB_STATEMENT_TIMEOUT_MS}ms statement"
)

verification_started = time.monotonic()
verification_deadline = verification_started + RECORD_TIMEOUT_SECONDS

found_record = None
poll_attempts = 0
last_database_error = None

SELECT_TEST_RECORD_SQL = """
    SELECT
        order_id,
        customer_id,
        order_status,
        payment_sequential,
        payment_type,
        payment_installments,
        payment_value,
        order_event_time,
        payment_event_time
    FROM order_payment_events
    WHERE order_id = %s
      AND payment_sequential = %s
"""


while True:
    remaining_seconds = verification_deadline - time.monotonic()
    if remaining_seconds <= 0:
        break

    poll_attempts += 1

    try:
        # Each connection/query is bounded, preventing a single poll
        # from waiting indefinitely. Keep the query itself read-only.
        with connect_to_database() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    SELECT_TEST_RECORD_SQL,
                    (TEST_ORDER_ID, 1),
                )
                found_record = cur.fetchone()

        last_database_error = None

    except psycopg.Error as exc:
        last_database_error = str(exc)
        elapsed = time.monotonic() - verification_started
        print(
            f"[{elapsed:.1f}s] Database polling error on attempt "
            f"{poll_attempts}: {exc}",
            flush=True,
        )

    elapsed = time.monotonic() - verification_started

    if found_record is not None:
        print(
            f"Record found after {elapsed:.1f} seconds "
            f"and {poll_attempts} polling attempts.",
            flush=True,
        )
        break

    if poll_attempts == 1 or poll_attempts % 10 == 0:
        print(
            f"[{elapsed:.1f}s] Attempt {poll_attempts}: "
            "test record not found yet.",
            flush=True,
        )

    # Do not intentionally sleep beyond the verification deadline.
    remaining_seconds = verification_deadline - time.monotonic()
    if remaining_seconds > 0:
        time.sleep(min(POLL_INTERVAL_SECONDS, remaining_seconds))


# ============================================================
# 8. REPORT THE RESULT
# ============================================================

elapsed_total = time.monotonic() - verification_started

print("\n" + "=" * 65)

if found_record is None:
    print("TEST RESULT: NOT VERIFIED WITHIN DEADLINE")
    print("=" * 65)
    print(
        "The expected joined record was not observed before the "
        "verification deadline. This does not prove that it can "
        "never be inserted."
    )
    print(f"Test order ID: {TEST_ORDER_ID}")
    print(f"Elapsed verification time: {elapsed_total:.1f} seconds")
    print(f"Polling attempts: {poll_attempts}")
    print(f"Baseline PostgreSQL records: {baseline_count}")

    try:
        with connect_to_database() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT COUNT(*) FROM order_payment_events")
                final_count = cur.fetchone()[0]

                cur.execute(
                    SELECT_TEST_RECORD_SQL,
                    (TEST_ORDER_ID, 1),
                )
                final_check = cur.fetchone()

        print(f"Current PostgreSQL records: {final_count}")
        print(f"Final direct query result: {final_check}")

        if final_check is not None:
            print(
                "DIAGNOSTIC: The final query found the record after "
                "the polling loop ended. Verification timing or a "
                "transient database/query issue may be involved."
            )

    except psycopg.Error as exc:
        print(f"Final database diagnostic failed: {exc}")

    if last_database_error:
        print(f"Last database polling error: {last_database_error}")

    print(
        "\nInspect the existing Spark terminal before attempting "
        "another test run."
    )
    sys.exit(1)


print("TEST RESULT: PASS")
print("=" * 65)
print(f"Order ID: {found_record[0]}")
print(f"Customer ID: {found_record[1]}")
print(f"Order status: {found_record[2]}")
print(f"Payment sequence: {found_record[3]}")
print(f"Payment type: {found_record[4]}")
print(f"Installments: {found_record[5]}")
print(f"Payment value: {found_record[6]}")
print(f"Order event time: {found_record[7]}")
print(f"Payment event time: {found_record[8]}")
print(f"Verification time: {elapsed_total:.1f} seconds")
print(f"Polling attempts: {poll_attempts}")

with connect_to_database() as conn:
    with conn.cursor() as cur:
        cur.execute("SELECT COUNT(*) FROM order_payment_events")
        final_count = cur.fetchone()[0]

print(f"\nBaseline PostgreSQL records: {baseline_count}")
print(f"Current PostgreSQL records: {final_count}")
print("\nThe test record has been retained for inspection.")
print(f"Test order ID: {TEST_ORDER_ID}")
