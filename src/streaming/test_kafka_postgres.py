import json
import os
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
    setting
    for setting in REQUIRED_SETTINGS
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

TEST_RUN_ID = uuid.uuid4().hex[:12]
TEST_ORDER_ID = f"commercepulse_e2e_{TEST_RUN_ID}"
TEST_CUSTOMER_ID = "commercepulse_test_customer"

# Use a naive UTC timestamp for consistency with the Spark
# session timezone configured in stream_to_postgres.py.
BASE_EVENT_TIME = (
    datetime.now(timezone.utc)
    .replace(tzinfo=None, microsecond=0)
)

print("=" * 60)
print("COMMERCEPULSE KAFKA-TO-POSTGRESQL INTEGRATION TEST")
print("=" * 60)
print(f"Test order ID: {TEST_ORDER_ID}")
print(f"Base event time: {BASE_EVENT_TIME.isoformat()}")


# ============================================================
# 2. VERIFY POSTGRESQL CONNECTION
# ============================================================

with psycopg.connect(**DB_CONFIG) as conn:
    with conn.cursor() as cur:
        cur.execute(
            "SELECT COUNT(*) FROM order_payment_events"
        )
        baseline_count = cur.fetchone()[0]

print(f"Baseline PostgreSQL records: {baseline_count}")


# ============================================================
# 3. EVENT BUILDERS
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
# 4. WAIT FOR SPARK READINESS
# ============================================================

print("\nWaiting for the Spark streaming query...")

deadline = time.time() + 120

while time.time() < deadline:
    if READY_FILE.is_file():
        break

    time.sleep(1)
else:
    raise TimeoutError(
        "Spark readiness signal was not received within "
        "120 seconds. Check the stream_to_postgres.py terminal."
    )

print("Spark readiness signal received.")


# ============================================================
# 5. PUBLISH THE MATCHING ORDER AND PAYMENT
# ============================================================

producer = KafkaProducer(
    bootstrap_servers=KAFKA_SERVER,
    value_serializer=lambda value: json.dumps(
        value
    ).encode("utf-8"),
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

    # Publish distinct marker events at later event times.
    # These are not expected to create PostgreSQL join records.
    # Their purpose is to advance the input event-time watermarks.
    print("\nPublishing watermark-advancement markers...")

    for marker_number, minutes_ahead in enumerate((20, 40), start=1):
        marker_time = BASE_EVENT_TIME + timedelta(
            minutes=minutes_ahead
        )

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
# 6. WAIT FOR THE EXPECTED RECORD IN POSTGRESQL
# ============================================================

print("\nWaiting for the joined test record in PostgreSQL...")

deadline = time.time() + 180
found_record = None

while time.time() < deadline:
    with psycopg.connect(**DB_CONFIG) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
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
                """,
                (TEST_ORDER_ID, 1),
            )

            found_record = cur.fetchone()

    if found_record is not None:
        break

    time.sleep(2)


# ============================================================
# 7. REPORT THE RESULT
# ============================================================

if found_record is None:
    print("\n" + "=" * 60)
    print("TEST RESULT: NOT CONFIRMED")
    print("=" * 60)
    print(
        "The expected record was not found within 180 seconds."
    )
    print("Inspect the Spark terminal before retrying.")
    print(f"Test order ID: {TEST_ORDER_ID}")

else:
    print("\n" + "=" * 60)
    print("PASS: TEST RECORD FOUND IN POSTGRESQL")
    print("=" * 60)

    print(f"Order ID: {found_record[0]}")
    print(f"Customer ID: {found_record[1]}")
    print(f"Order status: {found_record[2]}")
    print(f"Payment sequence: {found_record[3]}")
    print(f"Payment type: {found_record[4]}")
    print(f"Installments: {found_record[5]}")
    print(f"Payment value: {found_record[6]}")
    print(f"Order event time: {found_record[7]}")
    print(f"Payment event time: {found_record[8]}")

    with psycopg.connect(**DB_CONFIG) as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT COUNT(*) FROM order_payment_events"
            )
            final_count = cur.fetchone()[0]

    print(f"\nBaseline records: {baseline_count}")
    print(f"Current records: {final_count}")

    print(
        "\nThe test record has been retained for inspection."
    )
    print(f"Test order ID: {TEST_ORDER_ID}")