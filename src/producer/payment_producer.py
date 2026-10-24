import json
import time
from pathlib import Path
from datetime import datetime, timezone

import pandas as pd
from kafka import KafkaProducer


# ==========================================
# 1. Configuration
# ==========================================

PROJECT_ROOT = Path(__file__).resolve().parents[2]

PAYMENTS_FILE = (
    PROJECT_ROOT
    / "data"
    / "raw"
    / "olist_order_payments_dataset.csv"
)

ORDERS_FILE = (
    PROJECT_ROOT
    / "data"
    / "raw"
    / "olist_orders_dataset.csv"
)

KAFKA_TOPIC = "olist-payments"
KAFKA_SERVER = "localhost:19092"

STREAM_DELAY_SECONDS = 0
PROGRESS_INTERVAL = 1000


# ==========================================
# 2. Load datasets
# ==========================================

print("Loading Olist payment and order datasets...")

payments = pd.read_csv(PAYMENTS_FILE)
orders = pd.read_csv(ORDERS_FILE)

print(f"Payment records loaded: {len(payments):,}")
print(f"Order records loaded: {len(orders):,}")


# ==========================================
# 3. Enrich payments with order timestamps
# ==========================================

orders_lookup = orders[
    [
        "order_id",
        "order_purchase_timestamp",
    ]
]

payments_enriched = payments.merge(
    orders_lookup,
    on="order_id",
    how="left",
    validate="many_to_one",
)

missing_order_time = (
    payments_enriched["order_purchase_timestamp"]
    .isna()
    .sum()
)

print(
    "Payments without matching order timestamp:",
    f"{missing_order_time:,}",
)


# ==========================================
# 4. Create Kafka producer
# ==========================================

producer = KafkaProducer(
    bootstrap_servers=KAFKA_SERVER,
    value_serializer=lambda value: json.dumps(
        value,
        default=str,
    ).encode("utf-8"),
    acks="all",
    retries=5,
)


# ==========================================
# 5. Publish all payment records
# ==========================================

sent_count = 0
failed_count = 0

try:
    for _, payment in payments_enriched.iterrows():

        order_id = payment["order_id"]
        order_timestamp = payment["order_purchase_timestamp"]

        # Skip records that cannot satisfy the Spark
        # event-time join because their order timestamp is missing.
        if pd.isna(order_timestamp):
            failed_count += 1
            continue

        payment_event_time = pd.to_datetime(
            order_timestamp,
            errors="coerce",
        )

        if pd.isna(payment_event_time):
            failed_count += 1
            continue

        payment_event = {
            "event_type": "payment",
            "event_time": payment_event_time.isoformat(),
            "processing_time": datetime.now(
                timezone.utc
            ).isoformat(),
            "data": {
                "order_id": str(order_id),
                "payment_sequential": int(
                    payment["payment_sequential"]
                ),
                "payment_type": str(
                    payment["payment_type"]
                ),
                "payment_installments": int(
                    payment["payment_installments"]
                ),
                "payment_value": float(
                    payment["payment_value"]
                ),
            },
        }

        try:
            producer.send(
                KAFKA_TOPIC,
                value=payment_event,
            ).get(timeout=60)

            sent_count += 1

        except Exception as exc:
            failed_count += 1
            print(
                f"Failed to send payment for order "
                f"{order_id}: {exc}"
            )

        if (
            sent_count > 0
            and sent_count % PROGRESS_INTERVAL == 0
        ):
            print(
                f"Payments published: {sent_count:,} "
                f"/ {len(payments_enriched):,}"
            )

        if STREAM_DELAY_SECONDS > 0:
            time.sleep(STREAM_DELAY_SECONDS)

except KeyboardInterrupt:
    print("\nPayment publishing interrupted by user.")

finally:
    producer.flush()
    producer.close()

    print("\nPAYMENT PRODUCER SUMMARY")
    print("-" * 35)
    print(f"Source records:      {len(payments_enriched):,}")
    print(f"Successfully sent:   {sent_count:,}")
    print(f"Failed or skipped:   {failed_count:,}")
    print(f"Kafka topic:         {KAFKA_TOPIC}")
    print("Producer closed.")

