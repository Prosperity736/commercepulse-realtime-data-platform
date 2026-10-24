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

ORDERS_FILE = PROJECT_ROOT / "data" / "raw" / "olist_orders_dataset.csv"

KAFKA_TOPIC = "olist-orders"
KAFKA_BOOTSTRAP_SERVERS = "localhost:19092"

# Set to 0 to publish as quickly as Kafka permits.
# Use a positive value to simulate slower, real-time arrivals.
STREAM_DELAY_SECONDS = 0

PROGRESS_INTERVAL = 1000


# ==========================================
# 2. Load orders
# ==========================================

print("Loading Olist orders...")

orders = pd.read_csv(ORDERS_FILE)

print(f"Total orders loaded: {len(orders):,}")


# ==========================================
# 3. Create Kafka producer
# ==========================================

producer = KafkaProducer(
    bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
    value_serializer=lambda value: json.dumps(
        value,
        default=str
    ).encode("utf-8"),
    acks="all",
    retries=5,
)


# ==========================================
# 4. Publish all orders
# ==========================================

sent_count = 0
failed_count = 0

try:
    for _, order in orders.iterrows():

        order_data = order.to_dict()

        # Convert order timestamp to ISO 8601.
        event_time = pd.to_datetime(
            order_data["order_purchase_timestamp"],
            errors="coerce"
        )

        if pd.isna(event_time):
            print(
                "Skipping order with invalid event timestamp:",
                order_data.get("order_id")
            )
            failed_count += 1
            continue

        event_time = event_time.isoformat()

        # Preserve the existing event envelope.
        event = {
            "event_type": "order",
            "event_time": event_time,
            "processing_time": datetime.now(
                timezone.utc
            ).isoformat(),
            "data": order_data,
        }

        # Wait for the broker acknowledgement.
        try:
            producer.send(
                KAFKA_TOPIC,
                value=event
            ).get(timeout=60)

            sent_count += 1

        except Exception as exc:
            failed_count += 1
            print(
                f"Failed to send order "
                f"{order_data.get('order_id')}: {exc}"
            )

        # Report progress periodically.
        if sent_count > 0 and sent_count % PROGRESS_INTERVAL == 0:
            print(
                f"Orders published: {sent_count:,} "
                f"/ {len(orders):,}"
            )

        if STREAM_DELAY_SECONDS > 0:
            time.sleep(STREAM_DELAY_SECONDS)

except KeyboardInterrupt:
    print("\nPublishing interrupted by user.")

finally:
    producer.flush()
    producer.close()

    print("\nORDER PRODUCER SUMMARY")
    print("-" * 35)
    print(f"Source records:      {len(orders):,}")
    print(f"Successfully sent:   {sent_count:,}")
    print(f"Failed or skipped:   {failed_count:,}")
    print(f"Kafka topic:         {KAFKA_TOPIC}")
    print("Producer closed.")

