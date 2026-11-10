import json
from datetime import datetime, timezone

from kafka import KafkaProducer


# ==================================================
# KAFKA CONFIGURATION
# ==================================================

KAFKA_SERVER = "localhost:19092"
TOPIC = "olist-payments"


# ==================================================
# PRODUCER
# ==================================================

producer = KafkaProducer(
    bootstrap_servers=[KAFKA_SERVER],
    value_serializer=lambda value: json.dumps(value).encode("utf-8")
)


# ==================================================
# INVALID PAYMENT EVENT
# ==================================================

current_time = datetime.now(timezone.utc).isoformat()

invalid_payment = {
    "event_type": "payment",
    "event_time": current_time,
    "processing_time": current_time,
    "data": {
        "order_id": "INVALID-PAYMENT-001",
        "payment_sequential": 1,
        "payment_type": "credit_card",
        "payment_installments": 1,
        "payment_value": -25.50
    }
}


# ==================================================
# SEND EVENT
# ==================================================

try:

    future = producer.send(
        TOPIC,
        value=invalid_payment
    )

    metadata = future.get(timeout=10)

    print("\nInvalid payment event successfully sent!")

    print(f"Topic: {metadata.topic}")
    print(f"Partition: {metadata.partition}")
    print(f"Offset: {metadata.offset}")

    print("\nTest payment value: -25.50")

finally:

    producer.flush()
    producer.close()