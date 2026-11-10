import json
from datetime import datetime, timezone

from kafka import KafkaProducer


# ==========================================
# KAFKA CONFIGURATION
# ==========================================

KAFKA_SERVER = "localhost:19092"
TOPIC = "olist-orders"


# ==========================================
# PRODUCER
# ==========================================

producer = KafkaProducer(
    bootstrap_servers=[KAFKA_SERVER],
    value_serializer=lambda value: json.dumps(value).encode("utf-8")
)


# ==========================================
# WRONG EVENT TYPE
# ==========================================

current_time = datetime.now(timezone.utc).isoformat()

wrong_event = {
    "event_type": "payment",
    "event_time": current_time,
    "processing_time": current_time,
    "data": {
        "order_id": "WRONG-EVENT-TYPE-001",
        "customer_id": "TEST-CUSTOMER-001",
        "order_status": "processing"
    }
}


# ==========================================
# SEND EVENT
# ==========================================

try:

    future = producer.send(
        TOPIC,
        value=wrong_event
    )

    metadata = future.get(timeout=10)

    print("\nWrong event type successfully sent!")

    print(f"Topic: {metadata.topic}")
    print(f"Partition: {metadata.partition}")
    print(f"Offset: {metadata.offset}")

    print(f"Event type: {wrong_event['event_type']}")

finally:

    producer.flush()
    producer.close()