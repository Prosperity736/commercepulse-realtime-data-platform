import json
from datetime import datetime, timezone

from kafka import KafkaProducer

KAFKA_SERVER = "localhost:19092"
TOPIC = "olist-orders"

producer = KafkaProducer(
    bootstrap_servers=[KAFKA_SERVER],
    value_serializer=lambda value: json.dumps(value).encode("utf-8")
)

current_time = datetime.now(timezone.utc).isoformat()

invalid_order = {
    "event_type": "order",
    "event_time": "INVALID-ORDER-TIMESTAMP",
    "processing_time": current_time,
    "data": {
        "order_id": "INVALID-ORDER-TIMESTAMP-001",
        "customer_id": "TEST-CUSTOMER-001",
        "order_status": "processing"
    }
}

try:
    future = producer.send(TOPIC, value=invalid_order)
    metadata = future.get(timeout=10)

    print("Invalid order timestamp sent successfully!")
    print(f"Topic: {metadata.topic}")
    print(f"Partition: {metadata.partition}")
    print(f"Offset: {metadata.offset}")

finally:
    producer.flush()
    producer.close()