import json
from kafka import KafkaProducer

KAFKA_SERVER = "localhost:19092"
TOPIC = "olist-orders"

producer = KafkaProducer(
    bootstrap_servers=[KAFKA_SERVER],
    value_serializer=lambda value: json.dumps(value).encode("utf-8")
)

# Intentionally invalid: missing order_id and customer_id.
invalid_event = {
    "event_type": "order",
    "event_time": "2026-11-05T09:30:00",
    "processing_time": "2026-11-05T09:30:00",
    "data": {
        "order_status": "processing"
    }
}

future = producer.send(TOPIC, value=invalid_event)

metadata = future.get(timeout=10)

print("Invalid test event successfully sent!")
print(f"Topic: {metadata.topic}")
print(f"Partition: {metadata.partition}")
print(f"Offset: {metadata.offset}")

producer.flush()
producer.close()