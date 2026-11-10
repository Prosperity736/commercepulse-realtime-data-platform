import json
from datetime import datetime, timezone

from kafka import KafkaProducer


KAFKA_SERVER = "localhost:19092"
TOPIC = "olist-payments"


producer = KafkaProducer(
    bootstrap_servers=[KAFKA_SERVER],
    value_serializer=lambda value: json.dumps(value).encode("utf-8")
)


invalid_payment = {
    "event_type": "payment",
    "event_time": "NOT-A-VALID-TIMESTAMP",
    "processing_time": datetime.now(timezone.utc).isoformat(),
    "data": {
        "order_id": "INVALID-TIMESTAMP-001",
        "payment_sequential": 1,
        "payment_type": "credit_card",
        "payment_installments": 1,
        "payment_value": 150.00
    }
}


try:
    future = producer.send(
        TOPIC,
        value=invalid_payment
    )

    metadata = future.get(timeout=10)

    print("Invalid timestamp event sent successfully!")
    print(f"Topic: {metadata.topic}")
    print(f"Partition: {metadata.partition}")
    print(f"Offset: {metadata.offset}")

finally:
    producer.flush()
    producer.close()