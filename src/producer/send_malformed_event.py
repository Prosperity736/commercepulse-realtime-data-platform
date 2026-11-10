from kafka import KafkaProducer

KAFKA_SERVER = "localhost:19092"
TOPIC = "olist-orders"

producer = KafkaProducer(
    bootstrap_servers=[KAFKA_SERVER]
)

# Intentionally malformed JSON.
# Missing quotes and closing braces.
malformed_event = b'{"event_type": "order", "event_time": "2026-11-05T09:30:00", "data": {"order_id": "BROKEN-001" '

try:
    future = producer.send(
        TOPIC,
        value=malformed_event
    )

    metadata = future.get(timeout=10)

    print("Malformed event successfully sent!")
    print(f"Topic: {metadata.topic}")
    print(f"Partition: {metadata.partition}")
    print(f"Offset: {metadata.offset}")

finally:
    producer.flush()
    producer.close()