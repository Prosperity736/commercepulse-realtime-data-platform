import json
from kafka import KafkaConsumer, TopicPartition


BOOTSTRAP_SERVERS = "localhost:19092"

TARGET_ORDER_ID = "e481f51cbdc54678b7cc49136f2d6af7"

TOPICS = ["olist-orders", "olist-payments"]


def read_topic(topic):
    """Read existing Kafka records without committing offsets."""

    consumer = KafkaConsumer(
        bootstrap_servers=BOOTSTRAP_SERVERS,
        enable_auto_commit=False,
        group_id=None,
        consumer_timeout_ms=5000,
        value_deserializer=lambda value: json.loads(
            value.decode("utf-8")
        ),
    )

    partitions = consumer.partitions_for_topic(topic)

    if not partitions:
        print(f"\nNo partitions found for {topic}")
        consumer.close()
        return []

    topic_partitions = [
        TopicPartition(topic, partition)
        for partition in sorted(partitions)
    ]

    consumer.assign(topic_partitions)
    consumer.seek_to_beginning(*topic_partitions)

    records = []

    try:
        for message in consumer:
            records.append(message.value)
    finally:
        consumer.close()

    return records


def main():
    print("Starting lightweight Kafka join diagnostic...")
    print(f"Broker: {BOOTSTRAP_SERVERS}")

    orders = read_topic("olist-orders")
    payments = read_topic("olist-payments")

    print(f"\nOrder events read: {len(orders)}")
    print(f"Payment events read: {len(payments)}")

    target_orders = [
        event
        for event in orders
        if event.get("event_type") == "order"
        and event.get("data", {}).get("order_id") == TARGET_ORDER_ID
    ]

    target_payments = [
        event
        for event in payments
        if event.get("event_type") == "payment"
        and event.get("data", {}).get("order_id") == TARGET_ORDER_ID
    ]

    print(f"\nTarget order events: {len(target_orders)}")
    print(f"Target payment events: {len(target_payments)}")

    for order in target_orders:
        print("\nORDER")
        print("  order_id:", order.get("data", {}).get("order_id"))
        print("  event_time:", order.get("event_time"))
        print("  processing_time:", order.get("processing_time"))

    for payment in target_payments:
        data = payment.get("data", {})
        print("\nPAYMENT")
        print("  order_id:", data.get("order_id"))
        print("  payment_sequential:", data.get("payment_sequential"))
        print("  payment_type:", data.get("payment_type"))
        print("  payment_value:", data.get("payment_value"))
        print("  event_time:", payment.get("event_time"))
        print("  processing_time:", payment.get("processing_time"))

    matches = []

    for order in target_orders:
        for payment in target_payments:
            order_time = order.get("event_time")
            payment_time = payment.get("event_time")

            if order_time and payment_time:
                if order_time == payment_time:
                    matches.append((order, payment))

    print(f"\nExact timestamp matches: {len(matches)}")

    print("\nDiagnostic finished.")


if __name__ == "__main__":
    main()

