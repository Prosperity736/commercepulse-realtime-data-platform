from datetime import datetime, timezone

from src.common.schemas import EventType, create_event_envelope
from src.common.config import get_settings
from src.producer.kafka_producer import CommercePulseKafkaProducer

ORDER_ID = "COMMERCEPULSE_E2E_TEST_20261106"

settings = get_settings()
producer = CommercePulseKafkaProducer()

event_time = datetime.now(timezone.utc).isoformat()

order = create_event_envelope(
    event_type=EventType.ORDER,
    event_time=event_time,
    data={
        "order_id": ORDER_ID,
        "customer_id": "COMMERCEPULSE_TEST_CUSTOMER",
        "order_status": "delivered",
    },
)

payment = create_event_envelope(
    event_type=EventType.PAYMENT,
    event_time=event_time,
    event_time_source="order_purchase_timestamp_proxy",
    data={
        "order_id": ORDER_ID,
        "payment_sequential": 1,
        "payment_type": "credit_card",
        "payment_installments": 1,
        "payment_value": 1.00,
    },
)

try:
    print("Publishing test order...")
    order_sent = producer.publish(
        settings.orders_topic,
        order,
        ORDER_ID,
    )
    print(f"Order acknowledged: {order_sent}")

    if not order_sent:
        raise RuntimeError("Order was not acknowledged. Payment not published.")

    print("\nPublishing matching test payment...")
    payment_sent = producer.publish(
        settings.payments_topic,
        payment,
        ORDER_ID,
    )
    print(f"Payment acknowledged: {payment_sent}")

    if not payment_sent:
        raise RuntimeError("Payment was not acknowledged.")

finally:
    producer.close()

print("\nControlled test publishing completed.")
print(f"Test order ID: {ORDER_ID}")
