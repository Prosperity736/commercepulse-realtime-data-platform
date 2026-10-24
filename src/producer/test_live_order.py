import json
from datetime import datetime, timezone

from kafka import KafkaProducer


producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
    value_serializer=lambda value: json.dumps(value).encode("utf-8")
)


event = {
    "event_type": "order",
    "event_time": "2018-08-08T08:45:00",
    "processing_time": datetime.now(timezone.utc).isoformat(),
    "data": {
        "order_id": "LIVE-TEST-001",
        "customer_id": "TEST-CUSTOMER-001",
        "order_status": "delivered",
        "order_purchase_timestamp": "2018-08-08 08:45:00",
        "order_approved_at": None,
        "order_delivered_carrier_date": None,
        "order_delivered_customer_date": None,
        "order_estimated_delivery_date": None
    }
}


producer.send(
    "olist-orders",
    value=event
)

producer.flush()
producer.close()

print("Live test order sent successfully.")