import json
from datetime import datetime, timezone
from kafka import KafkaProducer

producer = KafkaProducer(
    bootstrap_servers="localhost:19092",
    value_serializer=lambda value: json.dumps(value).encode("utf-8")
)

event = {
    "event_type": "payment",
   "event_time": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S"),
    "processing_time": datetime.now(timezone.utc).isoformat(),
    "data": {
        "order_id": "LIVE-TEST-003",
        "payment_sequential": 1,
        "payment_type": "credit_card",
        "payment_installments": 1,
        "payment_value": 1.23,
        "order_match_status": "matched"
    }
}

producer.send(
    "olist-payments",
    value=event
)

producer.flush()
producer.close()

print("Live test payment sent successfully.")