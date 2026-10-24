from src.utils.event_recovery import preserve_failed_event


test_event = {
    "event_type": "recovery_test",
    "event_time": "2026-10-24T10:00:00+01:00",
    "processing_time": "2026-10-24T10:01:00+01:00",
    "data": {
        "order_id": "commercepulse_recovery_test",
        "payment_sequential": 1,
        "payment_type": "test",
        "payment_installments": 1,
        "payment_value": 0.01,
    },
}

preserve_failed_event(
    source_topic="olist-payments",
    source_partition=999,
    source_offset=999999999,
    event=test_event,
    failure_reason="commercepulse_recovery_test",
)

print("Recovery test completed.")