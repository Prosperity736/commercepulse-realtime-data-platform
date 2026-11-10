"""
Shared Kafka producer infrastructure for CommercePulse.

Responsibilities:
- Initialize Kafka connections.
- Serialize event envelopes.
- Publish events using Kafka message keys.
- Wait for broker acknowledgements.
- Log delivery results.
"""

import json
from typing import Any

from kafka import KafkaProducer
from kafka.errors import KafkaError

from src.common.config import get_settings
from src.common.logging import get_logger


logger = get_logger(__name__)


class CommercePulseKafkaProducer:
    """Reusable Kafka producer for CommercePulse."""

    def __init__(self):
        settings = get_settings()

        self.producer = KafkaProducer(
            bootstrap_servers=settings.kafka_bootstrap_servers.split(","),
            key_serializer=lambda key: key.encode("utf-8"),
            value_serializer=lambda value: json.dumps(
                value,
                ensure_ascii=False,
                allow_nan=False,
                separators=(",", ":"),
            ).encode("utf-8"),
            acks="all",
            retries=10,
            max_in_flight_requests_per_connection=5,
            enable_idempotence=True,
            compression_type="gzip",
            linger_ms=20,
            request_timeout_ms=30000,
            delivery_timeout_ms=120000,
        )

        logger.info(
            "Kafka producer initialized | Brokers: %s",
            settings.kafka_bootstrap_servers,
        )

    def publish(
        self,
        topic: str,
        event: dict[str, Any],
        message_key: str,
        timeout: int = 60,
    ) -> bool:
        """
        Publish one event and wait for broker acknowledgement.

        Returns True when Kafka acknowledges the message.
        """

        if not topic:
            raise ValueError("Kafka topic cannot be empty.")

        if not message_key:
            raise ValueError("Kafka message key cannot be empty.")

        try:
            future = self.producer.send(
                topic=topic,
                key=message_key,
                value=event,
            )

            metadata = future.get(timeout=timeout)

            logger.debug(
                "Event delivered | Topic: %s | Partition: %s | Offset: %s",
                metadata.topic,
                metadata.partition,
                metadata.offset,
            )

            return True

        except (KafkaError, TimeoutError, OSError) as exc:
            logger.exception(
                "Kafka delivery failed | Topic: %s | Key: %s | Error: %s",
                topic,
                message_key,
                exc,
            )

            return False

    def flush(self) -> None:
        """Wait for outstanding messages to be delivered."""

        self.producer.flush()

    def close(self) -> None:
        """Flush outstanding messages and close the connection."""

        try:
            self.producer.flush()
        finally:
            self.producer.close()

        logger.info("Kafka producer connection closed.")