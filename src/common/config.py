"""
Centralized configuration for CommercePulse.

Loads environment variables from the project root and provides
validated, immutable application settings.
"""

from __future__ import annotations

import os
from dataclasses import dataclass
from functools import lru_cache
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[2]


def _load_environment() -> None:
    """Load project environment variables."""

    try:
        from dotenv import load_dotenv
    except ImportError as exc:
        raise RuntimeError(
            "python-dotenv is required. Install project dependencies."
        ) from exc

    load_dotenv(PROJECT_ROOT / ".env", override=False)


def _get_required(name: str) -> str:
    """Retrieve a required environment variable."""

    value = os.getenv(name)

    if value is None or not value.strip():
        raise ValueError(
            f"Required environment variable '{name}' is missing."
        )

    return value.strip()


def _get_int(name: str, default: int) -> int:
    """Retrieve and validate an integer environment variable."""

    value = os.getenv(name)

    if value is None:
        return default

    try:
        result = int(value)
    except ValueError as exc:
        raise ValueError(
            f"Environment variable '{name}' must be an integer."
        ) from exc

    if result <= 0:
        raise ValueError(
            f"Environment variable '{name}' must be greater than zero."
        )

    return result


@dataclass(frozen=True, slots=True)
class Settings:
    """Immutable application configuration."""

    # Project
    project_name: str
    environment: str
    project_root: Path

    # Kafka
    kafka_bootstrap_servers: str
    orders_topic: str
    payments_topic: str
    dead_letter_topic: str

    # PostgreSQL
    db_host: str
    db_port: int
    db_name: str
    db_user: str
    db_password: str
    db_connect_timeout: int

    # Streaming
    spark_timezone: str
    spark_checkpoint_path: Path
    streaming_watermark: str

    # Logging
    log_level: str
    log_directory: Path


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    """
    Load and validate application settings.

    Settings are loaded once per process and cached.
    """

    _load_environment()

    environment = os.getenv(
        "APP_ENV",
        "development"
    ).strip().lower()

    log_level = os.getenv(
        "LOG_LEVEL",
        "INFO"
    ).strip().upper()

    valid_log_levels = {
        "DEBUG",
        "INFO",
        "WARNING",
        "ERROR",
        "CRITICAL",
    }

    if log_level not in valid_log_levels:
        raise ValueError(
            f"Invalid LOG_LEVEL: {log_level}"
        )

    settings = Settings(
        project_name="CommercePulse",
        environment=environment,
        project_root=PROJECT_ROOT,

        # Kafka
        kafka_bootstrap_servers=os.getenv(
            "KAFKA_BOOTSTRAP_SERVERS",
            "localhost:19092",
        ),
        orders_topic=os.getenv(
            "KAFKA_ORDERS_TOPIC",
            "olist-orders",
        ),
        payments_topic=os.getenv(
            "KAFKA_PAYMENTS_TOPIC",
            "olist-payments",
        ),
        dead_letter_topic=os.getenv(
            "KAFKA_DLQ_TOPIC",
            "commercepulse-dead-letter",
        ),

        # PostgreSQL
        db_host=_get_required("DB_HOST"),
        db_port=_get_int("DB_PORT", 5432),
        db_name=_get_required("DB_NAME"),
        db_user=_get_required("DB_USER"),
        db_password=_get_required("DB_PASSWORD"),
        db_connect_timeout=_get_int(
            "DB_CONNECT_TIMEOUT",
            10,
        ),

        # Streaming
        spark_timezone=os.getenv(
            "SPARK_TIMEZONE",
            "UTC",
        ),
        spark_checkpoint_path=(
            PROJECT_ROOT
            / "checkpoints"
            / "order_payment_postgres_v1"
        ),
        streaming_watermark=os.getenv(
            "STREAMING_WATERMARK",
            "10 minutes",
        ),

        # Logging
        log_level=log_level,
        log_directory=PROJECT_ROOT / "logs",
    )

    return settings