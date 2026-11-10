"""
Centralized logging configuration for CommercePulse.

Provides consistent console and rotating file logging
across all application components.
"""

import logging
from logging.handlers import RotatingFileHandler
from pathlib import Path

from src.common.config import get_settings


def configure_logging() -> None:
    """
    Configure application-wide logging.

    Creates:
        1. Console logging.
        2. Rotating file logging.
    """

    root_logger = logging.getLogger()

    # Prevent duplicate handlers when configuration is called again.
    if getattr(root_logger, "_commercepulse_configured", False):
        return

    settings = get_settings()

    # Ensure the log directory exists.
    log_directory: Path = settings.log_directory
    log_directory.mkdir(parents=True, exist_ok=True)

    log_file = log_directory / "commercepulse.log"

    # Define a consistent log format.
    formatter = logging.Formatter(
        fmt=(
            "%(asctime)s | "
            "%(levelname)s | "
            "%(name)s | "
            "%(message)s"
        ),
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    # Console handler.
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)

    # Rotating file handler.
    file_handler = RotatingFileHandler(
        filename=log_file,
        maxBytes=10 * 1024 * 1024,
        backupCount=5,
        encoding="utf-8",
    )
    file_handler.setFormatter(formatter)

    # Configure the root logger.
    root_logger.setLevel(settings.log_level)

    root_logger.addHandler(console_handler)
    root_logger.addHandler(file_handler)

    # Mark configuration as complete.
    root_logger._commercepulse_configured = True

    root_logger.info(
        "CommercePulse logging initialized | Environment: %s",
        settings.environment,
    )


def get_logger(name: str) -> logging.Logger:
    """
    Return a configured logger for an application component.

    Example:
        logger = get_logger(__name__)
    """

    configure_logging()

    return logging.getLogger(name)