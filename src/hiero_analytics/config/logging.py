"""Logging configuration for hiero_analytics.

This module centralizes basic logging setup for the application. The
log level can be configured via the ``LOG_LEVEL`` environment variable;
if it is not set, the default level ``INFO`` is used.
"""
import logging
import os


def setup_logging() -> None:
    level_name = os.getenv("LOG_LEVEL", "INFO").upper()
    level = getattr(logging, level_name, logging.INFO)

    logging.basicConfig(
        level=level,
        format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
        force=True,
    )