"""Một cửa fatal duy nhất cho mọi entrypoint."""

import logging
import sys

from utils.logging import LOGGER_NAME, setup_logging

logger = logging.getLogger(LOGGER_NAME)


def handle_fatal_error(message, exc=None, code=1):
    """Log full stacktrace rồi exit code chuẩn để orchestrator nhận biết job fail."""
    setup_logging()
    if exc is not None:
        logger.error("FATAL: %s", message, exc_info=exc)
    else:
        logger.error("FATAL: %s", message)
    sys.exit(code)
