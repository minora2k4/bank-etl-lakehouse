"""Logging tập trung qua stdlib (không dùng print).

Giữ nguyên API `log_info(event, **fields)` mà toàn project đang dùng, nhưng định tuyến
qua `logging` để có timestamp, level và có thể gắn handler/format một chỗ.
"""

import logging
import sys

LOGGER_NAME = "bank_etl"
_NOISY_LIBS = ("py4j", "kafka", "botocore", "urllib3", "s3transfer")
_configured = False


def setup_logging(level=logging.INFO):
    """Cấu hình logger một lần (idempotent): ghi stdout, format gọn, bịt log ồn của lib."""
    global _configured
    if _configured:
        return
    # Log tiếng Việt: ép UTF-8 để không vỡ trên console cp1252 (Windows); Docker đã sẵn UTF-8.
    try:
        sys.stdout.reconfigure(encoding="utf-8")
    except (AttributeError, ValueError):
        pass
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(
        logging.Formatter("%(asctime)s %(levelname)s %(name)s %(message)s", datefmt="%Y-%m-%d %H:%M:%S")
    )
    logger = logging.getLogger(LOGGER_NAME)
    logger.setLevel(level)
    logger.handlers.clear()
    logger.addHandler(handler)
    logger.propagate = False
    for noisy in _NOISY_LIBS:
        logging.getLogger(noisy).setLevel(logging.WARNING)
    _configured = True


def log_info(event, **fields):
    """Log một event dạng key=value ngắn gọn, có cấu trúc."""
    setup_logging()
    parts = [f"event={event}"]
    parts.extend(f"{key}={value}" for key, value in fields.items())
    logging.getLogger(LOGGER_NAME).info(" ".join(parts))
