import json
import os
import time
from contextlib import contextmanager

import aws_xray_sdk.core as xray


def setup_logging():
    import logging

    level = os.environ.get("LOG_LEVEL", "INFO").upper()
    logging.basicConfig(level=level)
    return logging.getLogger("webhooks")


LOGGER = setup_logging()


@contextmanager
def timed(operation: str, **kwargs):
    start = time.time()
    try:
        yield
    finally:
        duration_ms = int((time.time() - start) * 1000)
        LOGGER.info(json.dumps({"operation": operation, "duration_ms": duration_ms, **kwargs}))


def capture_exception(ex: Exception):
    LOGGER.exception("Unhandled exception", exc_info=ex)
    xray.capture_exception()
