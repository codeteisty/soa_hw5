import logging
import time

log = logging.getLogger(__name__)


def retry(operation, *, attempts=4, delay=0.5, exceptions=(Exception,)):
    for attempt in range(attempts):
        try:
            return operation()
        except exceptions as exc:
            if attempt == attempts - 1:
                raise
            wait = min(delay * 2**attempt, 8)
            log.warning("retry attempt=%s wait_seconds=%s error=%s", attempt + 1, wait, exc)
            time.sleep(wait)
