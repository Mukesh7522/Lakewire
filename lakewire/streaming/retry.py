"""Retry for Delta commits that lose an optimistic-concurrency race.

Inside the streaming driver the Silver MERGE and the periodic OPTIMIZE both commit to the same table.
Delta aborts whichever commits second with a ConcurrentModification error and says "try again".
The MERGE is insert-only on event_id, so retrying it is idempotent.
"""
from __future__ import annotations

import logging
import time
from collections.abc import Callable
from typing import TypeVar

log = logging.getLogger("lakewire.retry")

T = TypeVar("T")

CONFLICT_MARKERS = (
    "ConcurrentAppendException",
    "ConcurrentDeleteReadException",
    "ConcurrentDeleteDeleteException",
    "ConcurrentTransactionException",
    "DELTA_CONCURRENT_",
)


def is_concurrent_conflict(exc: BaseException) -> bool:
    text = f"{type(exc).__name__}: {exc}"
    return any(marker in text for marker in CONFLICT_MARKERS)


def retry_on_conflict(fn: Callable[[], T], attempts: int = 5, base_delay: float = 1.0,
                      sleep: Callable[[float], None] = time.sleep) -> T:
    """Run fn, retrying only on Delta conflict errors. Any other error, or the last conflict, is raised."""
    for attempt in range(1, attempts + 1):
        try:
            return fn()
        except Exception as exc:
            if attempt == attempts or not is_concurrent_conflict(exc):
                raise
            delay = base_delay * attempt
            log.warning("Delta commit conflict (attempt %d/%d), retrying in %.0fs", attempt, attempts, delay)
            sleep(delay)
    raise AssertionError("unreachable")  # pragma: no cover
