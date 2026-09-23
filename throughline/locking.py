"""Waiting out DuckDB's one-writer-per-file lock.

DuckDB takes an exclusive write lock on a database file, so two processes that
both want to write the same file cannot: the second one raises rather than
queues. Under a LocalExecutor every task is its own process, so this is not an
exotic case — two DAGs triggered together, or two tasks of one DAG running in
parallel, hit it immediately.

Retrying is the right answer rather than a workaround, because the lock is
held for the length of one statement. The contention is real but brief, and a
short backoff turns a hard failure into a wait of a few hundred milliseconds.

What this deliberately does *not* do is retry anything else. A permission
error or a malformed query is not going to improve after twenty seconds of
backoff, and hiding it behind one is worse than failing.
"""

from __future__ import annotations

import time
from collections.abc import Callable
from typing import TypeVar

T = TypeVar("T")

#: Twelve attempts on a rising backoff is about twenty seconds in total, which
#: comfortably outlasts any single statement this project writes.
RETRIES = 12
BACKOFF = 0.25


def is_lock_conflict(exc: Exception) -> bool:
    """Whether an exception is another process holding the write lock."""
    text = str(exc).lower()
    return "lock" in text or "being used by another" in text or "conflict" in text


def with_retry(open_it: Callable[[], T], what: str) -> T:
    """Call ``open_it``, waiting out lock conflicts and nothing else."""
    last: Exception | None = None
    for attempt in range(RETRIES):
        try:
            return open_it()
        except Exception as exc:
            if not is_lock_conflict(exc):
                raise
            last = exc
            time.sleep(BACKOFF * (attempt + 1))
    raise RuntimeError(f"could not open {what}: still locked after {RETRIES} attempts") from last
