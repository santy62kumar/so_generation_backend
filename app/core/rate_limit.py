"""
rate_limit.py
─────────────────────────────────────────────────────────────────────────────
Per-client sliding-window throttle.

Before this, only /auth/login was throttled. The four heavy endpoints
(/process-xlsx and the three /generate-* routes) are unauthenticated and each
one spends a Chromium render or a full workbook parse, so anything that could
reach the port could loop them: two requests in flight fill every generation
slot and everyone else gets a 503. Body-size caps and the slot semaphore bound
how *big* and how *concurrent* the work is, never how *often* it arrives.

Per-process, like the render slots — see the note in main.py about workers.
"""

import logging
import os
import time
from collections import defaultdict, deque
from threading import Lock

from fastapi import HTTPException, Request

logger = logging.getLogger(__name__)

# request.client.host is the proxy's address behind a load balancer, which
# throttles every user as one bucket. Opt in only once the proxy is known to
# overwrite client-supplied copies of the header, otherwise a caller picks its
# own bucket by sending whatever it likes.
TRUST_PROXY_HEADER = os.getenv("TRUST_PROXY_HEADER", "").strip().lower() in {"1", "true", "yes"}

# Keys are client-controlled, so cap how many we will track.
MAX_TRACKED_CLIENTS = 10_000


def client_key(request: Request) -> str:
    if TRUST_PROXY_HEADER:
        forwarded = request.headers.get("x-forwarded-for", "")
        if forwarded:
            return forwarded.split(",")[0].strip()
    return request.client.host if request.client else "unknown"


def env_int(name: str, default: int, *, minimum: int = 1) -> int:
    raw = os.getenv(name)
    if raw is None or not raw.strip():
        return default
    try:
        value = int(raw)
    except ValueError as exc:
        raise RuntimeError(f"{name} must be a whole number.") from exc
    if value < minimum:
        raise RuntimeError(f"{name} must be at least {minimum}.")
    return value


class SlidingWindowLimiter:
    """`limit` events per `window` seconds per key.

    Split into check/record/reset rather than one call because the login
    throttle counts only failed attempts and clears the bucket on success,
    while the request throttle counts every arrival.
    """

    def __init__(self, name: str, limit: int, window: int):
        self.name = name
        self.limit = limit
        self.window = window
        self._events: dict[str, deque[float]] = defaultdict(deque)
        self._lock = Lock()

    def _prune(self, now: float) -> None:
        """Drop keys whose events have all aged out. Caller holds the lock."""
        for key, events in list(self._events.items()):
            while events and now - events[0] >= self.window:
                events.popleft()
            if not events:
                del self._events[key]

    def check(self, key: str, *, record: bool = False) -> None:
        """Raise 429 at the limit; optionally record this request atomically."""
        with self._lock:
            now = time.monotonic()
            self._prune(now)
            # .get avoids creating a bucket for every well-behaved caller.
            if len(self._events.get(key, ())) >= self.limit:
                logger.warning("Rate limit hit on %s by %s", self.name, key)
                raise HTTPException(
                    status_code=429,
                    detail=f"Too many requests. Try again in up to {self.window} seconds.",
                    headers={"Retry-After": str(self.window)},
                )
            if record and (key in self._events or len(self._events) < MAX_TRACKED_CLIENTS):
                self._events[key].append(now)

    def record(self, key: str) -> None:
        with self._lock:
            if key in self._events or len(self._events) < MAX_TRACKED_CLIENTS:
                self._events[key].append(time.monotonic())

    def reset(self, key: str) -> None:
        with self._lock:
            self._events.pop(key, None)

    def clear(self) -> None:
        with self._lock:
            self._events.clear()

    def dependency(self):
        """FastAPI dependency that counts every request through the route."""

        def guard(request: Request) -> None:
            self.check(client_key(request), record=True)

        return guard
