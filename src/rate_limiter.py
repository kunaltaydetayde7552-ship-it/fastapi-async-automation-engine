"""
Token bucket rate limiter implementation for FastAPI asynchronous routes.
Controls request concurrency and prevents downstream endpoint saturation.
"""

import asyncio
import time
from typing import Dict, Optional


class TokenBucketLimiter:
    """
    Thread-safe token bucket rate limiter supporting per-client or global quotas.
    Tokens refill continuously at refill_rate per second up to max capacity.
    """

    def __init__(self, capacity: int = 10, refill_rate: float = 5.0):
        if capacity <= 0:
            raise ValueError("Capacity must be greater than zero.")
        if refill_rate <= 0:
            raise ValueError("Refill rate must be greater than zero.")

        self.capacity = float(capacity)
        self.refill_rate = float(refill_rate)
        self._lock = asyncio.Lock()
        self._buckets: Dict[str, Dict[str, float]] = {}

    def _get_bucket(self, key: str, current_time: float) -> Dict[str, float]:
        if key not in self._buckets:
            self._buckets[key] = {
                "tokens": self.capacity,
                "last_refill": current_time,
            }
        return self._buckets[key]

    async def check_allow(self, client_id: str = "default", tokens: float = 1.0) -> bool:
        """
        Non-blocking token check.
        Returns True if tokens are available and consumed, False otherwise.
        """
        async with self._lock:
            now = time.monotonic()
            bucket = self._get_bucket(client_id, now)

            # Refill tokens based on elapsed time
            elapsed = now - bucket["last_refill"]
            if elapsed > 0:
                refilled = elapsed * self.refill_rate
                bucket["tokens"] = min(self.capacity, bucket["tokens"] + refilled)
                bucket["last_refill"] = now

            if bucket["tokens"] >= tokens:
                bucket["tokens"] -= tokens
                return True
            return False

    async def get_available_tokens(self, client_id: str = "default") -> float:
        """Returns the current estimated available tokens without consuming them."""
        async with self._lock:
            now = time.monotonic()
            bucket = self._get_bucket(client_id, now)
            elapsed = now - bucket["last_refill"]
            if elapsed > 0:
                refilled = elapsed * self.refill_rate
                return min(self.capacity, bucket["tokens"] + refilled)
            return bucket["tokens"]

    def reset(self, client_id: Optional[str] = None) -> None:
        """Resets bucket state for testing or administrative actions."""
        if client_id is not None:
            self._buckets.pop(client_id, None)
        else:
            self._buckets.clear()
