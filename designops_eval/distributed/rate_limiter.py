"""Provider-level rate limiting shared across workers.

Every limiter exposes acquire(), which blocks until one request may be sent.
Workers call it ahead of each provider attempt, retries included.

Table schema for the shared limiter (infra/terraform/distributed.tf):

    partition key  provider  (S)

One item per provider holds that provider's bucket state:

    tokens_micro  (N)  tokens available at updated_ms, in millionths of a token
    updated_ms    (N)  wall-clock time the token count was computed, in ms

Integers are used because the DynamoDB resource API rejects float.
"""

import random
import threading
import time
from dataclasses import dataclass
from typing import Callable, Protocol

from boto3.dynamodb.conditions import Attr
from botocore.exceptions import ClientError

_MICRO = 1_000_000


class RateLimitTimeout(Exception):
    """acquire() gave up waiting for capacity."""


@dataclass(frozen=True)
class BucketConfig:
    capacity: float
    refill_per_second: float

    def __post_init__(self) -> None:
        if self.capacity < 1:
            raise ValueError("capacity must be at least 1")
        if self.refill_per_second <= 0:
            raise ValueError("refill_per_second must be positive")

    @classmethod
    def from_limit(
        cls,
        limit: int,
        window_seconds: float = 60.0,
        burst: int | None = None,
    ) -> "BucketConfig":
        """Configure a bucket that stays within `limit` requests per sliding window.

        A bucket of capacity B refilled at r per second can grant up to
        B + r * W requests in a window of W seconds: a full bucket spent at
        the start, plus everything refilled during the window. Setting
        r = limit / W therefore overshoots by B. This uses
        r = (limit - B) / W so the worst case is exactly `limit`.

        burst defaults to 5% of the limit, at least 1.
        """
        if burst is None:
            burst = max(1, limit // 20)
        if not 1 <= burst < limit:
            raise ValueError("burst must be at least 1 and less than limit")
        return cls(capacity=float(burst), refill_per_second=(limit - burst) / window_seconds)


@dataclass(frozen=True)
class AcquireResult:
    acquired: bool
    # When not acquired: seconds until one token will be available.
    retry_after: float = 0.0


class Limiter(Protocol):
    def acquire(self, timeout: float | None = None) -> float: ...


class DynamoTokenBucket:
    """A token bucket whose state is one DynamoDB item, shared by all workers.

    The bucket holds up to config.capacity tokens and gains
    config.refill_per_second tokens per second. Each request spends one. A
    provider with no item yet has a full bucket.

    No worker holds a lock. Each one reads the item, computes the new state,
    and writes it back only if the item is unchanged since the read
    (optimistic concurrency). A worker that loses the race re-reads.

    Each worker uses its own wall clock, so skew between workers shifts refill
    accounting by the size of the skew. A clock behind the stored timestamp
    is treated as no elapsed time, and the stored timestamp never moves
    backwards, so a slow clock cannot mint tokens.
    """

    def __init__(
        self,
        table,
        provider: str,
        config: BucketConfig,
        clock: Callable[[], float] = time.time,
        sleep: Callable[[float], None] = time.sleep,
        rng: Callable[[], float] = random.random,
    ) -> None:
        """table is a boto3 DynamoDB Table resource keyed on `provider`."""
        self._table = table
        self.provider = provider
        self.config = config
        self._clock = clock
        self._sleep = sleep
        self._rng = rng

    def try_acquire(self) -> AcquireResult:
        """Take one token if one is available, without waiting for capacity.

        Contention (another worker changed the item between this worker's
        read and write) is retried internally and never reported as an empty
        bucket. Errors other than the conditional-check failure propagate.
        """
        capacity = int(self.config.capacity * _MICRO)
        refill_per_ms = self.config.refill_per_second * _MICRO / 1000

        while True:
            item = self._table.get_item(
                Key={"provider": self.provider}, ConsistentRead=True
            ).get("Item")
            now_ms = int(self._clock() * 1000)

            if item is None:
                tokens = capacity
                updated_ms = now_ms
                condition = Attr("provider").not_exists()
            else:
                stored_tokens = int(item["tokens_micro"])
                stored_ms = int(item["updated_ms"])
                elapsed_ms = max(0, now_ms - stored_ms)
                tokens = min(capacity, stored_tokens + int(elapsed_ms * refill_per_ms))
                updated_ms = max(now_ms, stored_ms)
                condition = (
                    Attr("tokens_micro").eq(stored_tokens) & Attr("updated_ms").eq(stored_ms)
                )

            if tokens < _MICRO:
                missing = (_MICRO - tokens) / _MICRO
                return AcquireResult(False, missing / self.config.refill_per_second)

            try:
                self._table.put_item(
                    Item={
                        "provider": self.provider,
                        "tokens_micro": tokens - _MICRO,
                        "updated_ms": updated_ms,
                    },
                    ConditionExpression=condition,
                )
                return AcquireResult(True)
            except ClientError as exc:
                if exc.response["Error"]["Code"] != "ConditionalCheckFailedException":
                    raise

    def acquire(self, timeout: float | None = None) -> float:
        """Block until a token is acquired. Returns the seconds spent waiting.

        Sleeps for about the reported retry_after plus up to 10% jitter, so
        waiting workers do not wake in the same instant and collide again.
        With a timeout, raises RateLimitTimeout as soon as the next wait would
        exceed it, without taking a token.
        """
        start = self._clock()
        while True:
            result = self.try_acquire()
            waited = self._clock() - start
            if result.acquired:
                return waited
            delay = result.retry_after * (1 + 0.1 * self._rng()) + 0.001
            if timeout is not None and waited + delay > timeout:
                raise RateLimitTimeout(
                    f"no {self.provider} token within {timeout:.3f}s"
                )
            self._sleep(delay)


class LocalPacer:
    """A process-local limiter that spaces requests at a fixed interval.

    Each worker process has its own, so N workers together send N times the
    configured rate. It exists as the comparison case for the shared limiter.
    """

    def __init__(
        self,
        requests_per_second: float,
        clock: Callable[[], float] = time.monotonic,
        sleep: Callable[[float], None] = time.sleep,
    ) -> None:
        if requests_per_second <= 0:
            raise ValueError("requests_per_second must be positive")
        self._interval = 1.0 / requests_per_second
        self._clock = clock
        self._sleep = sleep
        self._lock = threading.Lock()
        self._next_slot = 0.0

    def acquire(self, timeout: float | None = None) -> float:
        with self._lock:
            now = self._clock()
            slot = max(now, self._next_slot)
            wait = slot - now
            if timeout is not None and wait > timeout:
                raise RateLimitTimeout(f"local pacer wait {wait:.3f}s exceeds timeout")
            self._next_slot = slot + self._interval
        if wait > 0:
            self._sleep(wait)
        return wait


class NullLimiter:
    """No limiting. Used for providers with no configured limit."""

    def acquire(self, timeout: float | None = None) -> float:
        return 0.0
