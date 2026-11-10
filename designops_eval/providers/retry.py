import os
import random
import time
from dataclasses import dataclass
from typing import Callable, TypeVar

from .errors import ErrorKind, classify_error, retry_after_seconds

T = TypeVar("T")

# Indirection so tests can replace the wait without touching time.sleep globally.
_sleep = time.sleep


@dataclass(frozen=True)
class RetryPolicy:
    max_attempts: int = 5
    base_delay: float = 0.5
    max_delay: float = 30.0

    @classmethod
    def from_env(cls) -> "RetryPolicy":
        return cls(
            max_attempts=int(os.getenv("DESIGNOPS_RETRY_MAX_ATTEMPTS", cls.max_attempts)),
            base_delay=float(os.getenv("DESIGNOPS_RETRY_BASE_DELAY", cls.base_delay)),
            max_delay=float(os.getenv("DESIGNOPS_RETRY_MAX_DELAY", cls.max_delay)),
        )


def backoff_delay(
    attempt: int,
    policy: RetryPolicy,
    rng: Callable[[], float] = random.random,
) -> float:
    """Full-jitter exponential backoff: uniform in [0, min(max, base * 2^(attempt-1))]."""
    ceiling = min(policy.max_delay, policy.base_delay * (2 ** (attempt - 1)))
    return rng() * ceiling


def call_with_retries(
    fn: Callable[[], T],
    *,
    policy: RetryPolicy | None = None,
    before_attempt: Callable[[], None] | None = None,
    sleep: Callable[[float], None] | None = None,
    rng: Callable[[], float] = random.random,
    on_retry: Callable[[int, BaseException, float], None] | None = None,
) -> T:
    """Call fn, retrying only errors that classify_error marks RETRYABLE.

    fn is invoked unchanged on every attempt; this helper never alters the
    request. before_attempt runs ahead of each attempt, including retries,
    which is where a rate limiter token is acquired.

    The wait before a retry is the larger of the jittered backoff and the
    provider's Retry-After hint. A Retry-After longer than max_delay is still
    honoured in full. After max_attempts the last error is re-raised as is,
    so the caller can classify it again.
    """
    policy = policy or RetryPolicy.from_env()
    attempt = 0
    while True:
        attempt += 1
        if before_attempt is not None:
            before_attempt()
        try:
            return fn()
        except Exception as exc:
            if classify_error(exc) is not ErrorKind.RETRYABLE:
                raise
            if attempt >= policy.max_attempts:
                raise
            delay = backoff_delay(attempt, policy, rng)
            hint = retry_after_seconds(exc)
            if hint is not None:
                delay = max(delay, hint)
            if on_retry is not None:
                on_retry(attempt, exc, delay)
            (sleep or _sleep)(delay)
