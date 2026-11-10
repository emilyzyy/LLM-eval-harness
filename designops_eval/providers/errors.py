from enum import Enum


class ProviderError(Exception):
    """A failed provider call, normalised across SDKs and the fake provider.

    status_code is the HTTP status when one exists. retry_after is the
    provider's Retry-After hint in seconds, when it sent one.
    """

    def __init__(
        self,
        message: str,
        *,
        status_code: int | None = None,
        retry_after: float | None = None,
    ) -> None:
        super().__init__(message)
        self.status_code = status_code
        self.retry_after = retry_after


class ErrorKind(Enum):
    RETRYABLE = "retryable"
    PERMANENT = "permanent"


_RETRYABLE_STATUSES = {408, 429}


def classify_error(exc: BaseException) -> ErrorKind:
    """Decide whether a failed provider call may be retried.

    This is the single place that policy lives. The retry helper retries only
    RETRYABLE errors; the runner and the worker record PERMANENT errors as
    failed cases.

    - HTTP 408, 429 and 5xx are RETRYABLE. Rate limits and timeouts must never
      be PERMANENT: a PERMANENT result is committed as a failure and skipped
      on every later delivery.
    - Any other 4xx is PERMANENT: it will fail identically on every attempt.
    - The status is read from a `status_code` attribute on any exception, so
      SDK errors that are not ProviderError subclasses are covered.
    - TimeoutError and ConnectionError (and subclasses) are RETRYABLE.
    - ValueError, raised before a request is sent, is PERMANENT.
    - Anything else is RETRYABLE. Retries are bounded and an unresolved shard
      ends in the dead-letter queue, where it is visible; a wrongly committed
      failure would be silent.
    """
    status = getattr(exc, "status_code", None)
    if isinstance(status, int):
        if status in _RETRYABLE_STATUSES or status >= 500:
            return ErrorKind.RETRYABLE
        if 400 <= status < 500:
            return ErrorKind.PERMANENT
    if isinstance(exc, (TimeoutError, ConnectionError)):
        return ErrorKind.RETRYABLE
    if isinstance(exc, ValueError):
        return ErrorKind.PERMANENT
    return ErrorKind.RETRYABLE


def retry_after_seconds(exc: BaseException) -> float | None:
    """Return the provider's Retry-After hint in seconds, if the error has one."""
    hint = getattr(exc, "retry_after", None)
    if hint is not None:
        try:
            return max(0.0, float(hint))
        except (TypeError, ValueError):
            return None

    headers = getattr(exc, "headers", None)
    if headers:
        for name in ("retry-after", "Retry-After"):
            try:
                value = headers.get(name)
            except AttributeError:
                return None
            if value is not None:
                try:
                    return max(0.0, float(value))
                except (TypeError, ValueError):
                    return None
    return None
