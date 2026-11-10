"""Tests for providers.errors.classify_error."""

import pytest

from designops_eval.providers.errors import ErrorKind, ProviderError, classify_error


class SdkStyleError(Exception):
    """Shaped like cohere's ApiError: a status_code, not a ProviderError."""

    def __init__(self, status_code: int) -> None:
        super().__init__(f"status {status_code}")
        self.status_code = status_code


@pytest.mark.parametrize("status", [408, 429, 500, 502, 503, 504])
def test_retryable_statuses(status):
    assert classify_error(ProviderError("x", status_code=status)) is ErrorKind.RETRYABLE


@pytest.mark.parametrize("status", [400, 401, 403, 404, 422])
def test_permanent_statuses(status):
    assert classify_error(ProviderError("x", status_code=status)) is ErrorKind.PERMANENT


@pytest.mark.parametrize("status,expected", [
    (429, ErrorKind.RETRYABLE),
    (503, ErrorKind.RETRYABLE),
    (400, ErrorKind.PERMANENT),
])
def test_status_is_read_from_any_exception_type(status, expected):
    assert classify_error(SdkStyleError(status)) is expected


@pytest.mark.parametrize("exc", [
    TimeoutError("timed out"),
    ConnectionError("refused"),
    ConnectionResetError("reset"),
    BrokenPipeError("pipe"),
])
def test_network_failures_are_retryable(exc):
    assert classify_error(exc) is ErrorKind.RETRYABLE


def test_rate_limit_is_retryable_even_with_retry_after():
    exc = ProviderError("slow down", status_code=429, retry_after=30)
    assert classify_error(exc) is ErrorKind.RETRYABLE


def test_value_error_is_permanent():
    assert classify_error(ValueError("Unknown Cohere model alias")) is ErrorKind.PERMANENT


def test_provider_error_without_status_is_retryable():
    assert classify_error(ProviderError("no status")) is ErrorKind.RETRYABLE


def test_unrecognised_exception_is_retryable():
    assert classify_error(RuntimeError("something else")) is ErrorKind.RETRYABLE
