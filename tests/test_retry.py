"""Tests for the retry helper.

classify_error is replaced with a local stand-in here so that these tests
exercise only the helper's own behaviour.
"""

import pytest

from designops_eval.providers import retry
from designops_eval.providers.errors import ErrorKind, ProviderError, retry_after_seconds
from designops_eval.providers.retry import RetryPolicy, backoff_delay, call_with_retries


class Flaky(Exception):
    pass


class Fatal(Exception):
    pass


@pytest.fixture(autouse=True)
def stand_in_classifier(monkeypatch):
    monkeypatch.setattr(
        retry,
        "classify_error",
        lambda exc: ErrorKind.RETRYABLE if isinstance(exc, (Flaky, ProviderError)) else ErrorKind.PERMANENT,
    )


def failing_then(result, *errors):
    remaining = list(errors)
    calls = {"n": 0}

    def fn():
        calls["n"] += 1
        if remaining:
            raise remaining.pop(0)
        return result

    return fn, calls


POLICY = RetryPolicy(max_attempts=4, base_delay=1.0, max_delay=5.0)


def test_returns_immediately_on_success():
    fn, calls = failing_then("ok")
    sleeps: list[float] = []
    assert call_with_retries(fn, policy=POLICY, sleep=sleeps.append) == "ok"
    assert calls["n"] == 1 and sleeps == []


def test_retries_retryable_errors_until_success():
    fn, calls = failing_then("ok", Flaky(), Flaky())
    sleeps: list[float] = []
    assert call_with_retries(fn, policy=POLICY, sleep=sleeps.append) == "ok"
    assert calls["n"] == 3 and len(sleeps) == 2


def test_permanent_error_is_not_retried():
    fn, calls = failing_then("ok", Fatal("no"))
    with pytest.raises(Fatal):
        call_with_retries(fn, policy=POLICY, sleep=lambda s: None)
    assert calls["n"] == 1


def test_attempts_are_bounded_and_last_error_is_raised():
    errors = [Flaky(str(i)) for i in range(10)]
    fn, calls = failing_then("ok", *errors)
    with pytest.raises(Flaky) as info:
        call_with_retries(fn, policy=POLICY, sleep=lambda s: None)
    assert calls["n"] == POLICY.max_attempts
    assert info.value is errors[POLICY.max_attempts - 1]


def test_backoff_grows_exponentially_and_is_capped():
    delays = [backoff_delay(n, POLICY, rng=lambda: 1.0) for n in range(1, 6)]
    assert delays == [1.0, 2.0, 4.0, 5.0, 5.0]


def test_backoff_uses_full_jitter():
    assert backoff_delay(3, POLICY, rng=lambda: 0.0) == 0.0
    assert backoff_delay(3, POLICY, rng=lambda: 0.25) == 1.0


def test_retry_after_is_honoured_when_longer_than_backoff():
    fn, _ = failing_then("ok", ProviderError("slow down", status_code=429, retry_after=12))
    sleeps: list[float] = []
    call_with_retries(fn, policy=POLICY, sleep=sleeps.append, rng=lambda: 1.0)
    assert sleeps == [12.0]  # beyond max_delay, still honoured in full


def test_backoff_is_used_when_longer_than_retry_after():
    fn, _ = failing_then("ok", ProviderError("slow down", status_code=429, retry_after=0.2))
    sleeps: list[float] = []
    call_with_retries(fn, policy=POLICY, sleep=sleeps.append, rng=lambda: 1.0)
    assert sleeps == [1.0]


def test_before_attempt_runs_ahead_of_every_attempt():
    order: list[str] = []
    fn, _ = failing_then("ok", Flaky(), Flaky())

    def traced():
        order.append("call")
        return fn()

    call_with_retries(traced, policy=POLICY, sleep=lambda s: None,
                      before_attempt=lambda: order.append("before"))
    assert order == ["before", "call"] * 3


def test_on_retry_reports_attempt_error_and_delay():
    error = Flaky()
    fn, _ = failing_then("ok", error)
    seen = []
    call_with_retries(fn, policy=POLICY, sleep=lambda s: None, rng=lambda: 0.5,
                      on_retry=lambda attempt, exc, delay: seen.append((attempt, exc, delay)))
    assert seen == [(1, error, 0.5)]


def test_policy_from_env(monkeypatch):
    monkeypatch.setenv("DESIGNOPS_RETRY_MAX_ATTEMPTS", "7")
    monkeypatch.setenv("DESIGNOPS_RETRY_BASE_DELAY", "0.25")
    monkeypatch.setenv("DESIGNOPS_RETRY_MAX_DELAY", "9")
    assert RetryPolicy.from_env() == RetryPolicy(7, 0.25, 9.0)


class TestRetryAfterSeconds:
    def test_reads_attribute(self):
        assert retry_after_seconds(ProviderError("x", status_code=429, retry_after=3)) == 3.0

    def test_reads_header_on_sdk_style_errors(self):
        class SdkError(Exception):
            headers = {"retry-after": "7"}

        assert retry_after_seconds(SdkError()) == 7.0

    def test_none_when_absent_or_unparseable(self):
        class SdkError(Exception):
            headers = {"retry-after": "Wed, 21 Oct 2026 07:28:00 GMT"}

        assert retry_after_seconds(ValueError("x")) is None
        assert retry_after_seconds(SdkError()) is None
