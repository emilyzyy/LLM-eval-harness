import threading
import time

import pytest
from botocore.exceptions import ClientError

from designops_eval.distributed.rate_limiter import (
    BucketConfig,
    DynamoTokenBucket,
    LocalPacer,
    NullLimiter,
    RateLimitTimeout,
)
from designops_eval.distributed.worker import build_limiter_factory
from tests.helpers import AtomicTable, FakeClock


class TestBucketConfig:
    def test_from_limit_bounds_any_window_to_the_limit(self):
        config = BucketConfig.from_limit(600, window_seconds=60, burst=30)
        assert config.capacity == 30
        assert config.capacity + config.refill_per_second * 60 == pytest.approx(600)

    def test_default_burst_is_five_percent(self):
        assert BucketConfig.from_limit(600).capacity == 30
        assert BucketConfig.from_limit(10).capacity == 1

    @pytest.mark.parametrize("burst", [0, 600, 601])
    def test_invalid_burst(self, burst):
        with pytest.raises(ValueError):
            BucketConfig.from_limit(600, burst=burst)

    def test_invalid_values(self):
        with pytest.raises(ValueError):
            BucketConfig(capacity=0, refill_per_second=1)
        with pytest.raises(ValueError):
            BucketConfig(capacity=1, refill_per_second=0)


class TestLocalPacer:
    def test_spaces_requests_at_fixed_interval(self):
        clock = FakeClock()
        pacer = LocalPacer(10, clock=clock, sleep=clock.sleep)
        waits = [pacer.acquire() for _ in range(4)]
        assert waits == pytest.approx([0.0, 0.1, 0.1, 0.1])

    def test_idle_time_is_not_banked(self):
        clock = FakeClock()
        pacer = LocalPacer(10, clock=clock, sleep=clock.sleep)
        pacer.acquire()
        clock.advance(5)
        assert pacer.acquire() == 0.0
        assert pacer.acquire() == pytest.approx(0.1)

    def test_timeout(self):
        clock = FakeClock()
        pacer = LocalPacer(0.1, clock=clock, sleep=clock.sleep)
        pacer.acquire()
        with pytest.raises(RateLimitTimeout):
            pacer.acquire(timeout=1)

    def test_two_pacers_do_not_coordinate(self):
        clock = FakeClock()
        first = LocalPacer(10, clock=clock, sleep=clock.sleep)
        second = LocalPacer(10, clock=clock, sleep=clock.sleep)
        assert first.acquire() == 0.0 and second.acquire() == 0.0


class TestLimiterFactory:
    def test_unconfigured_provider_is_unlimited(self):
        limiter_for = build_limiter_factory("local", {"fake-a": 600})
        assert isinstance(limiter_for("fake-b"), NullLimiter)
        assert isinstance(limiter_for("fake-a"), LocalPacer)

    def test_none_mode_is_unlimited(self):
        assert isinstance(build_limiter_factory("none", {"fake-a": 600})("fake-a"), NullLimiter)

    def test_shared_mode_builds_one_bucket_per_provider(self, rate_limit_table):
        limiter_for = build_limiter_factory(
            "shared", {"fake-a": 600, "fake-b": 60}, rate_limit_table=rate_limit_table
        )
        a, b = limiter_for("fake-a"), limiter_for("fake-b")
        assert isinstance(a, DynamoTokenBucket) and a.provider == "fake-a"
        assert a.config == BucketConfig.from_limit(600)
        assert b.config == BucketConfig.from_limit(60)
        assert limiter_for("fake-a") is a

    def test_shared_mode_requires_table(self):
        with pytest.raises(ValueError):
            build_limiter_factory("shared", {"fake-a": 600})


# ---------------------------------------------------------------------------
# DynamoTokenBucket
# ---------------------------------------------------------------------------

@pytest.fixture
def table(rate_limit_table) -> AtomicTable:
    return AtomicTable(rate_limit_table)


@pytest.fixture
def clock() -> FakeClock:
    return FakeClock()


def make_bucket(table, clock, provider="fake-a", capacity=5, rate=1.0) -> DynamoTokenBucket:
    return DynamoTokenBucket(
        table, provider, BucketConfig(capacity=capacity, refill_per_second=rate),
        clock=clock, sleep=clock.sleep, rng=lambda: 0.5,
    )


def drain(bucket, attempts: int) -> int:
    return sum(bucket.try_acquire().acquired for _ in range(attempts))


class TestTryAcquire:
    def test_new_bucket_grants_exactly_capacity(self, table, clock):
        assert drain(make_bucket(table, clock, capacity=5), 12) == 5

    def test_empty_bucket_reports_time_until_next_token(self, table, clock):
        bucket = make_bucket(table, clock, capacity=2, rate=4.0)
        drain(bucket, 2)
        result = bucket.try_acquire()
        assert result.acquired is False
        assert result.retry_after == pytest.approx(0.25, abs=0.01)

    def test_retry_after_shrinks_as_time_passes(self, table, clock):
        bucket = make_bucket(table, clock, capacity=1, rate=1.0)
        drain(bucket, 1)
        clock.advance(0.6)
        assert bucket.try_acquire().retry_after == pytest.approx(0.4, abs=0.01)

    def test_tokens_refill_with_time(self, table, clock):
        bucket = make_bucket(table, clock, capacity=5, rate=2.0)
        drain(bucket, 5)
        clock.advance(1.5)  # 3 tokens
        assert drain(bucket, 10) == 3

    def test_partial_refill_is_not_lost_by_a_failed_attempt(self, table, clock):
        bucket = make_bucket(table, clock, capacity=1, rate=1.0)
        drain(bucket, 1)
        for _ in range(4):  # four failed attempts, 0.2 s apart
            clock.advance(0.2)
            assert bucket.try_acquire().acquired is False
        clock.advance(0.25)  # 1.05 s since the bucket emptied
        assert bucket.try_acquire().acquired is True

    def test_refill_never_exceeds_capacity(self, table, clock):
        bucket = make_bucket(table, clock, capacity=5, rate=2.0)
        drain(bucket, 5)
        clock.advance(3600)
        assert drain(bucket, 20) == 5

    def test_steady_rate_matches_refill(self, table, clock):
        bucket = make_bucket(table, clock, capacity=1, rate=10.0)
        granted = 0
        for _ in range(1000):  # 10 simulated seconds in 10 ms steps
            granted += bucket.try_acquire().acquired
            clock.advance(0.01)
        assert 99 <= granted <= 101

    def test_providers_have_independent_buckets(self, table, clock):
        a = make_bucket(table, clock, provider="fake-a", capacity=3)
        b = make_bucket(table, clock, provider="fake-b", capacity=3)
        assert drain(a, 5) == 3
        assert drain(b, 5) == 3

    def test_instances_share_one_bucket(self, table, clock):
        first = make_bucket(table, clock, capacity=5)
        second = make_bucket(table, clock, capacity=5)
        assert drain(first, 3) == 3
        assert drain(second, 10) == 2
        assert first.try_acquire().acquired is False

    def test_state_survives_a_new_instance(self, table, clock):
        drain(make_bucket(table, clock, capacity=5), 5)
        assert make_bucket(table, clock, capacity=5).try_acquire().acquired is False

    def test_reads_are_strongly_consistent(self, rate_limit_table, clock):
        seen: list[dict] = []

        class Spy(AtomicTable):
            def __getattr__(self, name):
                attr = super().__getattr__(name)
                if name not in ("get_item", "query", "scan"):
                    return attr

                def call(*args, **kwargs):
                    seen.append(kwargs)
                    return attr(*args, **kwargs)

                return call

        drain(make_bucket(Spy(rate_limit_table), clock, capacity=2), 3)
        assert seen and all(kwargs.get("ConsistentRead") is True for kwargs in seen)

    def test_other_errors_propagate(self, dynamodb, clock):
        bucket = make_bucket(dynamodb.Table("table-that-does-not-exist"), clock)
        with pytest.raises(ClientError) as info:
            bucket.try_acquire()
        assert info.value.response["Error"]["Code"] == "ResourceNotFoundException"


def rival_takes_one_before_next_write(table: AtomicTable, rival: DynamoTokenBucket) -> list[bool]:
    """Make `rival` acquire a token just before the next write through `table`.

    The hook fires once, whichever write operation the implementation uses.
    """
    fired: list[bool] = []

    def hook():
        if not fired:
            fired.append(rival.try_acquire().acquired)

    table.before("put_item", hook)
    table.before("update_item", hook)
    return fired


class TestConcurrentModification:
    def test_rival_creates_the_item_between_read_and_write(self, table, rate_limit_table, clock):
        """Both workers read 'no item'. Only one of them may take the single token."""
        bucket = make_bucket(table, clock, capacity=1)
        rival = make_bucket(rate_limit_table, clock, capacity=1)
        fired = rival_takes_one_before_next_write(table, rival)

        result = bucket.try_acquire()

        assert fired == [True]
        assert result.acquired is False

    def test_rival_spends_the_last_token_between_read_and_write(self, table, rate_limit_table, clock):
        bucket = make_bucket(table, clock, capacity=3)
        rival = make_bucket(rate_limit_table, clock, capacity=3)
        drain(bucket, 2)  # one token left, item exists
        fired = rival_takes_one_before_next_write(table, rival)

        result = bucket.try_acquire()

        assert fired == [True]
        assert result.acquired is False

    def test_contention_is_retried_not_reported_as_empty(self, table, rate_limit_table, clock):
        """Losing a race while tokens remain must still end in a grant."""
        bucket = make_bucket(table, clock, capacity=3)
        rival = make_bucket(rate_limit_table, clock, capacity=3)
        fired = rival_takes_one_before_next_write(table, rival)

        result = bucket.try_acquire()

        assert fired == [True]
        assert result.acquired is True
        assert drain(bucket, 5) == 1  # 3 - rival's 1 - ours 1

    def test_threads_never_overspend_at_a_fixed_clock(self, table, clock):
        buckets = [make_bucket(table, clock, capacity=20) for _ in range(8)]
        granted = []
        lock = threading.Lock()
        start = threading.Barrier(len(buckets))

        def work(bucket):
            start.wait()
            count = drain(bucket, 10)
            with lock:
                granted.append(count)

        threads = [threading.Thread(target=work, args=(b,)) for b in buckets]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        assert sum(granted) == 20

    def test_threads_stay_within_rate_on_a_real_clock(self, table):
        capacity, rate, duration = 5, 40.0, 1.5
        config = BucketConfig(capacity=capacity, refill_per_second=rate)
        buckets = [DynamoTokenBucket(table, "fake-a", config) for _ in range(6)]
        granted = []
        lock = threading.Lock()
        started = time.time()
        deadline = started + duration

        def work(bucket):
            count = 0
            while time.time() < deadline:
                if bucket.try_acquire().acquired:
                    count += 1
                else:
                    time.sleep(0.002)
            with lock:
                granted.append(count)

        threads = [threading.Thread(target=work, args=(b,)) for b in buckets]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        elapsed = time.time() - started

        total = sum(granted)
        assert total <= capacity + rate * elapsed + 1
        assert total >= capacity + rate * duration * 0.6, "refill is being lost under contention"


class TestClockSkew:
    def test_worker_with_slow_clock_does_not_mint_tokens(self, table):
        """A write from a worker whose clock is behind must not move time backwards."""
        ahead, behind = FakeClock(1000.0), FakeClock(990.0)
        first = make_bucket(table, ahead, capacity=2, rate=1.0)
        second = make_bucket(table, behind, capacity=2, rate=1.0)

        assert first.try_acquire().acquired is True
        assert second.try_acquire().acquired is True
        assert first.try_acquire().acquired is False

    def test_slow_clock_sees_no_negative_refill(self, table):
        ahead, behind = FakeClock(1000.0), FakeClock(990.0)
        first = make_bucket(table, ahead, capacity=3, rate=1.0)
        second = make_bucket(table, behind, capacity=3, rate=1.0)

        drain(first, 1)
        assert drain(second, 5) == 2


class TestSlidingWindow:
    def test_from_limit_keeps_every_window_within_the_limit(self, table, clock):
        """A greedy caller on a from_limit bucket never exceeds limit per window."""
        limit, window = 60, 60.0
        bucket = DynamoTokenBucket(
            table, "fake-a", BucketConfig.from_limit(limit, window, burst=6),
            clock=clock, sleep=clock.sleep,
        )
        start = clock()
        grants: list[float] = []
        while clock() - start < 200:
            if bucket.try_acquire().acquired:
                grants.append(clock())
            else:
                clock.advance(0.25)

        worst = max(
            sum(1 for t in grants if begin <= t < begin + window) for begin in grants
        )
        assert worst <= limit
        assert len(grants) >= 6 + 0.9 * 200 - 2, "throughput is well below the configured rate"


class TestAcquire:
    def test_returns_immediately_when_token_available(self, table, clock):
        bucket = make_bucket(table, clock, capacity=2)
        assert bucket.acquire() == 0.0
        assert clock.sleeps == []

    def test_waits_for_refill(self, table, clock):
        bucket = make_bucket(table, clock, capacity=1, rate=2.0)
        bucket.acquire()
        waited = bucket.acquire()
        assert 0.5 <= waited <= 0.8
        assert sum(clock.sleeps) == pytest.approx(waited)

    def test_sequential_acquires_follow_the_refill_rate(self, table, clock):
        bucket = make_bucket(table, clock, capacity=1, rate=10.0)
        start = clock()
        for _ in range(21):
            bucket.acquire()
        assert 2.0 <= clock() - start <= 2.6

    def test_timeout_raises_without_taking_a_token(self, table, clock):
        bucket = make_bucket(table, clock, capacity=1, rate=0.1)  # 10 s per token
        bucket.acquire()
        with pytest.raises(RateLimitTimeout):
            bucket.acquire(timeout=1.0)
        assert sum(clock.sleeps) <= 1.0 + 1e-6

        clock.advance(10)
        assert bucket.try_acquire().acquired is True
        assert bucket.try_acquire().acquired is False

    def test_timeout_long_enough_succeeds(self, table, clock):
        bucket = make_bucket(table, clock, capacity=1, rate=2.0)
        bucket.acquire()
        assert bucket.acquire(timeout=5.0) >= 0.5
