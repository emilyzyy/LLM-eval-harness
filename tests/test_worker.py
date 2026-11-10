import threading
import time

import pytest

from designops_eval.distributed import worker as worker_module
from designops_eval.distributed.rate_limiter import NullLimiter
from designops_eval.distributed.results_store import (
    STATUS_FAILED,
    ResultRecord,
    ResultsStore,
)
from designops_eval.distributed.sharding import MalformedShardError
from designops_eval.distributed.worker import (
    CrashingStore,
    ShardOutcome,
    SimulatedCrash,
    WorkerDeps,
    process_shard,
    run_worker,
)
from designops_eval.providers import fake
from designops_eval.providers.fake import local_registry
from designops_eval.runner import evaluate_case, load_prompt_set
from tests.helpers import AtomicTable, make_trace, manual_shard, write_prompt_set


def quiet(*args, **kwargs) -> None:
    pass


def drain(queue, deps, **kwargs):
    """Run a worker until the queue has nothing visible."""
    options = {"wait_seconds": 0, "idle_exit_after": 0.0, "retry_delay": 0, "log": quiet}
    options.update(kwargs)
    return run_worker(queue, deps, **options)


# ---------------------------------------------------------------------------
# run_worker: message handling around process_shard
# ---------------------------------------------------------------------------

@pytest.fixture
def stub_deps() -> WorkerDeps:
    return WorkerDeps(store=None, limiter_for=lambda provider: NullLimiter(), worker_id="w1")


def a_shard(n: int = 0):
    return manual_shard("unused.yaml", [f"c{n}"], shard_id=f"shard-{n}")


class TestRunWorker:
    def test_complete_shard_is_deleted(self, make_queue, stub_deps, monkeypatch):
        queue, _ = make_queue(visibility_timeout=1)
        queue.publish([a_shard()])
        seen = []
        monkeypatch.setattr(worker_module, "process_shard", lambda shard, deps: (
            seen.append(shard), ShardOutcome(evaluated=1, complete=True))[1])

        stats = drain(queue, stub_deps)

        assert seen == [a_shard()]
        assert stats.shards_completed == 1 and stats.evaluated == 1
        time.sleep(1.2)
        assert queue.receive(wait_seconds=0) is None

    def test_incomplete_shard_is_left_for_redelivery(self, make_queue, stub_deps, monkeypatch):
        queue, _ = make_queue(max_receive_count=10)
        queue.publish([a_shard()])
        monkeypatch.setattr(worker_module, "process_shard",
                            lambda shard, deps: ShardOutcome(complete=False))

        stats = drain(queue, stub_deps, max_messages=3)

        assert stats.messages == 3  # the same message, redelivered
        assert stats.shards_incomplete == 3 and stats.shards_completed == 0
        assert queue.receive(wait_seconds=0).receive_count == 4

    def test_incomplete_shard_waits_for_retry_delay(self, make_queue, stub_deps, monkeypatch):
        queue, _ = make_queue()
        queue.publish([a_shard()])
        monkeypatch.setattr(worker_module, "process_shard",
                            lambda shard, deps: ShardOutcome(complete=False))

        stats = drain(queue, stub_deps, retry_delay=20)

        assert stats.messages == 1
        assert queue.receive(wait_seconds=0) is None  # invisible until the delay passes

    def test_unexpected_error_leaves_message_on_queue(self, make_queue, stub_deps, monkeypatch):
        queue, _ = make_queue(max_receive_count=10)
        queue.publish([a_shard()])

        def boom(shard, deps):
            raise RuntimeError("dynamodb unavailable")

        monkeypatch.setattr(worker_module, "process_shard", boom)
        stats = drain(queue, stub_deps, max_messages=1)

        assert stats.shards_incomplete == 1
        assert queue.receive(wait_seconds=0) is not None

    def test_incomplete_shard_reaches_dlq_after_max_receives(self, make_queue, stub_deps, monkeypatch):
        queue, dlq = make_queue(max_receive_count=3)
        queue.publish([a_shard()])
        monkeypatch.setattr(worker_module, "process_shard",
                            lambda shard, deps: ShardOutcome(complete=False))

        stats = drain(queue, stub_deps)

        assert stats.messages == 3
        assert dlq.receive(wait_seconds=0) is not None

    def test_malformed_message_is_not_processed_and_reaches_dlq(
        self, make_queue, sqs, stub_deps, monkeypatch
    ):
        queue, dlq = make_queue(visibility_timeout=1, max_receive_count=2)
        sqs.send_message(QueueUrl=queue.queue_url, MessageBody="not a shard")
        queue.publish([a_shard()])
        processed = []
        monkeypatch.setattr(worker_module, "process_shard", lambda shard, deps: (
            processed.append(shard.shard_id), ShardOutcome(complete=True))[1])

        stats = drain(queue, stub_deps, idle_exit_after=2.5)

        assert processed == ["shard-0"]
        assert stats.malformed == 2  # delivered twice, then dead-lettered
        assert dlq.receive(wait_seconds=0).body == "not a shard"

    def test_malformed_shard_error_from_processing_is_not_deleted(
        self, make_queue, stub_deps, monkeypatch
    ):
        queue, _ = make_queue(visibility_timeout=1)
        queue.publish([a_shard()])

        def reject(shard, deps):
            raise MalformedShardError("unknown case id")

        monkeypatch.setattr(worker_module, "process_shard", reject)
        stats = drain(queue, stub_deps, max_messages=1)

        assert stats.malformed == 1
        time.sleep(1.2)
        assert queue.receive(wait_seconds=0).receive_count == 2

    def test_heartbeat_keeps_long_shard_invisible(self, make_queue, stub_deps, monkeypatch):
        queue, _ = make_queue(visibility_timeout=1)
        queue.publish([a_shard()])
        redelivered_during_processing = []

        def slow(shard, deps):
            time.sleep(2.0)  # twice the visibility timeout
            redelivered_during_processing.append(queue.receive(wait_seconds=0))
            return ShardOutcome(complete=True)

        monkeypatch.setattr(worker_module, "process_shard", slow)
        stats = drain(queue, stub_deps, visibility_timeout=1, heartbeat_interval=0.3)

        assert redelivered_during_processing == [None]
        assert stats.messages == 1 and stats.shards_completed == 1

    def test_simulated_crash_abandons_the_message(self, make_queue, stub_deps, monkeypatch):
        queue, _ = make_queue(visibility_timeout=1)
        queue.publish([a_shard()])

        def crash(shard, deps):
            raise SimulatedCrash("killed")

        monkeypatch.setattr(worker_module, "process_shard", crash)
        with pytest.raises(SimulatedCrash):
            drain(queue, stub_deps, visibility_timeout=1, heartbeat_interval=0.3)

        time.sleep(1.2)
        assert queue.receive(wait_seconds=0).receive_count == 2

    def test_stops_when_asked(self, make_queue, stub_deps):
        queue, _ = make_queue()
        queue.publish([a_shard()])
        stats = drain(queue, stub_deps, should_stop=lambda: True)
        assert stats.messages == 0


class TestCrashingStore:
    class Recorder:
        def __init__(self):
            self.written = []

        def put_result_if_absent(self, record):
            self.written.append(record)
            return True

        def get_committed_case_ids(self, *args):
            return {"delegated"}

    def test_crashes_after_nth_commit(self):
        inner = self.Recorder()
        store = CrashingStore(inner, crash_after_commits=2, mode="raise")
        store.put_result_if_absent("r1")
        with pytest.raises(SimulatedCrash):
            store.put_result_if_absent("r2")
        assert inner.written == ["r1", "r2"]  # the second result was committed first

    def test_crashes_before_commit(self):
        inner = self.Recorder()
        store = CrashingStore(inner, crash_before_commit=1, mode="raise")
        store.put_result_if_absent("r1")
        with pytest.raises(SimulatedCrash):
            store.put_result_if_absent("r2")
        assert inner.written == ["r1"]  # the second result was lost

    def test_other_methods_are_delegated(self):
        store = CrashingStore(self.Recorder(), crash_after_commits=5, mode="raise")
        assert store.get_committed_case_ids("r", "m", "v") == {"delegated"}


# ---------------------------------------------------------------------------
# process_shard
# ---------------------------------------------------------------------------

@pytest.fixture
def prompt_set_path(tmp_path) -> str:
    worker_module.load_prompts_by_id.cache_clear()
    return write_prompt_set(tmp_path / "ten.yaml", 10)


@pytest.fixture
def case_ids(prompt_set_path) -> list[str]:
    return [p["id"] for p in load_prompt_set(prompt_set_path)["prompts"]]


@pytest.fixture
def table(results_table) -> AtomicTable:
    return AtomicTable(results_table)


class CountingLimiter:
    def __init__(self):
        self.acquired = 0

    def acquire(self, timeout=None):
        self.acquired += 1
        return 0.0


def make_deps(table, worker_id="w1", limiter=None, store=None) -> WorkerDeps:
    limiter = limiter or NullLimiter()
    return WorkerDeps(
        store=store or ResultsStore(table),
        limiter_for=lambda provider: limiter,
        worker_id=worker_id,
    )


def provider_stats() -> dict:
    return local_registry().stats().get("fake-a", {"requests_total": 0, "completed": 0,
                                                   "duplicate_completions": 0})


class TestProcessShard:
    def test_evaluates_and_commits_every_case(self, table, prompt_set_path, case_ids):
        outcome = process_shard(manual_shard(prompt_set_path, case_ids), make_deps(table))

        assert outcome == ShardOutcome(evaluated=10, complete=True)
        records = ResultsStore(table).list_results("run-1")
        assert sorted(r.case_id for r in records) == sorted(case_ids)
        assert {r.worker_id for r in records} == {"w1"}
        assert {r.shard_id for r in records} == {"shard-1"}

    def test_results_match_direct_evaluation(self, table, prompt_set_path, case_ids):
        process_shard(manual_shard(prompt_set_path, case_ids), make_deps(table))
        local_registry().reset()

        by_case = {r.case_id: r.trace for r in ResultsStore(table).list_results("run-1")}
        for prompt in load_prompt_set(prompt_set_path)["prompts"]:
            expected = evaluate_case(prompt, "fake-a")
            actual = by_case[prompt["id"]]
            assert actual.model_dump(exclude={"latency_ms"}) == expected.model_dump(exclude={"latency_ms"})

    def test_rerun_skips_everything_and_makes_no_provider_calls(self, table, prompt_set_path, case_ids):
        shard = manual_shard(prompt_set_path, case_ids)
        process_shard(shard, make_deps(table))
        calls_before = provider_stats()["requests_total"]

        outcome = process_shard(shard, make_deps(table, worker_id="w2"))

        assert outcome == ShardOutcome(skipped=10, complete=True)
        assert provider_stats()["requests_total"] == calls_before
        assert {r.worker_id for r in ResultsStore(table).list_results("run-1")} == {"w1"}

    def test_resumes_after_crash_without_repeating_committed_cases(
        self, table, prompt_set_path, case_ids
    ):
        shard = manual_shard(prompt_set_path, case_ids)
        crashing = CrashingStore(ResultsStore(table), crash_after_commits=4, mode="raise")
        with pytest.raises(SimulatedCrash):
            process_shard(shard, make_deps(table, worker_id="w1", store=crashing))
        before = {r.case_id: r for r in ResultsStore(table).list_results("run-1")}
        assert len(before) == 4

        outcome = process_shard(shard, make_deps(table, worker_id="w2"))

        assert outcome == ShardOutcome(evaluated=6, skipped=4, complete=True)
        after = {r.case_id: r for r in ResultsStore(table).list_results("run-1")}
        assert len(after) == 10
        assert all(after[case_id] == record for case_id, record in before.items())
        assert provider_stats()["duplicate_completions"] == 0

    def test_crash_between_response_and_commit_repeats_one_provider_call(
        self, table, prompt_set_path, case_ids
    ):
        """At-least-once: the lost response is fetched again; the result is stored once."""
        shard = manual_shard(prompt_set_path, case_ids)
        crashing = CrashingStore(ResultsStore(table), crash_before_commit=4, mode="raise")
        with pytest.raises(SimulatedCrash):
            process_shard(shard, make_deps(table, store=crashing))

        outcome = process_shard(shard, make_deps(table, worker_id="w2"))

        assert outcome == ShardOutcome(evaluated=6, skipped=4, complete=True)
        assert len(ResultsStore(table).list_results("run-1")) == 10
        assert provider_stats()["duplicate_completions"] == 1

    def test_each_result_is_committed_before_the_next_case_starts(
        self, table, prompt_set_path, case_ids
    ):
        committed_when_called: list[int] = []
        store = ResultsStore(table)

        class Observer:
            def acquire(self, timeout=None):
                committed_when_called.append(len(store.list_results("run-1")))
                return 0.0

        process_shard(manual_shard(prompt_set_path, case_ids), make_deps(table, limiter=Observer()))
        assert committed_when_called == list(range(10))

    def test_limiter_is_acquired_for_every_provider_attempt(
        self, table, prompt_set_path, case_ids, monkeypatch, retry_sleeps
    ):
        monkeypatch.setenv("FAKE_PROVIDER_FAILURE_RATE", "0.3")
        limiter = CountingLimiter()

        outcome = process_shard(manual_shard(prompt_set_path, case_ids),
                                make_deps(table, limiter=limiter))

        assert outcome.complete is True
        stats = provider_stats()
        assert stats["transient_failures"] > 0, "seed produced no failures; test is vacuous"
        assert limiter.acquired == stats["requests_total"]

    def test_permanent_failure_is_committed_as_failed(self, table, tmp_path):
        worker_module.load_prompts_by_id.cache_clear()
        path = write_prompt_set(tmp_path / "failing.yaml", 10, permanent_failures=2)
        ids = [p["id"] for p in load_prompt_set(path)["prompts"]]

        outcome = process_shard(manual_shard(path, ids), make_deps(table))

        assert outcome == ShardOutcome(evaluated=8, failed=2, complete=True)
        failed = [r for r in ResultsStore(table).list_results("run-1") if r.status == STATUS_FAILED]
        assert len(failed) == 2
        for record in failed:
            assert record.trace.status == "failed"
            assert record.trace.factual_score == 0.0 and record.trace.json_validity_score == 0.0
            assert "400" in record.trace.error

    def test_permanent_failure_is_not_retried_on_redelivery(self, table, tmp_path):
        worker_module.load_prompts_by_id.cache_clear()
        path = write_prompt_set(tmp_path / "failing.yaml", 10, permanent_failures=2)
        ids = [p["id"] for p in load_prompt_set(path)["prompts"]]
        process_shard(manual_shard(path, ids), make_deps(table))
        calls_before = provider_stats()["requests_total"]

        outcome = process_shard(manual_shard(path, ids), make_deps(table))

        assert outcome == ShardOutcome(skipped=10, complete=True)
        assert provider_stats()["requests_total"] == calls_before

    def test_rate_limited_cases_are_never_committed(
        self, table, prompt_set_path, case_ids, monkeypatch, retry_sleeps
    ):
        monkeypatch.setenv("FAKE_PROVIDER_RATE_LIMIT", "4")  # the other six get 429 on every attempt

        outcome = process_shard(manual_shard(prompt_set_path, case_ids), make_deps(table))

        records = ResultsStore(table).list_results("run-1")
        assert outcome.complete is False
        assert outcome.failed == 0
        assert len(records) == 4
        assert all(r.status != STATUS_FAILED for r in records)
        assert provider_stats()["rate_limited"] > 0

    def test_rate_limited_shard_completes_on_a_later_delivery(
        self, table, prompt_set_path, case_ids, monkeypatch, retry_sleeps
    ):
        monkeypatch.setenv("FAKE_PROVIDER_RATE_LIMIT", "4")
        shard = manual_shard(prompt_set_path, case_ids)
        assert process_shard(shard, make_deps(table)).complete is False

        monkeypatch.delenv("FAKE_PROVIDER_RATE_LIMIT")
        local_registry().reset()
        outcome = process_shard(shard, make_deps(table))

        assert outcome == ShardOutcome(evaluated=6, skipped=4, complete=True)

    def test_timeouts_are_never_committed(
        self, table, prompt_set_path, case_ids, monkeypatch, retry_sleeps
    ):
        def timeout(*args, **kwargs):
            raise TimeoutError("provider timed out")

        monkeypatch.setattr(fake, "fake_attempt", timeout)

        outcome = process_shard(manual_shard(prompt_set_path, case_ids), make_deps(table))

        assert outcome.complete is False
        assert ResultsStore(table).list_results("run-1") == []

    def test_server_errors_are_never_committed(
        self, table, prompt_set_path, case_ids, monkeypatch, retry_sleeps
    ):
        monkeypatch.setenv("FAKE_PROVIDER_FAILURE_RATE", "1.0")

        outcome = process_shard(manual_shard(prompt_set_path, case_ids), make_deps(table))

        assert outcome.complete is False
        assert ResultsStore(table).list_results("run-1") == []

    def test_unknown_case_id_is_a_malformed_shard(self, table, prompt_set_path, case_ids):
        shard = manual_shard(prompt_set_path, case_ids + ["no_such_case"])
        with pytest.raises(MalformedShardError):
            process_shard(shard, make_deps(table))

    def test_losing_the_commit_race_is_not_an_error(self, table, results_table, prompt_set_path, case_ids):
        rival_record = ResultRecord.from_trace(
            run_id="run-1", model="fake-a", provider="fake-a", config_version="cfg1",
            shard_id="shard-1", trace=make_trace(case_ids[0], response="rival"), worker_id="rival",
        )
        table.before("put_item", lambda: ResultsStore(results_table).put_result_if_absent(rival_record))

        outcome = process_shard(manual_shard(prompt_set_path, case_ids), make_deps(table))

        assert outcome == ShardOutcome(evaluated=9, lost_races=1, complete=True)
        by_case = {r.case_id: r for r in ResultsStore(table).list_results("run-1")}
        assert by_case[case_ids[0]].worker_id == "rival"
        assert by_case[case_ids[0]].trace.response == "rival"

    def test_two_workers_on_the_same_shard_commit_each_case_once(
        self, table, prompt_set_path, case_ids
    ):
        shard = manual_shard(prompt_set_path, case_ids)
        outcomes = []
        lock = threading.Lock()
        start = threading.Barrier(2)

        def work(worker_id):
            start.wait()
            outcome = process_shard(shard, make_deps(table, worker_id=worker_id))
            with lock:
                outcomes.append(outcome)

        threads = [threading.Thread(target=work, args=(w,)) for w in ("w1", "w2")]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        records = ResultsStore(table).list_results("run-1")
        assert sorted(r.case_id for r in records) == sorted(case_ids)
        assert all(o.complete for o in outcomes)
        assert sum(o.evaluated for o in outcomes) == 10
        assert sum(o.evaluated + o.skipped + o.lost_races for o in outcomes) == 20

    def test_shards_are_scoped_by_config_version(self, table, prompt_set_path, case_ids):
        process_shard(manual_shard(prompt_set_path, case_ids, config_version="cfg1"), make_deps(table))
        outcome = process_shard(
            manual_shard(prompt_set_path, case_ids, config_version="cfg2"), make_deps(table))

        assert outcome == ShardOutcome(evaluated=10, complete=True)
        assert len(ResultsStore(table).list_results("run-1")) == 20
