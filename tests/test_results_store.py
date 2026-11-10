"""Tests for per-case result persistence in distributed/results_store.py."""

import threading

import pytest
from botocore.exceptions import ClientError

from designops_eval.distributed.results_store import (
    STATUS_COMPLETED,
    STATUS_FAILED,
    ResultRecord,
    ResultsStore,
)
from tests.helpers import AtomicTable, make_trace


def record(case_id: str, *, run_id="run-1", model="fake-a", config_version="cfg1",
           response="ok", status="completed", worker_id="w1") -> ResultRecord:
    return ResultRecord.from_trace(
        run_id=run_id, model=model, provider=model, config_version=config_version,
        shard_id="shard-1", trace=make_trace(case_id, response, status), worker_id=worker_id,
    )


@pytest.fixture
def store(results_table) -> ResultsStore:
    return ResultsStore(AtomicTable(results_table))


class TestPutResultIfAbsent:
    def test_first_write_commits(self, store):
        assert store.put_result_if_absent(record("c1")) is True
        [stored] = store.list_results("run-1")
        assert stored.case_id == "c1" and stored.status == STATUS_COMPLETED

    def test_second_write_is_rejected(self, store):
        assert store.put_result_if_absent(record("c1")) is True
        assert store.put_result_if_absent(record("c1")) is False
        assert len(store.list_results("run-1")) == 1

    def test_first_result_wins_and_is_not_modified(self, store):
        store.put_result_if_absent(record("c1", response="first", worker_id="w1"))
        store.put_result_if_absent(record("c1", response="second", worker_id="w2"))
        [stored] = store.list_results("run-1")
        assert stored.trace.response == "first"
        assert stored.worker_id == "w1"

    def test_failed_result_is_terminal_too(self, store):
        assert store.put_result_if_absent(record("c1", status="failed")) is True
        assert store.put_result_if_absent(record("c1", status="completed")) is False
        [stored] = store.list_results("run-1")
        assert stored.status == STATUS_FAILED

    @pytest.mark.parametrize("other", [
        {"run_id": "run-2"},
        {"model": "fake-b"},
        {"config_version": "cfg2"},
    ])
    def test_identity_includes_run_model_and_config_version(self, store, other):
        assert store.put_result_if_absent(record("c1")) is True
        assert store.put_result_if_absent(record("c1", **other)) is True

    def test_uses_a_single_conditional_write(self, results_table):
        table = AtomicTable(results_table)
        ResultsStore(table).put_result_if_absent(record("c1"))
        assert table.calls["put_item"] == 1
        assert table.calls["get_item"] == 0 and table.calls["query"] == 0

    def test_write_that_lands_between_check_and_put_is_not_overwritten(self, results_table):
        """A competing commit just before the write must make this call return False."""
        table = AtomicTable(results_table)
        store = ResultsStore(table)
        rival = ResultsStore(results_table)
        table.before("put_item", lambda: rival.put_result_if_absent(
            record("c1", response="rival", worker_id="w2")))

        assert store.put_result_if_absent(record("c1", response="mine", worker_id="w1")) is False
        [stored] = store.list_results("run-1")
        assert stored.trace.response == "rival"

    def test_concurrent_writers_commit_exactly_once(self, results_table):
        store = ResultsStore(AtomicTable(results_table))
        outcomes: list[bool] = []
        lock = threading.Lock()
        start = threading.Barrier(16)

        def write(i: int) -> None:
            start.wait()
            written = store.put_result_if_absent(record("c1", response=f"r{i}", worker_id=f"w{i}"))
            with lock:
                outcomes.append(written)

        threads = [threading.Thread(target=write, args=(i,)) for i in range(16)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        assert outcomes.count(True) == 1
        assert len(store.list_results("run-1")) == 1

    def test_other_errors_propagate(self, dynamodb):
        store = ResultsStore(dynamodb.Table("table-that-does-not-exist"))
        with pytest.raises(ClientError) as info:
            store.put_result_if_absent(record("c1"))
        assert info.value.response["Error"]["Code"] == "ResourceNotFoundException"


class TestGetCommittedCaseIds:
    def test_empty_when_nothing_committed(self, store):
        assert store.get_committed_case_ids("run-1", "fake-a", "cfg1") == set()

    def test_returns_committed_cases(self, store):
        for case_id in ("c1", "c2", "c3"):
            store.put_result_if_absent(record(case_id))
        assert store.get_committed_case_ids("run-1", "fake-a", "cfg1") == {"c1", "c2", "c3"}

    def test_failed_cases_count_as_committed(self, store):
        store.put_result_if_absent(record("c1"))
        store.put_result_if_absent(record("c2", status="failed"))
        assert store.get_committed_case_ids("run-1", "fake-a", "cfg1") == {"c1", "c2"}

    def test_scoped_to_run_model_and_config_version(self, store):
        store.put_result_if_absent(record("mine"))
        store.put_result_if_absent(record("other-run", run_id="run-2"))
        store.put_result_if_absent(record("other-model", model="fake-b"))
        store.put_result_if_absent(record("other-config", config_version="cfg2"))
        assert store.get_committed_case_ids("run-1", "fake-a", "cfg1") == {"mine"}

    def test_model_name_prefix_does_not_leak(self, store):
        """'fake-a' must not match results stored for 'fake-ab'."""
        store.put_result_if_absent(record("c1", model="fake-a"))
        store.put_result_if_absent(record("c2", model="fake-ab"))
        assert store.get_committed_case_ids("run-1", "fake-a", "cfg1") == {"c1"}

    def test_reads_every_page(self, store):
        big = "x" * 6000  # 300 items of ~6 KB exceed the 1 MB page limit
        for i in range(300):
            store.put_result_if_absent(record(f"c{i:03d}", response=big))
        assert store.get_committed_case_ids("run-1", "fake-a", "cfg1") == {
            f"c{i:03d}" for i in range(300)
        }

    def test_uses_strongly_consistent_reads(self, results_table):
        seen: list[dict] = []

        class Spy(AtomicTable):
            def __getattr__(self, name):
                attr = super().__getattr__(name)
                if name not in ("query", "get_item", "scan", "batch_get_item"):
                    return attr

                def call(*args, **kwargs):
                    seen.append(kwargs)
                    return attr(*args, **kwargs)

                return call

        store = ResultsStore(Spy(results_table))
        store.put_result_if_absent(record("c1"))
        store.get_committed_case_ids("run-1", "fake-a", "cfg1")
        assert seen, "expected at least one read request"
        assert all(kwargs.get("ConsistentRead") is True for kwargs in seen)


class TestImplementedHelpers:

    def test_list_results_spans_models(self, store):
        store.put_result_if_absent(record("c1", model="fake-a"))
        store.put_result_if_absent(record("c1", model="fake-b"))
        store.put_result_if_absent(record("c1", run_id="run-2"))
        assert sorted(r.model for r in store.list_results("run-1")) == ["fake-a", "fake-b"]

    def test_record_round_trips_through_dynamodb(self, store):
        original = record("c1", status="failed")
        store.put_result_if_absent(original)
        assert store.list_results("run-1") == [original]

    def test_delete_failed_results_removes_only_failed(self, store):
        store.put_result_if_absent(record("ok"))
        store.put_result_if_absent(record("bad1", status="failed"))
        store.put_result_if_absent(record("bad2", status="failed"))
        store.put_result_if_absent(record("other-run-bad", run_id="run-2", status="failed"))

        assert store.delete_failed_results("run-1") == 2
        assert [r.case_id for r in store.list_results("run-1")] == ["ok"]
        assert len(store.list_results("run-2")) == 1

    def test_failed_case_can_be_committed_again_after_delete(self, store):
        store.put_result_if_absent(record("c1", status="failed"))
        store.delete_failed_results("run-1")
        assert store.put_result_if_absent(record("c1")) is True
        assert store.get_committed_case_ids("run-1", "fake-a", "cfg1") == {"c1"}
