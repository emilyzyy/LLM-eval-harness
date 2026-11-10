"""End-to-end fault injection against an in-process AWS emulation.

Each test runs the real pipeline: submit -> SQS -> worker -> DynamoDB ->
finalize. benchmarks/run.py repeats the same scenarios with separate worker
processes and reports timings.
"""

import json
import threading
import time
from pathlib import Path

import boto3
import pytest
from click.testing import CliRunner

from designops_eval.cli import cli
from designops_eval.distributed import worker as worker_module
from designops_eval.distributed.finalize import IncompleteRunError, finalize_run
from designops_eval.distributed.rate_limiter import NullLimiter
from designops_eval.distributed.results_store import STATUS_FAILED, ResultsStore
from designops_eval.distributed.sharding import build_shards
from designops_eval.distributed.worker import (
    CrashingStore,
    SimulatedCrash,
    WorkerDeps,
    run_worker,
)
from designops_eval.models.config import RunConfig
from designops_eval.providers.fake import local_registry
from designops_eval.runner import load_prompt_set, run_eval
from tests.helpers import AtomicTable, write_prompt_set

RUN_ID = "fault-run"


@pytest.fixture(autouse=True)
def fresh_prompt_cache():
    worker_module.load_prompts_by_id.cache_clear()


@pytest.fixture
def table(results_table) -> AtomicTable:
    return AtomicTable(results_table)


def deps_for(table, worker_id: str, store=None) -> WorkerDeps:
    return WorkerDeps(
        store=store or ResultsStore(table),
        limiter_for=lambda provider: NullLimiter(),
        worker_id=worker_id,
    )


def drain(queue, deps, **kwargs):
    options = {"wait_seconds": 0, "idle_exit_after": 0.0, "retry_delay": 0, "log": lambda *a, **k: None}
    options.update(kwargs)
    return run_worker(queue, deps, **options)


def shards_for(path: str, models=("fake-a",), shard_size: int = 25):
    return build_shards(RUN_ID, path, load_prompt_set(path), list(models), shard_size)


def assert_no_duplicate_results(records) -> None:
    identities = [(r.model, r.config_version, r.case_id) for r in records]
    assert len(identities) == len(set(identities))


class TestDuplicateDelivery:
    def test_same_shard_published_twice_commits_each_case_once(self, table, make_queue, tmp_path):
        path = write_prompt_set(tmp_path / "p.yaml", 25)
        queue, dlq = make_queue()
        [shard] = shards_for(path)

        # Two separate messages carrying the same shard.
        assert queue.publish([shard]) == 1
        assert queue.publish([shard]) == 1

        stats = drain(queue, deps_for(table, "w1"))

        records = ResultsStore(table).list_results(RUN_ID)
        assert stats.messages == 2 and stats.shards_completed == 2
        assert stats.evaluated == 25 and stats.skipped == 25
        assert len(records) == 25
        assert_no_duplicate_results(records)
        assert local_registry().stats()["fake-a"]["duplicate_completions"] == 0
        assert queue.receive(wait_seconds=0) is None and dlq.receive(wait_seconds=0) is None

    def test_duplicates_processed_concurrently_commit_each_case_once(
        self, table, make_queue, tmp_path
    ):
        path = write_prompt_set(tmp_path / "p.yaml", 50)
        queue, _ = make_queue()
        shards = shards_for(path, shard_size=25)
        for _ in range(3):
            queue.publish(shards)  # every shard three times

        all_stats = []
        start = threading.Barrier(3)

        def work(worker_id):
            start.wait()
            all_stats.append(drain(queue, deps_for(table, worker_id), idle_exit_after=0.3))

        threads = [threading.Thread(target=work, args=(f"w{i}",)) for i in range(3)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        records = ResultsStore(table).list_results(RUN_ID)
        assert len(records) == 50
        assert_no_duplicate_results(records)
        assert sum(s.evaluated for s in all_stats) == 50
        assert sum(s.shards_completed for s in all_stats) == 6

    def test_resubmitting_a_finished_run_evaluates_nothing(self, table, make_queue, tmp_path):
        path = write_prompt_set(tmp_path / "p.yaml", 25)
        queue, _ = make_queue()
        queue.publish(shards_for(path))
        drain(queue, deps_for(table, "w1"))
        calls = local_registry().stats()["fake-a"]["requests_total"]

        queue.publish(shards_for(path))
        stats = drain(queue, deps_for(table, "w2"))

        assert stats.evaluated == 0 and stats.skipped == 25
        assert local_registry().stats()["fake-a"]["requests_total"] == calls


class TestWorkerCrash:
    def test_second_worker_finishes_shard_after_crash_at_18_of_25(self, table, make_queue, tmp_path):
        path = write_prompt_set(tmp_path / "p.yaml", 25)
        queue, dlq = make_queue(visibility_timeout=1)
        queue.publish(shards_for(path))

        crashing = CrashingStore(ResultsStore(table), crash_after_commits=18, mode="raise")
        with pytest.raises(SimulatedCrash):
            drain(queue, deps_for(table, "w1", store=crashing),
                  visibility_timeout=1, heartbeat_interval=0.3)

        committed_before = {r.case_id: r for r in ResultsStore(table).list_results(RUN_ID)}
        assert len(committed_before) == 18

        time.sleep(1.3)  # the dead worker's heartbeat has stopped; the message reappears
        stats = drain(queue, deps_for(table, "w2"), visibility_timeout=1, heartbeat_interval=0.3)

        after = {r.case_id: r for r in ResultsStore(table).list_results(RUN_ID)}
        assert stats.evaluated == 7 and stats.skipped == 18
        assert len(after) == 25
        # Nothing committed before the crash was lost or rewritten.
        assert all(after[case_id] == record for case_id, record in committed_before.items())
        assert {r.worker_id for r in after.values()} == {"w1", "w2"}
        assert sum(r.worker_id == "w2" for r in after.values()) == 7
        provider = local_registry().stats()["fake-a"]
        assert provider["distinct_prompts_completed"] == 25
        assert provider["duplicate_completions"] == 0
        assert queue.receive(wait_seconds=0) is None and dlq.receive(wait_seconds=0) is None

    def test_repeated_crashes_still_converge(self, table, make_queue, tmp_path):
        path = write_prompt_set(tmp_path / "p.yaml", 25)
        queue, _ = make_queue(visibility_timeout=1, max_receive_count=10)
        queue.publish(shards_for(path))

        crashes = 0
        for attempt in range(8):
            crashing = CrashingStore(ResultsStore(table), crash_after_commits=6, mode="raise")
            try:
                drain(queue, deps_for(table, f"w{attempt}", store=crashing),
                      visibility_timeout=1, heartbeat_interval=0.3)
                break
            except SimulatedCrash:
                crashes += 1
                time.sleep(1.3)

        records = ResultsStore(table).list_results(RUN_ID)
        assert crashes == 4  # 6 + 6 + 6 + 6, then the last worker commits 1
        assert len(records) == 25
        assert_no_duplicate_results(records)
        assert local_registry().stats()["fake-a"]["duplicate_completions"] == 0


class TestPoisonMessages:
    def test_shard_naming_unknown_case_reaches_dlq_without_blocking_others(
        self, table, make_queue, tmp_path
    ):
        path = write_prompt_set(tmp_path / "p.yaml", 10)
        queue, dlq = make_queue(max_receive_count=2)
        [good] = shards_for(path)
        bad = good.__class__(**{**good.__dict__, "shard_id": "bad", "case_ids": ("no_such_case",)})
        queue.publish([bad, good])

        drain(queue, deps_for(table, "w1"))

        assert len(ResultsStore(table).list_results(RUN_ID)) == 10
        assert json.loads(dlq.receive(wait_seconds=0).body)["shard_id"] == "bad"
        assert queue.receive(wait_seconds=0) is None

    def test_provider_outage_dead_letters_shard_and_commits_no_failures(
        self, table, make_queue, tmp_path, monkeypatch, retry_sleeps
    ):
        monkeypatch.setenv("FAKE_PROVIDER_FAILURE_RATE", "1.0")
        path = write_prompt_set(tmp_path / "p.yaml", 5)
        queue, dlq = make_queue(max_receive_count=2)
        queue.publish(shards_for(path))

        stats = drain(queue, deps_for(table, "w1"))

        assert stats.shards_incomplete == 2
        assert ResultsStore(table).list_results(RUN_ID) == []
        assert dlq.receive(wait_seconds=0) is not None


@pytest.fixture
def cli_env(monkeypatch, results_table, make_queue):
    queue, dlq = make_queue()
    monkeypatch.setenv("DESIGNOPS_QUEUE_URL", queue.queue_url)
    monkeypatch.setenv("DESIGNOPS_RESULTS_TABLE", results_table.name)
    return queue, dlq


def invoke(*args: str):
    return CliRunner().invoke(cli, list(args), catch_exceptions=False)


class TestFailedCasesAndRetry:
    def test_failed_cases_are_terminal_counted_and_retryable_on_request(
        self, table, cli_env, tmp_path
    ):
        queue, _ = cli_env
        path = write_prompt_set(tmp_path / "p.yaml", 25, permanent_failures=2)
        submit = ("submit", "--run-id", RUN_ID, "--prompt-set", path, "--models", "fake-a")

        assert invoke(*submit).exit_code == 0
        drain(queue, deps_for(table, "w1"))
        records = ResultsStore(table).list_results(RUN_ID)
        assert sum(r.status == STATUS_FAILED for r in records) == 2

        # Failed cases count against the scores.
        [result] = finalize_run(ResultsStore(table), RUN_ID, load_prompt_set(path), ["fake-a"])
        assert result.scores.failed_cases == 2
        clean = write_prompt_set(tmp_path / "clean.yaml", 25)
        local_registry().reset()
        baseline = run_eval(RunConfig(prompt_set_path=clean, model="fake-a")).scores
        assert result.scores.factual_accuracy < baseline.factual_accuracy or \
            result.scores.json_validity < baseline.json_validity

        # A plain resubmission leaves failed cases alone.
        assert invoke(*submit).exit_code == 0
        assert drain(queue, deps_for(table, "w2")).evaluated == 0

        # The cause is fixed; --retry-failed re-evaluates exactly the failed cases.
        Path(path).write_text(Path(clean).read_text())
        worker_module.load_prompts_by_id.cache_clear()
        outcome = invoke(*submit, "--retry-failed")
        assert outcome.exit_code == 0 and "Deleted 2 failed" in outcome.output
        stats = drain(queue, deps_for(table, "w3"))

        assert stats.evaluated == 2 and stats.skipped == 23
        records = ResultsStore(table).list_results(RUN_ID)
        assert len(records) == 25 and all(r.status != STATUS_FAILED for r in records)
        [result] = finalize_run(ResultsStore(table), RUN_ID, load_prompt_set(path), ["fake-a"])
        assert result.scores.failed_cases == 0


COMPARED_SCORES = (
    "factual_accuracy", "json_validity", "total_input_tokens",
    "total_output_tokens", "estimated_cost_usd", "failed_cases",
)


class TestCorrectnessAgainstSingleWorker:
    MODELS = ["fake-a", "fake-b"]

    def run_distributed(self, table, queue, path, workers: int = 3):
        queue.publish(shards_for(path, self.MODELS, shard_size=20))
        threads = [
            threading.Thread(target=drain, args=(queue, deps_for(table, f"w{i}")),
                             kwargs={"idle_exit_after": 0.3})
            for i in range(workers)
        ]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

    @pytest.mark.parametrize("failure_rate", ["0", "0.2"])
    def test_distributed_results_equal_single_worker_results(
        self, table, make_queue, tmp_path, monkeypatch, retry_sleeps, failure_rate
    ):
        monkeypatch.setenv("FAKE_PROVIDER_FAILURE_RATE", failure_rate)
        path = write_prompt_set(tmp_path / "p.yaml", 90, permanent_failures=3)
        queue, dlq = make_queue()

        expected = {m: run_eval(RunConfig(prompt_set_path=path, model=m)) for m in self.MODELS}
        local_registry().reset()

        self.run_distributed(table, queue, path)
        actual = {
            r.model: r
            for r in finalize_run(ResultsStore(table), RUN_ID, load_prompt_set(path), self.MODELS)
        }

        assert dlq.receive(wait_seconds=0) is None
        for model in self.MODELS:
            want, got = expected[model], actual[model]
            # Completeness and order.
            assert [t.prompt_id for t in got.traces] == [t.prompt_id for t in want.traces]
            # Per-case responses and scores. Latency is measured, so it differs.
            for got_trace, want_trace in zip(got.traces, want.traces):
                assert got_trace.model_dump(exclude={"latency_ms"}) == \
                    want_trace.model_dump(exclude={"latency_ms"})
            # Aggregates.
            for name in COMPARED_SCORES:
                assert getattr(got.scores, name) == getattr(want.scores, name), name
            assert got.scores.failed_cases == 3
            assert got.prompt_set == want.prompt_set

    def test_finalize_refuses_an_incomplete_run(self, table, make_queue, tmp_path):
        path = write_prompt_set(tmp_path / "p.yaml", 25)
        queue, _ = make_queue(visibility_timeout=1)
        queue.publish(shards_for(path))
        crashing = CrashingStore(ResultsStore(table), crash_after_commits=10, mode="raise")
        with pytest.raises(SimulatedCrash):
            drain(queue, deps_for(table, "w1", store=crashing))

        with pytest.raises(IncompleteRunError) as info:
            finalize_run(ResultsStore(table), RUN_ID, load_prompt_set(path), ["fake-a"])
        assert len(info.value.missing["fake-a"]) == 15

        [partial] = finalize_run(ResultsStore(table), RUN_ID, load_prompt_set(path),
                                 ["fake-a"], allow_partial=True)
        assert len(partial.traces) == 10


class TestArtifactIntegrity:
    ARTIFACTS = ("latest.json", "traces.json", "report.md")

    def finish_run(self, table, queue, path):
        assert invoke("submit", "--run-id", RUN_ID, "--prompt-set", path,
                      "--models", "fake-a,fake-b").exit_code == 0
        drain(queue, deps_for(table, "w1"))

    def test_local_artifacts_are_complete_and_reproducible(self, table, cli_env, tmp_path):
        queue, _ = cli_env
        path = write_prompt_set(tmp_path / "p.yaml", 30)
        self.finish_run(table, queue, path)
        finalize = ("finalize", "--run-id", RUN_ID, "--prompt-set", path,
                    "--models", "fake-a,fake-b")

        first, second = tmp_path / "out1", tmp_path / "out2"
        assert invoke(*finalize, "--output-dir", str(first)).exit_code == 0
        assert invoke(*finalize, "--output-dir", str(second)).exit_code == 0

        for model in ("fake-a", "fake-b"):
            for name in self.ARTIFACTS:
                assert (first / model / name).read_bytes() == (second / model / name).read_bytes()
            latest = json.loads((first / model / "latest.json").read_text())
            traces = json.loads((first / model / "traces.json").read_text())["traces"]
            assert latest["run_id"] == f"{RUN_ID}/{model}" and latest["model"] == model
            assert [t["prompt_id"] for t in traces] == [f"gen_{i:04d}" for i in range(30)]
            assert f"`{RUN_ID}/{model}`" in (first / model / "report.md").read_text()

    def test_finalize_cli_reports_incomplete_run(self, cli_env, tmp_path):
        path = write_prompt_set(tmp_path / "p.yaml", 5)
        outcome = invoke("finalize", "--run-id", RUN_ID, "--prompt-set", path,
                         "--models", "fake-a", "--output-dir", str(tmp_path / "out"))
        assert outcome.exit_code == 2
        assert not (tmp_path / "out").exists()

    def test_aws_artifacts_match_local_copies(self, table, cli_env, dynamodb, tmp_path):
        queue, _ = cli_env
        path = write_prompt_set(tmp_path / "p.yaml", 30)
        self.finish_run(table, queue, path)
        s3 = boto3.client("s3", region_name="us-east-1")
        s3.create_bucket(Bucket="test-artifacts")
        dynamodb.create_table(
            TableName="test-runs",
            AttributeDefinitions=[{"AttributeName": "run_id", "AttributeType": "S"}],
            KeySchema=[{"AttributeName": "run_id", "KeyType": "HASH"}],
            BillingMode="PAY_PER_REQUEST",
        )

        out = tmp_path / "out"
        outcome = invoke("finalize", "--run-id", RUN_ID, "--prompt-set", path,
                         "--models", "fake-a,fake-b", "--output-dir", str(out),
                         "--storage", "aws", "--s3-bucket", "test-artifacts",
                         "--dynamodb-table", "test-runs")
        assert outcome.exit_code == 0

        for model in ("fake-a", "fake-b"):
            for name in self.ARTIFACTS:
                body = s3.get_object(
                    Bucket="test-artifacts", Key=f"runs/{RUN_ID}/{model}/{name}"
                )["Body"].read()
                assert body == (out / model / name).read_bytes()
            row = dynamodb.Table("test-runs").get_item(Key={"run_id": f"{RUN_ID}/{model}"})["Item"]
            assert row["model"] == model
