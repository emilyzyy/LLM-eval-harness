import pytest

from designops_eval.models.results import RunResult, RunScores, TraceEntry


@pytest.fixture
def sample_result() -> RunResult:
    return RunResult(
        run_id="test-run-id",
        model="mock",
        timestamp="2026-06-14T00:00:00+00:00",
        prompt_set="combined_eval",
        scores=RunScores(
            factual_accuracy=1.0,
            json_validity=1.0,
            avg_latency_ms=107.2,
            total_input_tokens=106,
            total_output_tokens=204,
            estimated_cost_usd=0.0,
        ),
        traces=[
            TraceEntry(
                prompt_id="bolt_spec_001",
                prompt="What are the standard thread pitch and nominal diameter for an M8 bolt?",
                response="An M8 bolt has 8mm diameter and 1.25mm pitch.",
                latency_ms=108.0,
                input_tokens=22,
                output_tokens=38,
                factual_score=1.0,
                json_validity_score=1.0,
                metadata={"category": "factual"},
            )
        ],
    )



@pytest.fixture(autouse=True)
def _isolated_provider_state(monkeypatch):
    """Reset fake-provider state and keep retry sleeps negligible in tests."""
    from designops_eval.providers.fake import local_registry

    for name in (
        "FAKE_PROVIDER_URL", "FAKE_PROVIDER_LATENCY_MS", "FAKE_PROVIDER_FAILURE_RATE",
        "FAKE_PROVIDER_RATE_LIMIT", "FAKE_PROVIDER_WINDOW_SECONDS", "FAKE_PROVIDER_SEED",
    ):
        monkeypatch.delenv(name, raising=False)
    monkeypatch.setenv("DESIGNOPS_RETRY_BASE_DELAY", "0.001")
    monkeypatch.setenv("DESIGNOPS_RETRY_MAX_DELAY", "0.005")
    local_registry().reset()
    yield
    local_registry().reset()


@pytest.fixture
def aws(monkeypatch):
    """An in-process AWS emulation (moto) with dummy credentials."""
    from moto import mock_aws

    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "test")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "test")
    monkeypatch.setenv("AWS_DEFAULT_REGION", "us-east-1")
    monkeypatch.delenv("AWS_ENDPOINT_URL", raising=False)
    monkeypatch.delenv("AWS_PROFILE", raising=False)
    with mock_aws():
        yield


@pytest.fixture
def dynamodb(aws):
    import boto3

    return boto3.resource("dynamodb", region_name="us-east-1")


@pytest.fixture
def results_table(dynamodb):
    from designops_eval.distributed.schema import create_results_table

    return create_results_table(dynamodb, "test-results")


@pytest.fixture
def rate_limit_table(dynamodb):
    from designops_eval.distributed.schema import create_rate_limit_table

    return create_rate_limit_table(dynamodb, "test-rate-limits")


@pytest.fixture
def sqs(aws):
    import boto3

    return boto3.client("sqs", region_name="us-east-1")


@pytest.fixture
def make_queue(sqs):
    """Factory: make_queue(visibility_timeout=30, max_receive_count=3) -> (queue, dlq)."""
    from designops_eval.distributed.queue import ShardQueue
    from designops_eval.distributed.schema import create_queues

    counter = {"n": 0}

    def factory(visibility_timeout: int = 30, max_receive_count: int = 3):
        counter["n"] += 1
        queue_url, dlq_url = create_queues(
            sqs, f"test-shards-{counter['n']}", visibility_timeout, max_receive_count
        )
        return ShardQueue(sqs, queue_url), ShardQueue(sqs, dlq_url)

    return factory


@pytest.fixture
def retry_sleeps(monkeypatch):
    """Skip retry waits, including Retry-After, and record what was requested."""
    from designops_eval.providers import retry

    sleeps: list[float] = []
    monkeypatch.setattr(retry, "_sleep", sleeps.append)
    return sleeps
