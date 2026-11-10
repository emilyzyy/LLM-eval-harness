from pathlib import Path

from designops_eval.models.config import RunConfig
from designops_eval.runner import run_eval

PROMPT_SET = str(Path(__file__).parent.parent / "evals" / "prompt_sets" / "combined_eval.yaml")
BASELINE = str(Path(__file__).parent.parent / "evals" / "baselines" / "baseline.json")


def test_run_eval_returns_result():
    config = RunConfig(prompt_set_path=PROMPT_SET, model="mock")
    result = run_eval(config)

    assert result.model == "mock"
    assert result.run_id
    assert result.timestamp
    assert result.prompt_set == "combined_eval"


def test_run_eval_scores_in_range():
    config = RunConfig(prompt_set_path=PROMPT_SET, model="mock")
    result = run_eval(config)

    assert 0.0 <= result.scores.factual_accuracy <= 1.0
    assert 0.0 <= result.scores.json_validity <= 1.0
    assert result.scores.avg_latency_ms > 0
    assert result.scores.total_input_tokens > 0
    assert result.scores.total_output_tokens > 0
    assert result.scores.estimated_cost_usd == 0.0  # mock model is free


def test_run_eval_produces_traces():
    config = RunConfig(prompt_set_path=PROMPT_SET, model="mock")
    result = run_eval(config)

    assert len(result.traces) == 5
    ids = {t.prompt_id for t in result.traces}
    assert "bolt_spec_001" in ids
    assert "part_lookup_001" in ids


def test_run_eval_mock_scores_correctly():
    """Mock model should hit all expected facts and produce valid JSON."""
    import json

    config = RunConfig(prompt_set_path=PROMPT_SET, model="mock")
    result = run_eval(config)

    assert result.scores.factual_accuracy == 1.0
    assert result.scores.json_validity == 1.0


def test_run_eval_with_baseline_no_regressions():
    import json

    baseline = json.loads(Path(BASELINE).read_text())
    config = RunConfig(prompt_set_path=PROMPT_SET, model="mock", baseline_path=BASELINE)
    result = run_eval(config, baseline=baseline)

    assert result.regressions == []


def test_run_eval_with_baseline_detects_regressions():
    config = RunConfig(prompt_set_path=PROMPT_SET, model="mock")
    # Artificially high baseline that will trigger regression
    strict_baseline = {"factual_accuracy": 1.5, "json_validity": 1.5}
    result = run_eval(config, baseline=strict_baseline)

    assert len(result.regressions) > 0


# ---------------------------------------------------------------------------
# Per-case evaluation, aggregation and the fake provider
# ---------------------------------------------------------------------------

import pytest

from designops_eval.providers.fake import PERMANENT_ERROR_MARKER
from designops_eval.providers.registry import model_config, provider_for
from designops_eval.runner import aggregate, evaluate_case, failed_trace, load_prompt_set

GENERATED = str(Path(__file__).parent.parent / "evals" / "prompt_sets" / "generated_300.yaml")


def test_evaluate_case_matches_run_eval_trace():
    prompts = load_prompt_set(PROMPT_SET)["prompts"]
    result = run_eval(RunConfig(prompt_set_path=PROMPT_SET, model="mock"))
    assert [evaluate_case(p, "mock") for p in prompts] == result.traces


def test_aggregate_matches_run_eval_scores():
    result = run_eval(RunConfig(prompt_set_path=PROMPT_SET, model="mock"))
    assert aggregate(result.traces, "mock") == result.scores
    assert result.scores.failed_cases == 0


def test_failed_cases_count_as_zero_in_aggregation():
    prompts = load_prompt_set(PROMPT_SET)["prompts"]
    traces = [evaluate_case(p, "mock") for p in prompts[:3]]
    traces.append(failed_trace(prompts[3], RuntimeError("HTTP 400")))

    scores = aggregate(traces, "mock")

    assert scores.failed_cases == 1
    assert scores.factual_accuracy == pytest.approx(3 / 4)
    assert scores.json_validity == pytest.approx(3 / 4)
    # Latency averages only the cases that got a response.
    assert scores.avg_latency_ms == pytest.approx(sum(t.latency_ms for t in traces[:3]) / 3)


def test_failed_trace_records_the_error():
    prompt = load_prompt_set(PROMPT_SET)["prompts"][0]
    trace = failed_trace(prompt, ValueError("bad request"))
    assert trace.status == "failed"
    assert trace.error == "ValueError: bad request"
    assert trace.response == "" and trace.factual_score == 0.0


def test_unsupported_model_is_rejected_before_any_call():
    with pytest.raises(ValueError, match="Unsupported model"):
        run_eval(RunConfig(prompt_set_path=PROMPT_SET, model="gpt-9000"))


def test_generated_prompt_set_has_at_least_300_unique_cases():
    ids = [p["id"] for p in load_prompt_set(GENERATED)["prompts"]]
    assert len(ids) >= 300 and len(set(ids)) == len(ids)


def test_generated_prompt_set_matches_generator_output():
    import importlib.util

    script = Path(__file__).parent.parent / "scripts" / "generate_prompt_set.py"
    spec = importlib.util.spec_from_file_location("generate_prompt_set", script)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    assert module.generate(300) == load_prompt_set(GENERATED)
    assert module.PERMANENT_ERROR_MARKER == PERMANENT_ERROR_MARKER


def test_fake_model_runs_generated_set_deterministically():
    config = RunConfig(prompt_set_path=GENERATED, model="fake-a")
    first, second = run_eval(config), run_eval(config)

    assert len(first.traces) == 300
    assert [t.response for t in first.traces] == [t.response for t in second.traces]
    assert first.scores.factual_accuracy == second.scores.factual_accuracy
    # The generated set is built so that scores are neither all 1.0 nor all 0.0.
    assert 0.5 < first.scores.factual_accuracy < 1.0
    assert 0.5 < first.scores.json_validity < 1.0
    assert first.scores.estimated_cost_usd == 0.0


def test_provider_for_groups_models_by_quota():
    assert provider_for("mock") == "mock"
    assert provider_for("cohere-command-a") == provider_for("cohere-command-r-plus") == "cohere"
    assert provider_for("fake-a") == "fake-a" != provider_for("fake-b")
    with pytest.raises(ValueError):
        provider_for("gpt-9000")


def test_model_config_exposes_template_and_parameters():
    for model in ("mock", "fake-a", "cohere-command-a", "cohere-command-r-plus"):
        config = model_config(model)
        assert set(config) == {"prompt_template", "model_params"}
    assert model_config("cohere-command-a") != model_config("cohere-command-r-plus")
    assert "JSON" in model_config("cohere-command-a")["prompt_template"]["system_when_json"]


def test_run_eval_records_permanent_failures_as_failed_cases(tmp_path):
    from tests.helpers import write_prompt_set

    path = write_prompt_set(tmp_path / "p.yaml", 20, permanent_failures=2)
    result = run_eval(RunConfig(prompt_set_path=path, model="fake-a"))

    assert len(result.traces) == 20
    assert result.scores.failed_cases == 2
    assert sum(t.status == "failed" for t in result.traces) == 2


def test_run_eval_retries_transient_failures(tmp_path, monkeypatch):
    from designops_eval.providers.fake import local_registry
    from tests.helpers import write_prompt_set

    monkeypatch.setenv("FAKE_PROVIDER_FAILURE_RATE", "0.3")
    path = write_prompt_set(tmp_path / "p.yaml", 20)
    result = run_eval(RunConfig(prompt_set_path=path, model="fake-a"))

    assert result.scores.failed_cases == 0
    assert local_registry().stats()["fake-a"]["transient_failures"] > 0
