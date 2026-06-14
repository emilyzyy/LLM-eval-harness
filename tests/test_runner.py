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
