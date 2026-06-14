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
