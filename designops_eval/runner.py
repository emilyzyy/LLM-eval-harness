import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Callable

import yaml

from .models.config import RunConfig
from .models.results import RunResult, RunScores, TraceEntry
from .providers.base import ModelResponse
from .providers.errors import ErrorKind, classify_error
from .providers.fake import FAKE_MODEL_PREFIX
from .providers.registry import provider_for
from .scorer import (
    detect_regressions,
    estimate_cost,
    score_factual_accuracy,
    score_json_validity,
)


_MOCK_RESPONSES: dict[str, ModelResponse] = {
    "bolt_spec_001": ModelResponse(
        text=(
            "An M8 bolt has a nominal diameter of 8mm with a standard coarse thread pitch of 1.25mm. "
            "The minor diameter is approximately 6.647mm. Thread pitch refers to the distance between "
            "adjacent thread crests."
        ),
        latency_ms=108,
        input_tokens=22,
        output_tokens=38,
        token_source="mock",
    ),
    "part_lookup_001": ModelResponse(
        text=(
            'Found the part. Here is the specification:\n```json\n'
            '{"sku": "4521", "part_number": "BRK-4521-A", '
            '"description": "Mild steel bracket", "material": "S235JR"}\n```'
        ),
        latency_ms=95,
        input_tokens=19,
        output_tokens=42,
        token_source="mock",
    ),
    "material_spec_001": ModelResponse(
        text=(
            "6061-T6 aluminum alloy has an ultimate tensile strength of 276 MPa (40,000 psi) "
            "and a yield strength of 241 MPa. It is widely used in aerospace, automotive, and "
            "structural applications."
        ),
        latency_ms=113,
        input_tokens=20,
        output_tokens=41,
        token_source="mock",
    ),
    "tolerance_001": ModelResponse(
        text=(
            "An H7/h6 fit is a clearance fit. The H7 hole tolerance paired with the h6 shaft "
            "tolerance produces a small clearance, making it suitable for precision sliding "
            "shaft assemblies where easy removal is required."
        ),
        latency_ms=102,
        input_tokens=21,
        output_tokens=44,
        token_source="mock",
    ),
    "design_param_001": ModelResponse(
        text=(
            'Here is the component specification:\n```json\n'
            '{"component_type": "bracket", "material": "steel", '
            '"wall_thickness_mm": 5.0, "finish": "powder_coat"}\n```'
        ),
        latency_ms=118,
        input_tokens=24,
        output_tokens=39,
        token_source="mock",
    ),
}

_DEFAULT_MOCK = ModelResponse(
    text="I don't have specific information about that component.",
    latency_ms=85,
    input_tokens=15,
    output_tokens=12,
    token_source="mock",
)


def _call_model(
    model: str,
    prompt_id: str,
    prompt_text: str,
    expects_json: bool = False,
    before_attempt: Callable[[], None] | None = None,
) -> ModelResponse:
    if model == "mock":
        return _MOCK_RESPONSES.get(prompt_id, _DEFAULT_MOCK)

    if model.startswith("cohere-"):
        from .providers.cohere_client import call_cohere
        return call_cohere(
            model, prompt_text, expects_json=expects_json, before_attempt=before_attempt
        )

    if model.startswith(FAKE_MODEL_PREFIX):
        from .providers.fake import call_fake
        return call_fake(
            model, prompt_id, prompt_text, expects_json=expects_json, before_attempt=before_attempt
        )

    raise ValueError(
        f"Unsupported model: {model!r}. "
        "Supported: 'mock', 'cohere-command-a', 'cohere-command-r-plus', 'fake-<name>'"
    )


def load_prompt_set(path: str) -> dict:
    return yaml.safe_load(Path(path).read_text(encoding="utf-8"))


def evaluate_case(
    prompt: dict,
    model: str,
    before_attempt: Callable[[], None] | None = None,
) -> TraceEntry:
    """Call the model for one prompt-set entry and score the response.

    before_attempt runs ahead of every provider attempt, including retries.
    Provider errors propagate; the caller decides what a failure means.
    """
    pid = prompt["id"]
    text = prompt["prompt"]

    expected_facts = prompt.get("expected_facts", [])
    tool_calls = prompt.get("expected_tool_calls", [])
    expected_schema = tool_calls[0].get("schema") if tool_calls else None
    expects_json = expected_schema is not None

    resp = _call_model(model, pid, text, expects_json=expects_json, before_attempt=before_attempt)

    return TraceEntry(
        prompt_id=pid,
        prompt=text,
        response=resp.text,
        latency_ms=resp.latency_ms,
        input_tokens=resp.input_tokens,
        output_tokens=resp.output_tokens,
        factual_score=score_factual_accuracy(resp.text, expected_facts),
        json_validity_score=score_json_validity(resp.text, expected_schema),
        metadata={
            "category": prompt.get("category", "unknown"),
            "token_source": resp.token_source,
        },
    )


def failed_trace(prompt: dict, error: BaseException) -> TraceEntry:
    """Build the trace recorded for a case whose provider call failed permanently."""
    return TraceEntry(
        prompt_id=prompt["id"],
        prompt=prompt["prompt"],
        response="",
        latency_ms=0.0,
        input_tokens=0,
        output_tokens=0,
        factual_score=0.0,
        json_validity_score=0.0,
        metadata={"category": prompt.get("category", "unknown"), "token_source": "none"},
        status="failed",
        error=f"{type(error).__name__}: {error}",
    )


def aggregate(traces: list[TraceEntry], model: str) -> RunScores:
    """Aggregate per-case traces into run scores.

    Failed cases count as 0.0 in both accuracy scores. Average latency covers
    only the cases that received a response.
    """
    n = len(traces)
    answered = [t for t in traces if t.status != "failed"]
    input_tokens = sum(t.input_tokens for t in traces)
    output_tokens = sum(t.output_tokens for t in traces)
    return RunScores(
        factual_accuracy=sum(t.factual_score for t in traces) / n,
        json_validity=sum(t.json_validity_score for t in traces) / n,
        avg_latency_ms=(
            sum(t.latency_ms for t in answered) / len(answered) if answered else 0.0
        ),
        total_input_tokens=input_tokens,
        total_output_tokens=output_tokens,
        estimated_cost_usd=estimate_cost(model, input_tokens, output_tokens),
        failed_cases=n - len(answered),
    )


def build_run_result(
    run_id: str,
    model: str,
    timestamp: str,
    prompt_set_name: str,
    traces: list[TraceEntry],
    baseline: dict | None = None,
) -> RunResult:
    scores = aggregate(traces, model)
    regressions = (
        detect_regressions(scores.model_dump(), baseline) if baseline else []
    )
    return RunResult(
        run_id=run_id,
        model=model,
        timestamp=timestamp,
        prompt_set=prompt_set_name,
        scores=scores,
        regressions=regressions,
        traces=traces,
    )


def run_eval(config: RunConfig, baseline: dict | None = None) -> RunResult:
    provider_for(config.model)  # rejects an unsupported model before any work starts
    prompt_set_data = load_prompt_set(config.prompt_set_path)
    run_id = str(uuid.uuid4())
    timestamp = datetime.now(timezone.utc).isoformat()

    traces: list[TraceEntry] = []
    for prompt in prompt_set_data["prompts"]:
        try:
            traces.append(evaluate_case(prompt, config.model))
        except Exception as exc:
            if classify_error(exc) is not ErrorKind.PERMANENT:
                raise
            traces.append(failed_trace(prompt, exc))

    return build_run_result(
        run_id, config.model, timestamp, prompt_set_data["name"], traces, baseline
    )
