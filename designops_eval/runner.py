import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path

import yaml

from .models.config import RunConfig
from .models.results import RunResult, RunScores, TraceEntry
from .scorer import (
    detect_regressions,
    estimate_cost,
    score_factual_accuracy,
    score_json_validity,
)


@dataclass
class _ModelResponse:
    text: str
    latency_ms: float
    input_tokens: int
    output_tokens: int


_MOCK_RESPONSES: dict[str, _ModelResponse] = {
    "bolt_spec_001": _ModelResponse(
        text=(
            "An M8 bolt has a nominal diameter of 8mm with a standard coarse thread pitch of 1.25mm. "
            "The minor diameter is approximately 6.647mm. Thread pitch refers to the distance between "
            "adjacent thread crests."
        ),
        latency_ms=108,
        input_tokens=22,
        output_tokens=38,
    ),
    "part_lookup_001": _ModelResponse(
        text=(
            'Found the part. Here is the specification:\n```json\n'
            '{"sku": "4521", "part_number": "BRK-4521-A", '
            '"description": "Mild steel bracket", "material": "S235JR"}\n```'
        ),
        latency_ms=95,
        input_tokens=19,
        output_tokens=42,
    ),
    "material_spec_001": _ModelResponse(
        text=(
            "6061-T6 aluminum alloy has an ultimate tensile strength of 276 MPa (40,000 psi) "
            "and a yield strength of 241 MPa. It is widely used in aerospace, automotive, and "
            "structural applications."
        ),
        latency_ms=113,
        input_tokens=20,
        output_tokens=41,
    ),
    "tolerance_001": _ModelResponse(
        text=(
            "An H7/h6 fit is a clearance fit. The H7 hole tolerance paired with the h6 shaft "
            "tolerance produces a small clearance, making it suitable for precision sliding "
            "shaft assemblies where easy removal is required."
        ),
        latency_ms=102,
        input_tokens=21,
        output_tokens=44,
    ),
    "design_param_001": _ModelResponse(
        text=(
            'Here is the component specification:\n```json\n'
            '{"component_type": "bracket", "material": "steel", '
            '"wall_thickness_mm": 5.0, "finish": "powder_coat"}\n```'
        ),
        latency_ms=118,
        input_tokens=24,
        output_tokens=39,
    ),
}

_DEFAULT_MOCK = _ModelResponse(
    text="I don't have specific information about that component.",
    latency_ms=85,
    input_tokens=15,
    output_tokens=12,
)


def _call_model(model: str, prompt_id: str, prompt_text: str) -> _ModelResponse:
    if model == "mock":
        return _MOCK_RESPONSES.get(prompt_id, _DEFAULT_MOCK)
    raise ValueError(f"Unsupported model: {model!r}. Supported: 'mock'")


def run_eval(config: RunConfig, baseline: dict | None = None) -> RunResult:
    prompt_set_data = yaml.safe_load(Path(config.prompt_set_path).read_text(encoding="utf-8"))
    run_id = str(uuid.uuid4())
    timestamp = datetime.now(timezone.utc).isoformat()

    traces: list[TraceEntry] = []
    for prompt in prompt_set_data["prompts"]:
        pid = prompt["id"]
        text = prompt["prompt"]
        resp = _call_model(config.model, pid, text)

        expected_facts = prompt.get("expected_facts", [])
        tool_calls = prompt.get("expected_tool_calls", [])
        expected_schema = tool_calls[0].get("schema") if tool_calls else None

        factual = score_factual_accuracy(resp.text, expected_facts)
        json_val = score_json_validity(resp.text, expected_schema)

        traces.append(TraceEntry(
            prompt_id=pid,
            prompt=text,
            response=resp.text,
            latency_ms=resp.latency_ms,
            input_tokens=resp.input_tokens,
            output_tokens=resp.output_tokens,
            factual_score=factual,
            json_validity_score=json_val,
            metadata={"category": prompt.get("category", "unknown")},
        ))

    n = len(traces)
    scores = RunScores(
        factual_accuracy=sum(t.factual_score for t in traces) / n,
        json_validity=sum(t.json_validity_score for t in traces) / n,
        avg_latency_ms=sum(t.latency_ms for t in traces) / n,
        total_input_tokens=sum(t.input_tokens for t in traces),
        total_output_tokens=sum(t.output_tokens for t in traces),
        estimated_cost_usd=estimate_cost(
            config.model,
            sum(t.input_tokens for t in traces),
            sum(t.output_tokens for t in traces),
        ),
    )

    regressions = (
        detect_regressions(scores.model_dump(), baseline) if baseline else []
    )

    return RunResult(
        run_id=run_id,
        model=config.model,
        timestamp=timestamp,
        prompt_set=prompt_set_data["name"],
        scores=scores,
        regressions=regressions,
        traces=traces,
    )
