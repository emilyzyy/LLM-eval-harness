import json
import re

import jsonschema

COST_PER_1K_INPUT = {
    "mock": 0.0,
    "claude-sonnet-4-6": 0.003,
    "claude-haiku-4-5-20251001": 0.00025,
}
COST_PER_1K_OUTPUT = {
    "mock": 0.0,
    "claude-sonnet-4-6": 0.015,
    "claude-haiku-4-5-20251001": 0.00125,
}


def score_factual_accuracy(response_text: str, expected_facts: list[str]) -> float:
    if not expected_facts:
        return 1.0
    hits = sum(1 for fact in expected_facts if fact.lower() in response_text.lower())
    return hits / len(expected_facts)


def _extract_json_objects(text: str) -> list[dict]:
    results = []

    for block in re.findall(r"```(?:json)?\s*(.*?)\s*```", text, re.DOTALL):
        try:
            obj = json.loads(block)
            if isinstance(obj, dict):
                results.append(obj)
        except json.JSONDecodeError:
            pass

    # Fallback: scan for raw JSON objects
    for i, ch in enumerate(text):
        if ch == "{":
            for j in range(len(text), i, -1):
                try:
                    obj = json.loads(text[i:j])
                    if isinstance(obj, dict):
                        results.append(obj)
                        break
                except json.JSONDecodeError:
                    continue

    return results


def score_json_validity(response_text: str, expected_schema: dict | None) -> float:
    if expected_schema is None:
        return 1.0

    for obj in _extract_json_objects(response_text):
        try:
            jsonschema.validate(obj, expected_schema)
            return 1.0
        except jsonschema.ValidationError:
            continue

    return 0.0


def estimate_cost(model: str, input_tokens: int, output_tokens: int) -> float:
    in_rate = COST_PER_1K_INPUT.get(model, 0.003)
    out_rate = COST_PER_1K_OUTPUT.get(model, 0.015)
    return (input_tokens / 1000) * in_rate + (output_tokens / 1000) * out_rate


def detect_regressions(
    current_scores: dict[str, float | int | None],
    baseline: dict[str, float | int],
    threshold: float = 0.05,
) -> list[str]:
    higher_is_better = {"factual_accuracy", "json_validity"}
    lower_is_better = {"avg_latency_ms", "estimated_cost_usd"}
    regressions = []

    for metric, current in current_scores.items():
        if metric not in baseline or current is None:
            continue
        base = float(baseline[metric])
        current = float(current)

        if metric in higher_is_better and current < base - threshold:
            regressions.append(
                f"{metric}: {current:.3f} (baseline {base:.3f}, delta {current - base:+.3f})"
            )
        elif metric in lower_is_better and base > 0 and current > base * (1 + threshold * 10):
            regressions.append(
                f"{metric}: {current:.1f} (baseline {base:.1f}, delta {current - base:+.1f})"
            )

    return regressions
