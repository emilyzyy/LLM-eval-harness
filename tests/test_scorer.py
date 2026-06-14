from designops_eval.scorer import (
    detect_regressions,
    estimate_cost,
    score_factual_accuracy,
    score_json_validity,
)


class TestFactualAccuracy:
    def test_all_found(self):
        score = score_factual_accuracy(
            "The M8 bolt has 8mm diameter and 1.25mm pitch.",
            ["8mm", "1.25mm", "pitch"],
        )
        assert score == 1.0

    def test_partial(self):
        score = score_factual_accuracy(
            "The M8 bolt has 8mm diameter.",
            ["8mm", "1.25mm", "pitch"],
        )
        assert abs(score - 1 / 3) < 0.01

    def test_none_found(self):
        score = score_factual_accuracy("No relevant content.", ["8mm", "1.25mm", "pitch"])
        assert score == 0.0

    def test_empty_facts_returns_perfect(self):
        assert score_factual_accuracy("Any text here.", []) == 1.0

    def test_case_insensitive(self):
        score = score_factual_accuracy("aluminum alloy", ["Aluminum"])
        assert score == 1.0


class TestJsonValidity:
    def test_valid_code_block(self):
        text = '```json\n{"sku": "4521", "part_number": "BRK"}\n```'
        schema = {
            "type": "object",
            "properties": {"sku": {"type": "string"}},
            "required": ["sku"],
        }
        assert score_json_validity(text, schema) == 1.0

    def test_missing_required_field(self):
        text = '```json\n{"part_number": "BRK"}\n```'
        schema = {
            "type": "object",
            "properties": {"sku": {"type": "string"}},
            "required": ["sku"],
        }
        assert score_json_validity(text, schema) == 0.0

    def test_no_json_in_text(self):
        schema = {"type": "object", "required": ["sku"]}
        assert score_json_validity("No JSON here.", schema) == 0.0

    def test_no_schema_returns_perfect(self):
        assert score_json_validity("anything", None) == 1.0

    def test_raw_json_without_code_block(self):
        text = 'Here: {"sku": "4521"}'
        schema = {
            "type": "object",
            "properties": {"sku": {"type": "string"}},
            "required": ["sku"],
        }
        assert score_json_validity(text, schema) == 1.0


class TestEstimateCost:
    def test_mock_model_is_free(self):
        assert estimate_cost("mock", 1000, 1000) == 0.0

    def test_known_model(self):
        cost = estimate_cost("claude-sonnet-4-6", 1000, 1000)
        assert cost > 0


class TestDetectRegressions:
    def test_no_regressions(self):
        current = {"factual_accuracy": 0.9, "json_validity": 0.95}
        baseline = {"factual_accuracy": 0.8, "json_validity": 0.9}
        assert detect_regressions(current, baseline) == []

    def test_factual_regression_detected(self):
        current = {"factual_accuracy": 0.5, "json_validity": 0.95}
        baseline = {"factual_accuracy": 0.8, "json_validity": 0.9}
        regressions = detect_regressions(current, baseline)
        assert len(regressions) == 1
        assert "factual_accuracy" in regressions[0]

    def test_empty_baseline(self):
        current = {"factual_accuracy": 0.5}
        assert detect_regressions(current, {}) == []

    def test_latency_regression(self):
        current = {"avg_latency_ms": 1000.0}
        baseline = {"avg_latency_ms": 100.0}
        regressions = detect_regressions(current, baseline)
        assert len(regressions) == 1
        assert "avg_latency_ms" in regressions[0]
