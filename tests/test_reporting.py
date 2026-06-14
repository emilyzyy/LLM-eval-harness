from designops_eval.reporting.markdown import generate_report


def test_report_contains_header(sample_result):
    report = generate_report(sample_result)
    assert "# DesignOps Eval Report" in report


def test_report_contains_run_id(sample_result):
    report = generate_report(sample_result)
    assert "test-run-id" in report


def test_report_contains_scores(sample_result):
    report = generate_report(sample_result)
    assert "1.000" in report  # factual_accuracy and json_validity are 1.0


def test_report_no_regressions_message(sample_result):
    report = generate_report(sample_result)
    assert "No regressions detected." in report


def test_report_regression_message(sample_result):
    sample_result.regressions = ["factual_accuracy: 0.500 (baseline 0.800, delta -0.300)"]
    report = generate_report(sample_result)
    assert "Regressions Detected" in report
    assert "factual_accuracy" in report


def test_report_includes_per_prompt_table(sample_result):
    report = generate_report(sample_result)
    assert "bolt_spec_001" in report
    assert "factual" in report


def test_report_with_baseline_shows_comparison(sample_result):
    baseline = {"factual_accuracy": 0.75, "json_validity": 0.90}
    report = generate_report(sample_result, baseline=baseline)
    assert "Baseline Comparison" in report
    assert "0.750" in report
