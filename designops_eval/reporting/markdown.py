from ..models.results import RunResult


def generate_report(result: RunResult, baseline: dict | None = None) -> str:
    s = result.scores
    lines = [
        "# DesignOps Eval Report",
        "",
        f"**Run ID:** `{result.run_id}`",
        f"**Model:** `{result.model}`",
        f"**Timestamp:** {result.timestamp}",
        f"**Prompt Set:** {result.prompt_set}",
        "",
        "## Scores",
        "",
        "| Metric | Value |",
        "|--------|-------|",
        f"| Factual Accuracy | {s.factual_accuracy:.3f} |",
        f"| JSON Validity | {s.json_validity:.3f} |",
        f"| Avg Latency (ms) | {s.avg_latency_ms:.1f} |",
        f"| Total Input Tokens | {s.total_input_tokens} |",
        f"| Total Output Tokens | {s.total_output_tokens} |",
        f"| Estimated Cost (USD) | ${s.estimated_cost_usd:.6f} |",
    ]
    if s.failed_cases:
        lines.append(f"| Failed Cases | {s.failed_cases} |")
    lines.append("")

    if baseline:
        lines += [
            "## Baseline Comparison",
            "",
            "| Metric | Current | Baseline | Delta |",
            "|--------|---------|----------|-------|",
        ]
        for metric in ("factual_accuracy", "json_validity", "avg_latency_ms", "estimated_cost_usd"):
            if metric not in baseline:
                continue
            current_val = getattr(s, metric, None)
            base_val = float(baseline[metric])
            if current_val is None:
                continue
            delta = float(current_val) - base_val
            lines.append(f"| {metric} | {current_val:.3f} | {base_val:.3f} | {delta:+.3f} |")
        lines.append("")

    if result.regressions:
        lines += ["## Regressions Detected", ""]
        for reg in result.regressions:
            lines.append(f"- {reg}")
        lines.append("")
    else:
        lines += ["## Regressions", "", "No regressions detected.", ""]

    lines += [
        "## Per-Prompt Results",
        "",
        "| Prompt ID | Category | Factual | JSON Valid | Latency (ms) | Tokens In | Tokens Out |",
        "|-----------|----------|---------|------------|--------------|-----------|------------|",
    ]
    for t in result.traces:
        category = t.metadata.get("category", "—")
        lines.append(
            f"| {t.prompt_id} | {category} | {t.factual_score:.2f} | "
            f"{t.json_validity_score:.2f} | {t.latency_ms:.0f} | "
            f"{t.input_tokens} | {t.output_tokens} |"
        )

    return "\n".join(lines) + "\n"
