"""Turns the raw benchmark output into a results table. Used by run.sh.

    summarize.py delta <before.prom> <after.prom>   counter changes during one run, as JSON
    summarize.py sustained <report.json> <rate>     exit 0 if the run kept up with the rate
    summarize.py table <results dir>                the Markdown report

Numbers are printed as measured. Nothing is estimated or filled in: a value
that was not measured is shown as "-".
"""

import json
import sys
from pathlib import Path

COUNTERS = [
    "gateway_requests_total",
    "gateway_capture_enqueued_total",
    "gateway_capture_dropped_total",
    "gateway_kafka_produce_errors_total",
    "gateway_proxy_overhead_seconds_sum",
    "gateway_proxy_overhead_seconds_count",
]

# A rate counts as sustained if at least this share of requests succeeded and
# the gateway completed them at least this close to the rate asked for.
MIN_SUCCESS = 0.999
MIN_THROUGHPUT_RATIO = 0.99


def read_counters(path: str) -> dict[str, float]:
    """Sum each counter over its label values (for example, over statuses)."""
    totals = dict.fromkeys(COUNTERS, 0.0)
    for line in Path(path).read_text().splitlines():
        if line.startswith("#") or not line.strip():
            continue
        name_and_labels, value = line.rsplit(" ", 1)
        name = name_and_labels.split("{", 1)[0]
        if name in totals:
            totals[name] += float(value)
    return totals


def delta(before_path: str, after_path: str) -> None:
    before, after = read_counters(before_path), read_counters(after_path)
    print(json.dumps({name: after[name] - before[name] for name in COUNTERS}, indent=2))


def ms(nanoseconds: float) -> str:
    return f"{nanoseconds / 1e6:.3f}"


def signed_ms(nanoseconds: float) -> str:
    return f"{nanoseconds / 1e6:+.3f}"


def load_runs(results: Path) -> dict:
    """Returns {(mode, target, rate): {"report": ..., "metrics": ... or None}}."""
    runs = {}
    for report_path in sorted((results / "raw").glob("*.json")):
        if report_path.name.endswith(".metrics.json"):
            continue
        mode, target, rate = report_path.stem.rsplit("-", 2)
        metrics_path = report_path.with_name(report_path.stem + ".metrics.json")
        runs[(mode, target, int(rate))] = {
            "report": json.loads(report_path.read_text()),
            "metrics": json.loads(metrics_path.read_text()) if metrics_path.exists() else None,
        }
    return runs


def sustained(report: dict, rate: int) -> bool:
    return report["success"] >= MIN_SUCCESS and report["throughput"] >= MIN_THROUGHPUT_RATIO * rate


def table(results_dir: str) -> None:
    results = Path(results_dir)
    runs = load_runs(results)
    modes = [m for m in ("nonstream", "stream") if any(k[0] == m for k in runs)]
    targets = ["direct", "proxy", "proxy_kafka"]
    rates = sorted({k[2] for k in runs})

    out = []
    out.append("# Gateway benchmark results")
    out.append("")
    out.append("Produced by `bench/run.sh`; do not edit by hand. Raw vegeta reports and")
    out.append("counter changes for every run are in `raw/`.")
    out.append("")
    out.append("## Setup")
    out.append("")
    out.append("```")
    out.append((results / "machine.txt").read_text().rstrip())
    out.append("```")
    out.append("")
    out.append("Targets: `direct` is the load generator calling the fake upstream; `proxy` goes")
    out.append("through the gateway with capture off; `proxy_kafka` goes through the gateway")
    out.append("with capture publishing to Redpanda.")
    out.append("")
    out.append("How to read the columns:")
    out.append("")
    out.append("- **achieved** is successful responses per second over the run. **ok** is the share of requests that got a 200.")
    out.append("- **p50/p95/p99** are end-to-end latencies seen by the load generator, in milliseconds. For streams this is the time to the end of the stream.")
    out.append("- **added** is this row's percentile minus the `direct` percentile at the same rate. It compares two separate runs, so differences of a few hundredths of a millisecond are within run-to-run noise and can be negative.")
    out.append("- **overhead** is the mean of the gateway's own `gateway_proxy_overhead_seconds` histogram during the run: time inside the gateway not spent waiting on the upstream. It excludes the extra network hop, which **added** includes.")
    out.append("- **lost captures** is (records dropped at the capture queue + records Kafka did not acknowledge) / requests handled, from the gateway's counters.")
    out.append("")

    for mode in modes:
        title = "Non-streaming" if mode == "nonstream" else "Streaming (SSE)"
        out.append(f"## {title}")
        out.append("")
        out.append("| rate (req/s) | target | achieved (req/s) | ok | p50 | p95 | p99 | added p50 | added p95 | added p99 | overhead (mean) | lost captures |")
        out.append("|---:|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|")
        for rate in rates:
            base = runs.get((mode, "direct", rate))
            for target in targets:
                run = runs.get((mode, target, rate))
                if run is None:
                    continue
                report, lat = run["report"], run["report"]["latencies"]
                added = ["-", "-", "-"]
                if target != "direct" and base is not None:
                    base_lat = base["report"]["latencies"]
                    added = [signed_ms(lat[p] - base_lat[p]) for p in ("50th", "95th", "99th")]
                overhead, lost = "-", "-"
                metrics = run["metrics"]
                if metrics is not None:
                    count = metrics["gateway_proxy_overhead_seconds_count"]
                    if count > 0:
                        overhead = f"{metrics['gateway_proxy_overhead_seconds_sum'] / count * 1000:.3f}"
                    handled = metrics["gateway_requests_total"]
                    if target == "proxy_kafka" and handled > 0:
                        dropped = metrics["gateway_capture_dropped_total"]
                        failed = metrics["gateway_kafka_produce_errors_total"]
                        lost = f"{(dropped + failed) / handled:.4%} ({int(dropped)} dropped + {int(failed)} failed of {int(handled)})"
                out.append(
                    f"| {rate} | {target} | {report['throughput']:.1f} | {report['success']:.4%} "
                    f"| {ms(lat['50th'])} | {ms(lat['95th'])} | {ms(lat['99th'])} "
                    f"| {added[0]} | {added[1]} | {added[2]} | {overhead} | {lost} |"
                )
        out.append("")

    out.append("## Maximum sustained throughput")
    out.append("")
    out.append(f"The highest tested rate at which at least {MIN_SUCCESS:.1%} of requests succeeded and")
    out.append(f"achieved throughput was at least {MIN_THROUGHPUT_RATIO:.0%} of the rate asked for. Rates tested:")
    out.append(f"{', '.join(str(r) for r in rates)} req/s. A value equal to the highest tested rate means the")
    out.append("limit was not reached. After a target fails at a rate, higher rates are not")
    out.append("run for it, so it has no rows above that rate. The load generator and the fake upstream share the")
    out.append("machine with the gateway, so the `direct` row is the ceiling of the test")
    out.append("setup itself.")
    out.append("")
    out.append("| mode | target | max sustained (req/s) | first rate that failed |")
    out.append("|---|---|---:|---:|")
    for mode in modes:
        for target in targets:
            tested = [r for r in rates if (mode, target, r) in runs]
            passed = [r for r in tested if sustained(runs[(mode, target, r)]["report"], r)]
            failed = [r for r in tested if r not in passed]
            best = max(passed) if passed else "none"
            first_failure = min(failed) if failed else "none"
            out.append(f"| {mode} | {target} | {best} | {first_failure} |")
    out.append("")
    print("\n".join(out))


if __name__ == "__main__":
    if len(sys.argv) == 4 and sys.argv[1] == "delta":
        delta(sys.argv[2], sys.argv[3])
    elif len(sys.argv) == 4 and sys.argv[1] == "sustained":
        report = json.loads(Path(sys.argv[2]).read_text())
        sys.exit(0 if sustained(report, int(sys.argv[3])) else 1)
    elif len(sys.argv) == 3 and sys.argv[1] == "table":
        table(sys.argv[2])
    else:
        sys.exit(__doc__)
