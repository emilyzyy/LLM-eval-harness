#!/usr/bin/env python3
"""Reproducible benchmarks and fault-injection experiments, run locally.

Every scenario starts its own emulated AWS endpoint and fake provider, runs
real worker processes, and writes a JSON report to results/benchmarks/. No
real AWS resource or paid API is used.

    python -m benchmarks.run baseline
    python -m benchmarks.run duplicate
    python -m benchmarks.run crash --trials 5
    python -m benchmarks.run rate-limit
    python -m benchmarks.run throughput
    python -m benchmarks.run correctness
    python -m benchmarks.run all
"""

import argparse
import hashlib
import importlib.util
import json
import sys
import tempfile
import time
from pathlib import Path

import yaml

from benchmarks.harness import (
    REPO_ROOT,
    LocalEnvironment,
    duplicate_result_count,
    write_report,
)
from designops_eval.distributed.finalize import finalize_run
from designops_eval.models.config import RunConfig
from designops_eval.providers.fake import FakeProviderConfig
from designops_eval.runner import load_prompt_set, run_eval

GENERATED_300 = "evals/prompt_sets/generated_300.yaml"
LOGS = REPO_ROOT / "results" / "benchmarks" / "logs"


def _generate_prompt_set(cases: int, directory: Path) -> str:
    spec = importlib.util.spec_from_file_location(
        "generate_prompt_set", REPO_ROOT / "scripts" / "generate_prompt_set.py"
    )
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    path = directory / f"generated_{cases}.yaml"
    path.write_text(yaml.safe_dump(module.generate(cases), sort_keys=False), encoding="utf-8")
    return str(path)


def _environment(name: str, **kwargs) -> LocalEnvironment:
    return LocalEnvironment(log_dir=LOGS / name, **kwargs)


# ---------------------------------------------------------------------------

def baseline(args) -> dict:
    """Single-process run_eval over the generated set against the fake provider."""
    config = FakeProviderConfig(latency_ms=args.latency_ms)
    with _environment("baseline", provider_config=config) as env:
        started = time.monotonic()
        result = run_eval(RunConfig(prompt_set_path=GENERATED_300, model="fake-a"))
        elapsed = time.monotonic() - started
        stats = env.provider_stats()["fake-a"]
    cases = len(result.traces)
    return {
        "scenario": "baseline",
        "cases": cases,
        "provider_latency_ms": args.latency_ms,
        "runtime_seconds": round(elapsed, 3),
        "throughput_cases_per_second": round(cases / elapsed, 2),
        "provider_requests": stats["requests_total"],
        "factual_accuracy": result.scores.factual_accuracy,
        "json_validity": result.scores.json_validity,
    }


def duplicate(args) -> dict:
    """Publish every shard `--copies` times and count committed results."""
    cases = len(load_prompt_set(GENERATED_300)["prompts"])
    config = FakeProviderConfig(latency_ms=args.latency_ms)
    with _environment("duplicate", provider_config=config) as env:
        for _ in range(args.copies):
            env.submit("dup-run", GENERATED_300, ["fake-a"], 25)
        published = env.queue_depth()
        started = time.monotonic()
        for i in range(args.workers):
            env.spawn_worker(f"worker-{i}")
        env.wait_for_workers(timeout=args.timeout)
        elapsed = time.monotonic() - started
        records = env.results("dup-run")
        stats = env.provider_stats()["fake-a"]
        completed = env.worker_events("shard_completed")
        report = {
            "scenario": "duplicate",
            "cases": cases,
            "copies_of_each_shard": args.copies,
            "messages_published": published,
            "redundant_deliveries": published - published // args.copies,
            "workers": args.workers,
            "committed_results": len(records),
            "duplicate_committed_results": duplicate_result_count(records),
            "lost_results": cases - len(records),
            "shard_deliveries_completed": len(completed),
            "cases_skipped_as_already_committed": sum(e["skipped"] for e in completed),
            "commit_races_lost": sum(e["lost_races"] for e in completed),
            "repeated_provider_calls": stats["duplicate_completions"],
            "messages_left_on_queue": env.queue_depth(),
            "messages_in_dlq": env.queue_depth(env.dlq_url),
            "runtime_seconds": round(elapsed, 3),
        }
    report["passed"] = (
        report["committed_results"] == cases
        and report["duplicate_committed_results"] == 0
        and report["messages_left_on_queue"] == 0
        and report["messages_in_dlq"] == 0
    )
    return report


def crash(args) -> dict:
    """Kill a worker after 18 of 25 commits; a second worker must finish the shard."""
    total, crash_after = 25, 18
    trials = []
    with tempfile.TemporaryDirectory() as tmp:
        prompt_set = _generate_prompt_set(total, Path(tmp))
        for trial in range(args.trials):
            config = FakeProviderConfig(latency_ms=args.latency_ms)
            with _environment(f"crash-{trial}", provider_config=config,
                              visibility_timeout=args.visibility_timeout) as env:
                run_id = f"crash-run-{trial}"
                env.submit(run_id, prompt_set, ["fake-a"], total)

                victim = env.spawn_worker("victim", "--crash-after-commits", str(crash_after))
                victim.wait(timeout=args.timeout)
                crashed_at = time.monotonic()
                before = {r.case_id: r for r in env.results(run_id)}

                env.spawn_worker("rescuer", idle_exit_seconds=args.visibility_timeout * 3)
                finished_at = env.wait_for_results(run_id, total, timeout=args.timeout)
                env.wait_for_workers(timeout=args.timeout)

                after = {r.case_id: r for r in env.results(run_id)}
                stats = env.provider_stats()["fake-a"]
                trials.append({
                    "victim_exit_code": victim.returncode,
                    "committed_at_crash": len(before),
                    "committed_after_recovery": len(after),
                    "committed_by_rescuer": sum(r.worker_id == "rescuer" for r in after.values()),
                    "pre_crash_results_lost_or_changed": sum(
                        after.get(case_id) != record for case_id, record in before.items()
                    ),
                    "duplicate_committed_results": duplicate_result_count(list(after.values())),
                    "repeated_provider_calls": stats["duplicate_completions"],
                    "recovery_seconds": round(finished_at - crashed_at, 3),
                    "messages_left_on_queue": env.queue_depth(),
                })
    recovery = sorted(t["recovery_seconds"] for t in trials)
    return {
        "scenario": "crash",
        "cases": total,
        "crash_after_commits": crash_after,
        "visibility_timeout_seconds": args.visibility_timeout,
        "worker_crashes": len(trials),
        "lost_committed_results": sum(t["pre_crash_results_lost_or_changed"] for t in trials),
        "recovery_seconds_min": recovery[0],
        "recovery_seconds_median": recovery[len(recovery) // 2],
        "recovery_seconds_max": recovery[-1],
        "trials": trials,
        "passed": all(
            t["victim_exit_code"] == 137
            and t["committed_at_crash"] == crash_after
            and t["committed_after_recovery"] == total
            and t["committed_by_rescuer"] == total - crash_after
            and t["pre_crash_results_lost_or_changed"] == 0
            and t["duplicate_committed_results"] == 0
            for t in trials
        ),
    }


def rate_limit(args) -> dict:
    """Three workers against one quota: shared limiter versus per-process limiters."""
    limits = f"fake-a={args.limit}"
    modes = {}
    with tempfile.TemporaryDirectory() as tmp:
        prompt_set = _generate_prompt_set(args.cases, Path(tmp))
        for mode in ("shared", "local"):
            config = FakeProviderConfig(
                latency_ms=args.latency_ms, rate_limit=args.limit, window_seconds=args.window,
            )
            with _environment(f"rate-limit-{mode}", provider_config=config,
                              visibility_timeout=120) as env:
                run_id = f"rate-{mode}"
                env.submit(run_id, prompt_set, ["fake-a"], 25)
                started = time.monotonic()
                for i in range(args.workers):
                    env.spawn_worker(
                        f"worker-{i}", "--limiter", mode, "--provider-limits", limits,
                        "--limit-window-seconds", str(args.window), "--retry-delay", "1",
                        env={"DESIGNOPS_RETRY_MAX_ATTEMPTS": "10"},
                        idle_exit_seconds=max(5.0, args.window / 4),
                    )
                finished = env.wait_for_results(run_id, args.cases, timeout=args.timeout)
                stats = env.provider_stats()["fake-a"]
                records = env.results(run_id)
                modes[mode] = {
                    "committed_results": len(records),
                    "duplicate_committed_results": duplicate_result_count(records),
                    "provider_requests": stats["requests_total"],
                    "rate_limit_rejections_429": stats["rate_limited"],
                    "max_requests_accepted_in_any_window": stats["max_window_count"],
                    "runtime_seconds": round(finished - started, 3),
                }
    return {
        "scenario": "rate-limit",
        "workers": args.workers,
        "cases": args.cases,
        "provider_limit": args.limit,
        "window_seconds": args.window,
        "equivalent_rpm": round(args.limit * 60 / args.window),
        **modes,
        "passed": (
            modes["shared"]["rate_limit_rejections_429"] == 0
            and modes["shared"]["max_requests_accepted_in_any_window"] <= args.limit
            and modes["shared"]["committed_results"] == args.cases
        ),
    }


def throughput(args) -> dict:
    """Three independently limited providers: one worker versus three, plus a control.

    The control runs three workers against a single provider with three times
    the cases. If extra workers alone raised throughput, the control would be
    as fast as the parallel run; a shared quota should prevent that.
    """
    models = ["fake-a", "fake-b", "fake-c"]
    limits = ",".join(f"{m}={args.limit}" for m in models)
    runs = {}
    with tempfile.TemporaryDirectory() as tmp:
        prompt_set = _generate_prompt_set(args.cases, Path(tmp))
        triple = _generate_prompt_set(args.cases * 3, Path(tmp))
        plans = {
            "one_worker_three_providers": (1, models, prompt_set),
            "three_workers_three_providers": (3, models, prompt_set),
            "three_workers_one_provider": (3, ["fake-a"], triple),
        }
        for name, (workers, run_models, path) in plans.items():
            config = FakeProviderConfig(
                latency_ms=args.latency_ms, rate_limit=args.limit, window_seconds=args.window,
            )
            total = len(load_prompt_set(path)["prompts"]) * len(run_models)
            with _environment(f"throughput-{name}", provider_config=config,
                              visibility_timeout=120) as env:
                env.submit(name, path, run_models, 25)
                started = time.monotonic()
                for i in range(workers):
                    env.spawn_worker(
                        f"worker-{i}", "--limiter", "shared", "--provider-limits", limits,
                        "--limit-window-seconds", str(args.window),
                        idle_exit_seconds=5.0,
                    )
                finished = env.wait_for_results(name, total, timeout=args.timeout)
                elapsed = finished - started
                stats = env.provider_stats()
                runs[name] = {
                    "workers": workers,
                    "providers": len(run_models),
                    "cases_total": total,
                    "runtime_seconds": round(elapsed, 3),
                    "throughput_cases_per_second": round(total / elapsed, 2),
                    "rate_limit_rejections_429": sum(s["rate_limited"] for s in stats.values()),
                }
    sequential = runs["one_worker_three_providers"]["runtime_seconds"]
    parallel = runs["three_workers_three_providers"]["runtime_seconds"]
    return {
        "scenario": "throughput",
        "cases_per_provider": args.cases,
        "provider_limit": args.limit,
        "window_seconds": args.window,
        "provider_latency_ms": args.latency_ms,
        **runs,
        "measured_speedup_parallel_over_sequential": round(sequential / parallel, 2),
        "single_provider_throughput_with_three_workers":
            runs["three_workers_one_provider"]["throughput_cases_per_second"],
    }


def _digest(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def correctness(args) -> dict:
    """Distributed results and artifacts versus the single-process harness."""
    models = ["fake-a", "fake-b"]
    prompt_set = load_prompt_set(GENERATED_300)
    config = FakeProviderConfig(latency_ms=args.latency_ms, failure_rate=args.failure_rate)
    with tempfile.TemporaryDirectory() as tmp, \
            _environment("correctness", provider_config=config) as env:
        expected = {m: run_eval(RunConfig(prompt_set_path=GENERATED_300, model=m)) for m in models}

        env.submit("correct-run", GENERATED_300, models, 25)
        for i in range(args.workers):
            env.spawn_worker(f"worker-{i}")
        env.wait_for_results("correct-run", len(prompt_set["prompts"]) * len(models),
                             timeout=args.timeout)
        env.wait_for_workers(timeout=args.timeout)
        actual = {r.model: r for r in finalize_run(env.store, "correct-run", prompt_set, models)}

        per_model = {}
        for model in models:
            want, got = expected[model], actual[model]
            mismatched = sum(
                g.model_dump(exclude={"latency_ms"}) != w.model_dump(exclude={"latency_ms"})
                for g, w in zip(got.traces, want.traces)
            )
            score_fields = ("factual_accuracy", "json_validity", "total_input_tokens",
                            "total_output_tokens", "failed_cases")
            per_model[model] = {
                "cases_expected": len(want.traces),
                "cases_present": len(got.traces),
                "order_matches": [t.prompt_id for t in got.traces] == [t.prompt_id for t in want.traces],
                "mismatched_cases": mismatched,
                "aggregate_scores_match": all(
                    getattr(got.scores, f) == getattr(want.scores, f) for f in score_fields
                ),
            }

        outputs = []
        for attempt in ("first", "second"):
            out = Path(tmp) / attempt
            env.cli("finalize", "--run-id", "correct-run", "--prompt-set", GENERATED_300,
                    "--models", ",".join(models), "--output-dir", str(out))
            outputs.append(out)
        artifacts = [f"{m}/{name}" for m in models for name in ("latest.json", "traces.json", "report.md")]
        artifact_hashes = {a: _digest(outputs[0] / a) for a in artifacts}
        reproducible = all(_digest(outputs[1] / a) == artifact_hashes[a] for a in artifacts)

    return {
        "scenario": "correctness",
        "workers": args.workers,
        "provider_transient_failure_rate": args.failure_rate,
        "models": per_model,
        "artifacts_identical_across_two_finalize_runs": reproducible,
        "artifact_sha256": artifact_hashes,
        "passed": reproducible and all(
            m["cases_present"] == m["cases_expected"] and m["order_matches"]
            and m["mismatched_cases"] == 0 and m["aggregate_scores_match"]
            for m in per_model.values()
        ),
    }


SCENARIOS = {
    "baseline": baseline,
    "duplicate": duplicate,
    "crash": crash,
    "rate-limit": rate_limit,
    "throughput": throughput,
    "correctness": correctness,
}


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("scenario", choices=[*SCENARIOS, "all"])
    parser.add_argument("--latency-ms", type=float, default=20.0,
                        help="Fake provider response latency.")
    parser.add_argument("--workers", type=int, default=3)
    parser.add_argument("--timeout", type=float, default=600.0,
                        help="Seconds to wait for a scenario before giving up.")
    parser.add_argument("--copies", type=int, default=2,
                        help="duplicate: times each shard is published.")
    parser.add_argument("--trials", type=int, default=3, help="crash: number of crashes to inject.")
    parser.add_argument("--visibility-timeout", type=int, default=5,
                        help="crash: SQS visibility timeout, the upper bound on recovery delay.")
    parser.add_argument("--limit", type=int, default=600,
                        help="rate-limit/throughput: provider requests per window.")
    parser.add_argument("--window", type=float, default=60.0,
                        help="rate-limit/throughput: provider window in seconds.")
    parser.add_argument("--cases", type=int, default=None,
                        help="rate-limit: total cases (default 1.5x limit). "
                             "throughput: cases per provider (default 300).")
    parser.add_argument("--failure-rate", type=float, default=0.1,
                        help="correctness: fake provider transient failure rate.")
    args = parser.parse_args()

    names = list(SCENARIOS) if args.scenario == "all" else [args.scenario]
    failed = False
    for name in names:
        scenario_args = argparse.Namespace(**vars(args))
        if scenario_args.cases is None:
            scenario_args.cases = int(args.limit * 1.5) if name == "rate-limit" else 300
        try:
            report = SCENARIOS[name](scenario_args)
        except (RuntimeError, TimeoutError) as exc:
            print(f"[{name}] did not finish: {exc}", file=sys.stderr)
            failed = True
            continue
        path = write_report(name, report)
        print(json.dumps(report, indent=2))
        print(f"[{name}] report written to {path}", file=sys.stderr)
        if report.get("passed") is False:
            failed = True
    sys.exit(1 if failed else 0)


if __name__ == "__main__":
    main()
