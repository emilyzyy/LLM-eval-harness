"""Assemble committed per-case results into the harness's RunResult."""

from typing import Sequence

from ..models.results import RunResult
from ..providers.registry import model_config
from ..runner import build_run_result
from .results_store import ResultsStore
from .sharding import config_version


class IncompleteRunError(Exception):
    """Some cases have no committed result yet."""

    def __init__(self, missing: dict[str, list[str]]) -> None:
        self.missing = missing
        summary = ", ".join(f"{model}: {len(ids)}" for model, ids in missing.items())
        super().__init__(f"run is incomplete; cases without a result per model: {summary}")


def artifact_run_id(run_id: str, model: str) -> str:
    """The run id under which one model's artifacts are stored."""
    return f"{run_id}/{model}"


def finalize_run(
    store: ResultsStore,
    run_id: str,
    prompt_set: dict,
    models: Sequence[str],
    baseline: dict | None = None,
    allow_partial: bool = False,
) -> list[RunResult]:
    """Build one RunResult per model from the results table.

    Reads only; it can be run repeatedly and from any machine. Traces follow
    prompt-set order and the timestamp is the latest commit time, so the
    output for a finished run is the same every time.

    Raises IncompleteRunError if any case lacks a result, unless
    allow_partial is set, in which case missing cases are left out.
    """
    case_ids = [p["id"] for p in prompt_set["prompts"]]
    records = store.list_results(run_id)

    results: list[RunResult] = []
    missing: dict[str, list[str]] = {}
    for model in models:
        cfg = model_config(model)
        version = config_version(cfg["prompt_template"], cfg["model_params"])
        by_case = {
            r.case_id: r
            for r in records
            if r.model == model and r.config_version == version
        }
        absent = [c for c in case_ids if c not in by_case]
        if absent:
            missing[model] = absent
        ordered = [by_case[c] for c in case_ids if c in by_case]
        if not ordered:
            continue
        timestamp = max(r.committed_at for r in ordered)
        results.append(build_run_result(
            artifact_run_id(run_id, model),
            model,
            timestamp,
            prompt_set["name"],
            [r.trace for r in ordered],
            baseline,
        ))

    if missing and not allow_partial:
        raise IncompleteRunError(missing)
    return results
