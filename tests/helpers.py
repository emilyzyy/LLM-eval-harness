"""Shared test utilities for the distributed components."""

import threading
from collections import Counter
from typing import Callable

import yaml

from designops_eval.distributed.sharding import Shard
from designops_eval.models.results import TraceEntry


class AtomicTable:
    """Wraps a moto DynamoDB table so each request is atomic, as in DynamoDB.

    moto evaluates a condition and applies the write in separate Python steps,
    so two threads can interleave inside one request. Real DynamoDB never
    does. This proxy holds a lock for the duration of each call; threads can
    still interleave freely between calls, which is the concurrency the code
    under test has to handle.

    `calls` counts requests by operation name. `before(op, hook)` registers a
    hook that runs once, outside the lock, immediately before the next call
    to `op`. Tests use it to slip a competing write in between a caller's
    read and its conditional write.
    """

    def __init__(self, table) -> None:
        self._table = table
        self._lock = threading.Lock()
        self._hooks: dict[str, list[Callable[[], None]]] = {}
        self.calls: Counter[str] = Counter()

    def before(self, operation: str, hook: Callable[[], None]) -> None:
        self._hooks.setdefault(operation, []).append(hook)

    def __getattr__(self, name: str):
        attr = getattr(self._table, name)
        if not callable(attr):
            return attr

        def call(*args, **kwargs):
            hooks = self._hooks.get(name)
            hook = hooks.pop(0) if hooks else None
            if hook is not None:
                hook()
            with self._lock:
                self.calls[name] += 1
                return attr(*args, **kwargs)

        return call


class FakeClock:
    """A manually advanced clock. sleep() advances it instead of blocking."""

    def __init__(self, start: float = 1_700_000_000.0) -> None:
        self.now = start
        self.sleeps: list[float] = []
        self._lock = threading.Lock()

    def __call__(self) -> float:
        with self._lock:
            return self.now

    def advance(self, seconds: float) -> None:
        with self._lock:
            self.now += seconds

    def sleep(self, seconds: float) -> None:
        with self._lock:
            self.sleeps.append(seconds)
            self.now += seconds


def make_trace(case_id: str, response: str = "ok", status: str = "completed") -> TraceEntry:
    return TraceEntry(
        prompt_id=case_id,
        prompt=f"prompt for {case_id}",
        response=response,
        latency_ms=12.5,
        input_tokens=5,
        output_tokens=7,
        factual_score=1.0 if status == "completed" else 0.0,
        json_validity_score=1.0 if status == "completed" else 0.0,
        metadata={"category": "factual"},
        status=status,
        error=None if status == "completed" else "ProviderError: bad request",
    )


def write_prompt_set(path, cases: int, permanent_failures: int = 0) -> str:
    """Write a generated prompt set to `path` and return the path as a string."""
    import importlib.util
    from pathlib import Path

    script = Path(__file__).parent.parent / "scripts" / "generate_prompt_set.py"
    spec = importlib.util.spec_from_file_location("generate_prompt_set", script)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    data = module.generate(cases, permanent_failures)
    path.write_text(yaml.safe_dump(data, sort_keys=False), encoding="utf-8")
    return str(path)


def manual_shard(
    prompt_set_path: str,
    case_ids: list[str],
    *,
    run_id: str = "run-1",
    model: str = "fake-a",
    config_version: str = "cfg1",
    shard_id: str = "shard-1",
) -> Shard:
    """A Shard with hand-written identity, for tests that should not depend on sharding."""
    return Shard(
        run_id=run_id,
        model=model,
        provider=model,
        config_version=config_version,
        prompt_set_path=prompt_set_path,
        shard_index=0,
        shard_id=shard_id,
        case_ids=tuple(case_ids),
    )
