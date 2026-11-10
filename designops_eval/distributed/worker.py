"""The queue worker: receive a shard, process it, acknowledge it.

Delivery is at-least-once. A shard can be delivered twice, and two workers can
hold the same shard at the same moment. Correctness therefore rests on the
conditional result write, not on exclusive ownership of a message:

- A committed result is never duplicated or overwritten.
- A provider call can be repeated. If a worker dies after the provider
  responds and before the result is written, the next worker calls the
  provider again for that case.
"""

import json
import os
import socket
import sys
import time
import uuid
from dataclasses import dataclass, field
from functools import lru_cache
from typing import Callable

from ..providers.errors import ErrorKind, classify_error
from ..runner import evaluate_case, failed_trace, load_prompt_set
from .queue import ReceivedMessage, ShardQueue, VisibilityHeartbeat
from .rate_limiter import (
    BucketConfig,
    DynamoTokenBucket,
    Limiter,
    LocalPacer,
    NullLimiter,
)
from .results_store import ResultRecord, ResultsStore
from .sharding import MalformedShardError, Shard


def log_event(event: str, **fields) -> None:
    """Emit one JSON log line. CloudWatch Logs Insights can query these fields."""
    print(json.dumps({"ts": time.time(), "event": event, **fields}), flush=True)


def default_worker_id() -> str:
    return f"{socket.gethostname()}-{os.getpid()}-{uuid.uuid4().hex[:6]}"


@lru_cache(maxsize=8)
def load_prompts_by_id(prompt_set_path: str) -> dict[str, dict]:
    return {p["id"]: p for p in load_prompt_set(prompt_set_path)["prompts"]}


@dataclass
class WorkerDeps:
    store: ResultsStore
    limiter_for: Callable[[str], Limiter]
    worker_id: str = field(default_factory=default_worker_id)
    load_prompts: Callable[[str], dict[str, dict]] = load_prompts_by_id


@dataclass(frozen=True)
class ShardOutcome:
    # Cases this call evaluated and committed as completed.
    evaluated: int = 0
    # Cases that already had a committed result and were not evaluated.
    skipped: int = 0
    # Cases this call committed as permanently failed.
    failed: int = 0
    # Cases this call evaluated but another worker committed first.
    lost_races: int = 0
    # True only if every case in the shard now has a committed result.
    complete: bool = False


def process_shard(shard: Shard, deps: WorkerDeps) -> ShardOutcome:
    """Bring a shard to completion, resuming from whatever is already committed.

    Safe to run any number of times for the same shard, by any number of
    workers, including concurrently. Each result is committed before the next
    case starts, so a worker that dies partway leaves a checkpoint behind.

    - Cases that already have a committed result are skipped without a
      provider call.
    - A PERMANENT provider error is committed as a failed result.
    - A RETRYABLE error that outlasts the retries commits nothing; the shard
      is reported incomplete and redelivered later.
    - Losing the conditional write to another worker is counted, not raised.

    Raises MalformedShardError if the shard names a case id the prompt set
    does not contain. Provider errors are never raised.
    """
    prompts = deps.load_prompts(shard.prompt_set_path)
    unknown = [case_id for case_id in shard.case_ids if case_id not in prompts]
    if unknown:
        raise MalformedShardError(
            f"shard {shard.shard_id} names {len(unknown)} unknown case id(s), e.g. {unknown[0]!r}"
        )

    committed = deps.store.get_committed_case_ids(
        shard.run_id, shard.model, shard.config_version
    )
    limiter = deps.limiter_for(shard.provider)

    evaluated = skipped = failed = lost_races = 0
    complete = True
    for case_id in shard.case_ids:
        if case_id in committed:
            skipped += 1
            continue

        prompt = prompts[case_id]
        try:
            trace = evaluate_case(prompt, shard.model, before_attempt=limiter.acquire)
        except Exception as exc:
            if classify_error(exc) is not ErrorKind.PERMANENT:
                complete = False
                continue
            trace = failed_trace(prompt, exc)

        record = ResultRecord.from_trace(
            run_id=shard.run_id,
            model=shard.model,
            provider=shard.provider,
            config_version=shard.config_version,
            shard_id=shard.shard_id,
            trace=trace,
            worker_id=deps.worker_id,
        )
        if not deps.store.put_result_if_absent(record):
            lost_races += 1
        elif trace.status == "failed":
            failed += 1
        else:
            evaluated += 1

    return ShardOutcome(
        evaluated=evaluated,
        skipped=skipped,
        failed=failed,
        lost_races=lost_races,
        complete=complete,
    )


class SimulatedCrash(BaseException):
    """Raised by fault injection to abandon a shard mid-flight in-process.

    Derives from BaseException so that no `except Exception` handler on the
    way out can turn the crash into an orderly shutdown.
    """


class CrashingStore:
    """Fault injection: a ResultsStore that kills the worker partway through.

    crash_after_commits=N lets N results commit and then crashes immediately
    after the Nth. crash_before_commit=N crashes when the (N+1)th result is
    about to be written, after its provider call has already happened, which
    is the case where at-least-once processing repeats a provider call.

    mode "exit" ends the process with os._exit, skipping all cleanup, like a
    SIGKILL. mode "raise" raises SimulatedCrash for in-process tests.
    """

    def __init__(
        self,
        inner: ResultsStore,
        *,
        crash_after_commits: int | None = None,
        crash_before_commit: int | None = None,
        mode: str = "exit",
    ) -> None:
        self._inner = inner
        self._after = crash_after_commits
        self._before = crash_before_commit
        self._mode = mode
        self._commits = 0

    def _crash(self, reason: str) -> None:
        if self._mode == "exit":
            log_event("simulated_crash", reason=reason, commits=self._commits)
            sys.stdout.flush()
            os._exit(137)
        raise SimulatedCrash(reason)

    def put_result_if_absent(self, record: ResultRecord) -> bool:
        if self._before is not None and self._commits >= self._before:
            self._crash("before_commit")
        written = self._inner.put_result_if_absent(record)
        if written:
            self._commits += 1
            if self._after is not None and self._commits >= self._after:
                self._crash("after_commit")
        return written

    def __getattr__(self, name: str):
        return getattr(self._inner, name)


def build_limiter_factory(
    mode: str,
    limits: dict[str, int],
    *,
    window_seconds: float = 60.0,
    burst: int | None = None,
    rate_limit_table=None,
) -> Callable[[str], Limiter]:
    """Return a function mapping a provider name to its limiter.

    mode "shared" uses one DynamoDB-backed bucket per provider, common to all
    workers. mode "local" gives this process its own pacer per provider.
    mode "none", and any provider absent from `limits`, is unlimited.
    """
    if mode not in ("shared", "local", "none"):
        raise ValueError(f"unknown limiter mode {mode!r}")
    if mode == "shared" and rate_limit_table is None:
        raise ValueError("limiter mode 'shared' requires a rate-limit table")

    cache: dict[str, Limiter] = {}

    def limiter_for(provider: str) -> Limiter:
        if provider not in cache:
            limit = limits.get(provider)
            if mode == "none" or limit is None:
                cache[provider] = NullLimiter()
            elif mode == "shared":
                cache[provider] = DynamoTokenBucket(
                    rate_limit_table,
                    provider,
                    BucketConfig.from_limit(limit, window_seconds, burst),
                )
            else:
                cache[provider] = LocalPacer(limit / window_seconds)
        return cache[provider]

    return limiter_for


@dataclass
class WorkerStats:
    messages: int = 0
    shards_completed: int = 0
    shards_incomplete: int = 0
    malformed: int = 0
    evaluated: int = 0
    skipped: int = 0
    failed: int = 0
    lost_races: int = 0


def run_worker(
    queue: ShardQueue,
    deps: WorkerDeps,
    *,
    visibility_timeout: int = 120,
    heartbeat_interval: float | None = None,
    wait_seconds: int = 10,
    retry_delay: int = 30,
    idle_exit_after: float | None = None,
    max_messages: int | None = None,
    should_stop: Callable[[], bool] = lambda: False,
    log: Callable[..., None] = log_event,
) -> WorkerStats:
    """Receive and process shards until told to stop.

    A message is deleted only after process_shard reports the shard complete.
    In every other case it is left on the queue and SQS redelivers it:

    - Incomplete shard, unexpected error, or a shard that process_shard
      rejects as malformed: made visible again after retry_delay seconds.
    - Message that cannot be parsed at all: left untouched, so it reappears
      after the queue's visibility timeout.

    Each redelivery raises the message's receive count. Past the queue's
    maxReceiveCount SQS moves it to the dead-letter queue.

    idle_exit_after stops the worker once no message has arrived for that
    many seconds; max_messages stops it after that many deliveries. Both are
    for tests and benchmarks. A deployed worker runs until terminated.
    """
    if heartbeat_interval is None:
        heartbeat_interval = visibility_timeout / 3
    stats = WorkerStats()
    idle_since = time.monotonic()
    log("worker_started", worker_id=deps.worker_id, queue_url=queue.queue_url)

    while not should_stop():
        if max_messages is not None and stats.messages >= max_messages:
            break
        message = queue.receive(wait_seconds=wait_seconds)
        if message is None:
            if idle_exit_after is not None and time.monotonic() - idle_since >= idle_exit_after:
                break
            continue
        stats.messages += 1
        _handle_message(queue, deps, message, stats, visibility_timeout,
                        heartbeat_interval, retry_delay, log)
        idle_since = time.monotonic()

    log("worker_stopped", worker_id=deps.worker_id, **stats.__dict__)
    return stats


def _handle_message(
    queue: ShardQueue,
    deps: WorkerDeps,
    message: ReceivedMessage,
    stats: WorkerStats,
    visibility_timeout: int,
    heartbeat_interval: float,
    retry_delay: int,
    log: Callable[..., None],
) -> None:
    common = {"worker_id": deps.worker_id, "message_id": message.message_id,
              "receive_count": message.receive_count}
    try:
        shard = Shard.from_message(message.body)
    except MalformedShardError as exc:
        stats.malformed += 1
        log("message_malformed", error=str(exc), **common)
        return

    common.update(run_id=shard.run_id, shard_id=shard.shard_id, model=shard.model,
                  provider=shard.provider, cases=len(shard.case_ids))
    log("shard_received", **common)
    started = time.monotonic()

    try:
        with VisibilityHeartbeat(
            queue, message, heartbeat_interval, visibility_timeout,
            on_error=lambda exc: log("heartbeat_lost", error=str(exc), **common),
        ):
            outcome = process_shard(shard, deps)
    except MalformedShardError as exc:
        stats.malformed += 1
        log("message_malformed", error=str(exc), **common)
        _release_later(queue, message, retry_delay, log, common)
        return
    except Exception as exc:
        stats.shards_incomplete += 1
        log("shard_error", error=f"{type(exc).__name__}: {exc}", **common)
        _release_later(queue, message, retry_delay, log, common)
        return

    stats.evaluated += outcome.evaluated
    stats.skipped += outcome.skipped
    stats.failed += outcome.failed
    stats.lost_races += outcome.lost_races
    elapsed = round(time.monotonic() - started, 3)
    details = {"evaluated": outcome.evaluated, "skipped": outcome.skipped,
               "failed": outcome.failed, "lost_races": outcome.lost_races,
               "elapsed_seconds": elapsed}

    if not outcome.complete:
        stats.shards_incomplete += 1
        log("shard_incomplete", **details, **common)
        _release_later(queue, message, retry_delay, log, common)
        return

    try:
        queue.delete(message)
    except Exception as exc:
        # The results are committed. A redelivery will find every case done,
        # skip them all, and delete the message then.
        log("delete_failed", error=f"{type(exc).__name__}: {exc}", **common)
    stats.shards_completed += 1
    log("shard_completed", **details, **common)


def _release_later(queue, message, retry_delay, log, common) -> None:
    try:
        queue.set_visibility(message, retry_delay)
    except Exception as exc:
        log("release_failed", error=f"{type(exc).__name__}: {exc}", **common)
