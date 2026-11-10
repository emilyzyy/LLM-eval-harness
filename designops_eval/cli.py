import json
import os
import signal
import sys
from pathlib import Path

import click

from .models.config import AwsConfig, RunConfig, _resolve_region
from .reporting.markdown import generate_report
from .runner import load_prompt_set, run_eval
from .storage.aws import AwsStorage
from .storage.local import LocalStorage


@click.group()
def cli() -> None:
    """DesignOps LLM evaluation harness."""


@cli.command()
@click.option("--prompt-set", required=True, help="Path to YAML prompt set.")
@click.option("--model", default="mock", show_default=True, help="Model to evaluate.")
@click.option("--baseline", default=None, help="Path to baseline JSON for regression detection.")
@click.option("--output-dir", default="results/run", show_default=True, help="Local output directory.")
@click.option(
    "--storage",
    type=click.Choice(["local", "aws"]),
    default="local",
    show_default=True,
    help="Storage backend.",
)
@click.option("--s3-bucket", default=None, help="S3 bucket name (required for --storage aws).")
@click.option("--dynamodb-table", default=None, help="DynamoDB table name (required for --storage aws).")
@click.option(
    "--aws-region",
    default=None,
    help="AWS region override. Falls back to AWS_REGION, AWS_DEFAULT_REGION, then us-east-1.",
)
def run(
    prompt_set: str,
    model: str,
    baseline: str | None,
    output_dir: str,
    storage: str,
    s3_bucket: str | None,
    dynamodb_table: str | None,
    aws_region: str | None,
) -> None:
    """Run an evaluation against a prompt set."""
    if storage == "aws" and (not s3_bucket or not dynamodb_table):
        raise click.UsageError("--s3-bucket and --dynamodb-table are required for --storage aws")

    baseline_data: dict | None = None
    if baseline:
        baseline_data = json.loads(Path(baseline).read_text(encoding="utf-8"))

    aws_config: AwsConfig | None = None
    if storage == "aws":
        aws_config = AwsConfig.from_env(
            s3_bucket=s3_bucket,
            dynamodb_table=dynamodb_table,
            region=aws_region,
        )

    config = RunConfig(
        prompt_set_path=prompt_set,
        model=model,
        baseline_path=baseline,
        output_dir=output_dir,
        storage_mode=storage,
        aws_config=aws_config,
    )

    click.echo(f"Running eval: prompt_set={prompt_set}  model={model}  storage={storage}")
    result = run_eval(config, baseline=baseline_data)

    report_md = generate_report(result, baseline=baseline_data)

    if storage == "aws":
        backend = AwsStorage(aws_config)
    else:
        backend = LocalStorage()

    backend.save_run(result, report_md, output_dir)

    click.echo(f"Run ID: {result.run_id}")
    click.echo(f"Factual accuracy:  {result.scores.factual_accuracy:.3f}")
    click.echo(f"JSON validity:     {result.scores.json_validity:.3f}")
    click.echo(f"Avg latency (ms):  {result.scores.avg_latency_ms:.1f}")
    click.echo(f"Est. cost (USD):   ${result.scores.estimated_cost_usd:.6f}")

    if result.regressions:
        click.echo("\nRegressions detected:", err=True)
        for reg in result.regressions:
            click.echo(f"  {reg}", err=True)
        sys.exit(1)

    if storage == "local":
        click.echo(f"\nArtifacts written to: {output_dir}/")
    else:
        click.echo(
            f"\nArtifacts uploaded to: s3://{s3_bucket}/runs/{result.run_id}/\n"
            f"Metadata written to DynamoDB table: {dynamodb_table}\n"
            f"Local copy: {output_dir}/"
        )


def _dynamodb_table(name: str, aws_region: str | None):
    from .storage.aws import make_dynamodb_resource

    return make_dynamodb_resource(
        _resolve_region(aws_region), os.getenv("AWS_ENDPOINT_URL")
    ).Table(name)


def _shard_queue(queue_url: str, aws_region: str | None):
    from .distributed.queue import ShardQueue, make_sqs_client

    client = make_sqs_client(_resolve_region(aws_region), os.getenv("AWS_ENDPOINT_URL"))
    return ShardQueue(client, queue_url)


def _split_models(models: str) -> list[str]:
    parsed = [m.strip() for m in models.split(",") if m.strip()]
    if not parsed:
        raise click.UsageError("--models must name at least one model")
    return parsed


def _parse_provider_limits(spec: str | None) -> dict[str, int]:
    limits: dict[str, int] = {}
    for part in (spec or "").split(","):
        part = part.strip()
        if not part:
            continue
        name, sep, value = part.partition("=")
        if not sep or not name.strip() or not value.strip().isdigit():
            raise click.UsageError(
                f"invalid provider limit {part!r}; expected provider=requests, e.g. fake-a=600"
            )
        limits[name.strip()] = int(value)
    return limits


_region_option = click.option(
    "--aws-region",
    default=None,
    help="AWS region override. Falls back to AWS_REGION, AWS_DEFAULT_REGION, then us-east-1.",
)


@cli.command()
@click.option("--run-id", required=True,
              help="Name of the logical run. Resubmitting the same run id resumes it.")
@click.option("--prompt-set", required=True,
              help="Path to YAML prompt set, as workers will resolve it.")
@click.option("--models", required=True, help="Comma-separated models to evaluate.")
@click.option("--shard-size", default=25, show_default=True, help="Cases per shard.")
@click.option("--queue-url", envvar="DESIGNOPS_QUEUE_URL", required=True, help="SQS queue URL.")
@click.option("--results-table", envvar="DESIGNOPS_RESULTS_TABLE", default=None,
              help="DynamoDB results table (required for --retry-failed).")
@click.option("--retry-failed", is_flag=True,
              help="Delete this run's failed results first so they are evaluated again.")
@_region_option
def submit(
    run_id: str,
    prompt_set: str,
    models: str,
    shard_size: int,
    queue_url: str,
    results_table: str | None,
    retry_failed: bool,
    aws_region: str | None,
) -> None:
    """Shard a run and publish it to the queue.

    Safe to repeat: shard and result identities are stable, and workers skip
    every case that already has a committed result.
    """
    from .distributed.results_store import ResultsStore
    from .distributed.sharding import build_shards

    if retry_failed and not results_table:
        raise click.UsageError("--results-table is required for --retry-failed")

    shards = build_shards(
        run_id, prompt_set, load_prompt_set(prompt_set), _split_models(models), shard_size
    )

    if retry_failed:
        deleted = ResultsStore(_dynamodb_table(results_table, aws_region)).delete_failed_results(run_id)
        click.echo(f"Deleted {deleted} failed result(s) for retry.")

    # Interleave models so that workers pulling consecutive messages spread
    # across providers instead of queueing behind one provider's rate limit.
    shards.sort(key=lambda s: (s.shard_index, s.model))
    sent = _shard_queue(queue_url, aws_region).publish(shards)
    click.echo(f"Run ID: {run_id}")
    click.echo(f"Published {sent} shard(s) covering {sum(len(s.case_ids) for s in shards)} case(s).")


@cli.command()
@click.option("--queue-url", envvar="DESIGNOPS_QUEUE_URL", required=True, help="SQS queue URL.")
@click.option("--results-table", envvar="DESIGNOPS_RESULTS_TABLE", required=True,
              help="DynamoDB results table.")
@click.option("--rate-limit-table", envvar="DESIGNOPS_RATE_LIMIT_TABLE", default=None,
              help="DynamoDB rate-limit table (required for --limiter shared with limits).")
@click.option("--limiter", envvar="DESIGNOPS_LIMITER",
              type=click.Choice(["shared", "local", "none"]), default="shared", show_default=True,
              help="shared: one bucket per provider across all workers. local: per process.")
@click.option("--provider-limits", envvar="DESIGNOPS_PROVIDER_LIMITS", default=None,
              help="Requests per window by provider, e.g. 'fake-a=600,cohere=20'.")
@click.option("--limit-window-seconds", envvar="DESIGNOPS_LIMIT_WINDOW_SECONDS",
              default=60.0, show_default=True, help="Window the provider limits refer to.")
@click.option("--limit-burst", envvar="DESIGNOPS_LIMIT_BURST", type=int, default=None,
              help="Shared-bucket capacity. Defaults to 5% of the limit.")
@click.option("--visibility-timeout", envvar="DESIGNOPS_VISIBILITY_TIMEOUT",
              default=120, show_default=True,
              help="Seconds a shard stays invisible between heartbeats.")
@click.option("--retry-delay", envvar="DESIGNOPS_RETRY_DELAY", default=30, show_default=True,
              help="Seconds before an incomplete shard is redelivered.")
@click.option("--wait-seconds", default=10, show_default=True, help="SQS long-poll duration.")
@click.option("--idle-exit-seconds", type=float, default=None,
              help="Exit after this long with no messages. Default: run until terminated.")
@click.option("--max-messages", type=int, default=None, help="Exit after this many deliveries.")
@click.option("--worker-id", envvar="DESIGNOPS_WORKER_ID", default=None)
@click.option("--crash-after-commits", type=int, default=None,
              help="Fault injection: kill this process after N committed results.")
@click.option("--crash-before-commit", type=int, default=None,
              help="Fault injection: kill this process when about to commit result N+1.")
@_region_option
def worker(
    queue_url: str,
    results_table: str,
    rate_limit_table: str | None,
    limiter: str,
    provider_limits: str | None,
    limit_window_seconds: float,
    limit_burst: int | None,
    visibility_timeout: int,
    retry_delay: int,
    wait_seconds: int,
    idle_exit_seconds: float | None,
    max_messages: int | None,
    worker_id: str | None,
    crash_after_commits: int | None,
    crash_before_commit: int | None,
    aws_region: str | None,
) -> None:
    """Process shards from the queue until terminated."""
    from .distributed.results_store import ResultsStore
    from .distributed.worker import (
        CrashingStore,
        WorkerDeps,
        build_limiter_factory,
        default_worker_id,
        run_worker,
    )

    limits = _parse_provider_limits(provider_limits)
    if limiter == "shared" and limits and not rate_limit_table:
        raise click.UsageError("--rate-limit-table is required for --limiter shared")

    store = ResultsStore(_dynamodb_table(results_table, aws_region))
    if crash_after_commits is not None or crash_before_commit is not None:
        store = CrashingStore(
            store,
            crash_after_commits=crash_after_commits,
            crash_before_commit=crash_before_commit,
        )

    deps = WorkerDeps(
        store=store,
        limiter_for=build_limiter_factory(
            limiter if limits else "none",
            limits,
            window_seconds=limit_window_seconds,
            burst=limit_burst,
            rate_limit_table=(
                _dynamodb_table(rate_limit_table, aws_region) if rate_limit_table else None
            ),
        ),
        worker_id=worker_id or default_worker_id(),
    )

    stopping = {"flag": False}
    signal.signal(signal.SIGTERM, lambda *_: stopping.update(flag=True))

    run_worker(
        _shard_queue(queue_url, aws_region),
        deps,
        visibility_timeout=visibility_timeout,
        wait_seconds=wait_seconds,
        retry_delay=retry_delay,
        idle_exit_after=idle_exit_seconds,
        max_messages=max_messages,
        should_stop=lambda: stopping["flag"],
    )


@cli.command()
@click.option("--run-id", required=True, help="Run id passed to submit.")
@click.option("--prompt-set", required=True, help="Path to YAML prompt set.")
@click.option("--models", required=True, help="Comma-separated models passed to submit.")
@click.option("--results-table", envvar="DESIGNOPS_RESULTS_TABLE", required=True,
              help="DynamoDB results table.")
@click.option("--baseline", default=None, help="Path to baseline JSON for regression detection.")
@click.option("--output-dir", default="results/run", show_default=True,
              help="Local output directory. Each model gets a subdirectory.")
@click.option("--storage", type=click.Choice(["local", "aws"]), default="local", show_default=True,
              help="Storage backend.")
@click.option("--s3-bucket", default=None, help="S3 bucket name (required for --storage aws).")
@click.option("--dynamodb-table", default=None,
              help="DynamoDB run-metadata table (required for --storage aws).")
@click.option("--allow-partial", is_flag=True,
              help="Write artifacts even if some cases have no result yet.")
@_region_option
def finalize(
    run_id: str,
    prompt_set: str,
    models: str,
    results_table: str,
    baseline: str | None,
    output_dir: str,
    storage: str,
    s3_bucket: str | None,
    dynamodb_table: str | None,
    allow_partial: bool,
    aws_region: str | None,
) -> None:
    """Build reports and artifacts for a distributed run from its committed results."""
    from .distributed.finalize import IncompleteRunError, finalize_run
    from .distributed.results_store import ResultsStore

    if storage == "aws" and (not s3_bucket or not dynamodb_table):
        raise click.UsageError("--s3-bucket and --dynamodb-table are required for --storage aws")

    baseline_data: dict | None = None
    if baseline:
        baseline_data = json.loads(Path(baseline).read_text(encoding="utf-8"))

    store = ResultsStore(_dynamodb_table(results_table, aws_region))
    try:
        results = finalize_run(
            store, run_id, load_prompt_set(prompt_set), _split_models(models),
            baseline=baseline_data, allow_partial=allow_partial,
        )
    except IncompleteRunError as exc:
        click.echo(str(exc), err=True)
        sys.exit(2)

    if storage == "aws":
        backend = AwsStorage(AwsConfig.from_env(
            s3_bucket=s3_bucket, dynamodb_table=dynamodb_table, region=aws_region,
        ))
    else:
        backend = LocalStorage()

    regressed = False
    for result in results:
        model_dir = str(Path(output_dir) / result.model)
        backend.save_run(result, generate_report(result, baseline=baseline_data), model_dir)
        click.echo(
            f"{result.model}: cases={len(result.traces)} failed={result.scores.failed_cases} "
            f"factual={result.scores.factual_accuracy:.3f} "
            f"json={result.scores.json_validity:.3f} -> {model_dir}/"
        )
        if result.regressions:
            regressed = True
            for reg in result.regressions:
                click.echo(f"  regression: {reg}", err=True)

    if regressed:
        sys.exit(1)


if __name__ == "__main__":
    cli()
