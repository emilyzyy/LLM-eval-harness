import json
import sys
from pathlib import Path

import click

from .models.config import AwsConfig, RunConfig
from .reporting.markdown import generate_report
from .runner import run_eval
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
def run(
    prompt_set: str,
    model: str,
    baseline: str | None,
    output_dir: str,
    storage: str,
    s3_bucket: str | None,
    dynamodb_table: str | None,
) -> None:
    """Run an evaluation against a prompt set."""
    if storage == "aws" and (not s3_bucket or not dynamodb_table):
        raise click.UsageError("--s3-bucket and --dynamodb-table are required for --storage aws")

    baseline_data: dict | None = None
    if baseline:
        baseline_data = json.loads(Path(baseline).read_text(encoding="utf-8"))

    aws_config: AwsConfig | None = None
    if storage == "aws":
        aws_config = AwsConfig.from_env(s3_bucket=s3_bucket, dynamodb_table=dynamodb_table)

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


if __name__ == "__main__":
    cli()
