# designops-eval

A cloud-ready LLM evaluation harness for CAD design-assistant workflows.

Runs YAML-defined prompt sets against model APIs and scores responses for factual accuracy, JSON tool-call schema validity, latency, token cost, and cross-run consistency. Baseline regression detection flags drops across model or prompt changes, and a markdown report is produced for every run.

The project is AWS-compatible rather than AWS-dependent (poor uni student). By default, it runs locally and uses LocalStack to emulate S3 and DynamoDB. The same boto3-based storage clients can target real AWS by removing the LocalStack endpoint and providing normal AWS credentials or an IAM role.

This mirrors how a real evaluation service would be built: the eval runner is containerized, artifacts are stored in object storage, run metadata is persisted in a database, and the execution path is reproducible locally or in cloud infrastructure.

---

## Storage modes

| Mode | What happens |
|------|-------------|
| `--storage local` | Artifacts written to `--output-dir` on disk |
| `--storage aws` | Artifacts uploaded to S3, metadata row written to DynamoDB, local copy also saved |

The `--storage aws` mode works identically against **LocalStack** (with `AWS_ENDPOINT_URL` set) and against **real AWS** (with `AWS_ENDPOINT_URL` unset and real credentials). There is one code path.

> This project runs locally by default using LocalStack, but the S3/DynamoDB integrations are implemented with standard boto3 clients. Removing the LocalStack endpoint and providing real AWS credentials makes the same artifact-storage and metadata paths work against real AWS services.

---

## Quickstart — local mode (no Docker)

```bash
pip install -e ".[dev]"

python -m designops_eval.cli run \
  --prompt-set evals/prompt_sets/combined_eval.yaml \
  --model mock \
  --baseline evals/baselines/baseline.json \
  --output-dir results/demo_run \
  --storage local
```

Artifacts written to `results/demo_run/`:

```
results/demo_run/
  latest.json    # run summary and scores
  traces.json    # per-prompt responses and individual scores
  report.md      # markdown report with baseline comparison
```

---

## Quickstart — LocalStack mode (AWS-compatible)

```bash
# Start LocalStack
docker compose up -d localstack

# Create S3 bucket and DynamoDB table
python scripts/provision_localstack.py

# Run with AWS storage
export AWS_ENDPOINT_URL=http://localhost:4566
export AWS_ACCESS_KEY_ID=test
export AWS_SECRET_ACCESS_KEY=test
export AWS_DEFAULT_REGION=us-east-1

python -m designops_eval.cli run \
  --prompt-set evals/prompt_sets/combined_eval.yaml \
  --model mock \
  --baseline evals/baselines/baseline.json \
  --output-dir results/localstack_run \
  --storage aws \
  --s3-bucket designops-eval-artifacts \
  --dynamodb-table designops-eval-runs
```

This uploads artifacts to `s3://designops-eval-artifacts/runs/<run_id>/` and writes a metadata row to the `designops-eval-runs` DynamoDB table, using the same boto3 clients that would target real AWS.

---

## Quickstart — Docker

```bash
docker build -t designops-eval .
docker run --rm designops-eval
```

---

## Future real AWS run (no code changes required)

```bash
unset AWS_ENDPOINT_URL
export AWS_PROFILE=designops-eval

python -m designops_eval.cli run \
  --prompt-set evals/prompt_sets/combined_eval.yaml \
  --model mock \
  --baseline evals/baselines/baseline.json \
  --output-dir results/aws_run \
  --storage aws \
  --s3-bucket designops-eval-artifacts-prod \
  --dynamodb-table designops-eval-runs-prod
```

See [docs/AWS_DEPLOYMENT_PATH.md](docs/AWS_DEPLOYMENT_PATH.md) for resource provisioning, IAM requirements, and an optional ECS/Fargate migration path.

---

## Running tests

```bash
pip install -e ".[dev]"
pytest
```

---

## Project structure

```
designops_eval/
  cli.py            # click CLI entry point
  runner.py         # prompt-set loader, mock model, eval orchestration
  scorer.py         # factual accuracy, JSON schema validation, cost, regression detection
  models/
    config.py       # AwsConfig, RunConfig
    results.py      # RunResult, RunScores, TraceEntry
  storage/
    base.py         # StorageBackend abstract class
    local.py        # writes latest.json, traces.json, report.md to disk
    aws.py          # uploads to S3, writes to DynamoDB (same code for LocalStack or real AWS)
  reporting/
    markdown.py     # generates report.md

evals/
  prompt_sets/
    combined_eval.yaml   # CAD design-assistant evaluation suite
  baselines/
    baseline.json        # reference scores for regression detection

scripts/
  provision_localstack.py   # creates LocalStack S3 bucket and DynamoDB table
  check_aws_ready.py        # verifies real AWS resources exist and are reachable

docs/
  AWS_DEPLOYMENT_PATH.md    # migration guide from LocalStack to real AWS / ECS

tests/
  conftest.py
  test_scorer.py
  test_runner.py
  test_storage.py
  test_reporting.py
```

---

## Scoring metrics

| Metric | Description |
|--------|-------------|
| Factual accuracy | Fraction of `expected_facts` keywords found in the response |
| JSON validity | Whether the response contains a JSON object that validates against `expected_tool_calls[0].schema` |
| Avg latency (ms) | Mean response latency across all prompts |
| Total tokens | Sum of input and output tokens across the run |
| Estimated cost (USD) | Rough cost estimate based on per-model token pricing |

Regression detection compares current scores against `baseline.json`. Drops in factual accuracy or JSON validity beyond a threshold exit with code 1.
