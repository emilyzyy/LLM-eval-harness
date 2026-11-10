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

## Terraform / IaC

Terraform configuration in `infra/terraform/` provisions the S3 bucket and DynamoDB table used by the `--storage aws` mode. The resource names match the defaults used throughout the project.

**What it provisions:**

| Resource | Default name | Purpose |
|----------|-------------|---------|
| `aws_s3_bucket` | `designops-eval-artifacts` | Stores `latest.json`, `traces.json`, `report.md` per run |
| `aws_dynamodb_table` | `designops-eval-runs` | Stores one metadata row per eval run, keyed by `run_id` |

These are the only two resources. No ECS, no Secrets Manager, no CloudWatch. Billing mode is `PAY_PER_REQUEST`.

**LocalStack (recommended — no real AWS credentials):**

```bash
# 1. Start LocalStack
docker compose up -d localstack

# 2. Provision via Terraform
cd infra/terraform
terraform init
terraform fmt
terraform validate
terraform plan  -var-file=localstack.tfvars
terraform apply -var-file=localstack.tfvars
cd ../..

# 3. Run the harness against LocalStack
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

# 4. Destroy LocalStack resources when done
cd infra/terraform
terraform destroy -var-file=localstack.tfvars
```

**Real AWS (optional — incurs charges, requires credentials):**

> **Warning:** Creating real AWS resources means you are responsible for any charges. An S3 bucket and PAY_PER_REQUEST DynamoDB table are low-cost for small workloads, but nothing in this repo deploys to or manages real AWS by default.

```bash
cd infra/terraform
cp terraform.tfvars.example terraform.tfvars
# Edit terraform.tfvars: set bucket/table names, leave aws_endpoint_url blank
terraform init && terraform apply
```

Then run the harness with `unset AWS_ENDPOINT_URL` and real AWS credentials.

**Terraform files:**

```
infra/terraform/
  main.tf                    # Provider, S3 bucket, DynamoDB table
  variables.tf               # project_name, environment, region, bucket/table names, endpoint
  outputs.tf                 # bucket name, table name, ARNs
  localstack.tfvars          # Variable overrides for LocalStack
  terraform.tfvars.example   # Template for real AWS deployment
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

## Quickstart — Cohere models

> **Cost warning:** Cohere Command A and Command R+ are real paid API calls.
> A single run of `combined_eval.yaml` (5 prompts) is cheap (cents or less at current pricing),
> but running many times or with large prompt sets adds up. Check https://cohere.com/pricing first.

```bash
# 1. Install the Cohere SDK extra
pip install -e ".[cohere]"

# 2. Export your API key
export COHERE_API_KEY=your-key-here

# 3. Run with Command A
python3 -m designops_eval.cli run \
  --prompt-set evals/prompt_sets/combined_eval.yaml \
  --model cohere-command-a \
  --baseline evals/baselines/baseline.json \
  --output-dir results/cohere_command_a_run \
  --storage local

# 4. Or run with Command R+
python3 -m designops_eval.cli run \
  --prompt-set evals/prompt_sets/combined_eval.yaml \
  --model cohere-command-r-plus \
  --baseline evals/baselines/baseline.json \
  --output-dir results/cohere_command_r_plus_run \
  --storage local
```

The report is written to `results/<run>/report.md`. Open it to see scores against the baseline.

If `COHERE_API_KEY` is missing you'll get a clear error before any API calls are made.
Mock mode (`--model mock`) continues to work without any API key.

See [docs/COHERE_EVAL_FINDINGS.md](docs/COHERE_EVAL_FINDINGS.md) for the findings template to fill in after running.

---

## Distributed evaluation

A run can be split into shards on an SQS queue and processed by any number of workers, which commit each case result to DynamoDB as it is produced. Design, delivery semantics and tradeoffs are in `docs/PROJECT_TECHNICAL_OVERVIEW.md`, section 15.

**Status:** implemented and validated locally against an AWS emulator and the fake provider. The Terraform for SQS, DynamoDB and ECS Fargate is written and validated but has not been applied to a real account.

```bash
# Workers resolve the prompt-set path themselves; use a path relative to the repo root.
designops-eval submit   --run-id demo --prompt-set evals/prompt_sets/generated_300.yaml --models fake-a,fake-b
designops-eval worker   --idle-exit-seconds 5
designops-eval finalize --run-id demo --prompt-set evals/prompt_sets/generated_300.yaml --models fake-a,fake-b
```

`submit`, `worker` and `finalize` read `DESIGNOPS_QUEUE_URL`, `DESIGNOPS_RESULTS_TABLE` and `DESIGNOPS_RATE_LIMIT_TABLE` from the environment.

Model names starting with `fake-` use a deterministic fake provider with configurable latency, transient failures and a strict rate limit, so experiments cost nothing:

```bash
python -m designops_eval.providers.fake --port 8089 --latency-ms 50 --rate-limit 600
export FAKE_PROVIDER_URL=http://127.0.0.1:8089
```

Benchmarks and fault-injection experiments run entirely on this machine, with separate worker processes against a local AWS emulation:

```bash
python -m benchmarks.run baseline      # single-process reference
python -m benchmarks.run all           # every scenario
```

Reports are written to `results/benchmarks/`.

---

## Running tests

The project is pinned to Python 3.13 (`.python-version`, the Dockerfile and CI all use it).

```bash
pip install -e ".[dev]"
pytest
```

The Cohere tests mock the SDK via `sys.modules` — no real API key or `cohere` package needed to run the test suite.

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

infra/
  terraform/
    main.tf                    # provider, S3 bucket, DynamoDB table
    variables.tf               # project_name, environment, region, bucket/table, endpoint
    outputs.tf                 # bucket name, table name, ARNs
    localstack.tfvars          # overrides for LocalStack (http://localhost:4566)
    terraform.tfvars.example   # template for real AWS

evals/
  prompt_sets/
    combined_eval.yaml   # CAD design-assistant evaluation suite
  baselines/
    baseline.json        # reference scores for regression detection

scripts/
  provision_localstack.py   # creates LocalStack S3 bucket and DynamoDB table (boto3)
  check_aws_ready.py        # verifies real AWS resources exist and are reachable

docs/
  AWS_DEPLOYMENT_PATH.md         # migration guide from LocalStack to real AWS / ECS
  PROJECT_TECHNICAL_OVERVIEW.md  # full technical reference for the project

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
