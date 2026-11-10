# designops-eval — Technical Overview

A meticulous reference for the full codebase. Written to be re-read from scratch.

Sections 1–14 describe the single-process harness. Section 15 describes the distributed evaluation path built on top of it.

---

## 1. Project Purpose

`designops-eval` is a **Python evaluation harness for LLM-backed CAD design-assistant workflows**. It runs a YAML-defined set of prompts against a model, scores each response on multiple dimensions, compares scores against a stored baseline, produces a Markdown report, and persists artifacts either locally or to S3/DynamoDB.

The project is built to demonstrate what a production-grade eval system looks like structurally — cloud-compatible storage, multiple model providers, configurable scoring, regression detection — without requiring live infrastructure to run the core eval loop.

---

## 2. System Architecture

```
CLI (cli.py)
  └── run_eval()       (runner.py)
        ├── _call_model()
        │     ├── mock responses  (runner.py, _MOCK_RESPONSES dict)
        │     └── call_cohere()   (providers/cohere_client.py)
        ├── score_factual_accuracy()   (scorer.py)
        ├── score_json_validity()      (scorer.py)
        ├── estimate_cost()            (scorer.py)
        └── detect_regressions()       (scorer.py)
  └── generate_report()  (reporting/markdown.py)
  └── StorageBackend.save_run()
        ├── LocalStorage  (storage/local.py)
        └── AwsStorage    (storage/aws.py)
              ├── boto3 S3 client
              └── boto3 DynamoDB resource
                    ↕ LocalStack (localhost:4566) or real AWS
```

Data models (Pydantic):
- `RunConfig` — input parameters for a run
- `AwsConfig` — AWS connection settings (region, endpoint, bucket, table)
- `RunResult` — full run output: scores, regressions, all traces
- `RunScores` — aggregate numeric scores for the run
- `TraceEntry` — per-prompt response, individual scores, token counts

---

## 3. Main Execution Flow

1. **CLI parses arguments** (`cli.py:run`) — prompt set path, model, baseline, output dir, storage mode, S3 bucket, DynamoDB table.
2. **`run_eval(config, baseline)`** (`runner.py:104`) loads the YAML prompt set and iterates over each prompt:
   a. Calls `_call_model(model, prompt_id, prompt_text, expects_json)` to get a `ModelResponse`.
   b. Scores the response with `score_factual_accuracy` and `score_json_validity`.
   c. Appends a `TraceEntry` to the traces list.
3. Aggregates into `RunScores`: factual accuracy, JSON validity, avg latency, token totals, estimated cost.
4. Runs `detect_regressions` if a baseline was provided.
5. Returns a `RunResult` object.
6. Back in `cli.py`: `generate_report(result, baseline)` builds Markdown.
7. `StorageBackend.save_run(result, report_md, output_dir)` persists artifacts.
8. CLI prints scores and exits 0 (or 1 if regressions detected).

---

## 4. CLI Entry Point

**File:** `designops_eval/cli.py`

Entry point registered in `pyproject.toml`:
```
designops-eval = "designops_eval.cli:cli"
```

Also runnable as:
```bash
python -m designops_eval.cli run ...
```

Key flags:
| Flag | Default | Purpose |
|------|---------|---------|
| `--prompt-set` | required | Path to YAML prompt set |
| `--model` | `mock` | `mock`, `cohere-command-a`, `cohere-command-r-plus` |
| `--baseline` | None | Path to baseline JSON for regression detection |
| `--output-dir` | `results/run` | Local directory for output artifacts |
| `--storage` | `local` | `local` or `aws` |
| `--s3-bucket` | None | Required when `--storage aws` |
| `--dynamodb-table` | None | Required when `--storage aws` |

When `--storage aws`, the code reads `AWS_ENDPOINT_URL` from the environment via `AwsConfig.from_env()` (`models/config.py:12`). If set, that endpoint is passed to boto3 clients, routing traffic to LocalStack. If unset, boto3 uses real AWS endpoints.

---

## 5. Prompt Set Format

**File:** `evals/prompt_sets/combined_eval.yaml`

```yaml
name: combined_eval
version: "1.0"
prompts:
  - id: bolt_spec_001
    prompt: "What are the standard thread pitch and nominal diameter for an M8 bolt?"
    expected_facts:
      - "8mm"
      - "1.25mm"
      - "pitch"
    category: factual

  - id: part_lookup_001
    prompt: "Look up the part specifications for SKU-4521 and return the result as JSON."
    expected_tool_calls:
      - schema:
          type: object
          properties:
            sku: { type: string }
          required: [sku]
    category: tool_use
```

- `expected_facts`: list of strings; factual accuracy = fraction found (case-insensitive) in the response.
- `expected_tool_calls[0].schema`: JSON Schema dict; JSON validity = 1.0 if any JSON object in the response validates against it, else 0.0.
- If neither field is present, both scores default to 1.0 for that prompt.

---

## 6. Model Provider Layer

### 6a. Mock Client

**Location:** `runner.py`, `_MOCK_RESPONSES` dict and `_DEFAULT_MOCK` fallback (lines 18–82).

Keyed by `prompt_id`. Returns deterministic `ModelResponse` objects with fixed text, latency, and token counts. No network calls. Used for all tests and for cost-free demo runs.

```python
class ModelResponse:     # providers/base.py
    text: str
    latency_ms: float
    input_tokens: int
    output_tokens: int
    token_source: str    # "mock" | "api" | "estimated"
```

### 6b. Cohere Client

**File:** `designops_eval/providers/cohere_client.py`

- Lazy-imports `cohere` to avoid breaking mock-mode environments without the package.
- Reads `COHERE_API_KEY` from environment; raises `EnvironmentError` clearly if missing.
- Supported model aliases → API IDs:
  - `cohere-command-a` → `command-a-03-2025`
  - `cohere-command-r-plus` → `command-r-plus-08-2024`
- When `expects_json=True`: prepends a `_SYSTEM_JSON_HINT` system message and sets `response_format={"type": "json_object"}`. The request is built once and sent unchanged on every attempt. Errors that `classify_error` marks retryable are retried with bounded, jittered backoff; a rejected request is raised to the caller, never resent in a different form.
- Extracts token counts from `response.usage.tokens.{input,output}_tokens`; falls back to character-based estimate (`token_source = "estimated"`) if the SDK response shape differs.

---

## 7. Scoring Layer

**File:** `designops_eval/scorer.py`

### 7a. Factual Accuracy

```python
def score_factual_accuracy(response_text: str, expected_facts: list[str]) -> float
```

Returns `hits / len(expected_facts)`. Hit = `fact.lower() in response_text.lower()`. Returns 1.0 if no expected facts.

### 7b. JSON Schema Validity

```python
def score_json_validity(response_text: str, expected_schema: dict | None) -> float
```

Extracts JSON objects from the response text:
1. Looks for ` ```json ... ``` ` or ` ``` ... ``` ` fenced blocks.
2. Falls back to a greedy scan for `{...}` substrings.

For each extracted dict, calls `jsonschema.validate(obj, schema)`. Returns 1.0 on first success, 0.0 if none validate. Returns 1.0 when `expected_schema` is None.

### 7c. Latency

Recorded per-prompt in `TraceEntry.latency_ms`. Aggregated to `RunScores.avg_latency_ms` = mean across all prompts.

### 7d. Token Usage

`total_input_tokens` and `total_output_tokens` are sums across all `TraceEntry` objects. Source is `"mock"` for mock runs, `"api"` for Cohere (from `response.usage`), or `"estimated"` for Cohere fallback.

### 7e. Estimated Cost

```python
def estimate_cost(model: str, input_tokens: int, output_tokens: int) -> float
```

Lookup tables `COST_PER_1K_INPUT` and `COST_PER_1K_OUTPUT` keyed by model name. Mock is 0.0. Cohere rates are approximate (~$2.50/$10.00 per 1M tokens as of mid-2025 — verify before citing). Unknown models fall back to Claude Sonnet 4.6 rates.

### 7f. Baseline Regression Detection

```python
def detect_regressions(current_scores, baseline, threshold=0.05) -> list[str]
```

Compares `factual_accuracy` and `json_validity` (higher-is-better: flags drops > 0.05) and `avg_latency_ms` / `estimated_cost_usd` (lower-is-better: flags increases > 50%). Returns a list of human-readable regression strings. Non-empty list causes CLI to exit with code 1.

**Baseline file:** `evals/baselines/baseline.json`

---

## 8. Storage Layer

### 8a. Abstract Interface

**File:** `designops_eval/storage/base.py`

```python
class StorageBackend(ABC):
    @abstractmethod
    def save_run(self, result: RunResult, report_md: str, output_dir: str) -> None: ...
```

### 8b. Local Storage

**File:** `designops_eval/storage/local.py`

Writes three files to `output_dir/`:
- `latest.json` — `RunResult` without traces (Pydantic `model_dump_json`)
- `traces.json` — `{"traces": [...]}`
- `report.md` — Markdown report from `generate_report()`

### 8c. AWS-Compatible Storage (S3 + DynamoDB)

**File:** `designops_eval/storage/aws.py`

`AwsStorage.save_run()`:
1. Uploads `latest.json`, `traces.json`, `report.md` to S3 under `runs/<run_id>/`.
2. Writes a metadata row to DynamoDB via `table.put_item()` with: `run_id`, `model`, `timestamp`, `prompt_set`, `s3_prefix`, all non-null scores as string attributes.
3. Calls `LocalStorage().save_run()` at the end to also write artifacts locally.

Boto3 clients are constructed in `make_s3_client()` and `make_dynamodb_resource()`, both accepting an optional `endpoint_url`. When `AWS_ENDPOINT_URL` is set, traffic goes to LocalStack. When unset, it goes to real AWS.

### 8d. LocalStack

**File:** `docker-compose.yml`

```yaml
services:
  localstack:
    image: localstack/localstack:3
    ports: ["4566:4566"]
    environment:
      SERVICES: s3,dynamodb
      DEFAULT_REGION: us-east-1
```

**Python provisioner:** `scripts/provision_localstack.py` — idempotent script that creates the S3 bucket and DynamoDB table using boto3 directly. Resource names are constants in that file:
- `BUCKET = "designops-eval-artifacts"`
- `TABLE = "designops-eval-runs"`

**Terraform provisioner:** `infra/terraform/` (added in this project) — provisions the same resources via Terraform HCL (see Section 9).

---

## 9. Terraform / IaC Layer

**Directory:** `infra/terraform/`

### Files

| File | Purpose |
|------|---------|
| `main.tf` | Terraform config, AWS provider, S3 bucket, DynamoDB table |
| `variables.tf` | Input variables with defaults matching the harness |
| `outputs.tf` | Outputs: bucket name, table name, ARNs |
| `localstack.tfvars` | Variable overrides for LocalStack provisioning |
| `terraform.tfvars.example` | Template for real AWS provisioning |

### What It Provisions

**S3 bucket** (`aws_s3_bucket.eval_artifacts`):
- Default name: `designops-eval-artifacts` — matches the `--s3-bucket` default in `provision_localstack.py` and the LocalStack quickstart in the README.
- Stores all eval run artifacts under `runs/<run_id>/`.

**DynamoDB table** (`aws_dynamodb_table.eval_runs`):
- Default name: `designops-eval-runs` — matches the `--dynamodb-table` default.
- Hash key: `run_id` (String) — matches the key written by `AwsStorage.save_run()` in `storage/aws.py:62`.
- Billing: `PAY_PER_REQUEST` — no provisioned capacity, no idle cost.

### Why These Two Resources

`AwsStorage` (`storage/aws.py`) makes exactly two AWS API calls:
1. `s3.put_object()` — requires an existing S3 bucket.
2. `dynamodb.Table.put_item()` — requires an existing DynamoDB table.

Terraform provisions exactly those two resources. No other AWS resources are created.

### LocalStack vs Real AWS

The `aws_endpoint_url` variable controls routing:

```hcl
locals {
  use_localstack = var.aws_endpoint_url != ""
}

provider "aws" {
  ...
  skip_credentials_validation = local.use_localstack
  s3_use_path_style           = local.use_localstack
  dynamic "endpoints" {
    for_each = local.use_localstack ? [1] : []
    content {
      s3       = var.aws_endpoint_url
      dynamodb = var.aws_endpoint_url
    }
  }
}
```

- `localstack.tfvars` sets `aws_endpoint_url = "http://localhost:4566"` → targets LocalStack, uses dummy credentials.
- Omitting/blanking `aws_endpoint_url` targets real AWS with normal credential resolution.

### State

Local state only (`terraform.tfstate` in `infra/terraform/`). `terraform.tfstate` is gitignored. No remote backend. For a shared setup, add a `backend "s3" {}` block to `main.tf`.

### Variables and Outputs

**Key variables** (all have defaults matching the harness):

| Variable | Default | Maps to |
|----------|---------|---------|
| `s3_bucket_name` | `designops-eval-artifacts` | `--s3-bucket` CLI flag |
| `dynamodb_table_name` | `designops-eval-runs` | `--dynamodb-table` CLI flag |
| `aws_region` | `us-east-1` | `AWS_DEFAULT_REGION` env var |
| `aws_endpoint_url` | `""` | `AWS_ENDPOINT_URL` env var |

**Outputs:** `s3_bucket_name`, `dynamodb_table_name`, `s3_bucket_arn`, `dynamodb_table_arn`.

---

## 10. Output Artifacts

Per run, under `--output-dir`:

| File | Contents |
|------|---------|
| `latest.json` | `RunResult` JSON (excludes traces). Fields: `run_id`, `model`, `timestamp`, `prompt_set`, `scores`, `regressions`. |
| `traces.json` | `{"traces": [...]}` — one object per prompt with response text, per-prompt scores, token counts, latency, metadata. |
| `report.md` | Markdown with scores table, optional baseline comparison table, regression list, per-prompt results table. |

When `--storage aws`:
- All three files also appear in S3 at `s3://<bucket>/runs/<run_id>/`.
- A DynamoDB row is written to `<table>` with scores denormalized as string attributes.
- Local copies are also written (both backends are called).

**Existing result artifacts in the repo:**
- `results/demo_run/` — mock model, local storage
- `results/localstack_run/` — mock model, LocalStack storage
- `results/cohere_command_r_plus_run/` — real Cohere Command R+ run

---

## 11. How to Run

### 11a. Mock Eval (no external dependencies)

```bash
pip install -e ".[dev]"
python -m designops_eval.cli run \
  --prompt-set evals/prompt_sets/combined_eval.yaml \
  --model mock \
  --baseline evals/baselines/baseline.json \
  --output-dir results/demo_run \
  --storage local
```

### 11b. Cohere Eval

```bash
pip install -e ".[cohere]"
export COHERE_API_KEY=your-key-here
python -m designops_eval.cli run \
  --prompt-set evals/prompt_sets/combined_eval.yaml \
  --model cohere-command-r-plus \
  --baseline evals/baselines/baseline.json \
  --output-dir results/cohere_command_r_plus_run \
  --storage local
```

Cost warning: real API call. 5 prompts ≈ cents at current pricing.

### 11c. LocalStack Storage Run (Python provisioner)

```bash
docker compose up -d localstack
python scripts/provision_localstack.py

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

### 11d. LocalStack Storage Run (Terraform provisioner)

```bash
docker compose up -d localstack

cd infra/terraform
terraform init
terraform fmt
terraform validate
terraform plan  -var-file=localstack.tfvars
terraform apply -var-file=localstack.tfvars
cd ../..

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

# To remove LocalStack resources provisioned by Terraform:
cd infra/terraform
terraform destroy -var-file=localstack.tfvars
```

### 11e. Real AWS (optional, incurs charges)

```bash
# Provision
cd infra/terraform
cp terraform.tfvars.example terraform.tfvars
# edit terraform.tfvars: set bucket/table names, leave aws_endpoint_url blank
terraform init && terraform apply

# Run
unset AWS_ENDPOINT_URL
export AWS_PROFILE=your-profile

python -m designops_eval.cli run \
  --prompt-set evals/prompt_sets/combined_eval.yaml \
  --model mock \
  --output-dir results/aws_run \
  --storage aws \
  --s3-bucket designops-eval-artifacts-dev \
  --dynamodb-table designops-eval-runs-dev
```

---

## 12. Testing Strategy

**Test runner:** pytest. **File:** `tests/`.

| Test file | What it covers |
|-----------|---------------|
| `test_scorer.py` | Factual accuracy, JSON validity, cost estimation, regression detection |
| `test_runner.py` | `run_eval()` with mock model; verifies scores, traces, regression exit code |
| `test_storage.py` | Local file writes; AWS writes via `moto` (mock AWS SDK) |
| `test_reporting.py` | Markdown report generation |
| `test_cohere_client.py` | Cohere client via `sys.modules` mock — no real API key needed |

**`moto`** is used in `test_storage.py` to intercept boto3 calls and verify S3 uploads and DynamoDB writes without real AWS or LocalStack.

**No API keys or Docker required to run the test suite:**
```bash
pip install -e ".[dev]"
pytest
```

---

## 13. Current Limitations and Honest Claims

### What is real and verifiable

- **Mock eval runs** produce deterministic scores. The artifacts in `results/demo_run/` were produced by the harness.
- **LocalStack integration** is implemented and tested: `provision_localstack.py` and `tests/test_storage.py` (via moto) verify the S3 and DynamoDB code paths.
- **Terraform files** in `infra/terraform/` are syntactically valid and provision the correct S3 bucket and DynamoDB table. Running `terraform apply -var-file=localstack.tfvars` against a live LocalStack instance creates those resources.
- **Cohere integration** is implemented in `providers/cohere_client.py`. The `results/cohere_command_r_plus_run/` artifacts were produced by a real Cohere API call.
- **Test suite** passes against `moto`-mocked AWS and does not require real credentials.

### What is not deployed / not production

- **Not deployed to real AWS.** No S3 bucket or DynamoDB table exists in a real AWS account as part of this project. The `terraform.tfvars.example` is a template only.
- **Not running in ECS/Fargate.** The Dockerfile exists and the app is containerizable, but no ECS task definition or deployment pipeline has been created.
- **No CloudWatch logging.** LocalStack does not run the CloudWatch service for this project.
- **Cohere token counts** may be `"estimated"` if the SDK response shape differs from what the client expects — check `token_source` in `traces.json`.

### Resume-safe summary bullets

- Built a Python LLM evaluation harness with pluggable model providers (mock, Cohere Command R+/A), multi-metric scoring (factual accuracy, JSON schema validity, latency, cost), and baseline regression detection with CI exit code.
- Implemented AWS-compatible storage with boto3 S3 and DynamoDB clients, switchable between LocalStack (local) and real AWS via a single environment variable, with the same code path for both.
- Added Terraform infrastructure in `infra/terraform/` provisioning an S3 bucket and DynamoDB table, with a LocalStack provider override for local testing — `terraform apply -var-file=localstack.tfvars` provisions resources against LocalStack with no real AWS credentials.
- Wrote integration tests with moto for all AWS storage paths; test suite requires no real credentials or API keys to pass.

---

## 14. File Reference

```
designops-eval/
├── designops_eval/
│   ├── cli.py                    CLI entry point (click)
│   ├── runner.py                 Eval orchestration, mock responses, _call_model()
│   ├── scorer.py                 All scoring functions + regression detection
│   ├── models/
│   │   ├── config.py             AwsConfig, RunConfig (Pydantic)
│   │   └── results.py            RunResult, RunScores, TraceEntry (Pydantic)
│   ├── providers/
│   │   ├── base.py               ModelResponse dataclass
│   │   └── cohere_client.py      Cohere SDK wrapper
│   ├── reporting/
│   │   └── markdown.py           generate_report() → Markdown string
│   └── storage/
│       ├── base.py               StorageBackend ABC
│       ├── local.py              LocalStorage — writes to disk
│       └── aws.py                AwsStorage — S3 + DynamoDB
├── infra/
│   └── terraform/
│       ├── main.tf               Provider, S3 bucket, DynamoDB table
│       ├── variables.tf          All input variables
│       ├── outputs.tf            Bucket/table names and ARNs
│       ├── localstack.tfvars     Variable overrides for LocalStack
│       └── terraform.tfvars.example  Template for real AWS
├── evals/
│   ├── prompt_sets/
│   │   └── combined_eval.yaml    5-prompt CAD eval suite
│   └── baselines/
│       └── baseline.json         Reference scores for regression detection
├── scripts/
│   ├── provision_localstack.py   Python-based LocalStack provisioner (boto3)
│   └── check_aws_ready.py        Pre-flight check for real AWS resources
├── tests/
│   ├── conftest.py               sample_result fixture
│   ├── test_scorer.py
│   ├── test_runner.py
│   ├── test_storage.py           Uses moto — no real AWS needed
│   ├── test_reporting.py
│   └── test_cohere_client.py     Uses sys.modules mock
├── results/
│   ├── demo_run/                 Mock model, local storage
│   ├── localstack_run/           Mock model, LocalStack storage
│   └── cohere_command_r_plus_run/  Real Cohere run
├── docs/
│   ├── AWS_DEPLOYMENT_PATH.md    Migration guide: LocalStack → real AWS
│   ├── COHERE_EVAL_FINDINGS.md   Findings from the Cohere run
│   └── PROJECT_TECHNICAL_OVERVIEW.md  ← this file
├── docker-compose.yml            LocalStack service definition
├── Dockerfile                    Container image for the harness
└── pyproject.toml                Package config, deps, entry point
```

---

## 15. Distributed Evaluation

The single-process harness keeps every result in memory until the run ends, so a crash loses the whole run. The distributed path splits a run into shards on an SQS queue, has workers commit each case result to DynamoDB as it is produced, and assembles the usual artifacts afterwards.

### 15.1 Where the core logic lives

| Concern | Function | File | Tests |
|---------|----------|------|-------|
| Retryable vs permanent errors | `classify_error` | `providers/errors.py` | `tests/test_error_classification.py` |
| Stable identities and sharding | `config_version`, `shard_cases`, `shard_id`, `result_key` | `distributed/sharding.py` | `tests/test_sharding.py` |
| Idempotent result commits | `ResultsStore.put_result_if_absent`, `get_committed_case_ids` | `distributed/results_store.py` | `tests/test_results_store.py` |
| Checkpointing and crash recovery | `process_shard` | `distributed/worker.py` | `tests/test_worker.py`, `tests/test_fault_injection.py` |
| Shared rate limiting | `DynamoTokenBucket.try_acquire`, `acquire` | `distributed/rate_limiter.py` | `tests/test_rate_limiter.py` |

The tests run against in-process emulation (moto). The distributed-systems properties are demonstrated locally by `benchmarks/run.py`; nothing has been run on real AWS.

### 15.2 Data flow

```
designops-eval submit --run-id R --models a,b
  └── build_shards()            one group of shards per model      (sharding.py)
        └── ShardQueue.publish  one SQS message per shard          (queue.py)

designops-eval worker           any number of processes            (worker.py)
  └── run_worker()
        ├── ShardQueue.receive
        ├── VisibilityHeartbeat      keeps the message invisible while working
        ├── process_shard()
        │     ├── get_committed_case_ids   skip what is already done
        │     ├── limiter.acquire          before every provider attempt
        │     ├── evaluate_case            existing scoring, shared with run_eval
        │     └── put_result_if_absent     conditional write, one item per case
        └── ShardQueue.delete        only when every case is committed

designops-eval finalize --run-id R --models a,b
  └── finalize_run()            reads results, rebuilds RunResult  (finalize.py)
        └── generate_report() + StorageBackend.save_run()          (unchanged)
```

`run_eval` and the worker both call `runner.evaluate_case` and `runner.aggregate`, so a distributed run can be compared with a single-process run case by case.

### 15.3 Identity

A result is identified by four values:

| Part | Source |
|------|--------|
| `run_id` | Chosen by the caller of `submit`. Resubmitting the same id resumes the run. |
| `model` | The model evaluated. |
| `config_version` | Hash of the prompt template and model parameters (`providers/registry.model_config`). Changing either gives results a new identity instead of mixing them with old ones. |
| `case_id` | The prompt-set entry's `id`. |

A shard is only a unit of delivery. Its id is stable across resubmissions, but nothing depends on shard boundaries staying the same, because the durable unit is the case.

### 15.4 Storage

| Table | Keys | Contents |
|-------|------|----------|
| runs (existing) | `run_id` | One summary row per finalized model, written by `AwsStorage.save_run`. For a distributed run the key is `<run_id>/<model>`. |
| results | `run_id`, `result_key` | One item per committed case: identity, status, the full trace as JSON, the worker that wrote it, commit time. |
| rate-limits | `provider` | One item per provider holding its token-bucket state. |

The existing runs table could not hold per-case results: it has no sort key and a DynamoDB key schema cannot be changed in place. S3 artifacts are unchanged and live under `runs/<run_id>/<model>/`.

### 15.5 Delivery semantics

SQS standard queues deliver at least once. A message can arrive twice, and a message whose visibility timeout lapses is handed to a second worker while the first may still be running. The design does not try to prevent this. It makes it harmless:

- **Committed results are exactly-once.** `put_result_if_absent` is a conditional write on the item key. Whichever worker writes first wins; every later write for the same case is rejected and leaves the item untouched.
- **Provider calls are at-least-once.** If a worker dies after the provider responds and before the write, the response is lost and the next worker calls the provider again for that one case. The skip check reduces repeated calls; it cannot eliminate them.
- **No leases or fencing tokens.** Two workers on one shard waste some provider calls but cannot corrupt results, so exclusive ownership would add coordination without adding correctness. Revisit this only if measured duplicate calls become a real cost.

A message is deleted only after every case in its shard has a committed result. Deleting earlier would lose the remaining cases if the worker then died.

### 15.6 Crash recovery

There is no recovery procedure, only the normal path run again. A worker commits each case before starting the next, so whatever it finished is already durable when it dies. Its heartbeat thread dies with it, the message becomes visible after at most the visibility timeout, and the next worker's skip check finds the committed cases. Recovery delay is therefore bounded by the visibility timeout (default 120 s), which is the tradeoff: a shorter timeout recovers faster but needs more frequent heartbeats and tolerates shorter stalls before a second worker starts on the same shard.

### 15.7 Failures

| Failure | Handling |
|---------|----------|
| Rate limit (429), timeout, 5xx, connection error | Retried with bounded exponential backoff and full jitter, honouring `Retry-After`. If retries run out, nothing is committed, the shard is left incomplete and redelivered after `--retry-delay`. |
| Permanent provider error (400, 401, 403, 404, 422) | Committed as a terminal `failed` result with scores of 0. It counts against the run's accuracy and appears as `failed_cases`. |
| Shard that cannot be parsed, or names an unknown case | Never deleted. SQS moves it to the dead-letter queue after `maxReceiveCount` deliveries. |
| Shard that stays incomplete (for example a long provider outage) | Same: dead-lettered after `maxReceiveCount` deliveries. |

Retryable errors are never committed as failures, because a committed result is skipped on every later delivery. `submit --retry-failed` deletes a run's `failed` results (each delete is conditional on the item still being failed) and republishes, so only those cases are evaluated again.

### 15.8 Rate limiting

Workers share one quota per provider, so adding workers does not raise throughput against a single provider. Each provider has one token-bucket item in DynamoDB. A worker reads it, computes the refilled count, and writes the decremented state only if the item is unchanged since the read; on a conflict it re-reads. No worker holds a lock.

DynamoDB was chosen over Redis because it is already in the stack, has no idle cost, and one conditional write is the only primitive needed. The cost is latency and a per-request charge on every provider call, which is acceptable at tens of requests per second and would not be at thousands.

Two limits of the approach:

- **Clocks.** Each worker uses its own wall clock. Skew between workers shifts refill accounting by the size of the skew.
- **Bursts.** A bucket of capacity B refilled at r per second can grant B + r·W requests in a window of W seconds. `BucketConfig.from_limit` sets r = (limit − B) / W so that the worst case equals the provider's limit rather than exceeding it by B.

`LocalPacer` is a per-process limiter kept as the comparison case: with N workers it sends N times the configured rate.

### 15.9 Fake provider

Model names starting with `fake-` are served by `providers/fake.py`. Each name is an independent provider with its own quota. Responses, transient failures and permanent failures are a pure function of the seed, provider, prompt id and attempt number, so runs are reproducible.

It runs in-process by default. With `FAKE_PROVIDER_URL` set, calls go to an HTTP server (`python -m designops_eval.providers.fake`) so that several worker processes hit one shared sliding-window quota and receive real 429 responses with `Retry-After`. `GET /stats` reports requests, rejections, the largest number of requests accepted in any window, and repeated completions per prompt.

### 15.10 Testing

```bash
pytest                   # everything; what CI runs
```

Tests use `moto` in-process. `tests/helpers.AtomicTable` wraps a moto table so each request is atomic, as in DynamoDB, and lets a test insert a competing write between a caller's read and its conditional write.

`benchmarks/run.py` repeats the fault-injection scenarios with separate worker processes against moto in server mode and the fake provider's HTTP server, and writes JSON reports to `results/benchmarks/`:

| Scenario | Measures |
|----------|----------|
| `baseline` | Single-process runtime and throughput over the 300-case generated set |
| `duplicate` | Committed and duplicate results after every shard is published twice |
| `crash` | Results lost and recovery time after a worker is killed at 18 of 25 commits |
| `rate-limit` | 429 responses with three workers: shared limiter versus per-process limiters |
| `throughput` | One worker versus three across three providers, plus three workers on one provider as a control |
| `correctness` | Per-case and aggregate equality with `run_eval`, and artifact hashes across two `finalize` runs |

### 15.11 Infrastructure

`infra/terraform/distributed.tf` adds the queue, dead-letter queue and the two tables. `infra/terraform/worker.tf` adds an ECR repository, two IAM roles, a log group and an ECS Fargate service; it is created only when `enable_worker_service = true`, and the service runs `worker_count` tasks, which defaults to 0.

The worker's task role can receive, delete and change visibility on the shard queue, put and read items in the results table, and read and write the rate-limit table. It cannot publish to the queue, delete or update a result, or reach S3. `submit` and `finalize` run from an operator's machine with their own credentials.

Tasks run in the default VPC's public subnets with a public IP and an egress-only security group, which avoids the fixed monthly cost of a NAT gateway. There is no autoscaling: worker count is a variable, and throughput is capped by provider quotas rather than by compute. See `docs/AWS_DEPLOYMENT_PATH.md` for the deployment checklist.

### 15.12 Files added for the distributed path

```
designops_eval/
├── providers/
│   ├── errors.py            ProviderError, classify_error, Retry-After parsing
│   ├── retry.py             Bounded backoff with jitter
│   ├── fake.py              Fake provider: core, HTTP server, client
│   └── registry.py          provider_for(), model_config()
└── distributed/
    ├── sharding.py          Shard, message format, stable IDs, build_shards()
    ├── results_store.py     ResultRecord, ResultsStore
    ├── rate_limiter.py      BucketConfig, DynamoTokenBucket, LocalPacer
    ├── queue.py             ShardQueue, VisibilityHeartbeat
    ├── worker.py            run_worker(), process_shard(), fault injection
    ├── finalize.py          finalize_run()
    └── schema.py            Table and queue definitions for local emulators
benchmarks/
├── harness.py               Local multi-process environment
└── run.py                   Benchmark and fault-injection scenarios
scripts/generate_prompt_set.py
evals/prompt_sets/generated_300.yaml
infra/terraform/distributed.tf, worker.tf
.github/workflows/tests.yml
```
