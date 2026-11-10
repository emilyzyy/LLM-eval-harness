# AWS Deployment Path

The project is not deployed to AWS by default (brokie). It is cloud-ready because the artifact and metadata paths use standard AWS SDK clients. LocalStack is only used as a local endpoint. Moving to real AWS means providing AWS credentials/IAM permissions in place of `AWS_ENDPOINT_URL`, , and provisioning the equivalent S3 bucket and DynamoDB table.

---

## How the cloud layer works

All artifact storage and run metadata use standard `boto3` clients. The only difference between LocalStack and real AWS is the `endpoint_url` passed to those clients, which is read from the `AWS_ENDPOINT_URL` environment variable:

```python
endpoint_url = os.getenv("AWS_ENDPOINT_URL")   # None → real AWS
boto3.client("s3", endpoint_url=endpoint_url)
boto3.resource("dynamodb", endpoint_url=endpoint_url)
```

When `AWS_ENDPOINT_URL=http://localhost:4566`, requests go to LocalStack.
When `AWS_ENDPOINT_URL` is unset, requests go to real AWS endpoints.

There is no separate LocalStack-specific code path.

---

## Minimal real AWS migration

### 1. Create an S3 bucket

```bash
aws s3 mb s3://designops-eval-artifacts-prod
```

### 2. Create a DynamoDB table

```bash
aws dynamodb create-table \
  --table-name designops-eval-runs-prod \
  --attribute-definitions AttributeName=run_id,AttributeType=S \
  --key-schema AttributeName=run_id,KeyType=HASH \
  --billing-mode PAY_PER_REQUEST
```

### 3. Unset the LocalStack endpoint

```bash
unset AWS_ENDPOINT_URL
```

### 4. Run with real bucket and table names

```bash
python -m designops_eval.cli run \
  --prompt-set evals/prompt_sets/combined_eval.yaml \
  --model mock \
  --baseline evals/baselines/baseline.json \
  --output-dir results/aws_run \
  --storage aws \
  --s3-bucket designops-eval-artifacts-prod \
  --dynamodb-table designops-eval-runs-prod
```

The application code does not change. Only the environment variables and provisioned resources differ.

---

## IAM permissions required

The caller (user, role, or task role) needs:

```json
{
  "Effect": "Allow",
  "Action": [
    "s3:PutObject",
    "s3:GetObject",
    "s3:DeleteObject",
    "s3:HeadBucket"
  ],
  "Resource": [
    "arn:aws:s3:::designops-eval-artifacts-prod",
    "arn:aws:s3:::designops-eval-artifacts-prod/*"
  ]
}
```

```json
{
  "Effect": "Allow",
  "Action": [
    "dynamodb:PutItem",
    "dynamodb:GetItem",
    "dynamodb:DeleteItem",
    "dynamodb:DescribeTable"
  ],
  "Resource": "arn:aws:dynamodb:*:*:table/designops-eval-runs-prod"
}
```

---

## Distributed workers on ECS Fargate

The distributed evaluation path (see `docs/PROJECT_TECHNICAL_OVERVIEW.md`, section 15) adds an SQS queue with a dead-letter queue, two DynamoDB tables and an optional ECS Fargate worker service. All of it is defined in `infra/terraform/distributed.tf` and `infra/terraform/worker.tf`.

Nothing in this section has been applied to a real AWS account. The Terraform passes `terraform validate`; it has not been planned or applied.

### What gets created

| Resource | When | Idle cost |
|----------|------|-----------|
| SQS queue and dead-letter queue | Always | None |
| DynamoDB results table and rate-limit table (on-demand) | Always | None beyond stored data |
| ECR repository, IAM roles, log group, ECS cluster, task definition, service | `enable_worker_service = true` | ECR image storage; logs retained 7 days |
| Running Fargate tasks | `worker_count > 0` | Billed per task-second while running |

The only resource with a meaningful running cost is a Fargate task. Check current Fargate pricing for your region before starting one, and set `worker_count` back to 0 when a run finishes.

### Review before any `terraform apply`

1. **Existing state.** `terraform.tfstate` already tracks the S3 bucket and runs table. Run `terraform plan` first and confirm it shows only additions, with nothing changed or destroyed.
2. **Plan size.** With `enable_worker_service = false` expect 4 resources to add (two queues, two tables). With it `true` expect 15.
3. **Account and region.** `aws sts get-caller-identity` shows the account you intend, and `aws_region` in `terraform.tfvars` is the region the existing resources are in.
4. **Billing alert.** The zero-spend budget from `docs/REAL_AWS_TERRAFORM_RUN.md` is still active.
5. **Local validation.** `pytest` passes and `python -m benchmarks.run all` reports `"passed": true` for every scenario.
6. **Default VPC.** The account has a default VPC in the region (`aws ec2 describe-vpcs --filters Name=isDefault,Values=true`). `worker.tf` places tasks in its subnets.
7. **IAM policies.** Read both policy documents in `worker.tf`. The task role should have no access beyond the shard queue and the two tables.
8. **Image architecture.** `worker_cpu_architecture` matches the machine that builds the image: `ARM64` for Apple Silicon, `X86_64` otherwise.
9. **Provider keys.** Leave `cohere_api_key_secret_arn` empty unless you intend real Cohere calls. With it empty, workers can only use the mock and fake providers.
10. **Worker count.** `worker_count = 0` for the first apply.

### Deploy

```bash
cd infra/terraform

# 1. Queues and tables only.
terraform plan  -var-file=terraform.tfvars
terraform apply -var-file=terraform.tfvars

# 2. Worker service with zero tasks. Set enable_worker_service = true first.
terraform plan  -var-file=terraform.tfvars
terraform apply -var-file=terraform.tfvars

# 3. Build and push the image.
REPO=$(terraform output -raw worker_ecr_repository_url)
aws ecr get-login-password | docker login --username AWS --password-stdin "${REPO%%/*}"
docker build -t "$REPO:latest" ../..
docker push "$REPO:latest"

# 4. Start workers. Set worker_count = 3, then:
terraform apply -var-file=terraform.tfvars
```

### Run

`submit` and `finalize` run from your machine. Prompt-set paths are resolved inside the worker container, so use paths relative to the repository root.

```bash
export DESIGNOPS_QUEUE_URL=$(terraform output -raw shard_queue_url)
export DESIGNOPS_RESULTS_TABLE=$(terraform output -raw results_table_name)

designops-eval submit --run-id aws-run-1 \
  --prompt-set evals/prompt_sets/generated_300.yaml --models fake-a

designops-eval finalize --run-id aws-run-1 \
  --prompt-set evals/prompt_sets/generated_300.yaml --models fake-a \
  --storage aws --s3-bucket <bucket> --dynamodb-table <runs table> \
  --output-dir results/aws_run_1
```

The fake provider runs inside each worker process on ECS, so its quota is not shared between tasks. Crash and duplicate-delivery experiments are valid there; shared rate-limit experiments need the fake provider's HTTP server and are run locally with `python -m benchmarks.run rate-limit`.

To inject a crash on ECS, stop a task while it is working (`aws ecs stop-task`) and watch the `shard_received` and `shard_completed` events in the log group.

### Tear down

```bash
# Stop compute first: set worker_count = 0 and apply.
# Then, to remove everything:
aws s3 rm s3://<bucket> --recursive
terraform destroy -var-file=terraform.tfvars
```

Confirm in the console that no ECS tasks are running and the ECR repository is gone.

---

## Verifying readiness before a real AWS run

```bash
python scripts/check_aws_ready.py \
  --s3-bucket designops-eval-artifacts-prod \
  --dynamodb-table designops-eval-runs-prod

# With write test:
python scripts/check_aws_ready.py \
  --s3-bucket designops-eval-artifacts-prod \
  --dynamodb-table designops-eval-runs-prod \
  --write-test
```
