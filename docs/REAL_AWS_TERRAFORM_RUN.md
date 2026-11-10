# Real AWS Terraform Run

A step-by-step guide for provisioning the designops-eval S3 bucket and DynamoDB table
in real AWS using the Terraform configuration in `infra/terraform/`.

---

## What this document covers

You will:

- Create **one S3 bucket** and **one DynamoDB table** in real AWS using Terraform.
- Run the eval harness against those real resources once to verify the boto3 code path works.
- Destroy the resources immediately after verifying.

You will NOT:

- Deploy the application to AWS (no ECS, EC2, Lambda, or Fargate).
- Pay for a running service — S3 and PAY_PER_REQUEST DynamoDB have no idle cost.
- Commit any credentials, state files, or `terraform.tfvars` to git.
- Create any resources other than the S3 bucket and DynamoDB table.

---

## What you can truthfully claim afterward

> Provisioned the S3 bucket and DynamoDB table using Terraform and ran the eval
> harness against real AWS resources to verify the boto3 artifact-storage and
> metadata code paths. Infrastructure was immediately destroyed after verification.

This is distinct from "the application is deployed to AWS." It is accurate and honest.

---

## Prerequisites

Before running any of the commands below, confirm:

1. **AWS account** — you have access to an AWS account.
2. **Billing alert** — see the next section. Set this up before anything else.
3. **Terraform ≥ 1.0** — `terraform version` should work. Install from
   https://developer.hashicorp.com/terraform/install if needed.
4. **AWS CLI** — `aws --version` should work. Install from https://aws.amazon.com/cli/.
5. **IAM permissions** — the `designops` profile must have at minimum:
   - `s3:CreateBucket`, `s3:DeleteBucket`, `s3:GetBucketAcl`,
     `s3:GetBucketLocation`, `s3:GetBucketTagging`, `s3:PutBucketTagging`,
     `s3:ListAllMyBuckets`
   - `dynamodb:CreateTable`, `dynamodb:DescribeTable`, `dynamodb:DeleteTable`,
     `dynamodb:ListTagsOfResource`, `dynamodb:TagResource`

---

## Step 0 — Set up a billing alert first

Before provisioning anything in real AWS, create a billing budget so you get an
email if charges exceed a small threshold.

**Option A — AWS Console (recommended, takes 2 minutes):**

1. Sign in to the AWS Console.
2. Search for **"Budgets"** in the top search bar.
3. Click **"Create budget"** → **"Use a template"** → **"Zero spend budget"**.
4. Enter your email address.
5. Click **"Create budget"**.

This sends an alert the moment any charge appears on your account.

**Option B — AWS CLI:**
```bash
aws budgets create-budget \
  --account-id $(aws sts get-caller-identity --query Account --output text \
      --profile designops) \
  --budget '{
    "BudgetName": "designops-eval-zero-spend",
    "BudgetLimit": {"Amount": "1", "Unit": "USD"},
    "TimeUnit": "MONTHLY",
    "BudgetType": "COST"
  }' \
  --notifications-with-subscribers '[{
    "Notification": {
      "NotificationType": "ACTUAL",
      "ComparisonOperator": "GREATER_THAN",
      "Threshold": 0,
      "ThresholdType": "ABSOLUTE_VALUE"
    },
    "Subscribers": [{"SubscriptionType": "EMAIL", "Address": "YOUR_EMAIL@example.com"}]
  }]' \
  --profile designops \
  --region us-east-1
```

The S3 and DynamoDB resources created here cost essentially nothing at small scale
(a few cents or less for a short-lived test), but having the alert means you will
know immediately if something unexpected happens.

---

## Step 1 — Configure AWS credentials

Use a named profile rather than default credentials so that Terraform and the AWS CLI
target the correct account.

```bash
aws configure --profile designops
```

When prompted, enter:
- `AWS Access Key ID` — your IAM access key (from the AWS Console → IAM → your user → Security credentials)
- `AWS Secret Access Key` — your IAM secret key
- `Default region name` — `us-east-2`
- `Default output format` — `json`

This writes credentials to `~/.aws/credentials` and config to `~/.aws/config`. Neither
file is inside this repo, so they are never committed to git.

**Verify credentials work before continuing:**
```bash
aws sts get-caller-identity --profile designops
```

Expected output (values will differ):
```json
{
    "UserId": "AIDAEXAMPLEID",
    "Account": "123456789012",
    "Arn": "arn:aws:iam::123456789012:user/your-iam-user"
}
```

If this command fails, stop. Do not proceed until it succeeds.

---

## Step 2 — Create terraform.tfvars

`terraform.tfvars` is excluded from git by `.gitignore`. You create it locally; it
is never committed.

```bash
cd /Users/emily/designops-eval/infra/terraform
cp terraform.tfvars.example terraform.tfvars
```

Then open `terraform.tfvars` and replace its contents with exactly:

```hcl
project_name        = "designops-eval"
environment         = "dev"
aws_region          = "us-east-2"
s3_bucket_name      = "designops-eval-emilyzy-dev-20260620"
dynamodb_table_name = "designops-eval-runs-dev"
# aws_endpoint_url is intentionally absent — defaults to "" which targets real AWS
```

**S3 bucket names must be globally unique across all AWS accounts.**
`designops-eval-emilyzy-dev-20260620` follows the pattern
`<project>-<initials>-<env>-<date>` which is unlikely to collide.

**Do not add `aws_endpoint_url`** to this file. Leaving it absent causes Terraform
to use the default value (`""`), which the `main.tf` logic correctly interprets as
"use real AWS endpoints."

---

## Step 3 — Initialize Terraform

```bash
cd /Users/emily/designops-eval/infra/terraform
terraform init
```

This downloads the AWS provider plugin if not already present. The provider version
is pinned in `.terraform.lock.hcl` (which is committed to git for reproducibility).

**Success:** `Terraform has been successfully initialized!`

---

## Step 4 — Format and validate (optional but recommended)

```bash
terraform fmt -check
terraform validate
```

`fmt -check` confirms the HCL files are properly formatted (exits 0 if clean).
`validate` confirms the configuration is syntactically valid.

**Success:** No output (fmt) and `Success! The configuration is valid.` (validate)

---

## Step 5 — Review the plan

```bash
export AWS_PROFILE=designops

terraform plan -var-file=terraform.tfvars
```

Read the plan output carefully before continuing.

### Plan inspection checklist

The plan is acceptable **only** if it shows exactly these two resource creations and nothing else:

```
# aws_s3_bucket.eval_artifacts will be created
  + resource "aws_s3_bucket" "eval_artifacts" {
      + bucket = "designops-eval-emilyzy-dev-20260620"
      ...
    }

# aws_dynamodb_table.eval_runs will be created
  + resource "aws_dynamodb_table" "eval_runs" {
      + name         = "designops-eval-runs-dev"
      + billing_mode = "PAY_PER_REQUEST"
      + hash_key     = "run_id"
      ...
    }

Plan: 2 to add, 0 to change, 0 to destroy.
```

**Stop immediately if the plan shows:**

- Any resource other than `aws_s3_bucket.eval_artifacts` and `aws_dynamodb_table.eval_runs`
- Any VPC, security group, EC2, ECS, Lambda, IAM role, CloudWatch, or Secrets Manager resource
- A count other than `2 to add`
- Any destroy (`to destroy > 0`) if you have not run `apply` yet

If the plan looks wrong, do not run apply. Run `terraform destroy` if anything was
already created, then investigate the configuration.

---

## Step 6 — Apply

Only run this after the plan passes the inspection checklist above.

```bash
terraform apply -var-file=terraform.tfvars
```

Terraform will print the plan again and prompt:

```
Do you want to perform these actions?
  Terraform will perform the actions described above.
  Only 'yes' will be accepted to approve.

  Enter a value:
```

Type `yes` and press Enter.

**Success output:**
```
aws_s3_bucket.eval_artifacts: Creating...
aws_s3_bucket.eval_artifacts: Creation complete after Xs [id=designops-eval-emilyzy-dev-20260620]
aws_dynamodb_table.eval_runs: Creating...
aws_dynamodb_table.eval_runs: Creation complete after Xs [id=designops-eval-runs-dev]

Apply complete! Resources: 2 added, 0 changed, 0 destroyed.

Outputs:

s3_bucket_name     = "designops-eval-emilyzy-dev-20260620"
dynamodb_table_name = "designops-eval-runs-dev"
s3_bucket_arn      = "arn:aws:s3:::designops-eval-emilyzy-dev-20260620"
dynamodb_table_arn = "arn:aws:dynamodb:us-east-2:XXXXXXXXXXXX:table/designops-eval-runs-dev"
```

---

## Step 7 — Verify resources via AWS CLI

```bash
# Confirm the S3 bucket exists
aws s3 ls --profile designops | grep designops-eval-emilyzy-dev

# Confirm the DynamoDB table exists
aws dynamodb list-tables \
  --profile designops \
  --region us-east-2 \
  --output table
```

Expected: bucket name and table name appear in the output.

---

## Step 8 — Run the harness against real AWS (optional)

To exercise the actual boto3 code path against real AWS resources:

```bash
cd /Users/emily/designops-eval

unset AWS_ENDPOINT_URL          # must be unset — no local endpoint
export AWS_PROFILE=designops
export AWS_DEFAULT_REGION=us-east-2

python3 -m designops_eval.cli run \
  --prompt-set evals/prompt_sets/combined_eval.yaml \
  --model mock \
  --baseline evals/baselines/baseline.json \
  --output-dir results/real_aws_run \
  --storage aws \
  --s3-bucket designops-eval-emilyzy-dev-20260620 \
  --dynamodb-table designops-eval-runs-dev
```

**Verify the artifacts were uploaded:**
```bash
aws s3 ls s3://designops-eval-emilyzy-dev-20260620 --recursive --profile designops
```

Expected — three files under `runs/<run_id>/`:
```
2026-06-20 ...   runs/<uuid>/latest.json
2026-06-20 ...   runs/<uuid>/traces.json
2026-06-20 ...   runs/<uuid>/report.md
```

**Verify the DynamoDB metadata row:**
```bash
aws dynamodb scan \
  --table-name designops-eval-runs-dev \
  --profile designops \
  --region us-east-2 \
  --output json
```

Expected: one item with `run_id`, `model`, `timestamp`, `s3_prefix`, and score fields.

---

## Step 9 — Destroy resources immediately

```bash
cd /Users/emily/designops-eval/infra/terraform

terraform destroy -var-file=terraform.tfvars
```

Terraform will print the destroy plan and prompt for `yes`.

**Before typing yes**, confirm the plan shows:
```
Plan: 0 to add, 0 to change, 2 to destroy.
```

And that only `aws_s3_bucket.eval_artifacts` and `aws_dynamodb_table.eval_runs` are
listed under `# ... will be destroyed`.

**Note on bucket contents:** Terraform cannot destroy an S3 bucket that contains
objects. If you ran Step 8 (the real AWS eval run), the bucket will have objects.
You need to empty it first:

```bash
aws s3 rm s3://designops-eval-emilyzy-dev-20260620 --recursive --profile designops
```

Then run `terraform destroy` again.

**Success:**
```
Destroy complete! Resources: 2 destroyed.
```

Verify with:
```bash
aws s3 ls --profile designops | grep designops-eval-emilyzy-dev
aws dynamodb list-tables --profile designops --region us-east-2
```

Neither resource should appear.

---

## What not to commit

These files must never be committed to git. The `.gitignore` in this repo already
excludes all of them:

| File | Why excluded |
|------|-------------|
| `infra/terraform/terraform.tfvars` | Contains real bucket/table names; could vary by env |
| `infra/terraform/terraform.tfstate` | Contains real AWS resource IDs and ARNs |
| `infra/terraform/terraform.tfstate.backup` | Same as above |
| `infra/terraform/.terraform/` | Provider binaries; large, platform-specific |
| `~/.aws/credentials` | AWS access keys — lives outside the repo, never inside |
| `~/.aws/config` | AWS profile config — lives outside the repo |

The following files **are** committed and safe to commit:

| File | Why committed |
|------|--------------|
| `infra/terraform/main.tf` | Resource definitions (no secrets) |
| `infra/terraform/variables.tf` | Variable declarations (no values, no secrets) |
| `infra/terraform/outputs.tf` | Output declarations (no secrets) |
| `infra/terraform/localstack.tfvars` | LocalStack config (fake credentials, local endpoint) |
| `infra/terraform/terraform.tfvars.example` | Template with placeholders (no real values) |
| `infra/terraform/.terraform.lock.hcl` | Provider version lock (no secrets) |

---

## Full copy-paste command sequence

```bash
# ── 0. Prerequisites ─────────────────────────────────────────────────────────
aws configure --profile designops
aws sts get-caller-identity --profile designops

# ── 1. Navigate to Terraform directory ──────────────────────────────────────
cd /Users/emily/designops-eval/infra/terraform

# ── 2. Create terraform.tfvars (not committed) ───────────────────────────────
cp terraform.tfvars.example terraform.tfvars
# Edit terraform.tfvars — set these values:
#   aws_region          = "us-east-2"
#   s3_bucket_name      = "designops-eval-emilyzy-dev-20260620"
#   dynamodb_table_name = "designops-eval-runs-dev"
#   environment         = "dev"
# (no aws_endpoint_url — leave it absent for real AWS)

# ── 3. Initialize ────────────────────────────────────────────────────────────
terraform init

# ── 4. Validate ──────────────────────────────────────────────────────────────
terraform fmt -check
terraform validate

# ── 5. Plan — READ THIS CAREFULLY ────────────────────────────────────────────
export AWS_PROFILE=designops
terraform plan -var-file=terraform.tfvars
# STOP here. Verify:
#   - Only aws_s3_bucket.eval_artifacts and aws_dynamodb_table.eval_runs
#   - "Plan: 2 to add, 0 to change, 0 to destroy"
#   - Nothing else in the plan

# ── 6. Apply ─────────────────────────────────────────────────────────────────
terraform apply -var-file=terraform.tfvars
# Type "yes" when prompted

# ── 7. Verify resources ──────────────────────────────────────────────────────
aws s3 ls --profile designops | grep designops-eval-emilyzy-dev
aws dynamodb list-tables --profile designops --region us-east-2

# ── 8. Run harness against real AWS (optional) ───────────────────────────────
cd /Users/emily/designops-eval
unset AWS_ENDPOINT_URL
export AWS_PROFILE=designops
export AWS_DEFAULT_REGION=us-east-2

python3 -m designops_eval.cli run \
  --prompt-set evals/prompt_sets/combined_eval.yaml \
  --model mock \
  --baseline evals/baselines/baseline.json \
  --output-dir results/real_aws_run \
  --storage aws \
  --s3-bucket designops-eval-emilyzy-dev-20260620 \
  --dynamodb-table designops-eval-runs-dev

# Verify S3 artifacts
aws s3 ls s3://designops-eval-emilyzy-dev-20260620 --recursive --profile designops

# Verify DynamoDB row
aws dynamodb scan \
  --table-name designops-eval-runs-dev \
  --profile designops \
  --region us-east-2

# ── 9. Destroy ───────────────────────────────────────────────────────────────
cd /Users/emily/designops-eval/infra/terraform

# If you ran Step 8: empty the bucket before destroy
aws s3 rm s3://designops-eval-emilyzy-dev-20260620 --recursive --profile designops

terraform destroy -var-file=terraform.tfvars
# Type "yes" when prompted
# Verify: "Destroy complete! Resources: 2 destroyed."

# Confirm resources are gone
aws s3 ls --profile designops | grep designops-eval-emilyzy-dev
aws dynamodb list-tables --profile designops --region us-east-2
```

---

## Resume-safe language (after completing this run)

**Exact bullet — accurate, not overclaiming:**

> Provisioned project S3 bucket and DynamoDB table in real AWS using Terraform
> (`PAY_PER_REQUEST` billing, `us-east-2`), verified artifact upload and metadata
> write using the same boto3 clients used against LocalStack, then destroyed
> infrastructure with `terraform destroy`.

**What this bullet honestly establishes:**
- You used Terraform IaC to provision real cloud resources.
- You verified the boto3 code path works against real AWS, not just LocalStack.
- You understand the destroy lifecycle.
- You understand the `AWS_ENDPOINT_URL` switching mechanism.

**What it does not claim:**
- That the application is running in AWS continuously.
- That a real LLM was evaluated (mock model only).
- That this is a production deployment.
