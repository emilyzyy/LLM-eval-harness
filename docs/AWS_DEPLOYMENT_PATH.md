# AWS Deployment Path

The project is not deployed to AWS by default. It is cloud-ready because the artifact and metadata paths use standard AWS SDK clients. LocalStack is only used as a local endpoint. Moving to real AWS means removing `AWS_ENDPOINT_URL`, providing AWS credentials/IAM permissions, and provisioning the equivalent S3 bucket and DynamoDB table.

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

## Optional ECS/Fargate migration

The application code does not need to change for containerized cloud deployment. Only deployment configuration changes:

1. **Build and push the Docker image to ECR:**
   ```bash
   aws ecr create-repository --repository-name designops-eval
   docker build -t designops-eval .
   docker tag designops-eval:latest <account>.dkr.ecr.<region>.amazonaws.com/designops-eval:latest
   docker push <account>.dkr.ecr.<region>.amazonaws.com/designops-eval:latest
   ```

2. **Create an ECS task definition** referencing the ECR image, and pass environment variables:
   - `AWS_DEFAULT_REGION`
   - `DESIGNOPS_S3_BUCKET` (or pass via CLI flags)
   - `DESIGNOPS_DYNAMODB_TABLE`
   - Do **not** set `AWS_ENDPOINT_URL` — leave it unset so boto3 uses real AWS

3. **Attach an IAM task role** with the S3 and DynamoDB permissions above. The application will pick up credentials automatically via the ECS metadata endpoint.

4. **Stream container logs to CloudWatch** by adding a `logConfiguration` block to the task definition.

The evaluation CLI command is unchanged — only the infrastructure around it changes.

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
