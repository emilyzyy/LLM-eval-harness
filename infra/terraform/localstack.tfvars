# Provision resources against LocalStack running on localhost:4566.
# Usage:
#   docker compose up -d localstack        # start LocalStack
#   cd infra/terraform
#   terraform init
#   terraform plan  -var-file=localstack.tfvars
#   terraform apply -var-file=localstack.tfvars
#
# After apply, run the harness with:
#   export AWS_ENDPOINT_URL=http://localhost:4566
#   export AWS_ACCESS_KEY_ID=test
#   export AWS_SECRET_ACCESS_KEY=test
#   export AWS_DEFAULT_REGION=us-east-1
#   python -m designops_eval.cli run \
#     --prompt-set evals/prompt_sets/combined_eval.yaml \
#     --model mock \
#     --baseline evals/baselines/baseline.json \
#     --output-dir results/localstack_run \
#     --storage aws \
#     --s3-bucket designops-eval-artifacts \
#     --dynamodb-table designops-eval-runs

project_name        = "designops-eval"
environment         = "local"
aws_region          = "us-east-1"
s3_bucket_name      = "designops-eval-artifacts"
dynamodb_table_name = "designops-eval-runs"
aws_endpoint_url    = "http://localhost:4566"
