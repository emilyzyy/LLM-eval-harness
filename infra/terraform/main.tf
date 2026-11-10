terraform {
  required_version = ">= 1.0"

  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
  }

  # Local state only — no remote backend.
  # For a shared team setup, add a backend "s3" {} or "remote" {} block here.
}

locals {
  use_localstack = var.aws_endpoint_url != ""
}

provider "aws" {
  region = var.aws_region

  # LocalStack uses dummy credentials.  Real AWS reads from the environment
  # (AWS_PROFILE, ~/.aws/credentials, or instance metadata) when these are null.
  access_key = local.use_localstack ? "test" : null
  secret_key = local.use_localstack ? "test" : null

  skip_credentials_validation = local.use_localstack
  skip_requesting_account_id  = local.use_localstack
  skip_metadata_api_check     = local.use_localstack
  s3_use_path_style           = local.use_localstack

  dynamic "endpoints" {
    for_each = local.use_localstack ? [1] : []
    content {
      s3       = var.aws_endpoint_url
      dynamodb = var.aws_endpoint_url
      sqs      = var.aws_endpoint_url
    }
  }
}

# ---------------------------------------------------------------------------
# S3 bucket — stores per-run artifacts:
#   runs/<run_id>/latest.json
#   runs/<run_id>/traces.json
#   runs/<run_id>/report.md
# ---------------------------------------------------------------------------
resource "aws_s3_bucket" "eval_artifacts" {
  bucket = var.s3_bucket_name

  tags = {
    Project     = var.project_name
    Environment = var.environment
  }
}

# ---------------------------------------------------------------------------
# DynamoDB table — stores one metadata row per eval run.
# Hash key: run_id (String) — matches the run_id field written by AwsStorage.
# ---------------------------------------------------------------------------
resource "aws_dynamodb_table" "eval_runs" {
  name         = var.dynamodb_table_name
  billing_mode = "PAY_PER_REQUEST"
  hash_key     = "run_id"

  attribute {
    name = "run_id"
    type = "S"
  }

  tags = {
    Project     = var.project_name
    Environment = var.environment
  }
}
