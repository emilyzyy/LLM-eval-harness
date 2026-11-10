# ---------------------------------------------------------------------------
# Distributed evaluation: work queue, per-case results, shared rate limits.
#
# Key schemas here must match designops_eval/distributed/schema.py, which
# creates the same resources in local emulators.
# ---------------------------------------------------------------------------

locals {
  tags = {
    Project     = var.project_name
    Environment = var.environment
  }
}

# Shards that fail max_receive_count deliveries land here for inspection.
resource "aws_sqs_queue" "shards_dlq" {
  name                      = "${var.project_name}-shards-dlq-${var.environment}"
  message_retention_seconds = 1209600 # 14 days, the maximum
  sqs_managed_sse_enabled   = true
  tags                      = local.tags
}

# Standard (not FIFO) queue: delivery is at-least-once and unordered. The
# worker is built for that, so FIFO's cost and throughput limits buy nothing.
resource "aws_sqs_queue" "shards" {
  name                       = "${var.project_name}-shards-${var.environment}"
  visibility_timeout_seconds = var.shard_visibility_timeout_seconds
  receive_wait_time_seconds  = 20
  message_retention_seconds  = 345600 # 4 days
  sqs_managed_sse_enabled    = true

  redrive_policy = jsonencode({
    deadLetterTargetArn = aws_sqs_queue.shards_dlq.arn
    maxReceiveCount     = var.shard_max_receive_count
  })

  tags = local.tags
}

# One item per committed case result.
#   run_id      partition key
#   result_key  sort key, built from model, config version and case id
resource "aws_dynamodb_table" "eval_results" {
  name         = "${var.project_name}-results-${var.environment}"
  billing_mode = "PAY_PER_REQUEST"
  hash_key     = "run_id"
  range_key    = "result_key"

  attribute {
    name = "run_id"
    type = "S"
  }

  attribute {
    name = "result_key"
    type = "S"
  }

  tags = local.tags
}

# One item per provider holding its token-bucket state.
resource "aws_dynamodb_table" "rate_limits" {
  name         = "${var.project_name}-rate-limits-${var.environment}"
  billing_mode = "PAY_PER_REQUEST"
  hash_key     = "provider"

  attribute {
    name = "provider"
    type = "S"
  }

  tags = local.tags
}
