output "s3_bucket_name" {
  description = "Name of the S3 bucket for eval artifacts."
  value       = aws_s3_bucket.eval_artifacts.bucket
}

output "dynamodb_table_name" {
  description = "Name of the DynamoDB table for run metadata."
  value       = aws_dynamodb_table.eval_runs.name
}

output "s3_bucket_arn" {
  description = "ARN of the S3 bucket."
  value       = aws_s3_bucket.eval_artifacts.arn
}

output "dynamodb_table_arn" {
  description = "ARN of the DynamoDB table."
  value       = aws_dynamodb_table.eval_runs.arn
}

output "shard_queue_url" {
  description = "URL of the SQS queue workers consume shards from (DESIGNOPS_QUEUE_URL)."
  value       = aws_sqs_queue.shards.url
}

output "shard_dlq_url" {
  description = "URL of the dead-letter queue for shards that repeatedly fail."
  value       = aws_sqs_queue.shards_dlq.url
}

output "results_table_name" {
  description = "DynamoDB table of per-case results (DESIGNOPS_RESULTS_TABLE)."
  value       = aws_dynamodb_table.eval_results.name
}

output "rate_limit_table_name" {
  description = "DynamoDB table of provider token buckets (DESIGNOPS_RATE_LIMIT_TABLE)."
  value       = aws_dynamodb_table.rate_limits.name
}

output "worker_ecr_repository_url" {
  description = "ECR repository to push the worker image to. Null unless enable_worker_service is true."
  value       = one(aws_ecr_repository.worker[*].repository_url)
}

output "worker_cluster_name" {
  description = "ECS cluster name. Null unless enable_worker_service is true."
  value       = one(aws_ecs_cluster.main[*].name)
}

output "worker_service_name" {
  description = "ECS service name. Null unless enable_worker_service is true."
  value       = one(aws_ecs_service.worker[*].name)
}

output "worker_log_group" {
  description = "CloudWatch log group for worker logs. Null unless enable_worker_service is true."
  value       = one(aws_cloudwatch_log_group.worker[*].name)
}
