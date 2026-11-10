variable "project_name" {
  type        = string
  default     = "designops-eval"
  description = "Project name prefix used in resource names and tags."
}

variable "environment" {
  type        = string
  default     = "local"
  description = "Deployment environment label (local, dev, prod)."
}

variable "aws_region" {
  type        = string
  default     = "us-east-1"
  description = "AWS region for all resources."
}

variable "s3_bucket_name" {
  type        = string
  default     = "designops-eval-artifacts"
  description = "S3 bucket name for eval artifacts (latest.json, traces.json, report.md)."
}

variable "dynamodb_table_name" {
  type        = string
  default     = "designops-eval-runs"
  description = "DynamoDB table name for per-run metadata rows."
}

variable "aws_endpoint_url" {
  type        = string
  default     = ""
  description = "Override endpoint for AWS-compatible services. Set to http://localhost:4566 for LocalStack. Leave empty for real AWS."
}

# ---------------------------------------------------------------------------
# Distributed evaluation
# ---------------------------------------------------------------------------

variable "shard_visibility_timeout_seconds" {
  type        = number
  default     = 120
  description = "How long a received shard stays invisible between worker heartbeats. Also the longest a crashed worker's shard waits before another worker can take it."
}

variable "shard_max_receive_count" {
  type        = number
  default     = 5
  description = "Deliveries of one shard message before SQS moves it to the dead-letter queue."
}

variable "enable_worker_service" {
  type        = bool
  default     = false
  description = "Create the ECR repository, IAM roles, log group and ECS Fargate service for workers. Leave false for LocalStack."
}

variable "worker_count" {
  type        = number
  default     = 0
  description = "Number of worker tasks to run. Fargate bills per running task; set back to 0 when a run is finished."
}

variable "worker_cpu" {
  type        = number
  default     = 256
  description = "Fargate CPU units per worker task (256 = 0.25 vCPU)."
}

variable "worker_memory" {
  type        = number
  default     = 512
  description = "Fargate memory per worker task, in MiB."
}

variable "worker_cpu_architecture" {
  type        = string
  default     = "ARM64"
  description = "ARM64 or X86_64. Must match the architecture the image was built for; an image built on Apple Silicon is ARM64."

  validation {
    condition     = contains(["ARM64", "X86_64"], var.worker_cpu_architecture)
    error_message = "worker_cpu_architecture must be ARM64 or X86_64."
  }
}

variable "worker_image_tag" {
  type        = string
  default     = "latest"
  description = "Tag of the worker image in the ECR repository."
}

variable "worker_log_retention_days" {
  type        = number
  default     = 7
  description = "CloudWatch Logs retention for worker logs."
}

variable "worker_provider_limits" {
  type        = string
  default     = ""
  description = "Shared rate limits as 'provider=requests_per_minute' pairs, comma separated, e.g. 'cohere=20'. Empty means unlimited."
}

variable "worker_extra_environment" {
  type        = map(string)
  default     = {}
  description = "Additional environment variables for the worker container, e.g. fake-provider settings."
}

variable "cohere_api_key_secret_arn" {
  type        = string
  default     = ""
  description = "Secrets Manager ARN holding COHERE_API_KEY. Leave empty when workers only use the fake provider."
}
