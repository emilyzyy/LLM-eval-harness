# ---------------------------------------------------------------------------
# ECS Fargate worker service.
#
# Everything in this file is created only when enable_worker_service = true.
# The service starts with worker_count tasks (default 0), so enabling it does
# not by itself start any billable compute.
#
# Not available on LocalStack Community; leave enable_worker_service = false
# there.
# ---------------------------------------------------------------------------

locals {
  worker_enabled = var.enable_worker_service ? 1 : 0
  worker_name    = "${var.project_name}-worker-${var.environment}"

  worker_environment = merge(
    {
      AWS_DEFAULT_REGION           = var.aws_region
      DESIGNOPS_QUEUE_URL          = aws_sqs_queue.shards.url
      DESIGNOPS_RESULTS_TABLE      = aws_dynamodb_table.eval_results.name
      DESIGNOPS_RATE_LIMIT_TABLE   = aws_dynamodb_table.rate_limits.name
      DESIGNOPS_LIMITER            = "shared"
      DESIGNOPS_PROVIDER_LIMITS    = var.worker_provider_limits
      DESIGNOPS_VISIBILITY_TIMEOUT = tostring(var.shard_visibility_timeout_seconds)
    },
    var.worker_extra_environment,
  )
}

resource "aws_ecr_repository" "worker" {
  count                = local.worker_enabled
  name                 = var.project_name
  image_tag_mutability = "MUTABLE"
  force_delete         = true

  image_scanning_configuration {
    scan_on_push = true
  }

  tags = local.tags
}

resource "aws_ecr_lifecycle_policy" "worker" {
  count      = local.worker_enabled
  repository = aws_ecr_repository.worker[0].name

  policy = jsonencode({
    rules = [{
      rulePriority = 1
      description  = "Keep the five most recent images"
      selection = {
        tagStatus   = "any"
        countType   = "imageCountMoreThan"
        countNumber = 5
      }
      action = { type = "expire" }
    }]
  })
}

resource "aws_cloudwatch_log_group" "worker" {
  count             = local.worker_enabled
  name              = "/ecs/${local.worker_name}"
  retention_in_days = var.worker_log_retention_days
  tags              = local.tags
}

# --- IAM -------------------------------------------------------------------

data "aws_iam_policy_document" "ecs_tasks_assume" {
  statement {
    actions = ["sts:AssumeRole"]

    principals {
      type        = "Service"
      identifiers = ["ecs-tasks.amazonaws.com"]
    }
  }
}

# Execution role: used by ECS itself to pull the image and write logs.
resource "aws_iam_role" "worker_execution" {
  count              = local.worker_enabled
  name               = "${local.worker_name}-execution"
  assume_role_policy = data.aws_iam_policy_document.ecs_tasks_assume.json
  tags               = local.tags
}

data "aws_iam_policy_document" "worker_execution" {
  count = local.worker_enabled

  statement {
    sid       = "EcrAuth"
    actions   = ["ecr:GetAuthorizationToken"]
    resources = ["*"] # this action does not support resource-level permissions
  }

  statement {
    sid = "EcrPull"
    actions = [
      "ecr:BatchCheckLayerAvailability",
      "ecr:BatchGetImage",
      "ecr:GetDownloadUrlForLayer",
    ]
    resources = [aws_ecr_repository.worker[0].arn]
  }

  statement {
    sid       = "WriteLogs"
    actions   = ["logs:CreateLogStream", "logs:PutLogEvents"]
    resources = ["${aws_cloudwatch_log_group.worker[0].arn}:*"]
  }

  dynamic "statement" {
    for_each = var.cohere_api_key_secret_arn != "" ? [1] : []
    content {
      sid       = "ReadProviderKey"
      actions   = ["secretsmanager:GetSecretValue"]
      resources = [var.cohere_api_key_secret_arn]
    }
  }
}

resource "aws_iam_role_policy" "worker_execution" {
  count  = local.worker_enabled
  name   = "execution"
  role   = aws_iam_role.worker_execution[0].id
  policy = data.aws_iam_policy_document.worker_execution[0].json
}

# Task role: what the worker code itself may do.
resource "aws_iam_role" "worker_task" {
  count              = local.worker_enabled
  name               = "${local.worker_name}-task"
  assume_role_policy = data.aws_iam_policy_document.ecs_tasks_assume.json
  tags               = local.tags
}

data "aws_iam_policy_document" "worker_task" {
  count = local.worker_enabled

  # Workers consume shards. They never publish, and never touch the DLQ.
  statement {
    sid = "ConsumeShards"
    actions = [
      "sqs:ReceiveMessage",
      "sqs:DeleteMessage",
      "sqs:ChangeMessageVisibility",
      "sqs:GetQueueAttributes",
    ]
    resources = [aws_sqs_queue.shards.arn]
  }

  # Workers append results and read them back. No update, no delete:
  # a committed result is immutable from a worker's point of view.
  statement {
    sid = "CommitResults"
    actions = [
      "dynamodb:PutItem",
      "dynamodb:GetItem",
      "dynamodb:BatchGetItem",
      "dynamodb:Query",
    ]
    resources = [aws_dynamodb_table.eval_results.arn]
  }

  statement {
    sid = "SharedRateLimit"
    actions = [
      "dynamodb:GetItem",
      "dynamodb:PutItem",
      "dynamodb:UpdateItem",
    ]
    resources = [aws_dynamodb_table.rate_limits.arn]
  }
}

resource "aws_iam_role_policy" "worker_task" {
  count  = local.worker_enabled
  name   = "task"
  role   = aws_iam_role.worker_task[0].id
  policy = data.aws_iam_policy_document.worker_task[0].json
}

# --- Network ---------------------------------------------------------------
# Tasks run in the default VPC's public subnets with a public IP and no
# inbound access. That avoids a NAT gateway, which would cost more per month
# than everything else here combined.

data "aws_vpc" "default" {
  count   = local.worker_enabled
  default = true
}

data "aws_subnets" "default" {
  count = local.worker_enabled

  filter {
    name   = "vpc-id"
    values = [data.aws_vpc.default[0].id]
  }
}

resource "aws_security_group" "worker" {
  count       = local.worker_enabled
  name        = local.worker_name
  description = "designops-eval workers: outbound only"
  vpc_id      = data.aws_vpc.default[0].id

  egress {
    description = "HTTPS to AWS APIs and model providers"
    from_port   = 443
    to_port     = 443
    protocol    = "tcp"
    cidr_blocks = ["0.0.0.0/0"]
  }

  tags = local.tags
}

# --- ECS -------------------------------------------------------------------

resource "aws_ecs_cluster" "main" {
  count = local.worker_enabled
  name  = "${var.project_name}-${var.environment}"
  tags  = local.tags
}

resource "aws_ecs_task_definition" "worker" {
  count                    = local.worker_enabled
  family                   = local.worker_name
  requires_compatibilities = ["FARGATE"]
  network_mode             = "awsvpc"
  cpu                      = var.worker_cpu
  memory                   = var.worker_memory
  execution_role_arn       = aws_iam_role.worker_execution[0].arn
  task_role_arn            = aws_iam_role.worker_task[0].arn

  runtime_platform {
    operating_system_family = "LINUX"
    cpu_architecture        = var.worker_cpu_architecture
  }

  container_definitions = jsonencode([{
    name      = "worker"
    image     = "${aws_ecr_repository.worker[0].repository_url}:${var.worker_image_tag}"
    essential = true
    command   = ["worker"]

    environment = [
      for name, value in local.worker_environment : { name = name, value = value }
    ]

    secrets = var.cohere_api_key_secret_arn != "" ? [
      { name = "COHERE_API_KEY", valueFrom = var.cohere_api_key_secret_arn }
    ] : []

    # On SIGTERM the worker finishes its current shard. If that takes longer
    # than this, ECS kills it and the shard is redelivered.
    stopTimeout = 120

    logConfiguration = {
      logDriver = "awslogs"
      options = {
        "awslogs-group"         = aws_cloudwatch_log_group.worker[0].name
        "awslogs-region"        = var.aws_region
        "awslogs-stream-prefix" = "worker"
      }
    }
  }])

  tags = local.tags
}

resource "aws_ecs_service" "worker" {
  count           = local.worker_enabled
  name            = local.worker_name
  cluster         = aws_ecs_cluster.main[0].id
  task_definition = aws_ecs_task_definition.worker[0].arn
  desired_count   = var.worker_count
  launch_type     = "FARGATE"

  network_configuration {
    subnets          = data.aws_subnets.default[0].ids
    security_groups  = [aws_security_group.worker[0].id]
    assign_public_ip = true
  }

  tags = local.tags
}
