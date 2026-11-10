"""Resource definitions for local emulators and tests.

Real AWS resources are created by Terraform (infra/terraform/distributed.tf).
These helpers create the same key schemas in moto or LocalStack and must be
kept in step with that file.
"""

import json

RESULTS_KEY_SCHEMA = [
    {"AttributeName": "run_id", "KeyType": "HASH"},
    {"AttributeName": "result_key", "KeyType": "RANGE"},
]
RESULTS_ATTRIBUTES = [
    {"AttributeName": "run_id", "AttributeType": "S"},
    {"AttributeName": "result_key", "AttributeType": "S"},
]
RATE_LIMIT_KEY_SCHEMA = [{"AttributeName": "provider", "KeyType": "HASH"}]
RATE_LIMIT_ATTRIBUTES = [{"AttributeName": "provider", "AttributeType": "S"}]


def create_results_table(dynamodb, name: str):
    return dynamodb.create_table(
        TableName=name,
        KeySchema=RESULTS_KEY_SCHEMA,
        AttributeDefinitions=RESULTS_ATTRIBUTES,
        BillingMode="PAY_PER_REQUEST",
    )


def create_rate_limit_table(dynamodb, name: str):
    return dynamodb.create_table(
        TableName=name,
        KeySchema=RATE_LIMIT_KEY_SCHEMA,
        AttributeDefinitions=RATE_LIMIT_ATTRIBUTES,
        BillingMode="PAY_PER_REQUEST",
    )


def create_queues(
    sqs,
    name: str,
    visibility_timeout: int = 120,
    max_receive_count: int = 5,
) -> tuple[str, str]:
    """Create a work queue with a dead-letter queue. Returns (queue_url, dlq_url)."""
    dlq_url = sqs.create_queue(QueueName=f"{name}-dlq")["QueueUrl"]
    dlq_arn = sqs.get_queue_attributes(QueueUrl=dlq_url, AttributeNames=["QueueArn"])[
        "Attributes"
    ]["QueueArn"]
    queue_url = sqs.create_queue(
        QueueName=name,
        Attributes={
            "VisibilityTimeout": str(visibility_timeout),
            "RedrivePolicy": json.dumps(
                {"deadLetterTargetArn": dlq_arn, "maxReceiveCount": max_receive_count}
            ),
        },
    )["QueueUrl"]
    return queue_url, dlq_url
