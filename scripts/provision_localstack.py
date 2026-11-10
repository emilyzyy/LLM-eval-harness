#!/usr/bin/env python3
"""Create the LocalStack S3 bucket, DynamoDB tables and SQS queues if they do not exist.

Run after `docker compose up -d localstack`:
    python scripts/provision_localstack.py
"""
import os
import sys

import boto3
from botocore.exceptions import ClientError

BUCKET = "designops-eval-artifacts"
TABLE = "designops-eval-runs"
RESULTS_TABLE = "designops-eval-results"
RATE_LIMIT_TABLE = "designops-eval-rate-limits"
QUEUE = "designops-eval-shards"
REGION = os.getenv("AWS_DEFAULT_REGION", "us-east-1")
ENDPOINT = os.getenv("AWS_ENDPOINT_URL", "http://localhost:4566")


def provision() -> None:
    s3 = boto3.client("s3", region_name=REGION, endpoint_url=ENDPOINT)
    dynamodb = boto3.resource("dynamodb", region_name=REGION, endpoint_url=ENDPOINT)

    # S3 bucket
    try:
        s3.head_bucket(Bucket=BUCKET)
        print(f"[skip] S3 bucket '{BUCKET}' already exists.")
    except ClientError:
        s3.create_bucket(Bucket=BUCKET)
        print(f"[ok]   Created S3 bucket '{BUCKET}'.")

    # DynamoDB table
    table = dynamodb.Table(TABLE)
    try:
        table.load()
        print(f"[skip] DynamoDB table '{TABLE}' already exists.")
    except ClientError as exc:
        code = exc.response["Error"]["Code"]
        if code not in ("ResourceNotFoundException", "ResourceNotFound"):
            raise
        dynamodb.create_table(
            TableName=TABLE,
            AttributeDefinitions=[{"AttributeName": "run_id", "AttributeType": "S"}],
            KeySchema=[{"AttributeName": "run_id", "KeyType": "HASH"}],
            BillingMode="PAY_PER_REQUEST",
        )
        print(f"[ok]   Created DynamoDB table '{TABLE}'.")

    queue_url = provision_distributed(dynamodb)

    print("\nLocalStack resources are ready.")
    print(f"  S3 bucket:         {BUCKET}")
    print(f"  DynamoDB table:    {TABLE}")
    print(f"  Results table:     {RESULTS_TABLE}")
    print(f"  Rate-limit table:  {RATE_LIMIT_TABLE}")
    print(f"  Shard queue URL:   {queue_url}")
    print(f"  Endpoint:          {ENDPOINT}")


def provision_distributed(dynamodb) -> str:
    """Create the distributed-evaluation tables and queues. Returns the queue URL."""
    from designops_eval.distributed.schema import (
        create_queues,
        create_rate_limit_table,
        create_results_table,
    )

    existing = {table.name for table in dynamodb.tables.all()}
    for name, create in (
        (RESULTS_TABLE, create_results_table),
        (RATE_LIMIT_TABLE, create_rate_limit_table),
    ):
        if name in existing:
            print(f"[skip] DynamoDB table '{name}' already exists.")
        else:
            create(dynamodb, name)
            print(f"[ok]   Created DynamoDB table '{name}'.")

    # create_queue is idempotent when the attributes are unchanged.
    sqs = boto3.client("sqs", region_name=REGION, endpoint_url=ENDPOINT)
    queue_url, _ = create_queues(sqs, QUEUE)
    print(f"[ok]   SQS queue '{QUEUE}' and '{QUEUE}-dlq' are present.")
    return queue_url


if __name__ == "__main__":
    provision()
