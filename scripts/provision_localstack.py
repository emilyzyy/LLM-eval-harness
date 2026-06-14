#!/usr/bin/env python3
"""Create LocalStack S3 bucket and DynamoDB table if they do not already exist.

Run after `docker compose up -d localstack`:
    python scripts/provision_localstack.py
"""
import os
import sys

import boto3
from botocore.exceptions import ClientError

BUCKET = "designops-eval-artifacts"
TABLE = "designops-eval-runs"
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

    print("\nLocalStack resources are ready.")
    print(f"  S3 bucket:       {BUCKET}")
    print(f"  DynamoDB table:  {TABLE}")
    print(f"  Endpoint:        {ENDPOINT}")


if __name__ == "__main__":
    provision()
