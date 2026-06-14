#!/usr/bin/env python3
"""Verify that the configured AWS resources are reachable.

Does NOT create real AWS resources. Only checks that they exist and are accessible.

Usage:
    python scripts/check_aws_ready.py \\
        --s3-bucket designops-eval-artifacts-prod \\
        --dynamodb-table designops-eval-runs-prod

    # With write test:
    python scripts/check_aws_ready.py \\
        --s3-bucket designops-eval-artifacts-prod \\
        --dynamodb-table designops-eval-runs-prod \\
        --write-test
"""
import argparse
import os
import sys
import uuid

import boto3
from botocore.exceptions import ClientError, NoCredentialsError


def check(s3_bucket: str, dynamodb_table: str, write_test: bool) -> bool:
    region = os.getenv("AWS_DEFAULT_REGION", "us-east-1")
    endpoint = os.getenv("AWS_ENDPOINT_URL")

    s3 = boto3.client("s3", region_name=region, endpoint_url=endpoint)
    sts = boto3.client("sts", region_name=region, endpoint_url=endpoint)
    dynamodb = boto3.resource("dynamodb", region_name=region, endpoint_url=endpoint)

    ok = True

    # Caller identity (best-effort)
    try:
        identity = sts.get_caller_identity()
        print(f"[ok]   Caller identity: {identity.get('Arn', 'unknown')}")
    except (ClientError, NoCredentialsError) as exc:
        print(f"[warn] Could not get caller identity: {exc}")

    # S3 bucket
    try:
        s3.head_bucket(Bucket=s3_bucket)
        print(f"[ok]   S3 bucket '{s3_bucket}' exists.")
    except ClientError as exc:
        print(f"[fail] S3 bucket '{s3_bucket}': {exc}")
        ok = False

    # DynamoDB table
    table = dynamodb.Table(dynamodb_table)
    try:
        table.load()
        print(f"[ok]   DynamoDB table '{dynamodb_table}' exists (status: {table.table_status}).")
    except ClientError as exc:
        print(f"[fail] DynamoDB table '{dynamodb_table}': {exc}")
        ok = False

    if write_test and ok:
        # S3 write test
        test_key = f"__healthcheck/{uuid.uuid4()}.txt"
        try:
            s3.put_object(Bucket=s3_bucket, Key=test_key, Body=b"healthcheck")
            s3.delete_object(Bucket=s3_bucket, Key=test_key)
            print("[ok]   S3 write/delete test passed.")
        except ClientError as exc:
            print(f"[fail] S3 write test: {exc}")
            ok = False

        # DynamoDB write test
        test_id = f"__healthcheck_{uuid.uuid4()}"
        try:
            table.put_item(Item={"run_id": test_id, "healthcheck": "true"})
            table.delete_item(Key={"run_id": test_id})
            print("[ok]   DynamoDB write/delete test passed.")
        except ClientError as exc:
            print(f"[fail] DynamoDB write test: {exc}")
            ok = False

    return ok


def main() -> None:
    parser = argparse.ArgumentParser(description="Check AWS readiness for designops-eval.")
    parser.add_argument("--s3-bucket", required=True, help="S3 bucket name.")
    parser.add_argument("--dynamodb-table", required=True, help="DynamoDB table name.")
    parser.add_argument("--write-test", action="store_true", help="Perform write+delete test.")
    args = parser.parse_args()

    success = check(args.s3_bucket, args.dynamodb_table, args.write_test)
    sys.exit(0 if success else 1)


if __name__ == "__main__":
    main()
