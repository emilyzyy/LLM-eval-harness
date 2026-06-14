import json
from pathlib import Path

import boto3
import pytest
from moto import mock_aws

from designops_eval.models.config import AwsConfig
from designops_eval.models.results import RunResult, RunScores, TraceEntry
from designops_eval.storage.aws import AwsStorage
from designops_eval.storage.local import LocalStorage


def test_local_storage_creates_files(tmp_path, sample_result):
    LocalStorage().save_run(sample_result, "# Report\n", str(tmp_path))

    assert (tmp_path / "latest.json").exists()
    assert (tmp_path / "traces.json").exists()
    assert (tmp_path / "report.md").exists()


def test_local_storage_latest_json_content(tmp_path, sample_result):
    LocalStorage().save_run(sample_result, "# Report\n", str(tmp_path))

    data = json.loads((tmp_path / "latest.json").read_text())
    assert data["run_id"] == "test-run-id"
    assert data["model"] == "mock"
    assert "traces" not in data  # traces go to traces.json


def test_local_storage_traces_json_content(tmp_path, sample_result):
    LocalStorage().save_run(sample_result, "# Report\n", str(tmp_path))

    data = json.loads((tmp_path / "traces.json").read_text())
    assert "traces" in data
    assert data["traces"][0]["prompt_id"] == "bolt_spec_001"


def test_local_storage_report_md_content(tmp_path, sample_result):
    LocalStorage().save_run(sample_result, "# Report\n", str(tmp_path))

    content = (tmp_path / "report.md").read_text()
    assert "# Report" in content


@mock_aws
def test_aws_storage_uploads_to_s3(tmp_path, sample_result):
    s3 = boto3.client("s3", region_name="us-east-1")
    s3.create_bucket(Bucket="test-bucket")

    dynamodb = boto3.resource("dynamodb", region_name="us-east-1")
    dynamodb.create_table(
        TableName="test-table",
        AttributeDefinitions=[{"AttributeName": "run_id", "AttributeType": "S"}],
        KeySchema=[{"AttributeName": "run_id", "KeyType": "HASH"}],
        BillingMode="PAY_PER_REQUEST",
    )

    config = AwsConfig(s3_bucket="test-bucket", dynamodb_table="test-table")
    AwsStorage(config).save_run(sample_result, "# Report\n", str(tmp_path))

    response = s3.get_object(Bucket="test-bucket", Key="runs/test-run-id/latest.json")
    data = json.loads(response["Body"].read())
    assert data["run_id"] == "test-run-id"

    response = s3.get_object(Bucket="test-bucket", Key="runs/test-run-id/traces.json")
    traces = json.loads(response["Body"].read())
    assert "traces" in traces

    response = s3.get_object(Bucket="test-bucket", Key="runs/test-run-id/report.md")
    assert b"Report" in response["Body"].read()


@mock_aws
def test_aws_storage_writes_dynamodb_row(tmp_path, sample_result):
    boto3.client("s3", region_name="us-east-1").create_bucket(Bucket="test-bucket")

    dynamodb = boto3.resource("dynamodb", region_name="us-east-1")
    dynamodb.create_table(
        TableName="test-table",
        AttributeDefinitions=[{"AttributeName": "run_id", "AttributeType": "S"}],
        KeySchema=[{"AttributeName": "run_id", "KeyType": "HASH"}],
        BillingMode="PAY_PER_REQUEST",
    )

    config = AwsConfig(s3_bucket="test-bucket", dynamodb_table="test-table")
    AwsStorage(config).save_run(sample_result, "# Report\n", str(tmp_path))

    table = dynamodb.Table("test-table")
    item = table.get_item(Key={"run_id": "test-run-id"})["Item"]
    assert item["model"] == "mock"
    assert item["prompt_set"] == "combined_eval"
    assert "s3_prefix" in item


@mock_aws
def test_aws_storage_also_writes_locally(tmp_path, sample_result):
    boto3.client("s3", region_name="us-east-1").create_bucket(Bucket="test-bucket")

    dynamodb = boto3.resource("dynamodb", region_name="us-east-1")
    dynamodb.create_table(
        TableName="test-table",
        AttributeDefinitions=[{"AttributeName": "run_id", "AttributeType": "S"}],
        KeySchema=[{"AttributeName": "run_id", "KeyType": "HASH"}],
        BillingMode="PAY_PER_REQUEST",
    )

    config = AwsConfig(s3_bucket="test-bucket", dynamodb_table="test-table")
    AwsStorage(config).save_run(sample_result, "# Report\n", str(tmp_path))

    assert (tmp_path / "latest.json").exists()
    assert (tmp_path / "traces.json").exists()
    assert (tmp_path / "report.md").exists()
