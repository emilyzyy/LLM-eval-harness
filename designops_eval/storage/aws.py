import json

import boto3

from .base import StorageBackend
from .local import LocalStorage
from ..models.config import AwsConfig
from ..models.results import RunResult


def make_s3_client(region_name: str, endpoint_url: str | None):
    return boto3.client(
        "s3",
        region_name=region_name,
        endpoint_url=endpoint_url,
    )


def make_dynamodb_resource(region_name: str, endpoint_url: str | None):
    return boto3.resource(
        "dynamodb",
        region_name=region_name,
        endpoint_url=endpoint_url,
    )


class AwsStorage(StorageBackend):
    def __init__(self, config: AwsConfig) -> None:
        self.config = config
        self._s3 = make_s3_client(config.region_name, config.endpoint_url)
        self._dynamo = make_dynamodb_resource(config.region_name, config.endpoint_url)

    def save_run(self, result: RunResult, report_md: str, output_dir: str) -> None:
        prefix = f"runs/{result.run_id}"

        self._s3.put_object(
            Bucket=self.config.s3_bucket,
            Key=f"{prefix}/latest.json",
            Body=result.model_dump_json(indent=2, exclude={"traces"}).encode(),
            ContentType="application/json",
        )
        traces_payload = {"traces": [t.model_dump() for t in result.traces]}
        self._s3.put_object(
            Bucket=self.config.s3_bucket,
            Key=f"{prefix}/traces.json",
            Body=json.dumps(traces_payload, indent=2).encode(),
            ContentType="application/json",
        )
        self._s3.put_object(
            Bucket=self.config.s3_bucket,
            Key=f"{prefix}/report.md",
            Body=report_md.encode(),
            ContentType="text/markdown",
        )

        table = self._dynamo.Table(self.config.dynamodb_table)
        scores = {
            k: str(v)
            for k, v in result.scores.model_dump().items()
            if v is not None
        }
        table.put_item(Item={
            "run_id": result.run_id,
            "model": result.model,
            "timestamp": result.timestamp,
            "prompt_set": result.prompt_set,
            "s3_prefix": f"s3://{self.config.s3_bucket}/{prefix}",
            "regressions": result.regressions,
            **scores,
        })

        # Also persist locally so the output_dir always has artifacts
        LocalStorage().save_run(result, report_md, output_dir)
