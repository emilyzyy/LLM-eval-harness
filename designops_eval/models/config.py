import os
from pydantic import BaseModel


class AwsConfig(BaseModel):
    region_name: str = "us-east-1"
    endpoint_url: str | None = None
    s3_bucket: str
    dynamodb_table: str

    @classmethod
    def from_env(cls, s3_bucket: str, dynamodb_table: str) -> "AwsConfig":
        return cls(
            endpoint_url=os.getenv("AWS_ENDPOINT_URL"),
            s3_bucket=s3_bucket,
            dynamodb_table=dynamodb_table,
        )


class RunConfig(BaseModel):
    prompt_set_path: str
    model: str = "mock"
    baseline_path: str | None = None
    output_dir: str = "results/run"
    storage_mode: str = "local"
    aws_config: AwsConfig | None = None
