import pytest

from designops_eval.models.config import AwsConfig, _resolve_region


class TestResolveRegion:
    def test_explicit_wins_over_everything(self, monkeypatch):
        monkeypatch.setenv("AWS_REGION", "eu-west-1")
        monkeypatch.setenv("AWS_DEFAULT_REGION", "ap-southeast-1")
        assert _resolve_region("us-east-2") == "us-east-2"

    def test_aws_region_env_used_when_no_explicit(self, monkeypatch):
        monkeypatch.setenv("AWS_REGION", "us-east-2")
        monkeypatch.delenv("AWS_DEFAULT_REGION", raising=False)
        assert _resolve_region() == "us-east-2"

    def test_aws_default_region_used_as_fallback(self, monkeypatch):
        monkeypatch.delenv("AWS_REGION", raising=False)
        monkeypatch.setenv("AWS_DEFAULT_REGION", "us-east-2")
        assert _resolve_region() == "us-east-2"

    def test_aws_region_beats_aws_default_region(self, monkeypatch):
        monkeypatch.setenv("AWS_REGION", "us-east-2")
        monkeypatch.setenv("AWS_DEFAULT_REGION", "eu-west-1")
        assert _resolve_region() == "us-east-2"

    def test_final_fallback_is_us_east_1(self, monkeypatch):
        monkeypatch.delenv("AWS_REGION", raising=False)
        monkeypatch.delenv("AWS_DEFAULT_REGION", raising=False)
        assert _resolve_region() == "us-east-1"

    def test_explicit_none_falls_through_to_env(self, monkeypatch):
        monkeypatch.setenv("AWS_REGION", "us-east-2")
        assert _resolve_region(None) == "us-east-2"


class TestAwsConfigFromEnv:
    def test_picks_up_aws_region(self, monkeypatch):
        monkeypatch.setenv("AWS_REGION", "us-east-2")
        monkeypatch.delenv("AWS_DEFAULT_REGION", raising=False)
        monkeypatch.delenv("AWS_ENDPOINT_URL", raising=False)
        cfg = AwsConfig.from_env(s3_bucket="b", dynamodb_table="t")
        assert cfg.region_name == "us-east-2"

    def test_picks_up_aws_default_region(self, monkeypatch):
        monkeypatch.delenv("AWS_REGION", raising=False)
        monkeypatch.setenv("AWS_DEFAULT_REGION", "us-east-2")
        monkeypatch.delenv("AWS_ENDPOINT_URL", raising=False)
        cfg = AwsConfig.from_env(s3_bucket="b", dynamodb_table="t")
        assert cfg.region_name == "us-east-2"

    def test_explicit_region_arg_overrides_env(self, monkeypatch):
        monkeypatch.setenv("AWS_REGION", "eu-west-1")
        monkeypatch.setenv("AWS_DEFAULT_REGION", "ap-southeast-1")
        monkeypatch.delenv("AWS_ENDPOINT_URL", raising=False)
        cfg = AwsConfig.from_env(s3_bucket="b", dynamodb_table="t", region="us-east-2")
        assert cfg.region_name == "us-east-2"

    def test_defaults_to_us_east_1_when_no_env(self, monkeypatch):
        monkeypatch.delenv("AWS_REGION", raising=False)
        monkeypatch.delenv("AWS_DEFAULT_REGION", raising=False)
        monkeypatch.delenv("AWS_ENDPOINT_URL", raising=False)
        cfg = AwsConfig.from_env(s3_bucket="b", dynamodb_table="t")
        assert cfg.region_name == "us-east-1"

    def test_endpoint_url_still_read_from_env(self, monkeypatch):
        monkeypatch.delenv("AWS_REGION", raising=False)
        monkeypatch.delenv("AWS_DEFAULT_REGION", raising=False)
        monkeypatch.setenv("AWS_ENDPOINT_URL", "http://localhost:4566")
        cfg = AwsConfig.from_env(s3_bucket="b", dynamodb_table="t")
        assert cfg.endpoint_url == "http://localhost:4566"

    def test_localstack_region_preserved(self, monkeypatch):
        """LocalStack setup exports AWS_DEFAULT_REGION=us-east-1 — should still work."""
        monkeypatch.delenv("AWS_REGION", raising=False)
        monkeypatch.setenv("AWS_DEFAULT_REGION", "us-east-1")
        monkeypatch.setenv("AWS_ENDPOINT_URL", "http://localhost:4566")
        cfg = AwsConfig.from_env(s3_bucket="b", dynamodb_table="t")
        assert cfg.region_name == "us-east-1"
        assert cfg.endpoint_url == "http://localhost:4566"
