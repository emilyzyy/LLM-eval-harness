"""Decode a capture record written by the Go gateway, using the Python code
generated from the same schema.

testdata/capture_record.bin is produced by the Go test in
gateway/internal/capture/fixture_test.go. The values checked here are the ones
that test writes.

Run from the repository root:

    pip install "protobuf>=7.36.2" pytest
    pytest gateway/schema_test
"""

import json
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

HERE = Path(__file__).parent
sys.path.insert(0, str(HERE.parent / "gen" / "python"))

import capture_pb2  # noqa: E402


def load_fixture() -> "capture_pb2.CaptureRecord":
    record = capture_pb2.CaptureRecord()
    record.ParseFromString((HERE / "testdata" / "capture_record.bin").read_bytes())
    return record


def test_scalar_fields_decode():
    record = load_fixture()
    assert record.request_id == "0195c2a0-7b1e-7c3a-9f00-0123456789ab"
    assert record.upstream == "fake"
    assert record.model == "fake-model"
    assert record.stream is True
    assert record.status_code == 200
    assert record.attempts == 2
    assert record.request_truncated is False
    assert record.response_truncated is True


def test_bodies_decode_as_the_original_bytes():
    record = load_fixture()
    request = json.loads(record.request_body)
    assert request["model"] == "fake-model"
    # Non-ASCII text survives the round trip.
    assert request["messages"][0]["content"] == "héllo"
    assert record.response_body.endswith(b"data: [DONE]\n\n")


def test_enum_and_nested_message_decode():
    record = load_fixture()
    assert record.outcome == capture_pb2.OUTCOME_COMPLETED
    assert capture_pb2.Outcome.Name(record.outcome) == "OUTCOME_COMPLETED"
    assert record.HasField("usage")
    assert (record.usage.prompt_tokens, record.usage.completion_tokens, record.usage.total_tokens) == (11, 22, 33)


def test_timestamps_and_durations_decode():
    record = load_fixture()
    received = record.received_at.ToDatetime(tzinfo=timezone.utc)
    assert received == datetime(2026, 1, 2, 3, 4, 5, 6000, tzinfo=timezone.utc)
    assert record.completed_at.ToDatetime(tzinfo=timezone.utc) - received == timedelta(milliseconds=1250)
    assert record.upstream_latency.ToTimedelta() == timedelta(milliseconds=1200)
    assert record.total_latency.ToTimedelta() == timedelta(milliseconds=1250)


def test_reencoding_gives_an_equal_record():
    record = load_fixture()
    again = capture_pb2.CaptureRecord()
    again.ParseFromString(record.SerializeToString())
    assert again == record
