"""Per-case result persistence in DynamoDB.

Table schema (infra/terraform/distributed.tf):

    partition key  run_id      (S)
    sort key       result_key  (S)   sharding.result_key(model, config_version, case_id)

One item is one committed result. An item is terminal: status "completed"
holds a scored response, status "failed" holds a permanent failure. Rate
limits, timeouts and other retryable errors are never written here.
"""

from datetime import datetime, timezone
from typing import Any

from boto3.dynamodb.conditions import Attr, Key
from botocore.exceptions import ClientError
from pydantic import BaseModel

from ..models.results import TraceEntry
from .sharding import result_key, result_key_prefix

STATUS_COMPLETED = "completed"
STATUS_FAILED = "failed"


class ResultRecord(BaseModel):
    run_id: str
    model: str
    provider: str
    config_version: str
    case_id: str
    shard_id: str
    status: str
    trace: TraceEntry
    worker_id: str
    committed_at: str

    @classmethod
    def from_trace(
        cls,
        *,
        run_id: str,
        model: str,
        provider: str,
        config_version: str,
        shard_id: str,
        trace: TraceEntry,
        worker_id: str,
    ) -> "ResultRecord":
        return cls(
            run_id=run_id,
            model=model,
            provider=provider,
            config_version=config_version,
            case_id=trace.prompt_id,
            shard_id=shard_id,
            status=STATUS_FAILED if trace.status == "failed" else STATUS_COMPLETED,
            trace=trace,
            worker_id=worker_id,
            committed_at=datetime.now(timezone.utc).isoformat(),
        )

    def key(self) -> dict[str, str]:
        return {
            "run_id": self.run_id,
            "result_key": result_key(self.model, self.config_version, self.case_id),
        }

    def to_item(self) -> dict[str, Any]:
        """Serialise to a DynamoDB item.

        The trace is stored as a JSON string because it contains floats, which
        the DynamoDB resource API rejects.
        """
        return {
            **self.key(),
            "model": self.model,
            "provider": self.provider,
            "config_version": self.config_version,
            "case_id": self.case_id,
            "shard_id": self.shard_id,
            "status": self.status,
            "trace_json": self.trace.model_dump_json(),
            "worker_id": self.worker_id,
            "committed_at": self.committed_at,
        }

    @classmethod
    def from_item(cls, item: dict[str, Any]) -> "ResultRecord":
        return cls(
            run_id=item["run_id"],
            model=item["model"],
            provider=item["provider"],
            config_version=item["config_version"],
            case_id=item["case_id"],
            shard_id=item["shard_id"],
            status=item["status"],
            trace=TraceEntry.model_validate_json(item["trace_json"]),
            worker_id=item["worker_id"],
            committed_at=item["committed_at"],
        )


class ResultsStore:
    def __init__(self, table) -> None:
        """table is a boto3 DynamoDB Table resource."""
        self._table = table

    def put_result_if_absent(self, record: ResultRecord) -> bool:
        """Commit a result unless one already exists for the same case.

        Returns True if this call wrote the item, False if an item with the
        same key was already present, in which case the stored item is left
        untouched: the first committed result wins.

        This is one conditional write, not a read followed by a write. Two
        workers can hold the same shard at once (a visibility timeout lapsed,
        or the message was delivered twice), and a read-then-write would let
        both pass the check before either wrote.

        Only the conditional-check failure becomes False. Any other error
        propagates, so a failed write is never mistaken for a duplicate.
        """
        try:
            self._table.put_item(
                Item=record.to_item(),
                ConditionExpression=Attr("run_id").not_exists(),
            )
            return True
        except ClientError as exc:
            if exc.response["Error"]["Code"] == "ConditionalCheckFailedException":
                return False
            raise

    def get_committed_case_ids(self, run_id: str, model: str, config_version: str) -> set[str]:
        """Return the case ids that already have a committed result.

        Completed and failed items both count; each is terminal. Reads are
        strongly consistent: an eventually consistent read could miss a write
        that finished a moment earlier and cost a repeated provider call. It
        could not cause a duplicate result, which the conditional write
        prevents regardless.
        """
        kwargs: dict[str, Any] = {
            "KeyConditionExpression": (
                Key("run_id").eq(run_id)
                & Key("result_key").begins_with(result_key_prefix(model, config_version))
            ),
            "ProjectionExpression": "case_id",
            "ConsistentRead": True,
        }
        committed: set[str] = set()
        while True:
            page = self._table.query(**kwargs)
            committed.update(item["case_id"] for item in page["Items"])
            last = page.get("LastEvaluatedKey")
            if not last:
                return committed
            kwargs["ExclusiveStartKey"] = last

    def list_results(self, run_id: str) -> list[ResultRecord]:
        """Return every committed result for a run, across all models."""
        records: list[ResultRecord] = []
        kwargs: dict[str, Any] = {
            "KeyConditionExpression": Key("run_id").eq(run_id),
            "ConsistentRead": True,
        }
        while True:
            page = self._table.query(**kwargs)
            records.extend(ResultRecord.from_item(item) for item in page["Items"])
            last = page.get("LastEvaluatedKey")
            if not last:
                return records
            kwargs["ExclusiveStartKey"] = last

    def delete_failed_results(self, run_id: str) -> int:
        """Delete this run's failed results so that resubmission retries them.

        Each delete is conditional on the item still being failed, so a result
        that is completed is never removed. Returns the number deleted.
        """
        deleted = 0
        for record in self.list_results(run_id):
            if record.status != STATUS_FAILED:
                continue
            try:
                self._table.delete_item(
                    Key=record.key(),
                    ConditionExpression=Attr("status").eq(STATUS_FAILED),
                )
                deleted += 1
            except ClientError as exc:
                if exc.response["Error"]["Code"] != "ConditionalCheckFailedException":
                    raise
        return deleted
