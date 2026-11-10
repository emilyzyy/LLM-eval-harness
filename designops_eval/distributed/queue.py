"""SQS access for shard messages, and the visibility-timeout heartbeat."""

import threading
from dataclasses import dataclass
from typing import Callable, Iterable

import boto3

from .sharding import Shard


def make_sqs_client(region_name: str, endpoint_url: str | None):
    return boto3.client("sqs", region_name=region_name, endpoint_url=endpoint_url)


@dataclass(frozen=True)
class ReceivedMessage:
    message_id: str
    receipt_handle: str
    body: str
    # How many times SQS has delivered this message, this delivery included.
    receive_count: int


class ShardQueue:
    def __init__(self, sqs_client, queue_url: str) -> None:
        self._sqs = sqs_client
        self.queue_url = queue_url

    def publish(self, shards: Iterable[Shard]) -> int:
        """Send one message per shard. Returns the number sent."""
        batch: list[dict] = []
        sent = 0
        for shard in shards:
            batch.append({"Id": str(len(batch)), "MessageBody": shard.to_message()})
            if len(batch) == 10:
                sent += self._send_batch(batch)
                batch = []
        if batch:
            sent += self._send_batch(batch)
        return sent

    def _send_batch(self, entries: list[dict]) -> int:
        response = self._sqs.send_message_batch(QueueUrl=self.queue_url, Entries=entries)
        failed = response.get("Failed", [])
        if failed:
            raise RuntimeError(f"SQS rejected {len(failed)} of {len(entries)} messages: {failed}")
        return len(entries)

    def receive(self, wait_seconds: int = 10) -> ReceivedMessage | None:
        """Long-poll for one message. Returns None when the poll times out."""
        response = self._sqs.receive_message(
            QueueUrl=self.queue_url,
            MaxNumberOfMessages=1,
            WaitTimeSeconds=wait_seconds,
            AttributeNames=["ApproximateReceiveCount"],
        )
        messages = response.get("Messages", [])
        if not messages:
            return None
        raw = messages[0]
        return ReceivedMessage(
            message_id=raw["MessageId"],
            receipt_handle=raw["ReceiptHandle"],
            body=raw["Body"],
            receive_count=int(raw.get("Attributes", {}).get("ApproximateReceiveCount", "1")),
        )

    def delete(self, message: ReceivedMessage) -> None:
        self._sqs.delete_message(QueueUrl=self.queue_url, ReceiptHandle=message.receipt_handle)

    def set_visibility(self, message: ReceivedMessage, seconds: int) -> None:
        """Make the message invisible for `seconds` from now. 0 releases it immediately."""
        self._sqs.change_message_visibility(
            QueueUrl=self.queue_url,
            ReceiptHandle=message.receipt_handle,
            VisibilityTimeout=seconds,
        )


class VisibilityHeartbeat:
    """Keeps a message invisible while its shard is being processed.

    A background thread resets the visibility timeout to `extend_to` seconds
    every `interval` seconds. If the worker process dies the heartbeat dies
    with it, and the message reappears at most `extend_to` seconds later.
    That bound is the crash-recovery delay.

    If an extension fails (for example the receipt handle expired because
    this worker stalled), the heartbeat stops and `lost` becomes True.
    Another worker may then hold the same shard. Result writes are
    conditional, so this cannot duplicate a committed result.
    """

    def __init__(
        self,
        queue: ShardQueue,
        message: ReceivedMessage,
        interval: float,
        extend_to: int,
        on_error: Callable[[Exception], None] | None = None,
    ) -> None:
        if interval >= extend_to:
            raise ValueError("interval must be shorter than extend_to")
        self._queue = queue
        self._message = message
        self._interval = interval
        self._extend_to = extend_to
        self._on_error = on_error
        self._stop = threading.Event()
        self._thread = threading.Thread(target=self._run, name="visibility-heartbeat", daemon=True)
        self.lost = False
        self.extensions = 0

    def _run(self) -> None:
        while not self._stop.wait(self._interval):
            try:
                self._queue.set_visibility(self._message, self._extend_to)
                self.extensions += 1
            except Exception as exc:
                self.lost = True
                if self._on_error is not None:
                    self._on_error(exc)
                return

    def __enter__(self) -> "VisibilityHeartbeat":
        self._queue.set_visibility(self._message, self._extend_to)
        self._thread.start()
        return self

    def __exit__(self, *exc_info) -> None:
        self._stop.set()
        self._thread.join(timeout=self._interval + 5)
