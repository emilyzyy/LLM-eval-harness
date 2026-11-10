import time

import pytest

from designops_eval.distributed.queue import VisibilityHeartbeat
from designops_eval.distributed.sharding import MalformedShardError, Shard
from tests.helpers import manual_shard


def shard(n: int = 0) -> Shard:
    return manual_shard("evals/x.yaml", [f"c{n}a", f"c{n}b"], shard_id=f"shard-{n}")


class TestShardMessage:
    def test_round_trip(self):
        original = shard()
        assert Shard.from_message(original.to_message()) == original

    def test_serialisation_is_deterministic(self):
        assert shard().to_message() == shard().to_message()

    @pytest.mark.parametrize("body", [
        "not json",
        "[]",
        "{}",
        '{"schema_version": 99}',
        shard().to_message().replace('"run-1"', '""'),
        shard().to_message().replace('["c0a", "c0b"]', "[]"),
        shard().to_message().replace('["c0a", "c0b"]', '"c0a"'),
    ])
    def test_malformed_bodies_raise(self, body):
        with pytest.raises(MalformedShardError):
            Shard.from_message(body)


class TestShardQueue:
    def test_publish_and_receive(self, make_queue):
        queue, _ = make_queue()
        assert queue.publish([shard()]) == 1
        message = queue.receive(wait_seconds=0)
        assert Shard.from_message(message.body) == shard()
        assert message.receive_count == 1

    def test_publish_batches_more_than_ten(self, make_queue):
        queue, _ = make_queue()
        assert queue.publish(shard(i) for i in range(23)) == 23
        received = set()
        while (message := queue.receive(wait_seconds=0)) is not None:
            received.add(Shard.from_message(message.body).shard_id)
            queue.delete(message)
        assert received == {f"shard-{i}" for i in range(23)}

    def test_receive_returns_none_when_empty(self, make_queue):
        queue, _ = make_queue()
        assert queue.receive(wait_seconds=0) is None

    def test_received_message_is_invisible_until_released(self, make_queue):
        queue, _ = make_queue(visibility_timeout=30)
        queue.publish([shard()])
        message = queue.receive(wait_seconds=0)
        assert queue.receive(wait_seconds=0) is None

        queue.set_visibility(message, 0)
        again = queue.receive(wait_seconds=0)
        assert again.message_id == message.message_id
        assert again.receive_count == 2

    def test_deleted_message_is_not_redelivered(self, make_queue):
        queue, _ = make_queue(visibility_timeout=1)
        queue.publish([shard()])
        queue.delete(queue.receive(wait_seconds=0))
        time.sleep(1.2)
        assert queue.receive(wait_seconds=0) is None

    def test_undeleted_message_reappears_after_visibility_timeout(self, make_queue):
        queue, _ = make_queue(visibility_timeout=1)
        queue.publish([shard()])
        queue.receive(wait_seconds=0)
        time.sleep(1.2)
        assert queue.receive(wait_seconds=0).receive_count == 2

    def test_message_moves_to_dlq_after_max_receive_count(self, make_queue):
        queue, dlq = make_queue(visibility_timeout=30, max_receive_count=2)
        queue.publish([shard()])
        for _ in range(2):
            queue.set_visibility(queue.receive(wait_seconds=0), 0)

        assert queue.receive(wait_seconds=0) is None
        dead = dlq.receive(wait_seconds=0)
        assert Shard.from_message(dead.body) == shard()


class TestVisibilityHeartbeat:
    def test_keeps_message_invisible_beyond_original_timeout(self, make_queue):
        queue, _ = make_queue(visibility_timeout=1)
        queue.publish([shard()])
        message = queue.receive(wait_seconds=0)

        with VisibilityHeartbeat(queue, message, interval=0.3, extend_to=1) as heartbeat:
            time.sleep(1.6)
            assert queue.receive(wait_seconds=0) is None
        assert heartbeat.extensions >= 3
        assert not heartbeat.lost

    def test_message_reappears_after_heartbeat_stops(self, make_queue):
        queue, _ = make_queue(visibility_timeout=1)
        queue.publish([shard()])
        message = queue.receive(wait_seconds=0)
        with VisibilityHeartbeat(queue, message, interval=0.3, extend_to=1):
            time.sleep(0.4)
        time.sleep(1.2)
        assert queue.receive(wait_seconds=0) is not None

    def test_failed_extension_marks_heartbeat_lost(self, make_queue):
        queue, _ = make_queue(visibility_timeout=30)
        queue.publish([shard()])
        message = queue.receive(wait_seconds=0)
        errors = []
        with VisibilityHeartbeat(queue, message, interval=0.1, extend_to=1,
                                 on_error=errors.append) as heartbeat:
            queue.delete(message)  # the receipt handle is now invalid
            time.sleep(0.5)
        assert heartbeat.lost
        assert len(errors) == 1

    def test_interval_must_be_shorter_than_extension(self, make_queue):
        queue, _ = make_queue()
        with pytest.raises(ValueError):
            VisibilityHeartbeat(queue, None, interval=5, extend_to=5)
