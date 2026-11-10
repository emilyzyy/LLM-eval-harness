"""Local multi-process environment for benchmarks and fault injection.

Starts an AWS emulation (moto in server mode) and the fake provider HTTP
server inside this process, creates the queue and tables, and launches real
worker processes against them. Nothing here touches real AWS or a paid API.
"""

import json
import os
import socket
import subprocess
import sys
import threading
import time
import urllib.request
from pathlib import Path

import boto3

from designops_eval.distributed.results_store import ResultRecord, ResultsStore
from designops_eval.distributed.schema import (
    create_queues,
    create_rate_limit_table,
    create_results_table,
)
from designops_eval.providers.fake import FakeProviderConfig, make_server

REPO_ROOT = Path(__file__).resolve().parent.parent
REGION = "us-east-1"
RESULTS_TABLE = "bench-results"
RATE_LIMIT_TABLE = "bench-rate-limits"


def _free_port() -> int:
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


class LocalEnvironment:
    """Context manager owning the emulated AWS endpoint and the fake provider."""

    def __init__(
        self,
        provider_config: FakeProviderConfig | None = None,
        visibility_timeout: int = 30,
        max_receive_count: int = 5,
        log_dir: Path | None = None,
    ) -> None:
        self._provider_config = provider_config or FakeProviderConfig()
        self._visibility_timeout = visibility_timeout
        self._max_receive_count = max_receive_count
        self.log_dir = log_dir or REPO_ROOT / "results" / "benchmarks" / "logs"
        self.workers: list[subprocess.Popen] = []
        self._saved_env: dict[str, str | None] = {}

    def __enter__(self) -> "LocalEnvironment":
        import logging

        from moto.server import ThreadedMotoServer

        logging.getLogger("werkzeug").setLevel(logging.ERROR)
        self.log_dir.mkdir(parents=True, exist_ok=True)
        for stale in self.log_dir.glob("*.log"):
            stale.unlink()
        aws_port = _free_port()
        self._moto = ThreadedMotoServer(ip_address="127.0.0.1", port=aws_port, verbose=False)
        self._moto.start()
        self.aws_endpoint = f"http://127.0.0.1:{aws_port}"
        # moto keeps state per process, so clear what an earlier scenario left.
        urllib.request.urlopen(
            urllib.request.Request(f"{self.aws_endpoint}/moto-api/reset", data=b"", method="POST")
        )

        self._provider = make_server(port=0, default_config=self._provider_config)
        threading.Thread(target=self._provider.serve_forever, daemon=True).start()
        host, port = self._provider.server_address[:2]
        self.provider_url = f"http://{host}:{port}"

        base = {
            "AWS_ENDPOINT_URL": self.aws_endpoint,
            "AWS_ACCESS_KEY_ID": "test",
            "AWS_SECRET_ACCESS_KEY": "test",
            "AWS_DEFAULT_REGION": REGION,
            "FAKE_PROVIDER_URL": self.provider_url,
        }
        self._set_env(base)
        os.environ.pop("AWS_PROFILE", None)

        self.sqs = boto3.client("sqs", region_name=REGION, endpoint_url=self.aws_endpoint)
        self.dynamodb = boto3.resource("dynamodb", region_name=REGION, endpoint_url=self.aws_endpoint)
        self.queue_url, self.dlq_url = create_queues(
            self.sqs, "bench-shards", self._visibility_timeout, self._max_receive_count
        )
        create_results_table(self.dynamodb, RESULTS_TABLE)
        create_rate_limit_table(self.dynamodb, RATE_LIMIT_TABLE)
        self.store = ResultsStore(self.dynamodb.Table(RESULTS_TABLE))
        self._set_env({
            "DESIGNOPS_QUEUE_URL": self.queue_url,
            "DESIGNOPS_RESULTS_TABLE": RESULTS_TABLE,
            "DESIGNOPS_RATE_LIMIT_TABLE": RATE_LIMIT_TABLE,
            "DESIGNOPS_VISIBILITY_TIMEOUT": str(self._visibility_timeout),
        })
        return self

    def __exit__(self, *exc_info) -> None:
        for proc in self.workers:
            if proc.poll() is None:
                proc.kill()
                proc.wait()
        self._provider.shutdown()
        self._provider.server_close()
        self._moto.stop()
        for key, value in self._saved_env.items():
            if value is None:
                os.environ.pop(key, None)
            else:
                os.environ[key] = value

    def _set_env(self, values: dict[str, str]) -> None:
        for key, value in values.items():
            self._saved_env.setdefault(key, os.environ.get(key))
            os.environ[key] = value

    # -- actions -----------------------------------------------------------

    def cli(self, *args: str) -> subprocess.CompletedProcess:
        proc = subprocess.run(
            [sys.executable, "-m", "designops_eval.cli", *args],
            cwd=REPO_ROOT, capture_output=True, text=True,
        )
        if proc.returncode != 0:
            last_line = (proc.stderr.strip().splitlines() or ["no output"])[-1]
            raise RuntimeError(f"`designops-eval {args[0]}` failed: {last_line}")
        return proc

    def submit(self, run_id: str, prompt_set: str, models: list[str], shard_size: int = 25,
               *extra: str) -> None:
        self.cli("submit", "--run-id", run_id, "--prompt-set", prompt_set,
                 "--models", ",".join(models), "--shard-size", str(shard_size), *extra)

    def spawn_worker(self, name: str, *args: str, env: dict[str, str] | None = None,
                     idle_exit_seconds: float = 3.0) -> subprocess.Popen:
        log = open(self.log_dir / f"{name}.log", "w")
        proc = subprocess.Popen(
            [sys.executable, "-m", "designops_eval.cli", "worker",
             "--worker-id", name, "--wait-seconds", "1",
             "--idle-exit-seconds", str(idle_exit_seconds), *args],
            cwd=REPO_ROOT, stdout=log, stderr=subprocess.STDOUT,
            env={**os.environ, **(env or {})},
        )
        self.workers.append(proc)
        return proc

    # -- observations ------------------------------------------------------

    def results(self, run_id: str) -> list[ResultRecord]:
        return self.store.list_results(run_id)

    def wait_for_results(self, run_id: str, count: int, timeout: float, poll: float = 0.05) -> float:
        """Block until the run has `count` committed results. Returns the time it was seen."""
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            if len(self.results(run_id)) >= count:
                return time.monotonic()
            crashed = [p for p in self.workers if p.poll() not in (None, 0, 137)]
            if crashed:
                raise RuntimeError(
                    f"a worker exited with code {crashed[0].returncode}; see {self.log_dir}"
                )
            time.sleep(poll)
        raise TimeoutError(
            f"run {run_id}: {len(self.results(run_id))}/{count} results after {timeout}s; "
            f"worker logs are in {self.log_dir}"
        )

    def wait_for_workers(self, timeout: float = 60.0) -> None:
        for proc in self.workers:
            proc.wait(timeout=timeout)

    def provider_stats(self) -> dict[str, dict]:
        return self._provider.registry.stats()

    def queue_depth(self, url: str | None = None) -> int:
        attributes = self.sqs.get_queue_attributes(
            QueueUrl=url or self.queue_url,
            AttributeNames=["ApproximateNumberOfMessages", "ApproximateNumberOfMessagesNotVisible"],
        )["Attributes"]
        return sum(int(v) for v in attributes.values())

    def worker_events(self, event: str) -> list[dict]:
        """Collect JSON log lines with the given event name from every worker log."""
        found = []
        for path in sorted(self.log_dir.glob("*.log")):
            for line in path.read_text().splitlines():
                try:
                    record = json.loads(line)
                except json.JSONDecodeError:
                    continue
                if record.get("event") == event:
                    found.append(record)
        return found


def duplicate_result_count(records: list[ResultRecord]) -> int:
    identities = [(r.model, r.config_version, r.case_id) for r in records]
    return len(identities) - len(set(identities))


def write_report(name: str, report: dict) -> Path:
    out = REPO_ROOT / "results" / "benchmarks"
    out.mkdir(parents=True, exist_ok=True)
    path = out / f"{name}.json"
    path.write_text(json.dumps(report, indent=2), encoding="utf-8")
    return path
