"""Deterministic fake LLM provider for development, benchmarks and fault injection.

Any model name starting with "fake-" is served here, and each distinct name is
an independent provider with its own quota (fake-a, fake-b, ...).

Two modes share one implementation (FakeProviderCore):

- In-process (default): state lives in the calling process. Suitable for unit
  tests and single-process baselines.
- HTTP (FAKE_PROVIDER_URL set): calls go to a server started with
  `python -m designops_eval.providers.fake`. Several worker processes then hit
  one shared quota, which is what a real provider enforces.

Behaviour is a pure function of (seed, provider, prompt id, attempt number),
so runs are reproducible regardless of request ordering.
"""

import argparse
import hashlib
import json
import math
import os
import socket
import threading
import time
import urllib.error
import urllib.request
from collections import deque
from dataclasses import asdict, dataclass
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Callable

from .base import ModelResponse
from .errors import ProviderError
from .retry import call_with_retries

FAKE_MODEL_PREFIX = "fake-"

# A prompt containing this marker always gets HTTP 400, to exercise the
# permanent-failure path.
PERMANENT_ERROR_MARKER = "[[fake:permanent-error]]"


@dataclass(frozen=True)
class FakeProviderConfig:
    latency_ms: float = 0.0
    # Probability that an attempt returns HTTP 503. Decided deterministically
    # per (seed, provider, prompt id, attempt number).
    failure_rate: float = 0.0
    # Maximum requests accepted in any sliding window of window_seconds.
    # None disables rate limiting. 600 with a 60 s window is 600 RPM.
    rate_limit: int | None = None
    window_seconds: float = 60.0
    seed: int = 0

    @classmethod
    def from_env(cls) -> "FakeProviderConfig":
        limit = os.getenv("FAKE_PROVIDER_RATE_LIMIT")
        return cls(
            latency_ms=float(os.getenv("FAKE_PROVIDER_LATENCY_MS", "0")),
            failure_rate=float(os.getenv("FAKE_PROVIDER_FAILURE_RATE", "0")),
            rate_limit=int(limit) if limit else None,
            window_seconds=float(os.getenv("FAKE_PROVIDER_WINDOW_SECONDS", "60")),
            seed=int(os.getenv("FAKE_PROVIDER_SEED", "0")),
        )


def fake_response_text(model: str, prompt_id: str, prompt_text: str, expects_json: bool) -> str:
    digest = hashlib.sha256(f"{model}|{prompt_id}|{prompt_text}".encode()).hexdigest()[:16]
    if expects_json:
        return json.dumps({"case_id": prompt_id, "model": model, "digest": digest})
    return f"{model} answer for {prompt_id} (ref {digest})."


def _unit_interval(*parts: object) -> float:
    digest = hashlib.sha256("|".join(str(p) for p in parts).encode()).digest()
    return int.from_bytes(digest[:8], "big") / 2**64


class FakeProviderCore:
    """One provider's state: its quota window, attempt counters and statistics."""

    def __init__(
        self,
        name: str,
        config: FakeProviderConfig,
        clock: Callable[[], float] = time.monotonic,
        sleep: Callable[[float], None] = time.sleep,
    ) -> None:
        self.name = name
        self.config = config
        self._clock = clock
        self._sleep = sleep
        self._lock = threading.Lock()
        self._window: deque[float] = deque()
        self._attempts: dict[str, int] = {}
        self._completions: dict[str, int] = {}
        self._stats = {
            "requests_total": 0,
            "rate_limited": 0,
            "transient_failures": 0,
            "permanent_failures": 0,
            "completed": 0,
            "max_window_count": 0,
        }

    def handle(
        self, prompt_id: str, prompt_text: str, expects_json: bool
    ) -> tuple[int, dict, dict[str, str]]:
        """Process one request. Returns (http_status, json_body, headers)."""
        cfg = self.config
        with self._lock:
            self._stats["requests_total"] += 1
            now = self._clock()

            if cfg.rate_limit is not None:
                cutoff = now - cfg.window_seconds
                while self._window and self._window[0] <= cutoff:
                    self._window.popleft()
                if len(self._window) >= cfg.rate_limit:
                    self._stats["rate_limited"] += 1
                    wait = self._window[0] + cfg.window_seconds - now
                    return (
                        429,
                        {"error": "rate limit exceeded"},
                        {"Retry-After": str(max(1, math.ceil(wait)))},
                    )
                self._window.append(now)
                self._stats["max_window_count"] = max(
                    self._stats["max_window_count"], len(self._window)
                )

            if PERMANENT_ERROR_MARKER in prompt_text:
                self._stats["permanent_failures"] += 1
                return 400, {"error": "malformed request"}, {}

            attempt = self._attempts.get(prompt_id, 0) + 1
            self._attempts[prompt_id] = attempt
            if _unit_interval(cfg.seed, self.name, prompt_id, attempt) < cfg.failure_rate:
                self._stats["transient_failures"] += 1
                return 503, {"error": "service unavailable"}, {}

        if cfg.latency_ms > 0:
            self._sleep(cfg.latency_ms / 1000)

        text = fake_response_text(self.name, prompt_id, prompt_text, expects_json)
        with self._lock:
            self._stats["completed"] += 1
            self._completions[prompt_id] = self._completions.get(prompt_id, 0) + 1
        return (
            200,
            {
                "text": text,
                "input_tokens": max(1, len(prompt_text) // 4),
                "output_tokens": max(1, len(text) // 4),
            },
            {},
        )

    def stats(self) -> dict:
        """Counters for this provider.

        duplicate_completions counts successful calls beyond the first for the
        same prompt id: the number of provider calls that at-least-once
        processing repeated.
        """
        with self._lock:
            duplicates = sum(n - 1 for n in self._completions.values() if n > 1)
            return {
                **self._stats,
                "distinct_prompts_completed": len(self._completions),
                "duplicate_completions": duplicates,
                "config": asdict(self.config),
            }


class FakeProviderRegistry:
    """Lazily creates one FakeProviderCore per provider name."""

    def __init__(
        self,
        default_config: FakeProviderConfig | None = None,
        overrides: dict[str, FakeProviderConfig] | None = None,
    ) -> None:
        self._default = default_config
        self._overrides = dict(overrides or {})
        self._cores: dict[str, FakeProviderCore] = {}
        self._lock = threading.Lock()

    def get(self, name: str) -> FakeProviderCore:
        with self._lock:
            core = self._cores.get(name)
            if core is None:
                config = self._overrides.get(name) or self._default or FakeProviderConfig.from_env()
                core = FakeProviderCore(name, config)
                self._cores[name] = core
            return core

    def stats(self) -> dict[str, dict]:
        with self._lock:
            cores = dict(self._cores)
        return {name: core.stats() for name, core in cores.items()}

    def reset(self) -> None:
        with self._lock:
            self._cores.clear()


_local_registry = FakeProviderRegistry()


def local_registry() -> FakeProviderRegistry:
    """The in-process registry used when FAKE_PROVIDER_URL is not set."""
    return _local_registry


def _raise_for_status(status: int, body: dict, headers: dict[str, str]) -> None:
    retry_after = headers.get("Retry-After")
    raise ProviderError(
        f"fake provider returned HTTP {status}: {body.get('error', '')}",
        status_code=status,
        retry_after=float(retry_after) if retry_after is not None else None,
    )


def _http_attempt(base_url: str, model: str, payload: dict) -> dict:
    request = urllib.request.Request(
        f"{base_url.rstrip('/')}/v1/{model}/chat",
        data=json.dumps(payload).encode(),
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    timeout = float(os.getenv("FAKE_PROVIDER_TIMEOUT", "30"))
    try:
        with urllib.request.urlopen(request, timeout=timeout) as response:
            return json.loads(response.read())
    except urllib.error.HTTPError as exc:
        try:
            body = json.loads(exc.read() or b"{}")
        except json.JSONDecodeError:
            body = {}
        _raise_for_status(exc.code, body, dict(exc.headers.items()))
    except urllib.error.URLError as exc:
        if isinstance(exc.reason, (socket.timeout, TimeoutError)):
            raise TimeoutError(f"fake provider timed out: {exc.reason}") from exc
        raise ConnectionError(f"fake provider unreachable: {exc.reason}") from exc
    raise AssertionError("unreachable")


def fake_attempt(model: str, prompt_id: str, prompt_text: str, expects_json: bool) -> ModelResponse:
    """Make exactly one request to the fake provider. Raises on any non-200."""
    t0 = time.perf_counter()
    base_url = os.getenv("FAKE_PROVIDER_URL")
    if base_url:
        body = _http_attempt(
            base_url,
            model,
            {"prompt_id": prompt_id, "prompt": prompt_text, "expects_json": expects_json},
        )
    else:
        status, body, headers = _local_registry.get(model).handle(
            prompt_id, prompt_text, expects_json
        )
        if status != 200:
            _raise_for_status(status, body, headers)

    return ModelResponse(
        text=body["text"],
        latency_ms=(time.perf_counter() - t0) * 1000,
        input_tokens=int(body["input_tokens"]),
        output_tokens=int(body["output_tokens"]),
        token_source="fake",
    )


def call_fake(
    model: str,
    prompt_id: str,
    prompt_text: str,
    expects_json: bool = False,
    before_attempt: Callable[[], None] | None = None,
) -> ModelResponse:
    """Call the fake provider with bounded retries on retryable errors."""
    return call_with_retries(
        lambda: fake_attempt(model, prompt_id, prompt_text, expects_json),
        before_attempt=before_attempt,
    )


def make_server(
    host: str = "127.0.0.1",
    port: int = 0,
    default_config: FakeProviderConfig | None = None,
    overrides: dict[str, FakeProviderConfig] | None = None,
) -> ThreadingHTTPServer:
    """Build the HTTP server. Port 0 picks a free port (see server.server_address).

    Routes:
        POST /v1/<provider>/chat   {"prompt_id", "prompt", "expects_json"}
        GET  /stats                counters for every provider seen so far
        POST /reset                clear all provider state
    """
    registry = FakeProviderRegistry(default_config or FakeProviderConfig.from_env(), overrides)

    class Handler(BaseHTTPRequestHandler):
        protocol_version = "HTTP/1.1"

        def _send(self, status: int, body: dict, headers: dict[str, str] | None = None) -> None:
            data = json.dumps(body).encode()
            self.send_response(status)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(data)))
            for key, value in (headers or {}).items():
                self.send_header(key, value)
            self.end_headers()
            self.wfile.write(data)

        def do_GET(self) -> None:
            if self.path == "/stats":
                self._send(200, registry.stats())
            else:
                self._send(404, {"error": "not found"})

        def do_POST(self) -> None:
            length = int(self.headers.get("Content-Length", "0"))
            raw = self.rfile.read(length) if length else b""
            parts = self.path.strip("/").split("/")
            if self.path == "/reset":
                registry.reset()
                self._send(200, {"ok": True})
                return
            if len(parts) != 3 or parts[0] != "v1" or parts[2] != "chat":
                self._send(404, {"error": "not found"})
                return
            try:
                payload = json.loads(raw)
                prompt_id = payload["prompt_id"]
                prompt = payload["prompt"]
            except (json.JSONDecodeError, KeyError, TypeError):
                self._send(400, {"error": "malformed request"})
                return
            status, body, headers = registry.get(parts[1]).handle(
                prompt_id, prompt, bool(payload.get("expects_json", False))
            )
            self._send(status, body, headers)

        def log_message(self, format: str, *args) -> None:
            pass

    server = ThreadingHTTPServer((host, port), Handler)
    server.daemon_threads = True
    server.registry = registry  # type: ignore[attr-defined]
    return server


def main() -> None:
    parser = argparse.ArgumentParser(description="Run the fake LLM provider HTTP server.")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=8089)
    parser.add_argument("--latency-ms", type=float, default=0.0)
    parser.add_argument("--failure-rate", type=float, default=0.0)
    parser.add_argument("--rate-limit", type=int, default=None,
                        help="Max requests per window, per provider. Omit for no limit.")
    parser.add_argument("--window-seconds", type=float, default=60.0)
    parser.add_argument("--seed", type=int, default=0)
    args = parser.parse_args()

    server = make_server(
        args.host,
        args.port,
        FakeProviderConfig(
            latency_ms=args.latency_ms,
            failure_rate=args.failure_rate,
            rate_limit=args.rate_limit,
            window_seconds=args.window_seconds,
            seed=args.seed,
        ),
    )
    host, port = server.server_address[:2]
    print(f"fake provider listening on http://{host}:{port}", flush=True)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        server.server_close()


if __name__ == "__main__":
    main()
