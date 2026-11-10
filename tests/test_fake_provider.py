import json
import threading
import urllib.request

import pytest

from designops_eval.providers.errors import ProviderError
from designops_eval.providers.fake import (
    PERMANENT_ERROR_MARKER,
    FakeProviderConfig,
    FakeProviderCore,
    fake_attempt,
    fake_response_text,
    local_registry,
    make_server,
)
from tests.helpers import FakeClock


def make_core(clock=None, **config) -> FakeProviderCore:
    clock = clock or FakeClock()
    return FakeProviderCore("fake-a", FakeProviderConfig(**config), clock=clock, sleep=clock.sleep)


class TestResponses:
    def test_response_is_deterministic(self):
        first = make_core().handle("case-1", "What is the pitch?", False)
        second = make_core().handle("case-1", "What is the pitch?", False)
        assert first == second
        assert first[0] == 200

    def test_response_contains_case_id(self):
        _, body, _ = make_core().handle("case-1", "prompt", False)
        assert "case-1" in body["text"]

    def test_response_differs_by_model(self):
        assert fake_response_text("fake-a", "c", "p", False) != fake_response_text("fake-b", "c", "p", False)

    def test_json_response_is_valid_json_with_case_id(self):
        _, body, _ = make_core().handle("case-1", "prompt", True)
        assert json.loads(body["text"])["case_id"] == "case-1"

    def test_latency_is_applied(self):
        clock = FakeClock()
        make_core(clock, latency_ms=250).handle("case-1", "prompt", False)
        assert clock.sleeps == [0.25]


class TestRateLimit:
    def test_rejects_request_over_limit_with_retry_after(self):
        clock = FakeClock()
        core = make_core(clock, rate_limit=3, window_seconds=60)
        assert [core.handle(f"c{i}", "p", False)[0] for i in range(3)] == [200, 200, 200]

        clock.advance(10)
        status, _, headers = core.handle("c3", "p", False)
        assert status == 429
        assert headers["Retry-After"] == "50"

    def test_window_slides(self):
        clock = FakeClock()
        core = make_core(clock, rate_limit=2, window_seconds=60)
        core.handle("c0", "p", False)
        clock.advance(30)
        core.handle("c1", "p", False)
        assert core.handle("c2", "p", False)[0] == 429

        clock.advance(31)  # the first request has left the window
        assert core.handle("c2", "p", False)[0] == 200
        assert core.handle("c3", "p", False)[0] == 429

    def test_rejected_requests_do_not_consume_quota(self):
        clock = FakeClock()
        core = make_core(clock, rate_limit=1, window_seconds=10)
        core.handle("c0", "p", False)
        for _ in range(5):
            assert core.handle("c1", "p", False)[0] == 429
        clock.advance(10.1)
        assert core.handle("c1", "p", False)[0] == 200

    def test_stats_record_limit_violation_attempts(self):
        core = make_core(rate_limit=2, window_seconds=60)
        for i in range(5):
            core.handle(f"c{i}", "p", False)
        stats = core.stats()
        assert stats["requests_total"] == 5
        assert stats["rate_limited"] == 3
        assert stats["completed"] == 2
        assert stats["max_window_count"] == 2

    def test_concurrent_requests_never_exceed_limit(self):
        core = FakeProviderCore("fake-a", FakeProviderConfig(rate_limit=25, window_seconds=60))
        statuses: list[int] = []
        lock = threading.Lock()

        def hit(i: int) -> None:
            status = core.handle(f"c{i}", "p", False)[0]
            with lock:
                statuses.append(status)

        threads = [threading.Thread(target=hit, args=(i,)) for i in range(100)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        assert statuses.count(200) == 25
        assert statuses.count(429) == 75


class TestFailures:
    def test_transient_failures_are_deterministic(self):
        def statuses():
            core = make_core(failure_rate=0.5, seed=7)
            return [core.handle(f"c{i}", "p", False)[0] for i in range(40)]

        first = statuses()
        assert first == statuses()
        assert 503 in first and 200 in first

    def test_transient_failure_depends_on_attempt_number(self):
        core = make_core(failure_rate=0.5, seed=7)
        failing = next(f"c{i}" for i in range(100) if core.handle(f"c{i}", "p", False)[0] == 503)
        outcomes = [core.handle(failing, "p", False)[0] for _ in range(20)]
        assert 200 in outcomes

    def test_seed_changes_failure_pattern(self):
        def pattern(seed):
            core = make_core(failure_rate=0.5, seed=seed)
            return [core.handle(f"c{i}", "p", False)[0] for i in range(40)]

        assert pattern(1) != pattern(2)

    def test_permanent_error_marker_returns_400_every_time(self):
        core = make_core()
        for _ in range(3):
            assert core.handle("c0", f"bad {PERMANENT_ERROR_MARKER}", False)[0] == 400
        assert core.stats()["permanent_failures"] == 3

    def test_duplicate_completions_are_counted(self):
        core = make_core()
        core.handle("c0", "p", False)
        core.handle("c0", "p", False)
        core.handle("c1", "p", False)
        stats = core.stats()
        assert stats["distinct_prompts_completed"] == 2
        assert stats["duplicate_completions"] == 1


class TestClient:
    def test_in_process_attempt_returns_model_response(self):
        response = fake_attempt("fake-a", "c0", "a prompt of some length", False)
        assert "c0" in response.text
        assert response.token_source == "fake"
        assert response.input_tokens > 0 and response.output_tokens > 0

    def test_in_process_config_comes_from_env(self, monkeypatch):
        monkeypatch.setenv("FAKE_PROVIDER_RATE_LIMIT", "1")
        fake_attempt("fake-a", "c0", "p", False)
        with pytest.raises(ProviderError) as info:
            fake_attempt("fake-a", "c1", "p", False)
        assert info.value.status_code == 429
        assert info.value.retry_after >= 1

    def test_each_fake_model_has_its_own_quota(self, monkeypatch):
        monkeypatch.setenv("FAKE_PROVIDER_RATE_LIMIT", "1")
        fake_attempt("fake-a", "c0", "p", False)
        fake_attempt("fake-b", "c0", "p", False)
        assert set(local_registry().stats()) == {"fake-a", "fake-b"}

    def test_permanent_error_raises_provider_error_400(self):
        with pytest.raises(ProviderError) as info:
            fake_attempt("fake-a", "c0", PERMANENT_ERROR_MARKER, False)
        assert info.value.status_code == 400


@pytest.fixture
def server():
    srv = make_server(port=0, default_config=FakeProviderConfig(rate_limit=2, window_seconds=60))
    thread = threading.Thread(target=srv.serve_forever, daemon=True)
    thread.start()
    host, port = srv.server_address[:2]
    yield f"http://{host}:{port}"
    srv.shutdown()
    srv.server_close()


class TestHttpServer:
    def test_round_trip_matches_in_process_response(self, server, monkeypatch):
        monkeypatch.setenv("FAKE_PROVIDER_URL", server)
        over_http = fake_attempt("fake-a", "c0", "prompt", True)
        assert over_http.text == fake_response_text("fake-a", "c0", "prompt", True)

    def test_429_carries_retry_after(self, server, monkeypatch):
        monkeypatch.setenv("FAKE_PROVIDER_URL", server)
        fake_attempt("fake-a", "c0", "p", False)
        fake_attempt("fake-a", "c1", "p", False)
        with pytest.raises(ProviderError) as info:
            fake_attempt("fake-a", "c2", "p", False)
        assert info.value.status_code == 429
        assert 1 <= info.value.retry_after <= 60

    def test_quota_is_shared_by_all_clients_and_separate_per_provider(self, server, monkeypatch):
        monkeypatch.setenv("FAKE_PROVIDER_URL", server)
        fake_attempt("fake-a", "c0", "p", False)
        fake_attempt("fake-a", "c1", "p", False)
        fake_attempt("fake-b", "c0", "p", False)  # separate quota
        with urllib.request.urlopen(f"{server}/stats") as response:
            stats = json.loads(response.read())
        assert stats["fake-a"]["completed"] == 2
        assert stats["fake-b"]["completed"] == 1

    def test_unreachable_server_raises_connection_error(self, monkeypatch):
        monkeypatch.setenv("FAKE_PROVIDER_URL", "http://127.0.0.1:9")
        with pytest.raises(ConnectionError):
            fake_attempt("fake-a", "c0", "p", False)

    def test_reset_clears_state(self, server, monkeypatch):
        monkeypatch.setenv("FAKE_PROVIDER_URL", server)
        fake_attempt("fake-a", "c0", "p", False)
        urllib.request.urlopen(urllib.request.Request(f"{server}/reset", data=b"", method="POST"))
        with urllib.request.urlopen(f"{server}/stats") as response:
            assert json.loads(response.read()) == {}
