"""
Tests for the Cohere provider client.

The real cohere SDK is NOT required — the module is lazy-imported inside
_get_client(), so patching sys.modules["cohere"] before calling call_cohere()
is enough to intercept all SDK interactions.
"""

import os
import sys
from unittest.mock import MagicMock

import pytest


def _make_mock_response(text: str, input_tokens: int = 20, output_tokens: int = 30) -> MagicMock:
    """Build a fake Cohere SDK response with the structure ClientV2.chat() returns."""
    response = MagicMock()
    response.message.content = [MagicMock(text=text)]

    tokens_mock = MagicMock()
    tokens_mock.input_tokens = input_tokens
    tokens_mock.output_tokens = output_tokens

    usage_mock = MagicMock()
    usage_mock.tokens = tokens_mock
    response.usage = usage_mock

    return response


@pytest.fixture
def mock_cohere(monkeypatch):
    """Inject a fake cohere module into sys.modules so the lazy import succeeds."""
    fake_module = MagicMock()
    monkeypatch.setitem(sys.modules, "cohere", fake_module)
    return fake_module


class TestCohereResponseMapping:
    """Verify that call_cohere maps Cohere SDK objects to the internal ModelResponse shape."""

    def test_maps_response_text(self, mock_cohere, monkeypatch):
        """Text is extracted from response.message.content[0].text."""
        mock_cohere.ClientV2.return_value.chat.return_value = _make_mock_response(
            "The thread pitch is 1.25mm."
        )
        monkeypatch.setenv("COHERE_API_KEY", "test-key")

        from designops_eval.providers.cohere_client import call_cohere
        result = call_cohere("cohere-command-r-plus", "What is the thread pitch of an M8 bolt?")

        assert result.text == "The thread pitch is 1.25mm."

    def test_maps_token_counts_from_api(self, mock_cohere, monkeypatch):
        """input_tokens and output_tokens come from response.usage.tokens."""
        mock_cohere.ClientV2.return_value.chat.return_value = _make_mock_response(
            "answer", input_tokens=55, output_tokens=23
        )
        monkeypatch.setenv("COHERE_API_KEY", "test-key")

        from designops_eval.providers.cohere_client import call_cohere
        result = call_cohere("cohere-command-a", "prompt text")

        assert result.input_tokens == 55
        assert result.output_tokens == 23
        assert result.token_source == "api"

    def test_latency_ms_is_positive(self, mock_cohere, monkeypatch):
        """latency_ms is measured with perf_counter and is a positive float."""
        mock_cohere.ClientV2.return_value.chat.return_value = _make_mock_response("hi")
        monkeypatch.setenv("COHERE_API_KEY", "test-key")

        from designops_eval.providers.cohere_client import call_cohere
        result = call_cohere("cohere-command-a", "test")

        assert isinstance(result.latency_ms, float)
        assert result.latency_ms > 0.0

    def test_token_source_falls_back_to_estimated(self, mock_cohere, monkeypatch):
        """When response.usage.tokens is None, token_source is 'estimated' and counts are non-zero."""
        mock_response = _make_mock_response("The answer is 42.")
        # Break the usage structure so the AttributeError fallback fires
        mock_response.usage.tokens = None
        mock_cohere.ClientV2.return_value.chat.return_value = mock_response
        monkeypatch.setenv("COHERE_API_KEY", "test-key")

        from designops_eval.providers.cohere_client import call_cohere
        result = call_cohere("cohere-command-a", "What is the answer?")

        assert result.token_source == "estimated"
        assert result.input_tokens >= 1
        assert result.output_tokens >= 1


class TestCohereErrorHandling:
    """Verify that configuration errors produce clear, actionable messages."""

    def test_missing_api_key_raises_environment_error(self, mock_cohere, monkeypatch):
        """Calling call_cohere without COHERE_API_KEY set raises EnvironmentError."""
        monkeypatch.delenv("COHERE_API_KEY", raising=False)

        from designops_eval.providers.cohere_client import call_cohere
        with pytest.raises(EnvironmentError, match="COHERE_API_KEY"):
            call_cohere("cohere-command-a", "test prompt")

    def test_unknown_model_alias_raises_value_error(self, mock_cohere, monkeypatch):
        """An unrecognised model alias raises ValueError before contacting the API."""
        monkeypatch.setenv("COHERE_API_KEY", "test-key")

        from designops_eval.providers.cohere_client import call_cohere
        with pytest.raises(ValueError, match="Unknown Cohere model alias"):
            call_cohere("cohere-gpt-9000", "test prompt")

    def test_missing_cohere_package_raises_import_error(self, monkeypatch):
        """If the cohere SDK isn't installed, ImportError has install instructions."""
        # Remove the cohere module entirely (not even a fake) to simulate missing SDK
        monkeypatch.delitem(sys.modules, "cohere", raising=False)

        # Make cohere un-importable by installing a broken sentinel
        broken = MagicMock()
        broken.ClientV2.side_effect = AttributeError("cohere not installed")

        # Simulate ImportError by patching builtins.__import__
        real_import = __builtins__.__import__ if hasattr(__builtins__, "__import__") else __import__

        def fake_import(name, *args, **kwargs):
            if name == "cohere":
                raise ImportError("No module named 'cohere'")
            return real_import(name, *args, **kwargs)

        monkeypatch.setattr("builtins.__import__", fake_import)

        from designops_eval.providers.cohere_client import call_cohere
        with pytest.raises(ImportError, match="cohere"):
            call_cohere("cohere-command-a", "test")


class TestCohereJsonMode:
    """Verify JSON-mode wiring when a prompt expects structured output."""

    def test_expects_json_prepends_system_message(self, mock_cohere, monkeypatch):
        """When expects_json=True, a system-role message is the first message sent."""
        mock_cohere.ClientV2.return_value.chat.return_value = _make_mock_response('{"sku": "123"}')
        monkeypatch.setenv("COHERE_API_KEY", "test-key")

        from designops_eval.providers.cohere_client import call_cohere
        call_cohere("cohere-command-r-plus", "Look up part.", expects_json=True)

        call_kwargs = mock_cohere.ClientV2.return_value.chat.call_args[1]
        messages = call_kwargs["messages"]
        assert messages[0]["role"] == "system"

    def test_expects_json_includes_response_format(self, mock_cohere, monkeypatch):
        """When expects_json=True, response_format={'type': 'json_object'} is sent."""
        mock_cohere.ClientV2.return_value.chat.return_value = _make_mock_response('{"sku": "123"}')
        monkeypatch.setenv("COHERE_API_KEY", "test-key")

        from designops_eval.providers.cohere_client import call_cohere
        call_cohere("cohere-command-r-plus", "Look up part.", expects_json=True)

        call_kwargs = mock_cohere.ClientV2.return_value.chat.call_args[1]
        assert call_kwargs.get("response_format") == {"type": "json_object"}

    def test_expects_json_false_no_system_message(self, mock_cohere, monkeypatch):
        """When expects_json=False, no system message or response_format is injected."""
        mock_cohere.ClientV2.return_value.chat.return_value = _make_mock_response("276 MPa")
        monkeypatch.setenv("COHERE_API_KEY", "test-key")

        from designops_eval.providers.cohere_client import call_cohere
        call_cohere("cohere-command-a", "What is the tensile strength of 6061-T6?", expects_json=False)

        call_kwargs = mock_cohere.ClientV2.return_value.chat.call_args[1]
        messages = call_kwargs["messages"]
        assert all(m["role"] != "system" for m in messages)
        assert "response_format" not in call_kwargs


class _ApiError(Exception):
    """Shaped like cohere.core.ApiError."""

    def __init__(self, status_code: int) -> None:
        super().__init__(f"status {status_code}")
        self.status_code = status_code


class TestCohereRetries:
    """Retries resend the original request unchanged."""


    def test_retryable_error_is_retried_with_identical_request(self, mock_cohere, monkeypatch):
        chat = mock_cohere.ClientV2.return_value.chat
        chat.side_effect = [_ApiError(429), _ApiError(503), _make_mock_response('{"sku": "1"}')]
        monkeypatch.setenv("COHERE_API_KEY", "test-key")

        from designops_eval.providers.cohere_client import call_cohere
        result = call_cohere("cohere-command-r-plus", "Look up part.", expects_json=True)

        assert result.text == '{"sku": "1"}'
        assert chat.call_count == 3
        first, second, third = (call.kwargs for call in chat.call_args_list)
        assert first == second == third
        assert third["response_format"] == {"type": "json_object"}
        assert third["messages"][0]["role"] == "system"

    def test_rejected_request_is_not_retried_in_a_different_form(self, mock_cohere, monkeypatch):
        chat = mock_cohere.ClientV2.return_value.chat
        chat.side_effect = _ApiError(400)
        monkeypatch.setenv("COHERE_API_KEY", "test-key")

        from designops_eval.providers.cohere_client import call_cohere
        with pytest.raises(_ApiError):
            call_cohere("cohere-command-r-plus", "Look up part.", expects_json=True)

        assert chat.call_count == 1
        assert "response_format" in chat.call_args.kwargs

    def test_retries_are_bounded(self, mock_cohere, monkeypatch):
        chat = mock_cohere.ClientV2.return_value.chat
        chat.side_effect = _ApiError(429)
        monkeypatch.setenv("COHERE_API_KEY", "test-key")
        monkeypatch.setenv("DESIGNOPS_RETRY_MAX_ATTEMPTS", "3")

        from designops_eval.providers.cohere_client import call_cohere
        with pytest.raises(_ApiError):
            call_cohere("cohere-command-a", "prompt")

        assert chat.call_count == 3

    def test_before_attempt_runs_for_each_attempt(self, mock_cohere, monkeypatch):
        chat = mock_cohere.ClientV2.return_value.chat
        chat.side_effect = [_ApiError(429), _make_mock_response("ok")]
        monkeypatch.setenv("COHERE_API_KEY", "test-key")
        calls = []

        from designops_eval.providers.cohere_client import call_cohere
        call_cohere("cohere-command-a", "prompt", before_attempt=lambda: calls.append(1))

        assert len(calls) == 2

    def test_latency_excludes_failed_attempts_and_backoff(self, mock_cohere, monkeypatch):
        import time as _time

        def slow_failure_then_success(**kwargs):
            if chat.call_count == 1:
                _time.sleep(0.2)
                raise _ApiError(503)
            return _make_mock_response("ok")

        chat = mock_cohere.ClientV2.return_value.chat
        chat.side_effect = slow_failure_then_success
        monkeypatch.setenv("COHERE_API_KEY", "test-key")

        from designops_eval.providers.cohere_client import call_cohere
        assert call_cohere("cohere-command-a", "prompt").latency_ms < 150
