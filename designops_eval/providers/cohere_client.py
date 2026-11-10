import os
import time
from typing import Callable

from .base import ModelResponse
from .retry import call_with_retries

# Internal Cohere API model identifiers, keyed by the CLI alias we expose.
# Check https://docs.cohere.com/docs/models for the latest available IDs.
_API_MODEL_IDS: dict[str, str] = {
    "cohere-command-a": "command-a-03-2025",
    "cohere-command-r-plus": "command-r-plus-08-2024",
}

# Injected as the first message when the prompt expects JSON output, alongside
# response_format, so the model is asked for JSON in two independent ways.
_SYSTEM_JSON_HINT = (
    "You must respond with a valid JSON object only. "
    "Do not include any explanation, markdown fences, or text outside the JSON object."
)


def _get_client():
    """Lazy-import and construct the Cohere client.

    Importing at module level would break all tests and mock-mode usage on
    machines that don't have the cohere package installed, so we defer it.
    """
    try:
        import cohere
    except ImportError as exc:
        raise ImportError(
            "The 'cohere' package is required for Cohere models. "
            "Install it with: pip install \"designops-eval[cohere]\""
        ) from exc

    api_key = os.environ.get("COHERE_API_KEY")
    if not api_key:
        raise EnvironmentError(
            "COHERE_API_KEY environment variable is not set.\n"
            "Export it before running a Cohere eval:\n"
            "  export COHERE_API_KEY=your-key-here"
        )

    return cohere.ClientV2(api_key=api_key)


def call_cohere(
    model_alias: str,
    prompt_text: str,
    expects_json: bool = False,
    before_attempt: Callable[[], None] | None = None,
) -> ModelResponse:
    """Call a Cohere model and return a normalised ModelResponse.

    The request is built once and sent unchanged on every attempt. Only errors
    that classify_error marks retryable are retried; anything else, including
    a rejected response_format, propagates to the caller.

    Args:
        model_alias: One of the keys in _API_MODEL_IDS (e.g. "cohere-command-a").
        prompt_text: The user prompt string.
        expects_json: When True, we request structured JSON output and prepend
            a system message asking for JSON-only output.
        before_attempt: Called ahead of every attempt, including retries.

    Returns:
        ModelResponse with real latency_ms and token counts from the API.
        token_source is "api" when counts came from response.usage, or
        "estimated" if the SDK response structure was unexpected.
    """
    api_model = _API_MODEL_IDS.get(model_alias)
    if api_model is None:
        raise ValueError(
            f"Unknown Cohere model alias {model_alias!r}. "
            f"Supported aliases: {list(_API_MODEL_IDS)}"
        )

    client = _get_client()

    messages: list[dict] = [{"role": "user", "content": prompt_text}]
    kwargs: dict = {"model": api_model, "messages": messages}

    if expects_json:
        kwargs["messages"] = [
            {"role": "system", "content": _SYSTEM_JSON_HINT},
            *messages,
        ]
        # Structured output mode, supported on Command R and newer.
        kwargs["response_format"] = {"type": "json_object"}

    def attempt():
        t0 = time.perf_counter()
        result = client.chat(**kwargs)
        return result, (time.perf_counter() - t0) * 1000

    response, latency_ms = call_with_retries(attempt, before_attempt=before_attempt)

    text = response.message.content[0].text

    # Extract token usage from the Cohere v5 SDK structure.
    # Field names vary across SDK versions; we handle the two known shapes
    # and fall back to a character-based estimate if neither works.
    token_source = "api"
    try:
        usage = response.usage
        # ClientV2 nests counts under usage.tokens
        tokens = usage.tokens if hasattr(usage, "tokens") else usage
        input_tokens = int(tokens.input_tokens)
        output_tokens = int(tokens.output_tokens)
    except (AttributeError, TypeError):
        input_tokens = max(1, len(prompt_text) // 4)
        output_tokens = max(1, len(text) // 4)
        token_source = "estimated"

    return ModelResponse(
        text=text,
        latency_ms=latency_ms,
        input_tokens=input_tokens,
        output_tokens=output_tokens,
        token_source=token_source,
    )
