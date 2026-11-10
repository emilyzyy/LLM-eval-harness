from dataclasses import dataclass


@dataclass
class ModelResponse:
    """Normalised response shape returned by all model clients."""

    text: str
    latency_ms: float
    input_tokens: int
    output_tokens: int
    # "api" = counts came directly from the provider response.
    # "estimated" = provider didn't return counts; we fell back to a character estimate.
    # "mock" = synthetic response, not a real API call.
    token_source: str = "api"
