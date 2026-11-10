from .fake import FAKE_MODEL_PREFIX

COHERE_MODEL_PREFIX = "cohere-"


def provider_for(model: str) -> str:
    """Return the provider whose quota a model's requests count against.

    Models that share a provider share one rate limit. Each fake model name is
    its own provider so that tests can set up independent quotas.
    """
    if model == "mock":
        return "mock"
    if model.startswith(COHERE_MODEL_PREFIX):
        return "cohere"
    if model.startswith(FAKE_MODEL_PREFIX):
        return model
    raise ValueError(
        f"Unsupported model: {model!r}. "
        "Supported: 'mock', 'cohere-command-a', 'cohere-command-r-plus', 'fake-<name>'"
    )


def model_config(model: str) -> dict:
    """Return the prompt template and model parameters sent for a model.

    These are the inputs to config_version: two runs whose values here differ
    can produce different outputs for the same case, so their results must not
    share an identity. Update this whenever a client changes what it sends.
    """
    if model == "mock":
        return {
            "prompt_template": {"user": "{prompt}"},
            "model_params": {"model": "mock"},
        }
    if model.startswith(COHERE_MODEL_PREFIX):
        from .cohere_client import _API_MODEL_IDS, _SYSTEM_JSON_HINT

        if model not in _API_MODEL_IDS:
            raise ValueError(f"Unsupported model: {model!r}")
        return {
            "prompt_template": {
                "user": "{prompt}",
                "system_when_json": _SYSTEM_JSON_HINT,
            },
            "model_params": {
                "api_model": _API_MODEL_IDS[model],
                "response_format_when_json": {"type": "json_object"},
            },
        }
    if model.startswith(FAKE_MODEL_PREFIX):
        return {
            "prompt_template": {"user": "{prompt}"},
            "model_params": {"model": model, "response_scheme": 1},
        }
    raise ValueError(f"Unsupported model: {model!r}")
