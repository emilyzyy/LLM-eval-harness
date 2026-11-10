"""Stable identities and deterministic sharding for distributed runs.

Identity hierarchy:

    run_id           chosen by the caller of `submit`; names one logical run
    model            which model is evaluated
    config_version   hash of the prompt template and model parameters
    case_id          the prompt-set entry's id

A result is identified by all four. A shard is a group of case ids for one
(run_id, model, config_version) and is only a unit of delivery: durable
idempotency lives at the case level, so shard boundaries can change between
submissions without duplicating or losing results.
"""

import hashlib
import json
import urllib.parse
from dataclasses import asdict, dataclass
from typing import Any, Sequence

from ..providers.registry import model_config, provider_for

MESSAGE_SCHEMA_VERSION = 1


class MalformedShardError(ValueError):
    """A queue message that cannot be parsed into a Shard."""


@dataclass(frozen=True)
class Shard:
    run_id: str
    model: str
    provider: str
    config_version: str
    prompt_set_path: str
    shard_index: int
    shard_id: str
    case_ids: tuple[str, ...]

    def to_message(self) -> str:
        return json.dumps(
            {"schema_version": MESSAGE_SCHEMA_VERSION, **asdict(self)},
            sort_keys=True,
        )

    @classmethod
    def from_message(cls, body: str) -> "Shard":
        try:
            data = json.loads(body)
            if not isinstance(data, dict):
                raise TypeError("message body is not a JSON object")
            if data.get("schema_version") != MESSAGE_SCHEMA_VERSION:
                raise ValueError(f"unsupported schema_version {data.get('schema_version')!r}")
            case_ids = data["case_ids"]
            if not isinstance(case_ids, list) or not case_ids:
                raise ValueError("case_ids must be a non-empty list")
            if not all(isinstance(c, str) and c for c in case_ids):
                raise ValueError("case_ids must be non-empty strings")
            fields = {
                name: data[name]
                for name in (
                    "run_id", "model", "provider", "config_version",
                    "prompt_set_path", "shard_id",
                )
            }
            if not all(isinstance(v, str) and v for v in fields.values()):
                raise ValueError("identity fields must be non-empty strings")
            return cls(
                shard_index=int(data["shard_index"]),
                case_ids=tuple(case_ids),
                **fields,
            )
        except (json.JSONDecodeError, KeyError, TypeError, ValueError) as exc:
            raise MalformedShardError(f"malformed shard message: {exc}") from exc


def _canonical_digest(value: Any) -> str:
    """SHA-256 of a canonical JSON encoding: sorted keys, no whitespace.

    Python's built-in hash() is salted per process and cannot be used for
    identities that must agree across workers.
    """
    encoded = json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False)
    return hashlib.sha256(encoded.encode("utf-8")).hexdigest()


def config_version(prompt_template: Any, model_params: Any) -> str:
    """Hash the prompt template and model parameters into a version string.

    Inputs are JSON-compatible values from providers.registry.model_config.
    The two arguments are hashed under separate named fields, so moving a
    value from one to the other changes the result. Key order does not
    matter at any nesting level.
    """
    return _canonical_digest(
        {"prompt_template": prompt_template, "model_params": model_params}
    )[:16]


def shard_cases(case_ids: Sequence[str], shard_size: int) -> list[tuple[str, ...]]:
    """Split case ids into deterministic groups of at most shard_size.

    Ids are sorted first, so the grouping depends only on the set of ids and
    reordering the prompt-set file does not reshuffle shards. The caller
    guarantees the ids are unique.
    """
    if shard_size < 1:
        raise ValueError("shard_size must be at least 1")
    ordered = sorted(case_ids)
    return [tuple(ordered[i:i + shard_size]) for i in range(0, len(ordered), shard_size)]


def shard_id(run_id: str, model: str, config_version: str, case_ids: Sequence[str]) -> str:
    """Return a stable identifier for one shard.

    The components are hashed as a JSON array, which keeps boundaries
    unambiguous whatever characters they contain. Case order does not matter.
    The result is 32 hex characters.
    """
    return _canonical_digest([run_id, model, config_version, sorted(case_ids)])[:32]


_KEY_SEPARATOR = "#"


def _key_part(value: str) -> str:
    # Percent-encoding escapes the separator, so components cannot collide.
    return urllib.parse.quote(value, safe="")


def result_key_prefix(model: str, config_version: str) -> str:
    """The common prefix of every result_key for one (model, config_version)."""
    return f"{_key_part(model)}{_KEY_SEPARATOR}{_key_part(config_version)}{_KEY_SEPARATOR}"


def result_key(model: str, config_version: str, case_id: str) -> str:
    """Return the results-table sort key for one case.

    The partition key is run_id. Keys for one (model, config_version) share
    result_key_prefix, so they can be read with a begins_with query. The
    prefix ends in the separator, so "fake-a" never matches "fake-ab".
    """
    return result_key_prefix(model, config_version) + _key_part(case_id)


def build_shards(
    run_id: str,
    prompt_set_path: str,
    prompt_set: dict,
    models: Sequence[str],
    shard_size: int,
) -> list[Shard]:
    """Build every shard for a run: one group of shards per model."""
    if not run_id:
        raise ValueError("run_id must not be empty")
    case_ids = [p["id"] for p in prompt_set["prompts"]]
    if len(set(case_ids)) != len(case_ids):
        seen: set[str] = set()
        dupes = sorted({c for c in case_ids if c in seen or seen.add(c)})
        raise ValueError(f"prompt set has duplicate case ids: {dupes[:5]}")
    if len(set(models)) != len(models):
        raise ValueError("models must be unique")

    shards: list[Shard] = []
    for model in models:
        cfg = model_config(model)
        version = config_version(cfg["prompt_template"], cfg["model_params"])
        for index, group in enumerate(shard_cases(case_ids, shard_size)):
            shards.append(Shard(
                run_id=run_id,
                model=model,
                provider=provider_for(model),
                config_version=version,
                prompt_set_path=prompt_set_path,
                shard_index=index,
                shard_id=shard_id(run_id, model, version, group),
                case_ids=tuple(group),
            ))
    return shards
