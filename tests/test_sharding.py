"""Tests for stable identities and sharding in distributed/sharding.py."""

import random
import re
import subprocess
import sys

import pytest

from designops_eval.distributed.sharding import (
    build_shards,
    config_version,
    result_key,
    shard_cases,
    shard_id,
)

CASES = [f"case_{i:03d}" for i in range(53)]


def in_fresh_process(expression: str, hash_seed: str) -> str:
    """Evaluate an expression in a new interpreter with a different hash salt."""
    code = (
        "from designops_eval.distributed.sharding import *\n"
        f"print({expression})"
    )
    result = subprocess.run(
        [sys.executable, "-c", code],
        capture_output=True, text=True, check=True,
        env={**__import__("os").environ, "PYTHONHASHSEED": hash_seed},
    )
    return result.stdout.strip()


class TestConfigVersion:
    TEMPLATE = {"user": "{prompt}", "system_when_json": "Respond with JSON."}
    PARAMS = {"api_model": "command-a", "temperature": 0.2, "nested": {"a": 1, "b": [1, 2]}}

    def test_deterministic(self):
        assert config_version(self.TEMPLATE, self.PARAMS) == config_version(self.TEMPLATE, self.PARAMS)

    def test_format_is_lowercase_hex(self):
        assert re.fullmatch(r"[0-9a-f]{12,64}", config_version(self.TEMPLATE, self.PARAMS))

    def test_independent_of_key_order(self):
        reordered_template = dict(reversed(list(self.TEMPLATE.items())))
        reordered_params = {
            "nested": {"b": [1, 2], "a": 1}, "temperature": 0.2, "api_model": "command-a",
        }
        assert config_version(reordered_template, reordered_params) == config_version(
            self.TEMPLATE, self.PARAMS
        )

    def test_changes_with_prompt_template(self):
        changed = {**self.TEMPLATE, "system_when_json": "Respond with JSON only."}
        assert config_version(changed, self.PARAMS) != config_version(self.TEMPLATE, self.PARAMS)

    def test_changes_with_model_parameter(self):
        changed = {**self.PARAMS, "temperature": 0.3}
        assert config_version(self.TEMPLATE, changed) != config_version(self.TEMPLATE, self.PARAMS)

    def test_changes_with_nested_value(self):
        changed = {**self.PARAMS, "nested": {"a": 1, "b": [2, 1]}}
        assert config_version(self.TEMPLATE, changed) != config_version(self.TEMPLATE, self.PARAMS)

    def test_template_and_params_are_not_interchangeable(self):
        assert config_version({"a": 1}, {}) != config_version({}, {"a": 1})

    def test_stable_across_processes(self):
        expression = "config_version({'user': '{prompt}'}, {'model': 'm', 't': 0.2})"
        expected = config_version({"user": "{prompt}"}, {"model": "m", "t": 0.2})
        assert in_fresh_process(expression, "1") == expected
        assert in_fresh_process(expression, "2") == expected


class TestShardCases:
    def test_every_case_appears_exactly_once(self):
        groups = shard_cases(CASES, 10)
        flat = [c for group in groups for c in group]
        assert sorted(flat) == sorted(CASES)

    def test_group_sizes(self):
        sizes = sorted(len(g) for g in shard_cases(CASES, 10))
        assert sizes == [3, 10, 10, 10, 10, 10]

    def test_exact_multiple_has_no_short_group(self):
        assert [len(g) for g in shard_cases(CASES[:50], 25)] == [25, 25]

    def test_shard_size_larger_than_input(self):
        groups = shard_cases(CASES[:4], 25)
        assert len(groups) == 1 and sorted(groups[0]) == CASES[:4]

    def test_deterministic(self):
        assert shard_cases(CASES, 10) == shard_cases(CASES, 10)

    def test_independent_of_input_order(self):
        shuffled = CASES[:]
        random.Random(3).shuffle(shuffled)
        as_sets = lambda groups: sorted(tuple(sorted(g)) for g in groups)  # noqa: E731
        assert as_sets(shard_cases(shuffled, 10)) == as_sets(shard_cases(CASES, 10))

    def test_empty_input(self):
        assert shard_cases([], 10) == []

    @pytest.mark.parametrize("size", [0, -1])
    def test_invalid_shard_size(self, size):
        with pytest.raises(ValueError):
            shard_cases(CASES, size)


class TestShardId:
    ARGS = ("run-1", "fake-a", "abc123", ("c1", "c2", "c3"))

    def test_deterministic(self):
        assert shard_id(*self.ARGS) == shard_id(*self.ARGS)

    def test_format(self):
        assert re.fullmatch(r"[A-Za-z0-9_-]{1,80}", shard_id(*self.ARGS))

    def test_format_holds_for_many_cases(self):
        assert re.fullmatch(r"[A-Za-z0-9_-]{1,80}", shard_id("run-1", "fake-a", "abc123", CASES * 20))

    def test_independent_of_case_order(self):
        assert shard_id("run-1", "fake-a", "abc123", ("c3", "c1", "c2")) == shard_id(*self.ARGS)

    @pytest.mark.parametrize("changed", [
        ("run-2", "fake-a", "abc123", ("c1", "c2", "c3")),
        ("run-1", "fake-b", "abc123", ("c1", "c2", "c3")),
        ("run-1", "fake-a", "abc124", ("c1", "c2", "c3")),
        ("run-1", "fake-a", "abc123", ("c1", "c2")),
        ("run-1", "fake-a", "abc123", ("c1", "c2", "c4")),
    ])
    def test_changes_with_each_component(self, changed):
        assert shard_id(*changed) != shard_id(*self.ARGS)

    def test_separator_characters_do_not_cause_collisions(self):
        for sep in "#|:/,- ":
            left = shard_id("a", f"b{sep}c", "v", ("x",))
            right = shard_id(f"a{sep}b", "c", "v", ("x",))
            assert left != right, f"collision with separator {sep!r}"

    def test_case_boundaries_do_not_cause_collisions(self):
        assert shard_id("r", "m", "v", ("ab", "c")) != shard_id("r", "m", "v", ("a", "bc"))

    def test_stable_across_processes(self):
        expression = "shard_id('run-1', 'fake-a', 'abc123', ('c1', 'c2', 'c3'))"
        assert in_fresh_process(expression, "1") == shard_id(*self.ARGS)
        assert in_fresh_process(expression, "2") == shard_id(*self.ARGS)


class TestResultKey:
    def test_deterministic(self):
        assert result_key("fake-a", "abc123", "c1") == result_key("fake-a", "abc123", "c1")

    @pytest.mark.parametrize("changed", [
        ("fake-b", "abc123", "c1"),
        ("fake-a", "abc124", "c1"),
        ("fake-a", "abc123", "c2"),
    ])
    def test_changes_with_each_component(self, changed):
        assert result_key(*changed) != result_key("fake-a", "abc123", "c1")

    def test_separator_characters_are_rejected_or_encoded(self):
        for sep in "#|:/":
            try:
                left = result_key(f"a{sep}b", "c", "x")
                right = result_key("a", f"b{sep}c", "x")
            except ValueError:
                continue
            assert left != right, f"collision with separator {sep!r}"

    def test_fits_sort_key_limit(self):
        key = result_key("cohere-command-r-plus", "f" * 64, "case_" + "x" * 200)
        assert len(key.encode()) <= 1024


class TestBuildShards:

    PROMPT_SET = {"name": "p", "prompts": [{"id": c, "prompt": c} for c in CASES]}

    def test_covers_every_case_for_every_model(self):
        shards = build_shards("run-1", "evals/p.yaml", self.PROMPT_SET, ["fake-a", "fake-b"], 10)
        for model in ("fake-a", "fake-b"):
            covered = [c for s in shards if s.model == model for c in s.case_ids]
            assert sorted(covered) == sorted(CASES)

    def test_resubmission_produces_identical_shards(self):
        first = build_shards("run-1", "evals/p.yaml", self.PROMPT_SET, ["fake-a"], 10)
        second = build_shards("run-1", "evals/p.yaml", self.PROMPT_SET, ["fake-a"], 10)
        assert first == second
        assert [s.to_message() for s in first] == [s.to_message() for s in second]

    def test_shard_ids_are_unique_within_a_run(self):
        shards = build_shards("run-1", "evals/p.yaml", self.PROMPT_SET, ["fake-a", "fake-b"], 10)
        assert len({s.shard_id for s in shards}) == len(shards)

    def test_different_run_ids_give_different_shard_ids(self):
        first = build_shards("run-1", "evals/p.yaml", self.PROMPT_SET, ["fake-a"], 10)
        second = build_shards("run-2", "evals/p.yaml", self.PROMPT_SET, ["fake-a"], 10)
        assert {s.shard_id for s in first}.isdisjoint(s.shard_id for s in second)

    def test_shards_carry_provider_and_config_identity(self):
        shards = build_shards("run-1", "evals/p.yaml", self.PROMPT_SET, ["fake-a", "mock"], 10)
        by_model = {s.model: s for s in shards}
        assert by_model["fake-a"].provider == "fake-a"
        assert by_model["mock"].provider == "mock"
        assert by_model["fake-a"].config_version != by_model["mock"].config_version

    def test_duplicate_case_ids_are_rejected(self):
        prompt_set = {"name": "p", "prompts": [{"id": "c1", "prompt": "a"}, {"id": "c1", "prompt": "b"}]}
        with pytest.raises(ValueError, match="duplicate"):
            build_shards("run-1", "evals/p.yaml", prompt_set, ["fake-a"], 10)
