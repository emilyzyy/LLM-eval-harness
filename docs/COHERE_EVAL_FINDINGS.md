# Cohere Eval Findings

**Status: Template — fill in after running the eval.**

This document records the results of running `combined_eval.yaml` against Cohere Command models.
Do not paste numbers here until you have actually run the CLI commands below and confirmed
the output JSON matches what you are claiming.

---

## How to run

```bash
# 1. Install Cohere SDK
pip install -e ".[cohere]"

# 2. Set your API key
export COHERE_API_KEY=your-key-here

# 3. Run Command A
python3 -m designops_eval.cli run \
  --prompt-set evals/prompt_sets/combined_eval.yaml \
  --model cohere-command-a \
  --baseline evals/baselines/baseline.json \
  --output-dir results/cohere_command_a_run \
  --storage local

# 4. Run Command R+
python3 -m designops_eval.cli run \
  --prompt-set evals/prompt_sets/combined_eval.yaml \
  --model cohere-command-r-plus \
  --baseline evals/baselines/baseline.json \
  --output-dir results/cohere_command_r_plus_run \
  --storage local
```

Results are written to:
- `results/<run>/latest.json` — summary scores
- `results/<run>/traces.json` — per-prompt responses
- `results/<run>/report.md` — markdown report with baseline comparison

---

## Command A results

> Replace every `__FILL_IN__` with the real value from `results/cohere_command_a_run/latest.json`
> after running the eval.

| Metric | Value |
|--------|-------|
| Run date | __FILL_IN__ |
| Model alias | `cohere-command-a` |
| API model ID | `command-a-03-2025` |
| Factual accuracy | __FILL_IN__ |
| JSON validity | __FILL_IN__ |
| Avg latency (ms) | __FILL_IN__ |
| Total input tokens | __FILL_IN__ |
| Total output tokens | __FILL_IN__ |
| Estimated cost (USD) | __FILL_IN__ |
| Regressions vs baseline | __FILL_IN__ |

**JSON schema adherence notes:**
> Fill in after inspecting `traces.json`. Note which prompts (if any) failed JSON validation
> and why. Do not claim "fully schema-compliant" unless `json_validity = 1.0`.

---

## Command R+ results

> Replace every `__FILL_IN__` with the real value from `results/cohere_command_r_plus_run/latest.json`.

| Metric | Value |
|--------|-------|
| Run date | __FILL_IN__ |
| Model alias | `cohere-command-r-plus` |
| API model ID | `command-r-plus-08-2024` |
| Factual accuracy | __FILL_IN__ |
| JSON validity | __FILL_IN__ |
| Avg latency (ms) | __FILL_IN__ |
| Total input tokens | __FILL_IN__ |
| Total output tokens | __FILL_IN__ |
| Estimated cost (USD) | __FILL_IN__ |
| Regressions vs baseline | __FILL_IN__ |

---

## Cross-model comparison

> Fill in after running both models. Add a bar chart or table comparing the two.

| Metric | Command A | Command R+ | Delta |
|--------|-----------|------------|-------|
| Factual accuracy | __FILL_IN__ | __FILL_IN__ | __FILL_IN__ |
| JSON validity | __FILL_IN__ | __FILL_IN__ | __FILL_IN__ |
| Avg latency (ms) | __FILL_IN__ | __FILL_IN__ | __FILL_IN__ |
| Estimated cost (USD) | __FILL_IN__ | __FILL_IN__ | __FILL_IN__ |

---

## Cost warning

Token pricing used in cost estimates is approximate (see `designops_eval/scorer.py`).
Verify current prices at https://cohere.com/pricing before citing cost figures in any report.

---

## Observations

> Fill in after reviewing the traces. Suggested sections:

**What worked well:**

**Where the model struggled:**

**JSON schema adherence:**

**Latency characteristics:**
