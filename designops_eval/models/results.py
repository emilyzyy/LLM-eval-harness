from typing import Any
from pydantic import BaseModel, Field


class TraceEntry(BaseModel):
    prompt_id: str
    prompt: str
    response: str
    latency_ms: float
    input_tokens: int
    output_tokens: int
    factual_score: float
    json_validity_score: float
    metadata: dict[str, Any] = Field(default_factory=dict)


class RunScores(BaseModel):
    factual_accuracy: float
    json_validity: float
    avg_latency_ms: float
    total_input_tokens: int
    total_output_tokens: int
    estimated_cost_usd: float
    consistency_delta: float | None = None


class RunResult(BaseModel):
    run_id: str
    model: str
    timestamp: str
    prompt_set: str
    scores: RunScores
    regressions: list[str] = Field(default_factory=list)
    traces: list[TraceEntry] = Field(default_factory=list)
