import json
from pathlib import Path

from .base import StorageBackend
from ..models.results import RunResult


class LocalStorage(StorageBackend):
    def save_run(self, result: RunResult, report_md: str, output_dir: str) -> None:
        out = Path(output_dir)
        out.mkdir(parents=True, exist_ok=True)

        (out / "latest.json").write_text(
            result.model_dump_json(indent=2, exclude={"traces"}),
            encoding="utf-8",
        )
        traces_payload = {"traces": [t.model_dump() for t in result.traces]}
        (out / "traces.json").write_text(
            json.dumps(traces_payload, indent=2),
            encoding="utf-8",
        )
        (out / "report.md").write_text(report_md, encoding="utf-8")
