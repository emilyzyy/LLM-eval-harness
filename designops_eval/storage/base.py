from abc import ABC, abstractmethod

from ..models.results import RunResult


class StorageBackend(ABC):
    @abstractmethod
    def save_run(self, result: RunResult, report_md: str, output_dir: str) -> None:
        pass
