"""Base abstraction for HTTP-backed data sources."""

from dataclasses import dataclass


@dataclass(slots=True)
class SourceAdapter:
    """Minimal request metadata required by ingestion pipelines."""

    source_name: str
    dataset: str
    base_url: str
    bronze_table: str

    def describe(self) -> dict[str, str]:
        return {
            "source_name": self.source_name,
            "dataset": self.dataset,
            "base_url": self.base_url,
            "bronze_table": self.bronze_table,
        }
