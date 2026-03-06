"""Common data models used across pipeline modules."""

from dataclasses import dataclass, field
from typing import Any


@dataclass(slots=True)
class LoadResult:
    """Bronze pipeline result returned by the notebook entrypoint."""

    status: str
    table_name: str
    rows_written: int
    run_id: str
    metadata: dict[str, Any] = field(default_factory=dict)

    def as_dict(self) -> dict[str, Any]:
        """Return a notebook-friendly dictionary payload."""

        return {
            "status": self.status,
            "table_name": self.table_name,
            "rows_written": self.rows_written,
            "run_id": self.run_id,
            "metadata": self.metadata,
        }
