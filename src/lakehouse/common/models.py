"""Common data models used across pipeline modules."""

from dataclasses import dataclass, field
from typing import Any


@dataclass(slots=True)
class LoadResult:
    """Generic load result used by pipeline entrypoints."""

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


@dataclass(slots=True)
class ProductIngestionStats:
    """Per-product API fetch statistics for Bronze ingestion."""

    api_rows_fetched: int = 0
    rows_in_requested_range: int = 0
    request_windows: int = 0

    def as_dict(self) -> dict[str, int]:
        return {
            "api_rows_fetched": self.api_rows_fetched,
            "rows_in_requested_range": self.rows_in_requested_range,
            "request_windows": self.request_windows,
        }


@dataclass(slots=True)
class BronzeIngestionResult:
    """Structured result returned by the Coinbase Bronze pipeline."""

    status: str
    mode: str
    product_ids: list[str]
    start_date: str
    end_date: str
    rows_fetched: int
    rows_after_filter: int
    rows_after_dedup: int
    rows_to_update: int
    rows_to_insert: int
    rows_merged: int
    run_id: str
    target_table: str
    per_product_stats: dict[str, ProductIngestionStats] = field(default_factory=dict)

    def as_dict(self) -> dict[str, Any]:
        return {
            "status": self.status,
            "mode": self.mode,
            "product_ids": self.product_ids,
            "start_date": self.start_date,
            "end_date": self.end_date,
            "rows_fetched": self.rows_fetched,
            "rows_after_filter": self.rows_after_filter,
            "rows_after_dedup": self.rows_after_dedup,
            "rows_to_update": self.rows_to_update,
            "rows_to_insert": self.rows_to_insert,
            "rows_merged": self.rows_merged,
            "run_id": self.run_id,
            "target_table": self.target_table,
            "per_product_stats": {
                product_id: stats.as_dict()
                for product_id, stats in self.per_product_stats.items()
            },
        }


@dataclass(slots=True)
class SilverIngestionResult:
    """Structured result returned by the Silver crypto OHLC pipeline."""

    status: str
    mode: str
    product_ids: list[str]
    start_date: str
    end_date: str
    source_table: str
    target_table: str
    quarantine_table: str
    rows_read: int
    rows_after_dedup: int
    rows_structural_invalid: int
    rows_rejected: int
    rows_quarantined: int
    rows_to_update: int
    rows_to_insert: int
    rows_merged: int
    run_id: str
    per_product_rows_read: dict[str, int] = field(default_factory=dict)
    per_product_rows_after_dedup: dict[str, int] = field(default_factory=dict)
    per_product_rows_rejected: dict[str, int] = field(default_factory=dict)
    per_product_rows_merged: dict[str, int] = field(default_factory=dict)

    def as_dict(self) -> dict[str, Any]:
        return {
            "status": self.status,
            "mode": self.mode,
            "product_ids": self.product_ids,
            "start_date": self.start_date,
            "end_date": self.end_date,
            "source_table": self.source_table,
            "target_table": self.target_table,
            "quarantine_table": self.quarantine_table,
            "rows_read": self.rows_read,
            "rows_after_dedup": self.rows_after_dedup,
            "rows_structural_invalid": self.rows_structural_invalid,
            "rows_rejected": self.rows_rejected,
            "rows_quarantined": self.rows_quarantined,
            "rows_to_update": self.rows_to_update,
            "rows_to_insert": self.rows_to_insert,
            "rows_merged": self.rows_merged,
            "run_id": self.run_id,
            "per_product_rows_read": self.per_product_rows_read,
            "per_product_rows_after_dedup": self.per_product_rows_after_dedup,
            "per_product_rows_rejected": self.per_product_rows_rejected,
            "per_product_rows_merged": self.per_product_rows_merged,
        }
