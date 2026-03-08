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
    """Per-identifier API fetch statistics for Bronze ingestion."""

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
class EcbBronzeIngestionResult:
    """Structured result returned by the ECB Bronze pipeline."""

    status: str
    mode: str
    quote_currencies: list[str]
    start_date: str
    end_date: str
    request_url: str
    series_key: str
    rows_fetched: int
    rows_after_filter: int
    rows_after_dedup: int
    rows_to_update: int
    rows_to_insert: int
    rows_merged: int
    run_id: str
    target_table: str
    per_currency_stats: dict[str, ProductIngestionStats] = field(default_factory=dict)

    def as_dict(self) -> dict[str, Any]:
        return {
            "status": self.status,
            "mode": self.mode,
            "quote_currencies": self.quote_currencies,
            "start_date": self.start_date,
            "end_date": self.end_date,
            "request_url": self.request_url,
            "series_key": self.series_key,
            "rows_fetched": self.rows_fetched,
            "rows_after_filter": self.rows_after_filter,
            "rows_after_dedup": self.rows_after_dedup,
            "rows_to_update": self.rows_to_update,
            "rows_to_insert": self.rows_to_insert,
            "rows_merged": self.rows_merged,
            "run_id": self.run_id,
            "target_table": self.target_table,
            "per_currency_stats": {
                quote_currency: stats.as_dict()
                for quote_currency, stats in self.per_currency_stats.items()
            },
        }


@dataclass(slots=True)
class FredSeriesIngestionStats:
    """Per-series API fetch statistics for FRED Bronze ingestion."""

    api_rows_fetched: int = 0
    request_pages: int = 0
    metadata_fetched: int = 0

    def as_dict(self) -> dict[str, int]:
        return {
            "api_rows_fetched": self.api_rows_fetched,
            "request_pages": self.request_pages,
            "metadata_fetched": self.metadata_fetched,
        }


@dataclass(slots=True)
class FredBronzeIngestionResult:
    """Structured result returned by the FRED Bronze pipeline."""

    status: str
    mode: str
    incremental_strategy: str
    series_ids: list[str]
    start_date: str
    end_date: str
    target_table: str
    metadata_table: str
    rows_fetched: int
    rows_after_filter: int
    rows_after_dedup: int
    rows_to_update: int
    rows_to_insert: int
    rows_merged: int
    rows_metadata_to_update: int
    rows_metadata_to_insert: int
    rows_metadata_merged: int
    run_id: str
    per_series_stats: dict[str, FredSeriesIngestionStats] = field(default_factory=dict)

    def as_dict(self) -> dict[str, Any]:
        return {
            "status": self.status,
            "mode": self.mode,
            "incremental_strategy": self.incremental_strategy,
            "series_ids": self.series_ids,
            "start_date": self.start_date,
            "end_date": self.end_date,
            "target_table": self.target_table,
            "metadata_table": self.metadata_table,
            "rows_fetched": self.rows_fetched,
            "rows_after_filter": self.rows_after_filter,
            "rows_after_dedup": self.rows_after_dedup,
            "rows_to_update": self.rows_to_update,
            "rows_to_insert": self.rows_to_insert,
            "rows_merged": self.rows_merged,
            "rows_metadata_to_update": self.rows_metadata_to_update,
            "rows_metadata_to_insert": self.rows_metadata_to_insert,
            "rows_metadata_merged": self.rows_metadata_merged,
            "run_id": self.run_id,
            "per_series_stats": {
                series_id: stats.as_dict() for series_id, stats in self.per_series_stats.items()
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


@dataclass(slots=True)
class EcbSilverIngestionResult:
    """Structured result returned by the Silver ECB FX pipeline."""

    status: str
    source_system: str
    mode: str
    quote_currencies: list[str]
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
    per_currency_rows_read: dict[str, int] = field(default_factory=dict)
    per_currency_rows_after_dedup: dict[str, int] = field(default_factory=dict)
    per_currency_rows_rejected: dict[str, int] = field(default_factory=dict)
    per_currency_rows_merged: dict[str, int] = field(default_factory=dict)

    def as_dict(self) -> dict[str, Any]:
        return {
            "status": self.status,
            "source_system": self.source_system,
            "mode": self.mode,
            "quote_currencies": self.quote_currencies,
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
            "per_currency_rows_read": self.per_currency_rows_read,
            "per_currency_rows_after_dedup": self.per_currency_rows_after_dedup,
            "per_currency_rows_rejected": self.per_currency_rows_rejected,
            "per_currency_rows_merged": self.per_currency_rows_merged,
        }


@dataclass(slots=True)
class GoldIngestionResult:
    """Structured result returned by the Gold market returns/volatility pipeline."""

    status: str
    mode: str
    product_ids: list[str]
    start_date: str
    end_date: str
    source_history_start_date: str
    source_table: str
    returns_table: str
    volatility_table: str
    rows_read: int
    rows_returns_ready: int
    rows_volatility_ready: int
    rows_returns_to_update: int
    rows_returns_to_insert: int
    rows_returns_merged: int
    rows_volatility_to_update: int
    rows_volatility_to_insert: int
    rows_volatility_merged: int
    run_id: str
    per_product_rows_read: dict[str, int] = field(default_factory=dict)
    per_product_rows_returns: dict[str, int] = field(default_factory=dict)
    per_product_rows_volatility: dict[str, int] = field(default_factory=dict)

    def as_dict(self) -> dict[str, Any]:
        return {
            "status": self.status,
            "mode": self.mode,
            "product_ids": self.product_ids,
            "start_date": self.start_date,
            "end_date": self.end_date,
            "source_history_start_date": self.source_history_start_date,
            "source_table": self.source_table,
            "returns_table": self.returns_table,
            "volatility_table": self.volatility_table,
            "rows_read": self.rows_read,
            "rows_returns_ready": self.rows_returns_ready,
            "rows_volatility_ready": self.rows_volatility_ready,
            "rows_returns_to_update": self.rows_returns_to_update,
            "rows_returns_to_insert": self.rows_returns_to_insert,
            "rows_returns_merged": self.rows_returns_merged,
            "rows_volatility_to_update": self.rows_volatility_to_update,
            "rows_volatility_to_insert": self.rows_volatility_to_insert,
            "rows_volatility_merged": self.rows_volatility_merged,
            "run_id": self.run_id,
            "per_product_rows_read": self.per_product_rows_read,
            "per_product_rows_returns": self.per_product_rows_returns,
            "per_product_rows_volatility": self.per_product_rows_volatility,
        }
