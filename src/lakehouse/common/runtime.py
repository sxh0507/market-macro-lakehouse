"""Shared runtime helpers for notebook-driven pipelines."""

from __future__ import annotations

from datetime import date, datetime, timedelta, timezone

UTC = timezone.utc


def parse_product_ids(raw_value: str) -> list[str]:
    """Parse a comma-separated product list into unique uppercase ids."""

    product_ids = [item.strip().upper() for item in raw_value.split(",") if item.strip()]
    product_ids = list(dict.fromkeys(product_ids))
    if not product_ids:
        raise ValueError("product_ids cannot be empty")
    return product_ids


def parse_iso_date(field_name: str, raw_value: str) -> date:
    """Parse a YYYY-MM-DD widget value."""

    try:
        return datetime.strptime(raw_value, "%Y-%m-%d").date()
    except ValueError as exc:
        raise ValueError(f"{field_name} must be in YYYY-MM-DD format") from exc


def resolve_date_window(
    mode: str,
    start_date_raw: str,
    end_date_raw: str,
    lookback_days_raw: str,
    latest_complete_date: date | None = None,
    latest_complete_timezone_label: str = "UTC",
) -> tuple[date, date]:
    """Resolve the inclusive ingestion window for backfill or incremental mode."""

    latest_complete_date = latest_complete_date or (datetime.now(UTC).date() - timedelta(days=1))

    if mode not in {"backfill", "incremental"}:
        raise ValueError("mode must be either 'backfill' or 'incremental'")

    if mode == "backfill":
        if not start_date_raw or not end_date_raw:
            raise ValueError("backfill mode requires both start_date and end_date")

        start_date = parse_iso_date("start_date", start_date_raw)
        end_date = parse_iso_date("end_date", end_date_raw)
    else:
        try:
            lookback_days = int(lookback_days_raw or "0")
        except ValueError as exc:
            raise ValueError("lookback_days must be an integer in incremental mode") from exc

        if lookback_days < 1:
            raise ValueError("lookback_days must be >= 1 in incremental mode")

        end_date = parse_iso_date("end_date", end_date_raw) if end_date_raw else latest_complete_date
        start_date = end_date - timedelta(days=lookback_days - 1)

    if start_date > end_date:
        raise ValueError("start_date cannot be after end_date")

    if end_date > latest_complete_date:
        raise ValueError(
            "end_date must be <= latest completed "
            f"{latest_complete_timezone_label} day: {latest_complete_date.isoformat()}"
        )

    return start_date, end_date
