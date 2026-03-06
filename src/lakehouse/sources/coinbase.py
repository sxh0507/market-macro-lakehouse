"""Coinbase source adapter and fetch helpers for Bronze ingestion."""

from __future__ import annotations

import hashlib
import json
import time as time_module
from datetime import date, datetime, time, timedelta, timezone

import requests

from lakehouse.common.models import ProductIngestionStats
from lakehouse.sources.base import SourceAdapter

UTC = timezone.utc


def parse_product_ids(raw_value: str) -> list[str]:
    """Parse a comma-separated product list into unique Coinbase ids."""

    product_ids = [item.strip().upper() for item in raw_value.split(",") if item.strip()]
    product_ids = list(dict.fromkeys(product_ids))
    if not product_ids:
        raise ValueError("product_ids cannot be empty")
    return product_ids


def parse_iso_date(field_name: str, raw_value: str) -> date:
    """Parse an ISO date string used by notebook widgets."""

    try:
        return datetime.strptime(raw_value, "%Y-%m-%d").date()
    except ValueError as exc:
        raise ValueError(f"{field_name} must be in YYYY-MM-DD format") from exc


def resolve_date_window(
    mode: str,
    start_date_raw: str,
    end_date_raw: str,
    lookback_days_raw: str,
) -> tuple[date, date]:
    """Resolve the inclusive ingestion window for backfill or incremental mode."""

    latest_complete_date = datetime.now(UTC).date() - timedelta(days=1)

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
            f"end_date must be <= latest completed UTC day: {latest_complete_date.isoformat()}"
        )

    return start_date, end_date


class CoinbaseSource(SourceAdapter):
    """Adapter and HTTP client configuration for Coinbase daily OHLC data."""

    max_candles_per_request = 300
    request_timeout_seconds = 15
    max_retries = 5
    request_pause_seconds = 0.3
    user_agent = "market-macro-lakehouse/phase1"

    def __init__(self, dataset: str = "market_crypto") -> None:
        super().__init__(
            source_name="coinbase",
            dataset=dataset,
            base_url="https://api.exchange.coinbase.com",
            bronze_table="market_macro.brz_market.raw_coinbase_ohlc_1d",
        )
        self.dataset = dataset

    def build_request_windows(
        self,
        start_date: date,
        end_date: date,
    ) -> list[tuple[datetime, datetime]]:
        """Split an inclusive date range into Coinbase-compatible request windows."""

        windows: list[tuple[datetime, datetime]] = []
        current_start = datetime.combine(start_date, time.min, tzinfo=UTC)
        end_exclusive = datetime.combine(end_date + timedelta(days=1), time.min, tzinfo=UTC)

        while current_start < end_exclusive:
            current_end = min(
                current_start + timedelta(days=self.max_candles_per_request),
                end_exclusive,
            )
            windows.append((current_start, current_end))
            current_start = current_end

        return windows

    @staticmethod
    def to_iso_z(value: datetime) -> str:
        return value.isoformat().replace("+00:00", "Z")

    def request_json(
        self,
        session: requests.Session,
        url: str,
        params: dict[str, str | int],
    ) -> list:
        """Perform a Coinbase API request with retry logic."""

        headers = {
            "Accept": "application/json",
            "User-Agent": self.user_agent,
        }

        for attempt in range(self.max_retries):
            try:
                response = session.get(
                    url,
                    params=params,
                    headers=headers,
                    timeout=self.request_timeout_seconds,
                )
                if response.status_code == 200:
                    return response.json()

                if response.status_code == 429 or 500 <= response.status_code < 600:
                    wait_seconds = (2**attempt) + 0.5
                    print(
                        f"Retryable Coinbase response {response.status_code} for {url}; "
                        f"waiting {wait_seconds:.1f}s"
                    )
                    time_module.sleep(wait_seconds)
                    continue

                raise RuntimeError(
                    f"Coinbase API error {response.status_code} for {url}: {response.text}"
                )
            except requests.RequestException as exc:
                if attempt == self.max_retries - 1:
                    raise RuntimeError(f"Coinbase request failed for {url}") from exc

                wait_seconds = (2**attempt) + 0.5
                print(f"Request exception for {url}: {exc}; waiting {wait_seconds:.1f}s before retry")
                time_module.sleep(wait_seconds)

        raise RuntimeError(f"Exhausted retries for {url}")

    def fetch_daily_candles(
        self,
        session: requests.Session,
        product_id: str,
        start_date: date,
        end_date: date,
        run_id: str,
    ) -> tuple[list[dict], ProductIngestionStats]:
        """Fetch and normalize daily candles for the requested inclusive date range."""

        url = f"{self.base_url}/products/{product_id}/candles"
        ingested_at = datetime.now(UTC)
        records: list[dict] = []
        stats = ProductIngestionStats()

        for window_start, window_end in self.build_request_windows(start_date, end_date):
            params = {
                "start": self.to_iso_z(window_start),
                "end": self.to_iso_z(window_end),
                "granularity": 86400,
            }

            payload = self.request_json(session, url, params)
            stats.request_windows += 1
            stats.api_rows_fetched += len(payload)

            for candle in payload:
                if not isinstance(candle, list) or len(candle) != 6:
                    continue

                timestamp_seconds, low, high, open_value, close_value, volume = candle
                bar_date = datetime.fromtimestamp(int(timestamp_seconds), tz=UTC).date()

                if bar_date < start_date or bar_date > end_date:
                    continue

                payload_hash = hashlib.sha256(
                    json.dumps(
                        {"product_id": product_id, "candle": candle},
                        separators=(",", ":"),
                    ).encode("utf-8")
                ).hexdigest()

                stats.rows_in_requested_range += 1
                records.append(
                    {
                        "product_id": product_id,
                        "bar_date": bar_date,
                        "open": float(open_value),
                        "high": float(high),
                        "low": float(low),
                        "close": float(close_value),
                        "volume": float(volume),
                        "source_window_start": window_start,
                        "source_window_end": window_end,
                        "ingested_at": ingested_at,
                        "run_id": run_id,
                        "payload_hash": payload_hash,
                    }
                )

            time_module.sleep(self.request_pause_seconds)

        return records, stats
