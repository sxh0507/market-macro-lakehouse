"""Coinbase source adapter and fetch helpers for Bronze ingestion."""

from __future__ import annotations

import hashlib
import json
import time as time_module
from datetime import date, datetime, time, timedelta

import requests

from lakehouse.common.models import ProductIngestionStats
from lakehouse.common.runtime import UTC
from lakehouse.sources.base import SourceAdapter


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
