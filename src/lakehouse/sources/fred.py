"""FRED source adapter and fetch helpers for Bronze ingestion."""

from __future__ import annotations

import hashlib
import json
import time as time_module
from datetime import date, datetime

import requests

from lakehouse.common.models import FredSeriesIngestionStats
from lakehouse.common.runtime import parse_iso_date
from lakehouse.sources.base import SourceAdapter


class FredSource(SourceAdapter):
    """Adapter and HTTP client configuration for FRED observations and metadata."""

    request_timeout_seconds = 30
    max_retries = 5
    user_agent = "market-macro-lakehouse/phase1"
    complete_realtime_start = "1776-07-04"
    complete_realtime_end = "9999-12-31"
    default_limit = 100000

    def __init__(self, dataset: str = "fred_series_observations") -> None:
        super().__init__(
            source_name="fred",
            dataset=dataset,
            base_url="https://api.stlouisfed.org/fred",
            bronze_table="market_macro.brz_macro.raw_fred_series",
        )
        self.dataset = dataset
        self.metadata_table = "market_macro.brz_macro.raw_fred_series_metadata"

    def request_json(
        self,
        session: requests.Session,
        endpoint: str,
        params: dict[str, str | int],
    ) -> dict:
        """Perform a FRED API request with retry logic."""

        request_url = f"{self.base_url}/{endpoint}"
        headers = {
            "Accept": "application/json",
            "User-Agent": self.user_agent,
        }

        for attempt in range(self.max_retries):
            try:
                response = session.get(
                    request_url,
                    params=params,
                    headers=headers,
                    timeout=self.request_timeout_seconds,
                )
                if response.status_code == 200:
                    return response.json()

                if response.status_code == 429 or 500 <= response.status_code < 600:
                    wait_seconds = (2**attempt) + 0.5
                    print(
                        f"Retryable FRED response {response.status_code} for {request_url}; "
                        f"waiting {wait_seconds:.1f}s"
                    )
                    time_module.sleep(wait_seconds)
                    continue

                raise RuntimeError(
                    f"FRED API error {response.status_code} for {endpoint}: {response.text[:500]}"
                )
            except requests.RequestException as exc:
                if attempt == self.max_retries - 1:
                    raise RuntimeError(
                        f"FRED request failed for endpoint {endpoint}"
                    ) from exc

                wait_seconds = (2**attempt) + 0.5
                print(
                    f"Request exception for {request_url}: {exc}; "
                    f"waiting {wait_seconds:.1f}s before retry"
                )
                time_module.sleep(wait_seconds)

        raise RuntimeError(f"Exhausted retries for FRED endpoint {endpoint}")

    @staticmethod
    def parse_optional_date(raw_value: str | None) -> date | None:
        """Parse an optional API date string."""

        if not raw_value:
            return None
        return datetime.strptime(raw_value, "%Y-%m-%d").date()

    @staticmethod
    def parse_value(value_raw: str | None) -> float | None:
        """Parse a FRED observation value while preserving raw text separately."""

        if value_raw is None:
            return None

        normalized = value_raw.strip()
        if normalized in {"", "."}:
            return None

        try:
            return float(normalized)
        except ValueError:
            return None

    def fetch_series_metadata(
        self,
        session: requests.Session,
        api_key: str,
        series_id: str,
        run_id: str,
        ingested_at: datetime,
    ) -> dict:
        """Fetch and normalize one series-level FRED metadata record."""

        payload = self.request_json(
            session=session,
            endpoint="series",
            params={
                "series_id": series_id,
                "api_key": api_key,
                "file_type": "json",
            },
        )

        series_items = payload.get("seriess", [])
        if not series_items:
            raise RuntimeError(
                f"No FRED series metadata returned for series_id={series_id}"
            )

        series_item = series_items[0]
        payload_hash = hashlib.sha256(
            json.dumps(series_item, sort_keys=True, separators=(",", ":")).encode(
                "utf-8"
            )
        ).hexdigest()

        return {
            "series_id": series_id,
            "title": series_item.get("title"),
            "frequency": series_item.get("frequency"),
            "frequency_short": series_item.get("frequency_short"),
            "units": series_item.get("units"),
            "units_short": series_item.get("units_short"),
            "seasonal_adjustment": series_item.get("seasonal_adjustment"),
            "seasonal_adjustment_short": series_item.get("seasonal_adjustment_short"),
            "observation_start": self.parse_optional_date(
                series_item.get("observation_start")
            ),
            "observation_end": self.parse_optional_date(
                series_item.get("observation_end")
            ),
            "last_updated": series_item.get("last_updated"),
            "notes": series_item.get("notes"),
            "ingested_at": ingested_at,
            "run_id": run_id,
            "payload_hash": payload_hash,
        }

    def fetch_series_observations(
        self,
        session: requests.Session,
        api_key: str,
        series_id: str,
        start_date: date,
        end_date: date,
        run_id: str,
        ingested_at: datetime,
    ) -> tuple[list[dict], FredSeriesIngestionStats]:
        """Fetch and normalize one FRED observation series over the requested window."""

        offset = 0
        total_count: int | None = None
        records: list[dict] = []
        stats = FredSeriesIngestionStats()

        while total_count is None or offset < total_count:
            payload = self.request_json(
                session=session,
                endpoint="series/observations",
                params={
                    "series_id": series_id,
                    "api_key": api_key,
                    "file_type": "json",
                    "observation_start": start_date.isoformat(),
                    "observation_end": end_date.isoformat(),
                    "realtime_start": self.complete_realtime_start,
                    "realtime_end": self.complete_realtime_end,
                    "units": "lin",
                    "output_type": 1,
                    "sort_order": "asc",
                    "limit": self.default_limit,
                    "offset": offset,
                },
            )

            stats.request_pages += 1
            total_count = int(payload.get("count", 0))
            limit = int(payload.get("limit", self.default_limit))
            observations = payload.get("observations", [])
            stats.api_rows_fetched += len(observations)

            for observation in observations:
                payload_hash = hashlib.sha256(
                    json.dumps(
                        observation, sort_keys=True, separators=(",", ":")
                    ).encode("utf-8")
                ).hexdigest()
                value_raw = observation.get("value")
                records.append(
                    {
                        "series_id": series_id,
                        "observation_date": parse_iso_date(
                            "observation_date", observation["date"]
                        ),
                        "realtime_start": parse_iso_date(
                            "realtime_start",
                            observation["realtime_start"],
                        ),
                        "realtime_end": parse_iso_date(
                            "realtime_end", observation["realtime_end"]
                        ),
                        "value_raw": value_raw,
                        "value": self.parse_value(value_raw),
                        "ingested_at": ingested_at,
                        "run_id": run_id,
                        "payload_hash": payload_hash,
                    }
                )

            if not observations:
                break

            offset += limit

        return records, stats
