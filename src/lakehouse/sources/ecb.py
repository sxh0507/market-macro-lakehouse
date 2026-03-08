"""ECB source adapter and fetch helpers for Bronze ingestion."""

from __future__ import annotations

import csv
import hashlib
import json
import time as time_module
from datetime import date, datetime
from io import StringIO
from zoneinfo import ZoneInfo

import requests

from lakehouse.common.models import ProductIngestionStats
from lakehouse.common.runtime import UTC
from lakehouse.sources.base import SourceAdapter


class EcbSource(SourceAdapter):
    """Adapter and HTTP client configuration for ECB reference FX data."""

    request_timeout_seconds = 30
    max_retries = 5
    user_agent = "market-macro-lakehouse/phase1"
    local_timezone = ZoneInfo("Europe/Berlin")

    def __init__(self, dataset: str = "ecb_fx_reference_rates") -> None:
        super().__init__(
            source_name="ecb",
            dataset=dataset,
            base_url="https://data-api.ecb.europa.eu/service/data/EXR",
            bronze_table="market_macro.brz_macro.raw_ecb_fx_ref_rates_daily",
        )
        self.dataset = dataset

    @staticmethod
    def parse_quote_currencies(raw_value: str) -> list[str]:
        """Parse a comma-separated currency list into unique uppercase ISO codes."""

        quote_currencies = [item.strip().upper() for item in raw_value.split(",") if item.strip()]
        quote_currencies = list(dict.fromkeys(quote_currencies))

        if not quote_currencies:
            raise ValueError("quote_currencies cannot be empty")

        for currency in quote_currencies:
            if len(currency) != 3 or not currency.isalpha():
                raise ValueError(f"quote_currencies must contain 3-letter ISO codes, got: {currency}")

        return quote_currencies

    @staticmethod
    def build_series_key(quote_currencies: list[str]) -> str:
        """Build the ECB EXR dataset key for the requested quote currencies."""

        return f"D.{'+'.join(quote_currencies)}.EUR.SP00.A"

    def request_csv(
        self,
        session: requests.Session,
        quote_currencies: list[str],
        start_date: date,
        end_date: date,
    ) -> tuple[str, str, str]:
        """Perform an ECB CSV request with retry logic."""

        series_key = self.build_series_key(quote_currencies)
        request_url = f"{self.base_url}/{series_key}"
        params = {
            "startPeriod": start_date.isoformat(),
            "endPeriod": end_date.isoformat(),
            "format": "csvdata",
        }
        headers = {
            "Accept": "text/csv",
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
                    return request_url, series_key, response.text

                if response.status_code == 429 or 500 <= response.status_code < 600:
                    wait_seconds = (2**attempt) + 0.5
                    print(
                        f"Retryable ECB response {response.status_code} for {request_url}; "
                        f"waiting {wait_seconds:.1f}s"
                    )
                    time_module.sleep(wait_seconds)
                    continue

                raise RuntimeError(f"ECB API error {response.status_code} for {request_url}: {response.text}")
            except requests.RequestException as exc:
                if attempt == self.max_retries - 1:
                    raise RuntimeError(f"ECB request failed for {request_url}") from exc

                wait_seconds = (2**attempt) + 0.5
                print(f"Request exception for {request_url}: {exc}; waiting {wait_seconds:.1f}s before retry")
                time_module.sleep(wait_seconds)

        raise RuntimeError(f"Exhausted retries for {request_url}")

    def fetch_reference_rates(
        self,
        session: requests.Session,
        quote_currencies: list[str],
        start_date: date,
        end_date: date,
        run_id: str,
    ) -> tuple[list[dict], dict[str, ProductIngestionStats], str, str]:
        """Fetch and normalize ECB daily FX reference rates for the requested window."""

        request_url, series_key, payload = self.request_csv(
            session=session,
            quote_currencies=quote_currencies,
            start_date=start_date,
            end_date=end_date,
        )

        ingested_at = datetime.now(UTC)
        quote_currency_set = set(quote_currencies)
        per_currency_stats = {
            quote_currency: ProductIngestionStats(request_windows=1)
            for quote_currency in quote_currencies
        }
        records: list[dict] = []
        reader = csv.DictReader(StringIO(payload))

        for row in reader:
            if (row.get("FREQ") or "").strip() != "D":
                continue

            quote_currency = (row.get("CURRENCY") or "").strip().upper()
            base_currency = (row.get("CURRENCY_DENOM") or "").strip().upper()
            time_period = (row.get("TIME_PERIOD") or "").strip()
            obs_value = (row.get("OBS_VALUE") or "").strip()

            if quote_currency not in quote_currency_set or base_currency != "EUR":
                continue
            if not time_period or not obs_value:
                continue

            rate_date = datetime.strptime(time_period, "%Y-%m-%d").date()
            if rate_date < start_date or rate_date > end_date:
                continue

            payload_hash = hashlib.sha256(
                json.dumps(row, sort_keys=True, separators=(",", ":")).encode("utf-8")
            ).hexdigest()

            per_currency_stats[quote_currency].api_rows_fetched += 1
            per_currency_stats[quote_currency].rows_in_requested_range += 1
            records.append(
                {
                    "base_currency": base_currency,
                    "quote_currency": quote_currency,
                    "rate_date": rate_date,
                    "rate": float(obs_value),
                    "ingested_at": ingested_at,
                    "run_id": run_id,
                    "payload_hash": payload_hash,
                }
            )

        for quote_currency in quote_currencies:
            per_currency_stats.setdefault(quote_currency, ProductIngestionStats(request_windows=1))

        return records, per_currency_stats, request_url, series_key
