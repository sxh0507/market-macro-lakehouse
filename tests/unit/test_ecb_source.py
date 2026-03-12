from __future__ import annotations

from datetime import date

import pytest

from lakehouse.sources.ecb import EcbSource


def test_parse_quote_currencies_normalizes_and_deduplicates() -> None:
    assert EcbSource.parse_quote_currencies("usd, GBP,usd") == ["USD", "GBP"]


def test_parse_quote_currencies_rejects_non_iso_values() -> None:
    with pytest.raises(ValueError, match="3-letter ISO codes"):
        EcbSource.parse_quote_currencies("USD,USDT")


def test_build_series_key_uses_expected_ecb_contract() -> None:
    source = EcbSource()

    assert source.build_series_key(["USD", "JPY"]) == "D.USD+JPY.EUR.SP00.A"


def test_fetch_reference_rates_parses_daily_rows_and_filters_invalid_payload(
    monkeypatch,
) -> None:
    source = EcbSource()
    csv_payload = """FREQ,CURRENCY,CURRENCY_DENOM,TIME_PERIOD,OBS_VALUE
D,USD,EUR,2024-01-02,1.09
D,USD,EUR,2024-01-03,1.08
M,USD,EUR,2024-01-31,1.07
D,JPY,EUR,2024-01-02,160.1
D,USD,GBP,2024-01-02,0.85
D,USD,EUR,,1.05
"""

    monkeypatch.setattr(
        source,
        "request_csv",
        lambda **kwargs: (
            "https://data-api.ecb.europa.eu/service/data/EXR/D.USD.EUR.SP00.A",
            "D.USD.EUR.SP00.A",
            csv_payload,
        ),
    )

    records, per_currency_stats, request_url, series_key = source.fetch_reference_rates(
        session=object(),  # type: ignore[arg-type]
        quote_currencies=["USD"],
        start_date=date(2024, 1, 2),
        end_date=date(2024, 1, 3),
        run_id="run-1",
    )

    assert request_url.endswith("/D.USD.EUR.SP00.A")
    assert series_key == "D.USD.EUR.SP00.A"
    assert len(records) == 2
    assert [record["rate_date"] for record in records] == [
        date(2024, 1, 2),
        date(2024, 1, 3),
    ]
    assert all(record["base_currency"] == "EUR" for record in records)
    assert all(record["quote_currency"] == "USD" for record in records)
    assert all(record["run_id"] == "run-1" for record in records)
    assert per_currency_stats["USD"].api_rows_fetched == 2
    assert per_currency_stats["USD"].rows_in_requested_range == 2
    assert per_currency_stats["USD"].request_windows == 1
