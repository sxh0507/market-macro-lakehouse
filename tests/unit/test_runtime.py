from __future__ import annotations

from datetime import date

import pytest

from lakehouse.common.runtime import (
    parse_indicator_ids,
    parse_product_ids,
    parse_quote_currencies,
    parse_series_ids,
    resolve_date_window,
)


def test_parse_product_ids_normalizes_case_and_deduplicates() -> None:
    assert parse_product_ids("btc-usd, ETH-USD,btc-usd , sol-usd") == [
        "BTC-USD",
        "ETH-USD",
        "SOL-USD",
    ]


def test_parse_quote_currencies_requires_three_letter_iso_codes() -> None:
    with pytest.raises(ValueError, match="3-letter ISO codes"):
        parse_quote_currencies("USD,USDT")


@pytest.mark.parametrize(
    ("parser", "raw_value", "expected"),
    [
        (parse_series_ids, "cpiaucsl, FEDFUNDS,cpiaucsl", ["CPIAUCSL", "FEDFUNDS"]),
        (
            parse_indicator_ids,
            "ecb_fx_ref_eur_usd, FRED_CPIAUCSL,ecb_fx_ref_eur_usd",
            ["ECB_FX_REF_EUR_USD", "FRED_CPIAUCSL"],
        ),
    ],
)
def test_comma_separated_parsers_normalize_and_deduplicate(
    parser, raw_value: str, expected: list[str]
) -> None:
    assert parser(raw_value) == expected


def test_resolve_date_window_backfill_respects_explicit_bounds() -> None:
    start_date, end_date = resolve_date_window(
        mode="backfill",
        start_date_raw="2024-01-01",
        end_date_raw="2024-01-10",
        lookback_days_raw="",
        latest_complete_date=date(2024, 1, 31),
    )

    assert start_date == date(2024, 1, 1)
    assert end_date == date(2024, 1, 10)


def test_resolve_date_window_incremental_uses_lookback_and_default_latest_complete_date() -> (
    None
):
    start_date, end_date = resolve_date_window(
        mode="incremental",
        start_date_raw="",
        end_date_raw="",
        lookback_days_raw="3",
        latest_complete_date=date(2024, 3, 5),
    )

    assert start_date == date(2024, 3, 3)
    assert end_date == date(2024, 3, 5)


def test_resolve_date_window_rejects_future_end_date_for_incremental() -> None:
    with pytest.raises(ValueError, match="latest completed UTC day: 2024-03-05"):
        resolve_date_window(
            mode="incremental",
            start_date_raw="",
            end_date_raw="2024-03-06",
            lookback_days_raw="3",
            latest_complete_date=date(2024, 3, 5),
        )
