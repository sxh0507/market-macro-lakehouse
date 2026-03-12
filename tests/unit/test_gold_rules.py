from __future__ import annotations

from datetime import date, datetime, timezone

from lakehouse.pipelines.gold import (
    CROSS_FILL_POLICY,
    FRED_INDICATOR_GROUPS,
    CrossGoldIngestionResult,
    MacroGoldIngestionResult,
    _sortable_optional,
    build_macro_asof_feature_map,
    select_latest_fred_revision_rows,
)


UTC = timezone.utc


def test_select_latest_fred_revision_rows_keeps_latest_revision_per_observation() -> (
    None
):
    rows = [
        {
            "series_id": "FEDFUNDS",
            "observation_date": date(2024, 1, 1),
            "realtime_start": date(2024, 1, 1),
            "realtime_end": date(2024, 1, 31),
            "ingested_at": datetime(2024, 2, 1, 10, 0, tzinfo=UTC),
            "value": 5.33,
        },
        {
            "series_id": "FEDFUNDS",
            "observation_date": date(2024, 1, 1),
            "realtime_start": date(2024, 2, 1),
            "realtime_end": date(2024, 2, 29),
            "ingested_at": datetime(2024, 3, 1, 10, 0, tzinfo=UTC),
            "value": 5.5,
        },
        {
            "series_id": "FEDFUNDS",
            "observation_date": date(2024, 2, 1),
            "realtime_start": date(2024, 2, 1),
            "realtime_end": date(2024, 2, 29),
            "ingested_at": datetime(2024, 3, 1, 10, 0, tzinfo=UTC),
            "value": 5.45,
        },
    ]

    latest_rows = select_latest_fred_revision_rows(rows)

    assert len(latest_rows) == 2
    assert latest_rows[0]["observation_date"] == date(2024, 1, 1)
    assert latest_rows[0]["value"] == 5.5
    assert latest_rows[1]["observation_date"] == date(2024, 2, 1)
    assert latest_rows[1]["value"] == 5.45


def test_select_latest_fred_revision_rows_uses_ingested_at_as_tie_breaker() -> None:
    rows = [
        {
            "series_id": "CPIAUCSL",
            "observation_date": date(2024, 1, 1),
            "realtime_start": date(2024, 2, 1),
            "realtime_end": date(2024, 2, 29),
            "ingested_at": datetime(2024, 3, 1, 9, 0, tzinfo=UTC),
            "value": 311.0,
        },
        {
            "series_id": "CPIAUCSL",
            "observation_date": date(2024, 1, 1),
            "realtime_start": date(2024, 2, 1),
            "realtime_end": date(2024, 2, 29),
            "ingested_at": datetime(2024, 3, 1, 10, 0, tzinfo=UTC),
            "value": 312.0,
        },
    ]

    latest_rows = select_latest_fred_revision_rows(rows)

    assert latest_rows == [rows[1]]


def test_build_macro_asof_feature_map_uses_latest_observation_not_after_feature_date() -> (
    None
):
    feature_dates = [date(2024, 1, 15), date(2024, 2, 15)]
    macro_rows = [
        {
            "indicator_id": "ECB_FX_REF_EUR_USD",
            "observation_date": date(2024, 1, 10),
            "value": 1.09,
            "computed_at": datetime(2024, 1, 10, 18, 0, tzinfo=UTC),
            "run_id": "run-1",
        },
        {
            "indicator_id": "ECB_FX_REF_EUR_USD",
            "observation_date": date(2024, 2, 10),
            "value": 1.08,
            "computed_at": datetime(2024, 2, 10, 18, 0, tzinfo=UTC),
            "run_id": "run-2",
        },
        {
            "indicator_id": "FRED_FEDFUNDS",
            "observation_date": date(2024, 1, 1),
            "value": 5.33,
            "computed_at": datetime(2024, 2, 1, 9, 0, tzinfo=UTC),
            "run_id": "run-3",
        },
        {
            "indicator_id": "FRED_FEDFUNDS",
            "observation_date": date(2024, 1, 1),
            "value": 5.34,
            "computed_at": datetime(2024, 2, 1, 10, 0, tzinfo=UTC),
            "run_id": "run-4",
        },
    ]

    feature_map = build_macro_asof_feature_map(feature_dates, macro_rows)

    assert feature_map[date(2024, 1, 15)] == {
        "ECB_FX_REF_EUR_USD": 1.09,
        "FRED_FEDFUNDS": 5.34,
    }
    assert feature_map[date(2024, 2, 15)] == {
        "ECB_FX_REF_EUR_USD": 1.08,
        "FRED_FEDFUNDS": 5.34,
    }


def test_build_macro_asof_feature_map_skips_null_values_and_missing_history() -> None:
    feature_map = build_macro_asof_feature_map(
        [date(2024, 1, 15)],
        [
            {
                "indicator_id": "FRED_GDP",
                "observation_date": date(2024, 2, 1),
                "value": 29000.0,
                "computed_at": datetime(2024, 2, 2, 10, 0, tzinfo=UTC),
                "run_id": "run-1",
            },
            {
                "indicator_id": "FRED_CPIAUCSL",
                "observation_date": date(2024, 1, 1),
                "value": None,
                "computed_at": datetime(2024, 1, 2, 10, 0, tzinfo=UTC),
                "run_id": "run-2",
            },
        ],
    )

    assert feature_map == {}


def test_build_macro_asof_feature_map_uses_computed_at_then_run_id_as_tie_breakers() -> (
    None
):
    feature_map = build_macro_asof_feature_map(
        [date(2024, 1, 15)],
        [
            {
                "indicator_id": "FRED_FEDFUNDS",
                "observation_date": date(2024, 1, 1),
                "value": 5.33,
                "computed_at": datetime(2024, 2, 1, 9, 0, tzinfo=UTC),
                "run_id": "run-1",
            },
            {
                "indicator_id": "FRED_FEDFUNDS",
                "observation_date": date(2024, 1, 1),
                "value": 5.34,
                "computed_at": datetime(2024, 2, 1, 10, 0, tzinfo=UTC),
                "run_id": "run-0",
            },
            {
                "indicator_id": "FRED_FEDFUNDS",
                "observation_date": date(2024, 1, 1),
                "value": 5.35,
                "computed_at": datetime(2024, 2, 1, 10, 0, tzinfo=UTC),
                "run_id": "run-9",
            },
        ],
    )

    assert feature_map == {date(2024, 1, 15): {"FRED_FEDFUNDS": 5.35}}


def test_sortable_optional_orders_none_before_present_values() -> None:
    assert _sortable_optional(None) < _sortable_optional("run-1")


def test_macro_gold_ingestion_result_exposes_optional_fred_fields() -> None:
    result = MacroGoldIngestionResult(
        status="success",
        source_system="fred",
        mode="incremental",
        start_date="2024-01-01",
        end_date="2024-03-31",
        source_table="market_macro.slv_macro.fred_series_clean",
        target_table="market_macro.gld_macro.dp_macro_indicators",
        run_id="run-1",
        series_ids=["CPIAUCSL", "FEDFUNDS", "GDP"],
        metadata_table="market_macro.brz_macro.raw_fred_series_metadata",
        revision_policy="latest_available",
        rows_read=17,
        rows_after_revision_collapse=7,
        rows_ready=7,
        rows_to_update=0,
        rows_to_insert=7,
        rows_merged=7,
        per_indicator_rows_ready={"FRED_CPIAUCSL": 3},
    )

    assert result.as_dict()["revision_policy"] == "latest_available"
    assert result.as_dict()["metadata_table"] == "market_macro.brz_macro.raw_fred_series_metadata"
    assert result.as_dict()["series_ids"] == ["CPIAUCSL", "FEDFUNDS", "GDP"]


def test_cross_gold_ingestion_result_keeps_fill_contract() -> None:
    result = CrossGoldIngestionResult(
        status="success",
        mode="incremental",
        product_ids=["BTC-USD"],
        macro_indicator_ids=["ECB_FX_REF_EUR_USD", "FRED_CPIAUCSL"],
        start_date="2024-01-01",
        end_date="2024-03-31",
        source_market_returns_table="market_macro.gld_market.dp_crypto_returns_1d",
        source_market_volatility_table="market_macro.gld_market.dp_crypto_volatility_1d",
        source_macro_table="market_macro.gld_macro.dp_macro_indicators",
        target_table="market_macro.gld_cross.dp_crypto_macro_features_1d",
        fill_policy=CROSS_FILL_POLICY,
        backtest_safe=False,
        run_id="run-1",
        rows_market_returns_read=91,
        rows_market_volatility_read=91,
        rows_market_base=91,
        rows_macro_selected=10,
        rows_macro_asof_ready=91,
        rows_ready=91,
        rows_to_update=0,
        rows_to_insert=91,
        rows_merged=91,
    )

    payload = result.as_dict()

    assert payload["fill_policy"] == CROSS_FILL_POLICY
    assert payload["backtest_safe"] is False
    assert payload["pipeline_name"] == "gold_cross_crypto_macro_features_1d"


def test_fred_indicator_groups_constant_is_stable() -> None:
    assert FRED_INDICATOR_GROUPS == {
        "CPIAUCSL": "inflation",
        "FEDFUNDS": "policy_rate",
        "GDP": "output",
    }
