from __future__ import annotations

from datetime import date, datetime, timezone

from lakehouse.pipelines.gold import (
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
