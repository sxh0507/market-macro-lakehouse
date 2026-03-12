from __future__ import annotations

from datetime import date, datetime

from lakehouse.common.runtime import UTC
from lakehouse.sources.fred import FredSource


def test_parse_optional_date_and_parse_value_handle_missing_and_numeric_inputs() -> None:
    assert FredSource.parse_optional_date("2024-01-01") == date(2024, 1, 1)
    assert FredSource.parse_optional_date("") is None
    assert FredSource.parse_value("123.45") == 123.45
    assert FredSource.parse_value(".") is None
    assert FredSource.parse_value("not-a-number") is None


def test_fetch_series_metadata_normalizes_expected_fields(monkeypatch) -> None:
    source = FredSource()
    ingested_at = datetime(2024, 3, 1, 10, 0, tzinfo=UTC)

    monkeypatch.setattr(
        source,
        "request_json",
        lambda **kwargs: {
            "seriess": [
                {
                    "title": "Consumer Price Index for All Urban Consumers",
                    "frequency": "Monthly",
                    "frequency_short": "M",
                    "units": "Index 1982-1984=100",
                    "units_short": "Index",
                    "seasonal_adjustment": "Seasonally Adjusted",
                    "seasonal_adjustment_short": "SA",
                    "observation_start": "1947-01-01",
                    "observation_end": "2026-01-01",
                    "last_updated": "2026-02-12 07:42:02-06",
                    "notes": "Sample note",
                }
            ]
        },
    )

    record = source.fetch_series_metadata(
        session=object(),  # type: ignore[arg-type]
        api_key="secret",
        series_id="CPIAUCSL",
        run_id="run-1",
        ingested_at=ingested_at,
    )

    assert record["series_id"] == "CPIAUCSL"
    assert record["frequency_short"] == "M"
    assert record["units"] == "Index 1982-1984=100"
    assert record["observation_start"] == date(1947, 1, 1)
    assert record["observation_end"] == date(2026, 1, 1)
    assert record["ingested_at"] == ingested_at
    assert record["run_id"] == "run-1"
    assert isinstance(record["payload_hash"], str)


def test_fetch_series_observations_paginates_and_preserves_value_raw(monkeypatch) -> None:
    source = FredSource()
    ingested_at = datetime(2024, 3, 1, 10, 0, tzinfo=UTC)
    responses = [
        {
            "count": 3,
            "limit": 2,
            "observations": [
                {
                    "date": "2024-01-01",
                    "realtime_start": "2024-02-01",
                    "realtime_end": "2024-02-29",
                    "value": "5.33",
                },
                {
                    "date": "2024-02-01",
                    "realtime_start": "2024-03-01",
                    "realtime_end": "2024-03-31",
                    "value": ".",
                },
            ],
        },
        {
            "count": 3,
            "limit": 2,
            "observations": [
                {
                    "date": "2024-03-01",
                    "realtime_start": "2024-04-01",
                    "realtime_end": "2024-04-30",
                    "value": "5.40",
                }
            ],
        },
    ]

    def fake_request_json(**kwargs):
        return responses.pop(0)

    monkeypatch.setattr(source, "request_json", fake_request_json)

    records, stats = source.fetch_series_observations(
        session=object(),  # type: ignore[arg-type]
        api_key="secret",
        series_id="FEDFUNDS",
        start_date=date(2024, 1, 1),
        end_date=date(2024, 3, 31),
        run_id="run-1",
        ingested_at=ingested_at,
    )

    assert len(records) == 3
    assert records[0]["value_raw"] == "5.33"
    assert records[0]["value"] == 5.33
    assert records[1]["value_raw"] == "."
    assert records[1]["value"] is None
    assert records[2]["observation_date"] == date(2024, 3, 1)
    assert all(record["run_id"] == "run-1" for record in records)
    assert stats.request_pages == 2
    assert stats.api_rows_fetched == 3
