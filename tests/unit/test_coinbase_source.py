from __future__ import annotations

from datetime import date, datetime

from lakehouse.common.runtime import UTC
from lakehouse.sources.coinbase import CoinbaseSource


def test_build_request_windows_splits_inclusive_range_into_coinbase_sized_chunks() -> (
    None
):
    source = CoinbaseSource()

    windows = source.build_request_windows(date(2024, 1, 1), date(2024, 12, 31))

    assert len(windows) == 2
    assert windows[0] == (
        datetime(2024, 1, 1, 0, 0, tzinfo=UTC),
        datetime(2024, 10, 27, 0, 0, tzinfo=UTC),
    )
    assert windows[1] == (
        datetime(2024, 10, 27, 0, 0, tzinfo=UTC),
        datetime(2025, 1, 1, 0, 0, tzinfo=UTC),
    )


def test_to_iso_z_serializes_utc_timestamp_in_api_format() -> None:
    assert CoinbaseSource.to_iso_z(datetime(2024, 1, 1, 12, 30, tzinfo=UTC)) == (
        "2024-01-01T12:30:00Z"
    )


def test_fetch_daily_candles_normalizes_records_and_filters_out_of_range_payloads(
    monkeypatch,
) -> None:
    source = CoinbaseSource()
    monkeypatch.setattr(source, "request_pause_seconds", 0.0)

    payload = [
        [1704153600, 41000.0, 43000.0, 42000.0, 42500.0, 1200.5],  # 2024-01-02
        [1704240000, 42000.0, 44000.0, 42500.0, 43000.0, 800.0],   # 2024-01-03
        [1704326400, 43000.0, 45000.0, 43000.0, 44000.0, 700.0],   # 2024-01-04
        [1704067200, 40000.0, 42000.0, 41000.0, 41500.0, 500.0],   # 2024-01-01
        {"unexpected": "shape"},
    ]

    monkeypatch.setattr(source, "request_json", lambda *args, **kwargs: payload)

    records, stats = source.fetch_daily_candles(
        session=object(),  # type: ignore[arg-type]
        product_id="BTC-USD",
        start_date=date(2024, 1, 2),
        end_date=date(2024, 1, 3),
        run_id="run-1",
    )

    assert len(records) == 2
    assert [record["bar_date"] for record in records] == [
        date(2024, 1, 2),
        date(2024, 1, 3),
    ]
    assert all(record["product_id"] == "BTC-USD" for record in records)
    assert all(record["run_id"] == "run-1" for record in records)
    assert all(isinstance(record["payload_hash"], str) for record in records)
    assert stats.api_rows_fetched == 5
    assert stats.rows_in_requested_range == 2
    assert stats.request_windows == 1
