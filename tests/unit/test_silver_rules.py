from __future__ import annotations

from datetime import date

import pytest

from lakehouse.pipelines.silver import (
    FRED_STRUCTURAL_DQ_REASONS,
    determine_fred_dq_reason,
)


@pytest.mark.parametrize(
    ("payload", "expected_reason"),
    [
        (
            {
                "series_id": None,
                "observation_date": date(2024, 1, 1),
                "realtime_start": date(2024, 1, 1),
                "realtime_end": date(2024, 1, 31),
                "metadata_present": True,
                "value": 1.0,
            },
            "MISSING_SERIES_ID",
        ),
        (
            {
                "series_id": "CPIAUCSL",
                "observation_date": None,
                "realtime_start": date(2024, 1, 1),
                "realtime_end": date(2024, 1, 31),
                "metadata_present": True,
                "value": 1.0,
            },
            "MISSING_OBSERVATION_DATE",
        ),
        (
            {
                "series_id": "CPIAUCSL",
                "observation_date": date(2024, 1, 1),
                "realtime_start": date(2024, 2, 1),
                "realtime_end": date(2024, 1, 31),
                "metadata_present": True,
                "value": 1.0,
            },
            "INVALID_REALTIME_ORDER",
        ),
        (
            {
                "series_id": "CPIAUCSL",
                "observation_date": date(2024, 1, 1),
                "realtime_start": date(2024, 1, 1),
                "realtime_end": date(2024, 1, 31),
                "metadata_present": None,
                "value": 1.0,
            },
            "MISSING_METADATA",
        ),
        (
            {
                "series_id": "CPIAUCSL",
                "observation_date": date(2024, 1, 1),
                "realtime_start": date(2024, 1, 1),
                "realtime_end": date(2024, 1, 31),
                "metadata_present": True,
                "value": None,
            },
            "NULL_VALUE",
        ),
        (
            {
                "series_id": "CPIAUCSL",
                "observation_date": date(2024, 1, 1),
                "realtime_start": date(2024, 1, 1),
                "realtime_end": date(2024, 1, 31),
                "metadata_present": True,
                "value": 312.4,
            },
            None,
        ),
    ],
)
def test_determine_fred_dq_reason_matches_expected_rule(
    payload: dict, expected_reason: str | None
) -> None:
    assert determine_fred_dq_reason(**payload) == expected_reason


def test_determine_fred_dq_reason_prioritizes_structure_before_missing_metadata() -> (
    None
):
    reason = determine_fred_dq_reason(
        series_id="CPIAUCSL",
        observation_date=date(2024, 1, 1),
        realtime_start=date(2024, 2, 1),
        realtime_end=date(2024, 1, 31),
        metadata_present=None,
        value=None,
    )

    assert reason == "INVALID_REALTIME_ORDER"


def test_fred_structural_dq_reasons_constant_is_stable() -> None:
    assert FRED_STRUCTURAL_DQ_REASONS == (
        "MISSING_SERIES_ID",
        "MISSING_OBSERVATION_DATE",
        "MISSING_REALTIME_START",
        "MISSING_REALTIME_END",
        "INVALID_REALTIME_ORDER",
    )
