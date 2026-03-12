from __future__ import annotations

from lakehouse.common.models import ObservabilityIngestionResult
from lakehouse.pipelines.obs import (
    build_dq_specs,
    build_metadata_json,
    build_monitored_pipelines,
)


def test_build_monitored_pipelines_covers_all_phase1_pipelines() -> None:
    monitored = build_monitored_pipelines("market_macro")

    assert len(monitored) == 11
    assert {spec["pipeline_name"] for spec in monitored} == {
        "bronze_market_coinbase",
        "bronze_macro_ecb_fx_ref_rates_daily",
        "bronze_macro_fred_series",
        "silver_market_crypto_ohlc_1d",
        "silver_macro_ecb_fx_ref_rates_daily",
        "silver_macro_fred_series_clean",
        "gold_market_crypto_returns_1d",
        "gold_market_crypto_volatility_1d",
        "gold_macro_indicators_ecb",
        "gold_macro_indicators_fred",
        "gold_cross_crypto_macro_features_1d",
    }


def test_build_monitored_pipelines_keeps_macro_gold_filters() -> None:
    monitored = {
        spec["pipeline_name"]: spec for spec in build_monitored_pipelines("market_macro")
    }

    assert monitored["gold_macro_indicators_ecb"]["filter_sql"] == "source_system = 'ecb'"
    assert monitored["gold_macro_indicators_fred"]["filter_sql"] == "source_system = 'fred'"


def test_build_dq_specs_targets_only_the_three_silver_quarantine_tables() -> None:
    dq_specs = build_dq_specs("market_macro")

    assert len(dq_specs) == 3
    assert {(spec["pipeline_name"], spec["source_name"]) for spec in dq_specs} == {
        ("silver_market_crypto_ohlc_1d", "coinbase"),
        ("silver_macro_ecb_fx_ref_rates_daily", "ecb"),
        ("silver_macro_fred_series_clean", "fred"),
    }


def test_build_metadata_json_is_deterministic() -> None:
    assert build_metadata_json({"b": 1, "a": 2}) == '{"a": 2, "b": 1}'


def test_observability_ingestion_result_as_dict_matches_notebook_contract() -> None:
    result = ObservabilityIngestionResult(
        status="success",
        pipeline_name="obs_pipeline_observability_metrics",
        catalog="market_macro",
        run_id="run-1",
        state_table="market_macro.obs.obs_ingestion_state",
        dq_metrics_table="market_macro.obs.obs_dq_metrics",
        run_log_table="market_macro.obs.obs_pipeline_run_log",
        pipelines_observed=11,
        pipelines_with_state=11,
        pipelines_missing_state=[],
        dq_source_tables=3,
        dq_source_rows_read=0,
        dq_rows_ready=0,
        dq_rows_to_update=0,
        dq_rows_to_insert=0,
        dq_rows_merged=0,
        total_observed_rows=1249,
        pipelines_without_rows=[],
        per_pipeline_observed_rows={"gold_cross_crypto_macro_features_1d": 101},
    )

    assert result.as_dict() == {
        "status": "success",
        "pipeline_name": "obs_pipeline_observability_metrics",
        "catalog": "market_macro",
        "run_id": "run-1",
        "state_table": "market_macro.obs.obs_ingestion_state",
        "dq_metrics_table": "market_macro.obs.obs_dq_metrics",
        "run_log_table": "market_macro.obs.obs_pipeline_run_log",
        "pipelines_observed": 11,
        "pipelines_with_state": 11,
        "pipelines_missing_state": [],
        "dq_source_tables": 3,
        "dq_source_rows_read": 0,
        "dq_rows_ready": 0,
        "dq_rows_to_update": 0,
        "dq_rows_to_insert": 0,
        "dq_rows_merged": 0,
        "total_observed_rows": 1249,
        "pipelines_without_rows": [],
        "per_pipeline_observed_rows": {
            "gold_cross_crypto_macro_features_1d": 101
        },
    }
