"""Observability pipeline orchestration."""

from __future__ import annotations

import json
from datetime import datetime
from typing import Any, Callable

from lakehouse.common.models import ObservabilityIngestionResult
from lakehouse.common.runtime import UTC


def build_monitored_pipelines(catalog: str) -> list[dict[str, str | None]]:
    """Return the monitored pipeline contracts for the phase-1 lakehouse DAG."""

    return [
        {
            "pipeline_name": "bronze_market_coinbase",
            "source_name": "coinbase",
            "target_table": f"{catalog}.brz_market.raw_coinbase_ohlc_1d",
            "filter_sql": None,
        },
        {
            "pipeline_name": "bronze_macro_ecb_fx_ref_rates_daily",
            "source_name": "ecb",
            "target_table": f"{catalog}.brz_macro.raw_ecb_fx_ref_rates_daily",
            "filter_sql": None,
        },
        {
            "pipeline_name": "bronze_macro_fred_series",
            "source_name": "fred",
            "target_table": f"{catalog}.brz_macro.raw_fred_series",
            "filter_sql": None,
        },
        {
            "pipeline_name": "silver_market_crypto_ohlc_1d",
            "source_name": "coinbase",
            "target_table": f"{catalog}.slv_market.crypto_ohlc_1d",
            "filter_sql": None,
        },
        {
            "pipeline_name": "silver_macro_ecb_fx_ref_rates_daily",
            "source_name": "ecb",
            "target_table": f"{catalog}.slv_macro.ecb_fx_ref_rates_daily",
            "filter_sql": None,
        },
        {
            "pipeline_name": "silver_macro_fred_series_clean",
            "source_name": "fred",
            "target_table": f"{catalog}.slv_macro.fred_series_clean",
            "filter_sql": None,
        },
        {
            "pipeline_name": "gold_market_crypto_returns_1d",
            "source_name": "coinbase",
            "target_table": f"{catalog}.gld_market.dp_crypto_returns_1d",
            "filter_sql": None,
        },
        {
            "pipeline_name": "gold_market_crypto_volatility_1d",
            "source_name": "coinbase",
            "target_table": f"{catalog}.gld_market.dp_crypto_volatility_1d",
            "filter_sql": None,
        },
        {
            "pipeline_name": "gold_macro_indicators_ecb",
            "source_name": "ecb",
            "target_table": f"{catalog}.gld_macro.dp_macro_indicators",
            "filter_sql": "source_system = 'ecb'",
        },
        {
            "pipeline_name": "gold_macro_indicators_fred",
            "source_name": "fred",
            "target_table": f"{catalog}.gld_macro.dp_macro_indicators",
            "filter_sql": "source_system = 'fred'",
        },
        {
            "pipeline_name": "gold_cross_crypto_macro_features_1d",
            "source_name": "cross",
            "target_table": f"{catalog}.gld_cross.dp_crypto_macro_features_1d",
            "filter_sql": None,
        },
    ]


def build_dq_specs(catalog: str) -> list[dict[str, str]]:
    """Return the Silver quarantine sources used for DQ aggregation."""

    return [
        {
            "pipeline_name": "silver_market_crypto_ohlc_1d",
            "layer": "silver",
            "source_name": "coinbase",
            "quarantine_table": f"{catalog}.slv_market.crypto_ohlc_1d_quarantine",
            "target_table": f"{catalog}.slv_market.crypto_ohlc_1d",
        },
        {
            "pipeline_name": "silver_macro_ecb_fx_ref_rates_daily",
            "layer": "silver",
            "source_name": "ecb",
            "quarantine_table": f"{catalog}.slv_macro.ecb_fx_ref_rates_daily_quarantine",
            "target_table": f"{catalog}.slv_macro.ecb_fx_ref_rates_daily",
        },
        {
            "pipeline_name": "silver_macro_fred_series_clean",
            "layer": "silver",
            "source_name": "fred",
            "quarantine_table": f"{catalog}.slv_macro.fred_series_clean_quarantine",
            "target_table": f"{catalog}.slv_macro.fred_series_clean",
        },
    ]


def build_metadata_json(payload: dict[str, Any]) -> str:
    """Serialize notebook-friendly metadata into a deterministic JSON string."""

    return json.dumps(payload, sort_keys=True, default=str)


def ensure_table_exists(spark: Any, table_name: str):
    """Assert that an expected Delta table exists before the pipeline runs."""

    if not spark.catalog.tableExists(table_name):
        raise RuntimeError(
            f"Required table {table_name} does not exist. "
            "Run 00_platform_setup_catalog_schema.ipynb first."
        )


def upsert_pipeline_run_log(spark: Any, log_table: str, payload: dict[str, Any]):
    """Upsert the current obs pipeline run event into the run log table."""

    from delta.tables import DeltaTable  # type: ignore import-not-found
    from pyspark.sql.types import (  # type: ignore import-not-found
        LongType,
        StringType,
        StructField,
        StructType,
        TimestampType,
    )

    schema = StructType(
        [
            StructField("run_id", StringType(), False),
            StructField("pipeline_name", StringType(), False),
            StructField("layer", StringType(), False),
            StructField("source_name", StringType(), True),
            StructField("target_table", StringType(), False),
            StructField("status", StringType(), False),
            StructField("rows_read", LongType(), True),
            StructField("rows_written", LongType(), True),
            StructField("started_at", TimestampType(), False),
            StructField("completed_at", TimestampType(), True),
            StructField("error_message", StringType(), True),
            StructField("metadata_json", StringType(), True),
        ]
    )

    log_df = spark.createDataFrame(
        [
            {
                "run_id": payload["run_id"],
                "pipeline_name": payload["pipeline_name"],
                "layer": payload["layer"],
                "source_name": payload.get("source_name"),
                "target_table": payload["target_table"],
                "status": payload["status"],
                "rows_read": payload.get("rows_read"),
                "rows_written": payload.get("rows_written"),
                "started_at": payload["started_at"],
                "completed_at": payload.get("completed_at"),
                "error_message": payload.get("error_message"),
                "metadata_json": payload.get("metadata_json"),
            }
        ],
        schema=schema,
    )

    DeltaTable.forName(spark, log_table).alias("tgt").merge(
        log_df.alias("src"),
        "tgt.run_id = src.run_id "
        "AND tgt.pipeline_name = src.pipeline_name "
        "AND tgt.target_table = src.target_table",
    ).whenMatchedUpdate(
        set={
            "layer": "src.layer",
            "source_name": "src.source_name",
            "status": "src.status",
            "rows_read": "src.rows_read",
            "rows_written": "src.rows_written",
            "started_at": "src.started_at",
            "completed_at": "src.completed_at",
            "error_message": "src.error_message",
            "metadata_json": "src.metadata_json",
        }
    ).whenNotMatchedInsertAll().execute()


def read_monitored_state(
    spark: Any,
    state_table: str,
    monitored_pipelines: list[dict[str, str | None]],
) -> tuple[Any, int, list[str]]:
    """Join the configured monitored pipeline contracts against obs_ingestion_state."""

    from pyspark.sql import functions as F  # type: ignore import-not-found
    from pyspark.sql.types import (  # type: ignore import-not-found
        StringType,
        StructField,
        StructType,
    )

    schema = StructType(
        [
            StructField("pipeline_name", StringType(), False),
            StructField("source_name", StringType(), True),
            StructField("target_table", StringType(), False),
        ]
    )
    monitored_df = spark.createDataFrame(
        [
            {
                "pipeline_name": str(spec["pipeline_name"]),
                "source_name": spec["source_name"],
                "target_table": str(spec["target_table"]),
            }
            for spec in monitored_pipelines
        ],
        schema=schema,
    )

    raw_state_df = spark.table(state_table).select(
        "pipeline_name",
        "source_name",
        "target_table",
        "watermark_value",
        "watermark_type",
        "last_success_at",
        "last_run_id",
        "status",
        "updated_at",
    )

    state_df = monitored_df.alias("m").join(
        raw_state_df.alias("s"),
        on=["pipeline_name", "source_name", "target_table"],
        how="left",
    ).select(
        "pipeline_name",
        "source_name",
        "target_table",
        "watermark_value",
        "watermark_type",
        "last_success_at",
        "last_run_id",
        "status",
        "updated_at",
    )

    pipelines_missing_state = [
        row["pipeline_name"]
        for row in state_df.filter(F.col("last_run_id").isNull())
        .select("pipeline_name")
        .collect()
    ]
    pipelines_with_state = state_df.filter(F.col("last_run_id").isNotNull()).count()
    return state_df, pipelines_with_state, pipelines_missing_state


def build_observed_row_counts(
    spark: Any,
    monitored_pipelines: list[dict[str, str | None]],
) -> tuple[dict[str, int], list[str]]:
    """Read current target-table row counts for each monitored pipeline."""

    from pyspark.sql import functions as F  # type: ignore import-not-found

    per_pipeline_observed_rows: dict[str, int] = {}
    pipelines_without_rows: list[str] = []

    for spec in monitored_pipelines:
        target_table = str(spec["target_table"])
        ensure_table_exists(spark, target_table)
        observed_df = spark.table(target_table)
        filter_sql = spec.get("filter_sql")
        if filter_sql:
            observed_df = observed_df.filter(F.expr(str(filter_sql)))

        row_count = observed_df.count()
        pipeline_name = str(spec["pipeline_name"])
        per_pipeline_observed_rows[pipeline_name] = row_count
        if row_count == 0:
            pipelines_without_rows.append(pipeline_name)

    return per_pipeline_observed_rows, pipelines_without_rows


def build_dq_metrics_df(
    spark: Any,
    run_id: str,
    dq_specs: list[dict[str, str]],
    measured_at: datetime,
) -> tuple[Any, int]:
    """Aggregate quarantine-table counts into the obs_dq_metrics shape."""

    from pyspark.sql.types import (  # type: ignore import-not-found
        LongType,
        StringType,
        StructField,
        StructType,
        TimestampType,
    )

    schema = StructType(
        [
            StructField("run_id", StringType(), False),
            StructField("pipeline_name", StringType(), False),
            StructField("layer", StringType(), False),
            StructField("source_name", StringType(), True),
            StructField("target_table", StringType(), False),
            StructField("dq_reason", StringType(), False),
            StructField("row_count", LongType(), False),
            StructField("measured_at", TimestampType(), False),
        ]
    )

    rows: list[dict[str, Any]] = []
    dq_source_rows_read = 0

    for spec in dq_specs:
        quarantine_table = spec["quarantine_table"]
        ensure_table_exists(spark, quarantine_table)
        quarantine_df = spark.table(quarantine_table)
        quarantine_row_count = quarantine_df.count()
        dq_source_rows_read += quarantine_row_count

        if quarantine_row_count == 0:
            continue

        for row in quarantine_df.groupBy("dq_reason").count().collect():
            rows.append(
                {
                    "run_id": run_id,
                    "pipeline_name": spec["pipeline_name"],
                    "layer": spec["layer"],
                    "source_name": spec["source_name"],
                    "target_table": spec["target_table"],
                    "dq_reason": row["dq_reason"],
                    "row_count": int(row["count"]),
                    "measured_at": measured_at,
                }
            )

    if not rows:
        return spark.createDataFrame([], schema=schema), dq_source_rows_read

    return spark.createDataFrame(rows, schema=schema), dq_source_rows_read


def merge_dq_metrics(spark: Any, dq_metrics_table: str, dq_metrics_df: Any) -> tuple[int, int, int]:
    """Merge aggregated dq metrics into the obs table."""

    from delta.tables import DeltaTable  # type: ignore import-not-found

    rows_ready = dq_metrics_df.count()
    if rows_ready == 0:
        return 0, 0, 0

    dq_key_columns = ["run_id", "pipeline_name", "target_table", "dq_reason"]
    existing_key_count = (
        dq_metrics_df.select(*dq_key_columns)
        .join(
            spark.table(dq_metrics_table).select(*dq_key_columns),
            on=dq_key_columns,
            how="inner",
        )
        .count()
    )

    DeltaTable.forName(spark, dq_metrics_table).alias("tgt").merge(
        dq_metrics_df.alias("src"),
        "tgt.run_id = src.run_id "
        "AND tgt.pipeline_name = src.pipeline_name "
        "AND tgt.target_table = src.target_table "
        "AND tgt.dq_reason = src.dq_reason",
    ).whenMatchedUpdate(
        set={
            "layer": "src.layer",
            "source_name": "src.source_name",
            "row_count": "src.row_count",
            "measured_at": "src.measured_at",
        }
    ).whenNotMatchedInsertAll().execute()

    rows_to_insert = rows_ready - existing_key_count
    return rows_ready, existing_key_count, rows_to_insert


def run_pipeline_observability_metrics(
    spark: Any,
    *,
    catalog: str = "market_macro",
    run_id: str,
    display_fn: Callable[[Any], None] | None = None,
) -> ObservabilityIngestionResult:
    """Summarize freshness and DQ state across the phase-1 lakehouse DAG."""

    from pyspark.sql import functions as F  # type: ignore import-not-found

    started_at = datetime.now(UTC)
    state_table = f"{catalog}.obs.obs_ingestion_state"
    run_log_table = f"{catalog}.obs.obs_pipeline_run_log"
    dq_metrics_table = f"{catalog}.obs.obs_dq_metrics"

    for table_name in [state_table, run_log_table, dq_metrics_table]:
        ensure_table_exists(spark, table_name)

    monitored_pipelines = build_monitored_pipelines(catalog)
    dq_specs = build_dq_specs(catalog)

    pipeline_name = "obs_pipeline_observability_metrics"
    target_table = dq_metrics_table
    start_metadata = {
        "state_table": state_table,
        "dq_metrics_table": dq_metrics_table,
        "monitored_pipeline_count": len(monitored_pipelines),
        "dq_source_count": len(dq_specs),
    }

    upsert_pipeline_run_log(
        spark,
        run_log_table,
        {
            "run_id": run_id,
            "pipeline_name": pipeline_name,
            "layer": "obs",
            "source_name": "system",
            "target_table": target_table,
            "status": "started",
            "rows_read": None,
            "rows_written": None,
            "started_at": started_at,
            "completed_at": None,
            "error_message": None,
            "metadata_json": build_metadata_json(start_metadata),
        },
    )

    try:
        measured_at = datetime.now(UTC)
        state_df, pipelines_with_state, pipelines_missing_state = read_monitored_state(
            spark,
            state_table,
            monitored_pipelines,
        )
        per_pipeline_observed_rows, pipelines_without_rows = build_observed_row_counts(
            spark,
            monitored_pipelines,
        )
        dq_metrics_df, dq_source_rows_read = build_dq_metrics_df(
            spark,
            run_id,
            dq_specs,
            measured_at,
        )
        dq_rows_ready, dq_rows_to_update, dq_rows_to_insert = merge_dq_metrics(
            spark,
            dq_metrics_table,
            dq_metrics_df,
        )
        dq_rows_merged = dq_rows_ready
        total_observed_rows = sum(per_pipeline_observed_rows.values())

        result = ObservabilityIngestionResult(
            status="success",
            pipeline_name=pipeline_name,
            catalog=catalog,
            run_id=run_id,
            state_table=state_table,
            dq_metrics_table=dq_metrics_table,
            run_log_table=run_log_table,
            pipelines_observed=len(monitored_pipelines),
            pipelines_with_state=pipelines_with_state,
            pipelines_missing_state=pipelines_missing_state,
            dq_source_tables=len(dq_specs),
            dq_source_rows_read=dq_source_rows_read,
            dq_rows_ready=dq_rows_ready,
            dq_rows_to_update=dq_rows_to_update,
            dq_rows_to_insert=dq_rows_to_insert,
            dq_rows_merged=dq_rows_merged,
            total_observed_rows=total_observed_rows,
            pipelines_without_rows=pipelines_without_rows,
            per_pipeline_observed_rows=per_pipeline_observed_rows,
        )

        completed_at = datetime.now(UTC)
        upsert_pipeline_run_log(
            spark,
            run_log_table,
            {
                "run_id": run_id,
                "pipeline_name": pipeline_name,
                "layer": "obs",
                "source_name": "system",
                "target_table": target_table,
                "status": result.status,
                "rows_read": pipelines_with_state
                + total_observed_rows
                + dq_source_rows_read,
                "rows_written": dq_rows_merged,
                "started_at": started_at,
                "completed_at": completed_at,
                "error_message": None,
                "metadata_json": build_metadata_json(result.as_dict()),
            },
        )

        if display_fn is not None:
            display_fn(state_df.orderBy("source_name", "pipeline_name"))
            if dq_rows_ready > 0:
                display_fn(dq_metrics_df.orderBy("pipeline_name", "run_id", "dq_reason"))
            recent_runs_df = (
                spark.table(run_log_table)
                .orderBy(F.col("started_at").desc())
                .limit(20)
            )
            display_fn(recent_runs_df)

        return result
    except Exception as exc:
        completed_at = datetime.now(UTC)
        upsert_pipeline_run_log(
            spark,
            run_log_table,
            {
                "run_id": run_id,
                "pipeline_name": pipeline_name,
                "layer": "obs",
                "source_name": "system",
                "target_table": target_table,
                "status": "failed",
                "rows_read": None,
                "rows_written": None,
                "started_at": started_at,
                "completed_at": completed_at,
                "error_message": str(exc),
                "metadata_json": build_metadata_json(start_metadata),
            },
        )
        raise
