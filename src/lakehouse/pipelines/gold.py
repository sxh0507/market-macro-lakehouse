"""Gold ingestion orchestration."""

from __future__ import annotations

from datetime import datetime, timedelta
from typing import Any, Callable, Mapping

from lakehouse.common.models import (
    CrossGoldIngestionResult,
    GoldIngestionResult,
    MacroGoldIngestionResult,
)
from lakehouse.common.runtime import (
    UTC,
    parse_indicator_ids,
    parse_product_ids,
    parse_quote_currencies,
    parse_series_ids,
    resolve_date_window,
)
from lakehouse.observability import PipelineObserver

MAX_VOLATILITY_LOOKBACK_DAYS = 90
CROSS_DEFAULT_LOOKBACK_DAYS = 120
CROSS_FILL_POLICY = "asof_observation_date_ffill_v1"
FRED_INDICATOR_GROUPS = {
    "CPIAUCSL": "inflation",
    "FEDFUNDS": "policy_rate",
    "GDP": "output",
}


def ensure_table_exists(
    spark: Any,
    table_name: str,
    notebook_name: str = "00_platform_setup_catalog_schema.ipynb",
):
    """Raise a consistent error when a required source or target table is missing."""

    if not spark.catalog.tableExists(table_name):
        raise RuntimeError(
            f"Required table {table_name} does not exist. Run {notebook_name} first."
        )


def collect_product_counts(df: Any, count_alias: str) -> dict[str, int]:
    """Collect row counts grouped by product_id into a Python dictionary."""

    counts = (
        df.groupBy("product_id")
        .count()
        .withColumnRenamed("count", count_alias)
        .collect()
    )
    return {row["product_id"]: int(row[count_alias]) for row in counts}


def collect_counts(df: Any, key_column: str, count_alias: str) -> dict[str, int]:
    """Collect grouped counts into a Python dictionary keyed by the selected column."""

    counts = (
        df.groupBy(key_column).count().withColumnRenamed("count", count_alias).collect()
    )
    return {row[key_column]: int(row[count_alias]) for row in counts}


def create_indicator_group_expr(F: Any):
    """Build a compact series_id -> indicator_group mapping expression for FRED Gold rows."""

    mapping_items = []
    for series_id, indicator_group in FRED_INDICATOR_GROUPS.items():
        mapping_items.extend([F.lit(series_id), F.lit(indicator_group)])
    mapping_expr = F.create_map(mapping_items)
    return F.element_at(mapping_expr, F.col("series_id"))


def _sortable_optional(value: Any) -> tuple[bool, Any]:
    """Convert an optional value into a comparison-safe tuple."""

    return (value is not None, value)


def select_latest_fred_revision_rows(
    rows: list[Mapping[str, Any]],
) -> list[dict[str, Any]]:
    """Collapse FRED revisions to the latest available row per (series_id, observation_date)."""

    latest_by_key: dict[tuple[Any, Any], dict[str, Any]] = {}
    for raw_row in rows:
        row = dict(raw_row)
        key = (row["series_id"], row["observation_date"])
        sort_key = (
            row["realtime_start"],
            row["realtime_end"],
            _sortable_optional(row.get("ingested_at")),
        )
        existing_row = latest_by_key.get(key)
        if existing_row is None:
            latest_by_key[key] = row
            continue

        existing_sort_key = (
            existing_row["realtime_start"],
            existing_row["realtime_end"],
            _sortable_optional(existing_row.get("ingested_at")),
        )
        if sort_key > existing_sort_key:
            latest_by_key[key] = row

    return [latest_by_key[key] for key in sorted(latest_by_key)]


def build_macro_asof_feature_map(
    feature_dates: list[Any],
    macro_rows: list[Mapping[str, Any]],
) -> dict[Any, dict[str, Any]]:
    """Build the per-feature-date macro MAP payload using observation-date as-of semantics."""

    feature_map: dict[Any, dict[str, Any]] = {}
    indicator_ids = sorted({row["indicator_id"] for row in macro_rows})

    for feature_date in sorted(feature_dates):
        values_for_date: dict[str, Any] = {}
        for indicator_id in indicator_ids:
            candidates = [
                row
                for row in macro_rows
                if row["indicator_id"] == indicator_id
                and row["value"] is not None
                and row["observation_date"] <= feature_date
            ]
            if not candidates:
                continue

            best_row = max(
                candidates,
                key=lambda row: (
                    row["observation_date"],
                    _sortable_optional(row.get("computed_at")),
                    _sortable_optional(row.get("run_id")),
                ),
            )
            values_for_date[indicator_id] = best_row["value"]

        if values_for_date:
            feature_map[feature_date] = values_for_date

    return feature_map


def run_gold_crypto_returns_and_volatility(
    spark: Any,
    raw_product_ids: str,
    mode: str,
    start_date_raw: str,
    end_date_raw: str,
    lookback_days_raw: str,
    run_id: str,
    catalog: str = "market_macro",
    source_table: str | None = None,
    returns_table: str | None = None,
    volatility_table: str | None = None,
    display_fn: Callable[[Any], None] | None = None,
) -> GoldIngestionResult:
    """Run the validated Gold market returns/volatility flow from package code."""

    from delta.tables import DeltaTable  # type: ignore import-not-found
    from pyspark.sql import functions as F  # type: ignore import-not-found
    from pyspark.sql.window import Window  # type: ignore import-not-found

    product_ids = parse_product_ids(raw_product_ids)
    start_date, end_date = resolve_date_window(
        mode, start_date_raw, end_date_raw, lookback_days_raw
    )
    source_start_date = start_date - timedelta(days=MAX_VOLATILITY_LOOKBACK_DAYS)

    source_table = source_table or f"{catalog}.slv_market.crypto_ohlc_1d"
    returns_table = returns_table or f"{catalog}.gld_market.dp_crypto_returns_1d"
    volatility_table = (
        volatility_table or f"{catalog}.gld_market.dp_crypto_volatility_1d"
    )

    if not spark.catalog.tableExists(source_table):
        raise RuntimeError(
            f"Source table {source_table} does not exist. Run the Silver pipeline first."
        )

    if not spark.catalog.tableExists(returns_table):
        raise RuntimeError(
            f"Target table {returns_table} does not exist. Run 00_platform_setup_catalog_schema.ipynb first."
        )

    if not spark.catalog.tableExists(volatility_table):
        raise RuntimeError(
            f"Target table {volatility_table} does not exist. Run 00_platform_setup_catalog_schema.ipynb first."
        )
    returns_observer = PipelineObserver(
        spark=spark,
        catalog=catalog,
        pipeline_name="gold_market_crypto_returns_1d",
        layer="gold",
        source_name="coinbase",
        target_table=returns_table,
        run_id=run_id,
        start_metadata={
            "mode": mode,
            "product_ids": product_ids,
            "source_table": source_table,
            "start_date": start_date.isoformat(),
            "end_date": end_date.isoformat(),
            "source_history_start_date": source_start_date.isoformat(),
        },
    )
    volatility_observer = PipelineObserver(
        spark=spark,
        catalog=catalog,
        pipeline_name="gold_market_crypto_volatility_1d",
        layer="gold",
        source_name="coinbase",
        target_table=volatility_table,
        run_id=run_id,
        start_metadata={
            "mode": mode,
            "product_ids": product_ids,
            "source_table": source_table,
            "start_date": start_date.isoformat(),
            "end_date": end_date.isoformat(),
            "source_history_start_date": source_start_date.isoformat(),
        },
    )

    with returns_observer, volatility_observer:
        silver_df = (
            spark.table(source_table)
            .select("product_id", "bar_date", "base_asset", "quote_currency", "close")
            .filter(F.col("product_id").isin(product_ids))
            .filter(
                (F.col("bar_date") >= F.lit(source_start_date))
                & (F.col("bar_date") <= F.lit(end_date))
            )
        )

        rows_read = silver_df.count()
        returns_observer.update_progress(rows_read=rows_read)
        volatility_observer.update_progress(rows_read=rows_read)
        per_product_rows_read = (
            collect_product_counts(silver_df, "rows_read") if rows_read else {}
        )

        if rows_read == 0:
            result = GoldIngestionResult(
                status="success_empty",
                mode=mode,
                product_ids=product_ids,
                start_date=start_date.isoformat(),
                end_date=end_date.isoformat(),
                source_history_start_date=source_start_date.isoformat(),
                source_table=source_table,
                returns_table=returns_table,
                volatility_table=volatility_table,
                rows_read=0,
                rows_returns_ready=0,
                rows_volatility_ready=0,
                rows_returns_to_update=0,
                rows_returns_to_insert=0,
                rows_returns_merged=0,
                rows_volatility_to_update=0,
                rows_volatility_to_insert=0,
                rows_volatility_merged=0,
                run_id=run_id,
                per_product_rows_read=per_product_rows_read,
            )
            returns_observer.succeed(
                status=result.status,
                rows_read=result.rows_read,
                rows_written=result.rows_returns_merged,
                metadata=result.as_dict(),
                watermark_type="bar_date",
                watermark_column="bar_date",
            )
            volatility_observer.succeed(
                status=result.status,
                rows_read=result.rows_read,
                rows_written=result.rows_volatility_merged,
                metadata=result.as_dict(),
                watermark_type="bar_date",
                watermark_column="bar_date",
            )
            return result

        order_window = Window.partitionBy("product_id").orderBy("bar_date")
        returns_base_df = (
            silver_df.withColumn("prev_close", F.lag("close").over(order_window))
            .withColumn(
                "simple_return_1d",
                F.when(
                    F.col("prev_close") > 0,
                    (F.col("close") / F.col("prev_close")) - F.lit(1.0),
                ),
            )
            .withColumn(
                "log_return_1d",
                F.when(
                    (F.col("close") > 0) & (F.col("prev_close") > 0),
                    F.log(F.col("close") / F.col("prev_close")),
                ),
            )
        )

        computed_at = datetime.now(UTC)
        returns_df = (
            returns_base_df.filter(
                (F.col("bar_date") >= F.lit(start_date))
                & (F.col("bar_date") <= F.lit(end_date))
            )
            .select(
                "product_id",
                "bar_date",
                "base_asset",
                "quote_currency",
                "close",
                "simple_return_1d",
                "log_return_1d",
            )
            .withColumn("computed_at", F.lit(computed_at))
            .withColumn("run_id", F.lit(run_id))
        )

        rows_returns_ready = returns_df.count()
        per_product_rows_returns = (
            collect_product_counts(returns_df, "rows_returns")
            if rows_returns_ready
            else {}
        )

        window_7 = order_window.rowsBetween(-6, 0)
        window_30 = order_window.rowsBetween(-29, 0)
        window_90 = order_window.rowsBetween(-89, 0)

        volatility_base_df = (
            returns_base_df.withColumn(
                "returns_count_7", F.count("simple_return_1d").over(window_7)
            )
            .withColumn("returns_count_30", F.count("simple_return_1d").over(window_30))
            .withColumn("returns_count_90", F.count("simple_return_1d").over(window_90))
            .withColumn(
                "volatility_7d",
                F.when(
                    F.col("returns_count_7") == 7,
                    F.stddev_samp("simple_return_1d").over(window_7),
                ),
            )
            .withColumn(
                "volatility_30d",
                F.when(
                    F.col("returns_count_30") == 30,
                    F.stddev_samp("simple_return_1d").over(window_30),
                ),
            )
            .withColumn(
                "volatility_90d",
                F.when(
                    F.col("returns_count_90") == 90,
                    F.stddev_samp("simple_return_1d").over(window_90),
                ),
            )
        )

        volatility_df = (
            volatility_base_df.filter(
                (F.col("bar_date") >= F.lit(start_date))
                & (F.col("bar_date") <= F.lit(end_date))
            )
            .select(
                "product_id",
                "bar_date",
                "base_asset",
                "quote_currency",
                "simple_return_1d",
                "volatility_7d",
                "volatility_30d",
                "volatility_90d",
            )
            .withColumn("computed_at", F.lit(computed_at))
            .withColumn("run_id", F.lit(run_id))
        )

        rows_volatility_ready = volatility_df.count()
        per_product_rows_volatility = (
            collect_product_counts(volatility_df, "rows_volatility")
            if rows_volatility_ready
            else {}
        )

        existing_returns_key_count = (
            returns_df.select("product_id", "bar_date")
            .join(
                spark.table(returns_table).select("product_id", "bar_date"),
                on=["product_id", "bar_date"],
                how="inner",
            )
            .count()
            if rows_returns_ready
            else 0
        )

        existing_volatility_key_count = (
            volatility_df.select("product_id", "bar_date")
            .join(
                spark.table(volatility_table).select("product_id", "bar_date"),
                on=["product_id", "bar_date"],
                how="inner",
            )
            .count()
            if rows_volatility_ready
            else 0
        )

        if rows_returns_ready > 0:
            DeltaTable.forName(spark, returns_table).alias("tgt").merge(
                returns_df.alias("src"),
                "tgt.product_id = src.product_id AND tgt.bar_date = src.bar_date",
            ).whenMatchedUpdate(
                set={
                    "base_asset": "src.base_asset",
                    "quote_currency": "src.quote_currency",
                    "close": "src.close",
                    "simple_return_1d": "src.simple_return_1d",
                    "log_return_1d": "src.log_return_1d",
                    "computed_at": "src.computed_at",
                    "run_id": "src.run_id",
                }
            ).whenNotMatchedInsertAll().execute()

        if rows_volatility_ready > 0:
            DeltaTable.forName(spark, volatility_table).alias("tgt").merge(
                volatility_df.alias("src"),
                "tgt.product_id = src.product_id AND tgt.bar_date = src.bar_date",
            ).whenMatchedUpdate(
                set={
                    "base_asset": "src.base_asset",
                    "quote_currency": "src.quote_currency",
                    "simple_return_1d": "src.simple_return_1d",
                    "volatility_7d": "src.volatility_7d",
                    "volatility_30d": "src.volatility_30d",
                    "volatility_90d": "src.volatility_90d",
                    "computed_at": "src.computed_at",
                    "run_id": "src.run_id",
                }
            ).whenNotMatchedInsertAll().execute()

        if display_fn is not None:
            if rows_returns_ready > 0:
                display_fn(returns_df.orderBy("product_id", "bar_date"))
            if rows_volatility_ready > 0:
                display_fn(volatility_df.orderBy("product_id", "bar_date"))

        result = GoldIngestionResult(
            status="success",
            mode=mode,
            product_ids=product_ids,
            start_date=start_date.isoformat(),
            end_date=end_date.isoformat(),
            source_history_start_date=source_start_date.isoformat(),
            source_table=source_table,
            returns_table=returns_table,
            volatility_table=volatility_table,
            rows_read=rows_read,
            rows_returns_ready=rows_returns_ready,
            rows_volatility_ready=rows_volatility_ready,
            rows_returns_to_update=existing_returns_key_count,
            rows_returns_to_insert=rows_returns_ready - existing_returns_key_count,
            rows_returns_merged=rows_returns_ready,
            rows_volatility_to_update=existing_volatility_key_count,
            rows_volatility_to_insert=rows_volatility_ready
            - existing_volatility_key_count,
            rows_volatility_merged=rows_volatility_ready,
            run_id=run_id,
            per_product_rows_read=per_product_rows_read,
            per_product_rows_returns=per_product_rows_returns,
            per_product_rows_volatility=per_product_rows_volatility,
        )
        returns_observer.succeed(
            status=result.status,
            rows_read=result.rows_read,
            rows_written=result.rows_returns_merged,
            metadata=result.as_dict(),
            watermark_type="bar_date",
            watermark_column="bar_date",
        )
        volatility_observer.succeed(
            status=result.status,
            rows_read=result.rows_read,
            rows_written=result.rows_volatility_merged,
            metadata=result.as_dict(),
            watermark_type="bar_date",
            watermark_column="bar_date",
        )
        return result


def run_gold_macro_indicators(
    spark: Any,
    source_system: str,
    mode: str,
    start_date_raw: str,
    end_date_raw: str,
    lookback_days_raw: str,
    run_id: str,
    catalog: str = "market_macro",
    raw_quote_currencies: str = "",
    raw_series_ids: str = "",
    target_table: str | None = None,
    ecb_source_table: str | None = None,
    fred_source_table: str | None = None,
    fred_metadata_table: str | None = None,
    display_fn: Callable[[Any], None] | None = None,
) -> MacroGoldIngestionResult:
    """Run the Gold macro indicator pipeline for a single source system."""

    from delta.tables import DeltaTable  # type: ignore import-not-found
    from pyspark.sql import functions as F  # type: ignore import-not-found
    from pyspark.sql.window import Window  # type: ignore import-not-found

    normalized_source_system = source_system.strip().lower()
    normalized_mode = mode.strip().lower()
    effective_lookback_days = lookback_days_raw.strip() or (
        "90" if normalized_source_system == "fred" else "7"
    )
    start_date, end_date = resolve_date_window(
        normalized_mode,
        start_date_raw,
        end_date_raw,
        effective_lookback_days,
    )

    target_table = target_table or f"{catalog}.gld_macro.dp_macro_indicators"
    ensure_table_exists(spark, target_table)
    pipeline_name = f"gold_macro_indicators_{normalized_source_system}"
    observer = PipelineObserver(
        spark=spark,
        catalog=catalog,
        pipeline_name=pipeline_name,
        layer="gold",
        source_name=normalized_source_system,
        target_table=target_table,
        run_id=run_id,
        start_metadata={
            "mode": normalized_mode,
            "source_system": normalized_source_system,
            "start_date": start_date.isoformat(),
            "end_date": end_date.isoformat(),
        },
    )

    with observer:
        if normalized_source_system == "ecb":
            quote_currencies = parse_quote_currencies(raw_quote_currencies)
            source_table = (
                ecb_source_table or f"{catalog}.slv_macro.ecb_fx_ref_rates_daily"
            )
            ensure_table_exists(spark, source_table)

            silver_df = (
                spark.table(source_table)
                .select(
                    F.upper(F.col("base_currency")).alias("base_currency"),
                    F.upper(F.col("quote_currency")).alias("quote_currency"),
                    "rate_date",
                    "rate",
                )
                .filter(F.col("quote_currency").isin(quote_currencies))
                .filter(
                    (F.col("rate_date") >= F.lit(start_date))
                    & (F.col("rate_date") <= F.lit(end_date))
                )
            )

            rows_read = silver_df.count()
            observer.update_progress(rows_read=rows_read)
            if rows_read == 0:
                result = MacroGoldIngestionResult(
                    status="success_empty",
                    source_system=normalized_source_system,
                    mode=normalized_mode,
                    start_date=start_date.isoformat(),
                    end_date=end_date.isoformat(),
                    source_table=source_table,
                    target_table=target_table,
                    run_id=run_id,
                    quote_currencies=quote_currencies,
                )
                observer.succeed(
                    status=result.status,
                    rows_read=result.rows_read,
                    rows_written=result.rows_merged,
                    metadata=result.as_dict(),
                    watermark_type="observation_date",
                    watermark_column="observation_date",
                    state_filter_sql="source_system = 'ecb'",
                )
                return result

            computed_at = datetime.now(UTC)
            gold_df = (
                silver_df.withColumn(
                    "indicator_id",
                    F.concat_ws(
                        "_",
                        F.lit("ECB"),
                        F.lit("FX"),
                        F.lit("REF"),
                        F.col("base_currency"),
                        F.col("quote_currency"),
                    ),
                )
                .withColumnRenamed("rate_date", "observation_date")
                .withColumn("source_system", F.lit("ecb"))
                .withColumn("indicator_group", F.lit("fx_ref_rate"))
                .withColumn("value", F.col("rate"))
                .withColumn(
                    "unit",
                    F.concat(
                        F.col("quote_currency"), F.lit(" per "), F.col("base_currency")
                    ),
                )
                .withColumn("frequency", F.lit("D"))
                .withColumn("is_official", F.lit(True))
                .withColumn("derivation_method", F.lit("official_source"))
                .withColumn("derived_from_indicator_id", F.lit(None).cast("string"))
                .withColumn("computed_at", F.lit(computed_at))
                .withColumn("run_id", F.lit(run_id))
                .select(
                    "indicator_id",
                    "observation_date",
                    "source_system",
                    "indicator_group",
                    "value",
                    "unit",
                    "frequency",
                    "is_official",
                    "derivation_method",
                    "derived_from_indicator_id",
                    "computed_at",
                    "run_id",
                )
            )

            duplicate_key_count = (
                gold_df.groupBy("indicator_id", "observation_date")
                .count()
                .filter(F.col("count") > 1)
                .count()
            )
            if duplicate_key_count > 0:
                raise RuntimeError(
                    f"Detected {duplicate_key_count} duplicate Gold ECB indicator keys after mapping."
                )

            rows_ready = gold_df.count()
            existing_key_count = (
                gold_df.select("indicator_id", "observation_date")
                .join(
                    spark.table(target_table).select(
                        "indicator_id", "observation_date"
                    ),
                    on=["indicator_id", "observation_date"],
                    how="inner",
                )
                .count()
            )
            per_indicator_rows_ready = collect_counts(
                gold_df, "indicator_id", "rows_ready"
            )

            DeltaTable.forName(spark, target_table).alias("tgt").merge(
                gold_df.alias("src"),
                "tgt.indicator_id = src.indicator_id AND tgt.observation_date = src.observation_date",
            ).whenMatchedUpdate(
                set={
                    "source_system": "src.source_system",
                    "indicator_group": "src.indicator_group",
                    "value": "src.value",
                    "unit": "src.unit",
                    "frequency": "src.frequency",
                    "is_official": "src.is_official",
                    "derivation_method": "src.derivation_method",
                    "derived_from_indicator_id": "src.derived_from_indicator_id",
                    "computed_at": "src.computed_at",
                    "run_id": "src.run_id",
                }
            ).whenNotMatchedInsertAll().execute()

            if display_fn is not None:
                display_fn(gold_df.orderBy("indicator_id", "observation_date"))

            result = MacroGoldIngestionResult(
                status="success",
                source_system=normalized_source_system,
                mode=normalized_mode,
                start_date=start_date.isoformat(),
                end_date=end_date.isoformat(),
                source_table=source_table,
                target_table=target_table,
                run_id=run_id,
                quote_currencies=quote_currencies,
                rows_read=rows_read,
                rows_ready=rows_ready,
                rows_to_update=existing_key_count,
                rows_to_insert=rows_ready - existing_key_count,
                rows_merged=rows_ready,
                per_indicator_rows_ready=per_indicator_rows_ready,
            )
            observer.succeed(
                status=result.status,
                rows_read=result.rows_read,
                rows_written=result.rows_merged,
                metadata=result.as_dict(),
                watermark_type="observation_date",
                watermark_column="observation_date",
                state_filter_sql="source_system = 'ecb'",
            )
            return result

        if normalized_source_system == "fred":
            series_ids = parse_series_ids(raw_series_ids)
            source_table = fred_source_table or f"{catalog}.slv_macro.fred_series_clean"
            metadata_table = (
                fred_metadata_table or f"{catalog}.brz_macro.raw_fred_series_metadata"
            )
            revision_policy = "latest_available"
            ensure_table_exists(spark, source_table)
            ensure_table_exists(spark, metadata_table)

            silver_df = (
                spark.table(source_table)
                .select(
                    "series_id",
                    "observation_date",
                    "realtime_start",
                    "realtime_end",
                    "value",
                    "ingested_at",
                )
                .filter(F.col("series_id").isin(series_ids))
                .filter(
                    (F.col("observation_date") >= F.lit(start_date))
                    & (F.col("observation_date") <= F.lit(end_date))
                )
            )

            rows_read = silver_df.count()
            observer.update_progress(rows_read=rows_read)
            if rows_read == 0:
                result = MacroGoldIngestionResult(
                    status="success_empty",
                    source_system=normalized_source_system,
                    mode=normalized_mode,
                    start_date=start_date.isoformat(),
                    end_date=end_date.isoformat(),
                    source_table=source_table,
                    target_table=target_table,
                    run_id=run_id,
                    series_ids=series_ids,
                    metadata_table=metadata_table,
                    revision_policy=revision_policy,
                )
                observer.succeed(
                    status=result.status,
                    rows_read=result.rows_read,
                    rows_written=result.rows_merged,
                    metadata=result.as_dict(),
                    watermark_type="observation_date",
                    watermark_column="observation_date",
                    state_filter_sql="source_system = 'fred'",
                )
                return result

            metadata_window = Window.partitionBy("series_id").orderBy(
                F.col("ingested_at").desc(),
                F.col("payload_hash").desc(),
            )
            metadata_latest_df = (
                spark.table(metadata_table)
                .select(
                    "series_id",
                    "units",
                    "frequency_short",
                    "ingested_at",
                    "payload_hash",
                )
                .filter(F.col("series_id").isin(series_ids))
                .withColumn("_row_number", F.row_number().over(metadata_window))
                .filter(F.col("_row_number") == 1)
                .drop("_row_number")
            )

            metadata_ids = {
                row["series_id"]
                for row in metadata_latest_df.select("series_id").collect()
            }
            missing_metadata_ids = sorted(set(series_ids) - metadata_ids)
            if missing_metadata_ids:
                raise RuntimeError(
                    "Missing FRED metadata for requested series_ids: "
                    + ", ".join(missing_metadata_ids)
                )

            metadata_missing_units = [
                row["series_id"]
                for row in metadata_latest_df.filter(
                    F.col("units").isNull() | (F.trim(F.col("units")) == "")
                )
                .select("series_id")
                .collect()
            ]
            if metadata_missing_units:
                raise RuntimeError(
                    "Missing FRED units for series_ids: "
                    + ", ".join(sorted(metadata_missing_units))
                )

            metadata_missing_frequency = [
                row["series_id"]
                for row in metadata_latest_df.filter(
                    F.col("frequency_short").isNull()
                    | (F.trim(F.col("frequency_short")) == "")
                )
                .select("series_id")
                .collect()
            ]
            if metadata_missing_frequency:
                raise RuntimeError(
                    "Missing FRED frequency_short for series_ids: "
                    + ", ".join(sorted(metadata_missing_frequency))
                )

            revision_window = Window.partitionBy(
                "series_id", "observation_date"
            ).orderBy(
                F.col("realtime_start").desc(),
                F.col("realtime_end").desc(),
                F.col("ingested_at").desc(),
            )
            latest_revision_df = (
                silver_df.withColumn(
                    "_row_number", F.row_number().over(revision_window)
                )
                .filter(F.col("_row_number") == 1)
                .drop("_row_number")
            )
            rows_after_revision_collapse = latest_revision_df.count()

            computed_at = datetime.now(UTC)
            gold_df = (
                latest_revision_df.join(
                    metadata_latest_df.select("series_id", "units", "frequency_short"),
                    on="series_id",
                    how="inner",
                )
                .withColumn(
                    "indicator_id", F.concat(F.lit("FRED_"), F.col("series_id"))
                )
                .withColumn("source_system", F.lit("fred"))
                .withColumn("indicator_group", create_indicator_group_expr(F))
                .withColumn("unit", F.col("units"))
                .withColumn("frequency", F.col("frequency_short"))
                .withColumn("is_official", F.lit(True))
                .withColumn("derivation_method", F.lit("official_source"))
                .withColumn("derived_from_indicator_id", F.lit(None).cast("string"))
                .withColumn("computed_at", F.lit(computed_at))
                .withColumn("run_id", F.lit(run_id))
                .select(
                    "indicator_id",
                    "observation_date",
                    "source_system",
                    "indicator_group",
                    "value",
                    "unit",
                    "frequency",
                    "is_official",
                    "derivation_method",
                    "derived_from_indicator_id",
                    "computed_at",
                    "run_id",
                )
            )

            duplicate_key_count = (
                gold_df.groupBy("indicator_id", "observation_date")
                .count()
                .filter(F.col("count") > 1)
                .count()
            )
            if duplicate_key_count > 0:
                raise RuntimeError(
                    f"Detected {duplicate_key_count} duplicate Gold FRED indicator keys after revision collapse."
                )

            rows_ready = gold_df.count()
            existing_key_count = (
                gold_df.select("indicator_id", "observation_date")
                .join(
                    spark.table(target_table).select(
                        "indicator_id", "observation_date"
                    ),
                    on=["indicator_id", "observation_date"],
                    how="inner",
                )
                .count()
            )
            per_indicator_rows_ready = collect_counts(
                gold_df, "indicator_id", "rows_ready"
            )

            DeltaTable.forName(spark, target_table).alias("tgt").merge(
                gold_df.alias("src"),
                "tgt.indicator_id = src.indicator_id AND tgt.observation_date = src.observation_date",
            ).whenMatchedUpdate(
                set={
                    "source_system": "src.source_system",
                    "indicator_group": "src.indicator_group",
                    "value": "src.value",
                    "unit": "src.unit",
                    "frequency": "src.frequency",
                    "is_official": "src.is_official",
                    "derivation_method": "src.derivation_method",
                    "derived_from_indicator_id": "src.derived_from_indicator_id",
                    "computed_at": "src.computed_at",
                    "run_id": "src.run_id",
                }
            ).whenNotMatchedInsertAll().execute()

            if display_fn is not None:
                display_fn(gold_df.orderBy("indicator_id", "observation_date"))

            result = MacroGoldIngestionResult(
                status="success",
                source_system=normalized_source_system,
                mode=normalized_mode,
                start_date=start_date.isoformat(),
                end_date=end_date.isoformat(),
                source_table=source_table,
                target_table=target_table,
                run_id=run_id,
                series_ids=series_ids,
                metadata_table=metadata_table,
                revision_policy=revision_policy,
                rows_read=rows_read,
                rows_after_revision_collapse=rows_after_revision_collapse,
                rows_ready=rows_ready,
                rows_to_update=existing_key_count,
                rows_to_insert=rows_ready - existing_key_count,
                rows_merged=rows_ready,
                per_indicator_rows_ready=per_indicator_rows_ready,
            )
            observer.succeed(
                status=result.status,
                rows_read=result.rows_read,
                rows_written=result.rows_merged,
                metadata=result.as_dict(),
                watermark_type="observation_date",
                watermark_column="observation_date",
                state_filter_sql="source_system = 'fred'",
            )
            return result

        raise ValueError("source_system must be one of: ecb, fred")


def run_gold_cross_crypto_macro_features(
    spark: Any,
    raw_product_ids: str,
    raw_macro_indicator_ids: str,
    mode: str,
    start_date_raw: str,
    end_date_raw: str,
    lookback_days_raw: str,
    run_id: str,
    catalog: str = "market_macro",
    returns_table: str | None = None,
    volatility_table: str | None = None,
    macro_table: str | None = None,
    target_table: str | None = None,
    display_fn: Callable[[Any], None] | None = None,
) -> CrossGoldIngestionResult:
    """Run the Gold cross-domain crypto + macro feature pipeline."""

    from delta.tables import DeltaTable  # type: ignore import-not-found
    from pyspark.sql import functions as F  # type: ignore import-not-found
    from pyspark.sql.window import Window  # type: ignore import-not-found

    normalized_mode = mode.strip().lower()
    product_ids = parse_product_ids(raw_product_ids)
    macro_indicator_ids = parse_indicator_ids(raw_macro_indicator_ids)
    effective_lookback_days = lookback_days_raw.strip() or str(
        CROSS_DEFAULT_LOOKBACK_DAYS
    )
    start_date, end_date = resolve_date_window(
        normalized_mode,
        start_date_raw,
        end_date_raw,
        effective_lookback_days,
    )

    returns_table = returns_table or f"{catalog}.gld_market.dp_crypto_returns_1d"
    volatility_table = (
        volatility_table or f"{catalog}.gld_market.dp_crypto_volatility_1d"
    )
    macro_table = macro_table or f"{catalog}.gld_macro.dp_macro_indicators"
    target_table = target_table or f"{catalog}.gld_cross.dp_crypto_macro_features_1d"

    ensure_table_exists(spark, returns_table)
    ensure_table_exists(spark, volatility_table)
    ensure_table_exists(spark, macro_table)
    ensure_table_exists(spark, target_table)

    observer = PipelineObserver(
        spark=spark,
        catalog=catalog,
        pipeline_name="gold_cross_crypto_macro_features_1d",
        layer="gold",
        source_name="cross",
        target_table=target_table,
        run_id=run_id,
        start_metadata={
            "mode": normalized_mode,
            "product_ids": product_ids,
            "macro_indicator_ids": macro_indicator_ids,
            "start_date": start_date.isoformat(),
            "end_date": end_date.isoformat(),
            "fill_policy": CROSS_FILL_POLICY,
            "backtest_safe": False,
            "returns_table": returns_table,
            "volatility_table": volatility_table,
            "macro_table": macro_table,
        },
    )

    with observer:
        market_returns_df = (
            spark.table(returns_table)
            .select(
                "product_id",
                F.col("bar_date").alias("feature_date"),
                "base_asset",
                "quote_currency",
                "simple_return_1d",
                "log_return_1d",
            )
            .filter(F.col("product_id").isin(product_ids))
            .filter(
                (F.col("feature_date") >= F.lit(start_date))
                & (F.col("feature_date") <= F.lit(end_date))
            )
        )
        rows_market_returns_read = market_returns_df.count()

        market_volatility_df = (
            spark.table(volatility_table)
            .select(
                "product_id",
                F.col("bar_date").alias("feature_date"),
                "volatility_7d",
                "volatility_30d",
            )
            .filter(F.col("product_id").isin(product_ids))
            .filter(
                (F.col("feature_date") >= F.lit(start_date))
                & (F.col("feature_date") <= F.lit(end_date))
            )
        )
        rows_market_volatility_read = market_volatility_df.count()

        rows_read = rows_market_returns_read + rows_market_volatility_read
        observer.update_progress(rows_read=rows_read)

        if rows_market_returns_read == 0:
            result = CrossGoldIngestionResult(
                status="success_empty",
                mode=normalized_mode,
                product_ids=product_ids,
                macro_indicator_ids=macro_indicator_ids,
                start_date=start_date.isoformat(),
                end_date=end_date.isoformat(),
                source_market_returns_table=returns_table,
                source_market_volatility_table=volatility_table,
                source_macro_table=macro_table,
                target_table=target_table,
                fill_policy=CROSS_FILL_POLICY,
                backtest_safe=False,
                run_id=run_id,
                rows_market_returns_read=rows_market_returns_read,
                rows_market_volatility_read=rows_market_volatility_read,
            )
            observer.succeed(
                status=result.status,
                rows_read=rows_read,
                rows_written=result.rows_merged,
                metadata=result.as_dict(),
                watermark_type="feature_date",
                watermark_column="feature_date",
            )
            return result

        market_base_df = (
            market_returns_df.alias("ret")
            .join(
                market_volatility_df.alias("vol"),
                on=["product_id", "feature_date"],
                how="left",
            )
            .select(
                "product_id",
                "feature_date",
                "base_asset",
                "quote_currency",
                "simple_return_1d",
                "log_return_1d",
                "volatility_7d",
                "volatility_30d",
            )
        )
        rows_market_base = market_base_df.count()

        duplicate_market_key_count = (
            market_base_df.groupBy("feature_date", "product_id")
            .count()
            .filter(F.col("count") > 1)
            .count()
        )
        if duplicate_market_key_count > 0:
            raise RuntimeError(
                f"Detected {duplicate_market_key_count} duplicate market feature keys before macro alignment."
            )

        macro_source_df = (
            spark.table(macro_table)
            .select(
                "indicator_id", "observation_date", "value", "computed_at", "run_id"
            )
            .filter(F.col("indicator_id").isin(macro_indicator_ids))
            .filter(F.col("observation_date") <= F.lit(end_date))
        )
        rows_macro_selected = macro_source_df.count()
        rows_read += rows_macro_selected
        observer.update_progress(rows_read=rows_read)

        available_macro_indicator_ids = [
            row["indicator_id"]
            for row in macro_source_df.select("indicator_id").distinct().collect()
        ]
        missing_macro_indicator_ids = [
            indicator_id
            for indicator_id in macro_indicator_ids
            if indicator_id not in available_macro_indicator_ids
        ]
        if missing_macro_indicator_ids:
            raise RuntimeError(
                "Missing requested macro indicators in gld_macro.dp_macro_indicators: "
                + ", ".join(missing_macro_indicator_ids)
            )

        feature_dates_df = market_base_df.select("feature_date").distinct()
        indicator_ids_df = spark.range(1).select(
            F.explode(
                F.array(*[F.lit(indicator_id) for indicator_id in macro_indicator_ids])
            ).alias("indicator_id")
        )

        macro_asof_candidates_df = (
            feature_dates_df.crossJoin(indicator_ids_df)
            .join(macro_source_df, on="indicator_id", how="left")
            .filter(
                F.col("observation_date").isNull()
                | (F.col("observation_date") <= F.col("feature_date"))
            )
        )

        asof_window = Window.partitionBy("feature_date", "indicator_id").orderBy(
            F.col("observation_date").desc(),
            F.col("computed_at").desc(),
            F.col("run_id").desc(),
        )
        macro_asof_df = (
            macro_asof_candidates_df.withColumn(
                "_row_number", F.row_number().over(asof_window)
            )
            .filter((F.col("_row_number") == 1) & F.col("value").isNotNull())
            .select("feature_date", "indicator_id", "value")
        )
        rows_macro_asof_ready = macro_asof_df.count()

        macro_features_df = macro_asof_df.groupBy("feature_date").agg(
            F.map_from_entries(
                F.collect_list(F.struct(F.col("indicator_id"), F.col("value")))
            ).alias("macro_features")
        )

        computed_at = datetime.now(UTC)
        cross_df = (
            market_base_df.join(macro_features_df, on="feature_date", how="left")
            .withColumn("fill_policy", F.lit(CROSS_FILL_POLICY))
            .withColumn("computed_at", F.lit(computed_at))
            .withColumn("run_id", F.lit(run_id))
            .select(
                "feature_date",
                "product_id",
                "base_asset",
                "quote_currency",
                "simple_return_1d",
                "log_return_1d",
                "volatility_7d",
                "volatility_30d",
                "macro_features",
                "fill_policy",
                "computed_at",
                "run_id",
            )
        )
        rows_ready = cross_df.count()

        duplicate_cross_key_count = (
            cross_df.groupBy("feature_date", "product_id")
            .count()
            .filter(F.col("count") > 1)
            .count()
        )
        if duplicate_cross_key_count > 0:
            raise RuntimeError(
                f"Detected {duplicate_cross_key_count} duplicate cross feature keys after macro alignment."
            )

        existing_key_count = (
            cross_df.select("feature_date", "product_id")
            .join(
                spark.table(target_table).select("feature_date", "product_id"),
                on=["feature_date", "product_id"],
                how="inner",
            )
            .count()
            if rows_ready > 0
            else 0
        )

        if rows_ready > 0:
            DeltaTable.forName(spark, target_table).alias("tgt").merge(
                cross_df.alias("src"),
                "tgt.feature_date = src.feature_date AND tgt.product_id = src.product_id",
            ).whenMatchedUpdate(
                set={
                    "base_asset": "src.base_asset",
                    "quote_currency": "src.quote_currency",
                    "simple_return_1d": "src.simple_return_1d",
                    "log_return_1d": "src.log_return_1d",
                    "volatility_7d": "src.volatility_7d",
                    "volatility_30d": "src.volatility_30d",
                    "macro_features": "src.macro_features",
                    "fill_policy": "src.fill_policy",
                    "computed_at": "src.computed_at",
                    "run_id": "src.run_id",
                }
            ).whenNotMatchedInsertAll().execute()

        if display_fn is not None:
            display_fn(cross_df.orderBy("product_id", "feature_date"))

        result = CrossGoldIngestionResult(
            status="success",
            mode=normalized_mode,
            product_ids=product_ids,
            macro_indicator_ids=macro_indicator_ids,
            start_date=start_date.isoformat(),
            end_date=end_date.isoformat(),
            source_market_returns_table=returns_table,
            source_market_volatility_table=volatility_table,
            source_macro_table=macro_table,
            target_table=target_table,
            fill_policy=CROSS_FILL_POLICY,
            backtest_safe=False,
            run_id=run_id,
            rows_market_returns_read=rows_market_returns_read,
            rows_market_volatility_read=rows_market_volatility_read,
            rows_market_base=rows_market_base,
            rows_macro_selected=rows_macro_selected,
            rows_macro_asof_ready=rows_macro_asof_ready,
            rows_ready=rows_ready,
            rows_to_update=existing_key_count,
            rows_to_insert=rows_ready - existing_key_count,
            rows_merged=rows_ready,
        )
        observer.succeed(
            status=result.status,
            rows_read=rows_read,
            rows_written=result.rows_merged,
            metadata=result.as_dict(),
            watermark_type="feature_date",
            watermark_column="feature_date",
        )
        return result
