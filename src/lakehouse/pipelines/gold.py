"""Gold ingestion orchestration."""

from __future__ import annotations

from datetime import datetime, timedelta
from typing import Any, Callable

from lakehouse.common.models import GoldIngestionResult, MacroGoldIngestionResult
from lakehouse.common.runtime import (
    UTC,
    parse_product_ids,
    parse_quote_currencies,
    parse_series_ids,
    resolve_date_window,
)

MAX_VOLATILITY_LOOKBACK_DAYS = 90
FRED_INDICATOR_GROUPS = {
    "CPIAUCSL": "inflation",
    "FEDFUNDS": "policy_rate",
    "GDP": "output",
}


def ensure_table_exists(spark: Any, table_name: str, notebook_name: str = "00_platform_setup_catalog_schema.ipynb"):
    """Raise a consistent error when a required source or target table is missing."""

    if not spark.catalog.tableExists(table_name):
        raise RuntimeError(f"Required table {table_name} does not exist. Run {notebook_name} first.")


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
        df.groupBy(key_column)
        .count()
        .withColumnRenamed("count", count_alias)
        .collect()
    )
    return {row[key_column]: int(row[count_alias]) for row in counts}


def create_indicator_group_expr(F: Any):
    """Build a compact series_id -> indicator_group mapping expression for FRED Gold rows."""

    mapping_items = []
    for series_id, indicator_group in FRED_INDICATOR_GROUPS.items():
        mapping_items.extend([F.lit(series_id), F.lit(indicator_group)])
    mapping_expr = F.create_map(mapping_items)
    return F.element_at(mapping_expr, F.col("series_id"))


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
    start_date, end_date = resolve_date_window(mode, start_date_raw, end_date_raw, lookback_days_raw)
    source_start_date = start_date - timedelta(days=MAX_VOLATILITY_LOOKBACK_DAYS)

    source_table = source_table or f"{catalog}.slv_market.crypto_ohlc_1d"
    returns_table = returns_table or f"{catalog}.gld_market.dp_crypto_returns_1d"
    volatility_table = volatility_table or f"{catalog}.gld_market.dp_crypto_volatility_1d"

    if not spark.catalog.tableExists(source_table):
        raise RuntimeError(f"Source table {source_table} does not exist. Run the Silver pipeline first.")

    if not spark.catalog.tableExists(returns_table):
        raise RuntimeError(
            f"Target table {returns_table} does not exist. Run 00_platform_setup_catalog_schema.ipynb first."
        )

    if not spark.catalog.tableExists(volatility_table):
        raise RuntimeError(
            f"Target table {volatility_table} does not exist. Run 00_platform_setup_catalog_schema.ipynb first."
        )

    silver_df = (
        spark.table(source_table)
        .select("product_id", "bar_date", "base_asset", "quote_currency", "close")
        .filter(F.col("product_id").isin(product_ids))
        .filter((F.col("bar_date") >= F.lit(source_start_date)) & (F.col("bar_date") <= F.lit(end_date)))
    )

    rows_read = silver_df.count()
    per_product_rows_read = collect_product_counts(silver_df, "rows_read") if rows_read else {}

    if rows_read == 0:
        return GoldIngestionResult(
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

    order_window = Window.partitionBy("product_id").orderBy("bar_date")
    returns_base_df = (
        silver_df
        .withColumn("prev_close", F.lag("close").over(order_window))
        .withColumn(
            "simple_return_1d",
            F.when(F.col("prev_close") > 0, (F.col("close") / F.col("prev_close")) - F.lit(1.0)),
        )
        .withColumn(
            "log_return_1d",
            F.when((F.col("close") > 0) & (F.col("prev_close") > 0), F.log(F.col("close") / F.col("prev_close"))),
        )
    )

    computed_at = datetime.now(UTC)
    returns_df = (
        returns_base_df
        .filter((F.col("bar_date") >= F.lit(start_date)) & (F.col("bar_date") <= F.lit(end_date)))
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
    per_product_rows_returns = collect_product_counts(returns_df, "rows_returns") if rows_returns_ready else {}

    window_7 = order_window.rowsBetween(-6, 0)
    window_30 = order_window.rowsBetween(-29, 0)
    window_90 = order_window.rowsBetween(-89, 0)

    volatility_base_df = (
        returns_base_df
        .withColumn("returns_count_7", F.count("simple_return_1d").over(window_7))
        .withColumn("returns_count_30", F.count("simple_return_1d").over(window_30))
        .withColumn("returns_count_90", F.count("simple_return_1d").over(window_90))
        .withColumn(
            "volatility_7d",
            F.when(F.col("returns_count_7") == 7, F.stddev_samp("simple_return_1d").over(window_7)),
        )
        .withColumn(
            "volatility_30d",
            F.when(F.col("returns_count_30") == 30, F.stddev_samp("simple_return_1d").over(window_30)),
        )
        .withColumn(
            "volatility_90d",
            F.when(F.col("returns_count_90") == 90, F.stddev_samp("simple_return_1d").over(window_90)),
        )
    )

    volatility_df = (
        volatility_base_df
        .filter((F.col("bar_date") >= F.lit(start_date)) & (F.col("bar_date") <= F.lit(end_date)))
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
        collect_product_counts(volatility_df, "rows_volatility") if rows_volatility_ready else {}
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

    return GoldIngestionResult(
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
        rows_volatility_to_insert=rows_volatility_ready - existing_volatility_key_count,
        rows_volatility_merged=rows_volatility_ready,
        run_id=run_id,
        per_product_rows_read=per_product_rows_read,
        per_product_rows_returns=per_product_rows_returns,
        per_product_rows_volatility=per_product_rows_volatility,
    )


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

    if normalized_source_system == "ecb":
        quote_currencies = parse_quote_currencies(raw_quote_currencies)
        source_table = ecb_source_table or f"{catalog}.slv_macro.ecb_fx_ref_rates_daily"
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
            .filter((F.col("rate_date") >= F.lit(start_date)) & (F.col("rate_date") <= F.lit(end_date)))
        )

        rows_read = silver_df.count()
        if rows_read == 0:
            return MacroGoldIngestionResult(
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

        computed_at = datetime.now(UTC)
        gold_df = (
            silver_df
            .withColumn(
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
            .withColumn("unit", F.concat(F.col("quote_currency"), F.lit(" per "), F.col("base_currency")))
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
                spark.table(target_table).select("indicator_id", "observation_date"),
                on=["indicator_id", "observation_date"],
                how="inner",
            )
            .count()
        )
        per_indicator_rows_ready = collect_counts(gold_df, "indicator_id", "rows_ready")

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

        return MacroGoldIngestionResult(
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

    if normalized_source_system == "fred":
        series_ids = parse_series_ids(raw_series_ids)
        source_table = fred_source_table or f"{catalog}.slv_macro.fred_series_clean"
        metadata_table = fred_metadata_table or f"{catalog}.brz_macro.raw_fred_series_metadata"
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
        if rows_read == 0:
            return MacroGoldIngestionResult(
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

        metadata_window = Window.partitionBy("series_id").orderBy(
            F.col("ingested_at").desc(),
            F.col("payload_hash").desc(),
        )
        metadata_latest_df = (
            spark.table(metadata_table)
            .select("series_id", "units", "frequency_short", "ingested_at", "payload_hash")
            .filter(F.col("series_id").isin(series_ids))
            .withColumn("_row_number", F.row_number().over(metadata_window))
            .filter(F.col("_row_number") == 1)
            .drop("_row_number")
        )

        metadata_ids = {row["series_id"] for row in metadata_latest_df.select("series_id").collect()}
        missing_metadata_ids = sorted(set(series_ids) - metadata_ids)
        if missing_metadata_ids:
            raise RuntimeError(
                "Missing FRED metadata for requested series_ids: " + ", ".join(missing_metadata_ids)
            )

        metadata_missing_units = [
            row["series_id"]
            for row in metadata_latest_df.filter(
                F.col("units").isNull() | (F.trim(F.col("units")) == "")
            ).select("series_id").collect()
        ]
        if metadata_missing_units:
            raise RuntimeError(
                "Missing FRED units for series_ids: " + ", ".join(sorted(metadata_missing_units))
            )

        metadata_missing_frequency = [
            row["series_id"]
            for row in metadata_latest_df.filter(
                F.col("frequency_short").isNull() | (F.trim(F.col("frequency_short")) == "")
            ).select("series_id").collect()
        ]
        if metadata_missing_frequency:
            raise RuntimeError(
                "Missing FRED frequency_short for series_ids: "
                + ", ".join(sorted(metadata_missing_frequency))
            )

        revision_window = Window.partitionBy("series_id", "observation_date").orderBy(
            F.col("realtime_start").desc(),
            F.col("realtime_end").desc(),
            F.col("ingested_at").desc(),
        )
        latest_revision_df = (
            silver_df
            .withColumn("_row_number", F.row_number().over(revision_window))
            .filter(F.col("_row_number") == 1)
            .drop("_row_number")
        )
        rows_after_revision_collapse = latest_revision_df.count()

        computed_at = datetime.now(UTC)
        gold_df = (
            latest_revision_df
            .join(
                metadata_latest_df.select("series_id", "units", "frequency_short"),
                on="series_id",
                how="inner",
            )
            .withColumn("indicator_id", F.concat(F.lit("FRED_"), F.col("series_id")))
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
                spark.table(target_table).select("indicator_id", "observation_date"),
                on=["indicator_id", "observation_date"],
                how="inner",
            )
            .count()
        )
        per_indicator_rows_ready = collect_counts(gold_df, "indicator_id", "rows_ready")

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

        return MacroGoldIngestionResult(
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

    raise ValueError("source_system must be one of: ecb, fred")
