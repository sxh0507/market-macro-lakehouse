"""Gold ingestion orchestration."""

from __future__ import annotations

from datetime import datetime, timedelta
from typing import Any, Callable

from lakehouse.common.models import GoldIngestionResult
from lakehouse.common.runtime import UTC, parse_product_ids, resolve_date_window

MAX_VOLATILITY_LOOKBACK_DAYS = 90


def collect_product_counts(df: Any, count_alias: str) -> dict[str, int]:
    """Collect row counts grouped by product_id into a Python dictionary."""

    counts = (
        df.groupBy("product_id")
        .count()
        .withColumnRenamed("count", count_alias)
        .collect()
    )
    return {row["product_id"]: int(row[count_alias]) for row in counts}


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
