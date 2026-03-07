"""Silver ingestion orchestration."""

from __future__ import annotations

from datetime import datetime
from typing import Any, Callable

from lakehouse.common.models import SilverIngestionResult
from lakehouse.common.runtime import UTC, parse_product_ids, resolve_date_window


def collect_product_counts(df: Any, count_alias: str) -> dict[str, int]:
    """Collect row counts grouped by product_id into a Python dictionary."""

    counts = (
        df.groupBy("product_id")
        .count()
        .withColumnRenamed("count", count_alias)
        .collect()
    )
    return {row["product_id"]: int(row[count_alias]) for row in counts}


def run_silver_crypto_ohlc_1d(
    spark: Any,
    raw_product_ids: str,
    mode: str,
    start_date_raw: str,
    end_date_raw: str,
    lookback_days_raw: str,
    run_id: str,
    catalog: str = "market_macro",
    source_table: str | None = None,
    target_table: str | None = None,
    quarantine_table: str | None = None,
    display_fn: Callable[[Any], None] | None = None,
) -> SilverIngestionResult:
    """Run the validated Silver market crypto OHLC flow from package code."""

    from delta.tables import DeltaTable  # type: ignore import-not-found
    from pyspark.sql import functions as F  # type: ignore import-not-found
    from pyspark.sql.window import Window  # type: ignore import-not-found

    product_ids = parse_product_ids(raw_product_ids)
    start_date, end_date = resolve_date_window(mode, start_date_raw, end_date_raw, lookback_days_raw)
    source_table = source_table or f"{catalog}.brz_market.raw_coinbase_ohlc_1d"
    target_table = target_table or f"{catalog}.slv_market.crypto_ohlc_1d"
    quarantine_table = quarantine_table or f"{catalog}.slv_market.crypto_ohlc_1d_quarantine"

    if not spark.catalog.tableExists(source_table):
        raise RuntimeError(
            f"Source table {source_table} does not exist. Run 00_platform_setup_catalog_schema.ipynb first."
        )

    if not spark.catalog.tableExists(target_table):
        raise RuntimeError(
            f"Target table {target_table} does not exist. Run 00_platform_setup_catalog_schema.ipynb first."
        )

    if not spark.catalog.tableExists(quarantine_table):
        raise RuntimeError(
            f"Quarantine table {quarantine_table} does not exist. Run 00_platform_setup_catalog_schema.ipynb first."
        )

    bronze_df = (
        spark.table(source_table)
        .select(
            "product_id",
            "bar_date",
            "open",
            "high",
            "low",
            "close",
            "volume",
            "source_window_start",
            "source_window_end",
            "ingested_at",
            "run_id",
            "payload_hash",
        )
        .filter(F.col("product_id").isin(product_ids))
        .filter((F.col("bar_date") >= F.lit(start_date)) & (F.col("bar_date") <= F.lit(end_date)))
    )

    rows_read = bronze_df.count()
    per_product_rows_read = collect_product_counts(bronze_df, "rows_read") if rows_read else {}

    if rows_read == 0:
        return SilverIngestionResult(
            status="success_empty",
            mode=mode,
            product_ids=product_ids,
            start_date=start_date.isoformat(),
            end_date=end_date.isoformat(),
            source_table=source_table,
            target_table=target_table,
            quarantine_table=quarantine_table,
            rows_read=0,
            rows_after_dedup=0,
            rows_structural_invalid=0,
            rows_rejected=0,
            rows_quarantined=0,
            rows_to_update=0,
            rows_to_insert=0,
            rows_merged=0,
            run_id=run_id,
            per_product_rows_read=per_product_rows_read,
        )

    dedup_window = Window.partitionBy("product_id", "bar_date").orderBy(
        F.col("source_window_end").desc(),
        F.col("ingested_at").desc(),
        F.col("payload_hash").desc(),
    )

    bronze_latest_df = (
        bronze_df
        .withColumn("_row_number", F.row_number().over(dedup_window))
        .filter(F.col("_row_number") == 1)
        .drop("_row_number")
    )

    rows_after_dedup = bronze_latest_df.count()
    per_product_rows_after_dedup = collect_product_counts(bronze_latest_df, "rows_after_dedup")

    product_parts = F.split(F.col("product_id"), "-")
    structural_invalid_df = bronze_latest_df.filter(
        F.col("product_id").isNull()
        | F.col("bar_date").isNull()
        | (F.size(product_parts) != 2)
        | (F.element_at(product_parts, 1) == "")
        | (F.element_at(product_parts, 2) == "")
    )

    rows_structural_invalid = structural_invalid_df.count()
    if rows_structural_invalid > 0:
        if display_fn is not None:
            display_fn(structural_invalid_df.orderBy("product_id", "bar_date"))
        raise RuntimeError(
            f"Found {rows_structural_invalid} structurally invalid Bronze rows. Fix Bronze data before loading Silver."
        )

    silver_ingested_at = datetime.now(UTC)
    transformed_df = (
        bronze_latest_df
        .withColumn("base_asset", F.element_at(product_parts, 1))
        .withColumn("quote_currency", F.element_at(product_parts, 2))
    )

    dq_reason = (
        F.when(F.col("open").isNull(), F.lit("open_null"))
        .when(F.col("high").isNull(), F.lit("high_null"))
        .when(F.col("low").isNull(), F.lit("low_null"))
        .when(F.col("close").isNull(), F.lit("close_null"))
        .when(F.col("volume").isNull(), F.lit("volume_null"))
        .when(F.col("open") < 0, F.lit("open_negative"))
        .when(F.col("high") < 0, F.lit("high_negative"))
        .when(F.col("low") < 0, F.lit("low_negative"))
        .when(F.col("close") < 0, F.lit("close_negative"))
        .when(F.col("volume") < 0, F.lit("volume_negative"))
        .when(F.col("high") < F.col("low"), F.lit("high_below_low"))
        .when(F.col("open") < F.col("low"), F.lit("open_below_low"))
        .when(F.col("open") > F.col("high"), F.lit("open_above_high"))
        .when(F.col("close") < F.col("low"), F.lit("close_below_low"))
        .when(F.col("close") > F.col("high"), F.lit("close_above_high"))
    )

    assessed_df = transformed_df.withColumn("dq_reason", dq_reason)
    rejected_df = assessed_df.filter(F.col("dq_reason").isNotNull())
    rows_rejected = rejected_df.count()
    per_product_rows_rejected = (
        collect_product_counts(rejected_df, "rows_rejected") if rows_rejected else {}
    )

    quarantine_df = rejected_df.select(
        F.lit(run_id).alias("run_id"),
        "product_id",
        "bar_date",
        "base_asset",
        "quote_currency",
        "open",
        "high",
        "low",
        "close",
        "volume",
        "dq_reason",
        "source_window_start",
        "source_window_end",
        F.col("ingested_at").alias("source_ingested_at"),
        F.col("run_id").alias("source_run_id"),
        "payload_hash",
        F.lit(silver_ingested_at).alias("quarantined_at"),
    )

    rows_quarantined = quarantine_df.count()
    if rows_quarantined > 0:
        quarantine_df.write.format("delta").mode("append").saveAsTable(quarantine_table)

    valid_df = (
        assessed_df
        .filter(F.col("dq_reason").isNull())
        .select(
            "product_id",
            "bar_date",
            "base_asset",
            "quote_currency",
            "open",
            "high",
            "low",
            "close",
            "volume",
        )
        .withColumn("ingested_at", F.lit(silver_ingested_at))
        .withColumn("run_id", F.lit(run_id))
    )

    rows_valid = valid_df.count()
    per_product_rows_merged = collect_product_counts(valid_df, "rows_merged") if rows_valid else {}

    if rows_valid == 0:
        if rows_quarantined > 0 and display_fn is not None:
            display_fn(quarantine_df.orderBy("product_id", "bar_date", "dq_reason"))

        return SilverIngestionResult(
            status="success_empty_valid",
            mode=mode,
            product_ids=product_ids,
            start_date=start_date.isoformat(),
            end_date=end_date.isoformat(),
            source_table=source_table,
            target_table=target_table,
            quarantine_table=quarantine_table,
            rows_read=rows_read,
            rows_after_dedup=rows_after_dedup,
            rows_structural_invalid=0,
            rows_rejected=rows_rejected,
            rows_quarantined=rows_quarantined,
            rows_to_update=0,
            rows_to_insert=0,
            rows_merged=0,
            run_id=run_id,
            per_product_rows_read=per_product_rows_read,
            per_product_rows_after_dedup=per_product_rows_after_dedup,
            per_product_rows_rejected=per_product_rows_rejected,
        )

    existing_key_count = (
        valid_df.select("product_id", "bar_date")
        .join(
            spark.table(target_table).select("product_id", "bar_date"),
            on=["product_id", "bar_date"],
            how="inner",
        )
        .count()
    )

    DeltaTable.forName(spark, target_table).alias("tgt").merge(
        valid_df.alias("src"),
        "tgt.product_id = src.product_id AND tgt.bar_date = src.bar_date",
    ).whenMatchedUpdate(
        set={
            "base_asset": "src.base_asset",
            "quote_currency": "src.quote_currency",
            "open": "src.open",
            "high": "src.high",
            "low": "src.low",
            "close": "src.close",
            "volume": "src.volume",
            "ingested_at": "src.ingested_at",
            "run_id": "src.run_id",
        }
    ).whenNotMatchedInsertAll().execute()

    if display_fn is not None:
        display_fn(valid_df.orderBy("product_id", "bar_date"))
        if rows_quarantined > 0:
            display_fn(quarantine_df.orderBy("product_id", "bar_date", "dq_reason"))

    return SilverIngestionResult(
        status="success",
        mode=mode,
        product_ids=product_ids,
        start_date=start_date.isoformat(),
        end_date=end_date.isoformat(),
        source_table=source_table,
        target_table=target_table,
        quarantine_table=quarantine_table,
        rows_read=rows_read,
        rows_after_dedup=rows_after_dedup,
        rows_structural_invalid=0,
        rows_rejected=rows_rejected,
        rows_quarantined=rows_quarantined,
        rows_to_update=existing_key_count,
        rows_to_insert=rows_valid - existing_key_count,
        rows_merged=rows_valid,
        run_id=run_id,
        per_product_rows_read=per_product_rows_read,
        per_product_rows_after_dedup=per_product_rows_after_dedup,
        per_product_rows_rejected=per_product_rows_rejected,
        per_product_rows_merged=per_product_rows_merged,
    )
