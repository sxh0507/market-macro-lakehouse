"""Silver ingestion orchestration."""

from __future__ import annotations

from datetime import datetime, timedelta
from typing import Any, Callable

from lakehouse.common.models import (
    EcbSilverIngestionResult,
    FredSilverIngestionResult,
    SilverIngestionResult,
)
from lakehouse.common.runtime import (
    UTC,
    parse_product_ids,
    parse_series_ids,
    resolve_date_window,
)
from lakehouse.observability import PipelineObserver
from lakehouse.sources.ecb import EcbSource

FRED_STRUCTURAL_DQ_REASONS = (
    "MISSING_SERIES_ID",
    "MISSING_OBSERVATION_DATE",
    "MISSING_REALTIME_START",
    "MISSING_REALTIME_END",
    "INVALID_REALTIME_ORDER",
)


def collect_counts(df: Any, key_column: str, count_alias: str) -> dict[str, int]:
    """Collect row counts grouped by the requested key into a Python dictionary."""

    counts = (
        df.groupBy(key_column).count().withColumnRenamed("count", count_alias).collect()
    )
    return {row[key_column]: int(row[count_alias]) for row in counts}


def determine_fred_dq_reason(
    *,
    series_id: str | None,
    observation_date: Any,
    realtime_start: Any,
    realtime_end: Any,
    metadata_present: bool | None,
    value: Any,
) -> str | None:
    """Apply the canonical FRED Silver DQ rule ordering to a single logical row."""

    if series_id is None:
        return "MISSING_SERIES_ID"
    if observation_date is None:
        return "MISSING_OBSERVATION_DATE"
    if realtime_start is None:
        return "MISSING_REALTIME_START"
    if realtime_end is None:
        return "MISSING_REALTIME_END"
    if realtime_start > realtime_end:
        return "INVALID_REALTIME_ORDER"
    if metadata_present is not True:
        return "MISSING_METADATA"
    if value is None:
        return "NULL_VALUE"
    return None


def build_fred_dq_reason_expr(F: Any) -> Any:
    """Build the Spark expression that mirrors the canonical FRED DQ rule ordering."""

    return (
        F.when(F.col("series_id").isNull(), F.lit("MISSING_SERIES_ID"))
        .when(F.col("observation_date").isNull(), F.lit("MISSING_OBSERVATION_DATE"))
        .when(F.col("realtime_start").isNull(), F.lit("MISSING_REALTIME_START"))
        .when(F.col("realtime_end").isNull(), F.lit("MISSING_REALTIME_END"))
        .when(
            F.col("realtime_start") > F.col("realtime_end"),
            F.lit("INVALID_REALTIME_ORDER"),
        )
        .when(
            F.col("metadata_present").isNull()
            | (F.col("metadata_present") == F.lit(False)),
            F.lit("MISSING_METADATA"),
        )
        .when(F.col("value").isNull(), F.lit("NULL_VALUE"))
    )


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
    start_date, end_date = resolve_date_window(
        mode, start_date_raw, end_date_raw, lookback_days_raw
    )
    source_table = source_table or f"{catalog}.brz_market.raw_coinbase_ohlc_1d"
    target_table = target_table or f"{catalog}.slv_market.crypto_ohlc_1d"
    quarantine_table = (
        quarantine_table or f"{catalog}.slv_market.crypto_ohlc_1d_quarantine"
    )

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
    observer = PipelineObserver(
        spark=spark,
        catalog=catalog,
        pipeline_name="silver_market_crypto_ohlc_1d",
        layer="silver",
        source_name="coinbase",
        target_table=target_table,
        run_id=run_id,
        start_metadata={
            "mode": mode,
            "product_ids": product_ids,
            "source_table": source_table,
            "quarantine_table": quarantine_table,
            "start_date": start_date.isoformat(),
            "end_date": end_date.isoformat(),
        },
    )

    with observer:
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
            .filter(
                (F.col("bar_date") >= F.lit(start_date))
                & (F.col("bar_date") <= F.lit(end_date))
            )
        )

        rows_read = bronze_df.count()
        observer.update_progress(rows_read=rows_read)
        per_product_rows_read = (
            collect_counts(bronze_df, "product_id", "rows_read") if rows_read else {}
        )

        if rows_read == 0:
            result = SilverIngestionResult(
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
            observer.succeed(
                status=result.status,
                rows_read=result.rows_read,
                rows_written=result.rows_merged,
                metadata=result.as_dict(),
                watermark_type="bar_date",
                watermark_column="bar_date",
            )
            return result

        dedup_window = Window.partitionBy("product_id", "bar_date").orderBy(
            F.col("source_window_end").desc(),
            F.col("ingested_at").desc(),
            F.col("payload_hash").desc(),
        )

        bronze_latest_df = (
            bronze_df.withColumn("_row_number", F.row_number().over(dedup_window))
            .filter(F.col("_row_number") == 1)
            .drop("_row_number")
        )

        rows_after_dedup = bronze_latest_df.count()
        per_product_rows_after_dedup = collect_counts(
            bronze_latest_df, "product_id", "rows_after_dedup"
        )

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
            observer.update_progress(rows_read=rows_read)
            if display_fn is not None:
                display_fn(structural_invalid_df.orderBy("product_id", "bar_date"))
            raise RuntimeError(
                f"Found {rows_structural_invalid} structurally invalid Bronze rows. Fix Bronze data before loading Silver."
            )

        silver_ingested_at = datetime.now(UTC)
        transformed_df = bronze_latest_df.withColumn(
            "base_asset", F.element_at(product_parts, 1)
        ).withColumn("quote_currency", F.element_at(product_parts, 2))

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
            collect_counts(rejected_df, "product_id", "rows_rejected")
            if rows_rejected
            else {}
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
            quarantine_df.write.format("delta").mode("append").saveAsTable(
                quarantine_table
            )

        valid_df = (
            assessed_df.filter(F.col("dq_reason").isNull())
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
        per_product_rows_merged = (
            collect_counts(valid_df, "product_id", "rows_merged") if rows_valid else {}
        )

        if rows_valid == 0:
            if rows_quarantined > 0 and display_fn is not None:
                display_fn(quarantine_df.orderBy("product_id", "bar_date", "dq_reason"))

            result = SilverIngestionResult(
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
            observer.succeed(
                status=result.status,
                rows_read=result.rows_read,
                rows_written=result.rows_merged,
                metadata=result.as_dict(),
                watermark_type="bar_date",
                watermark_column="bar_date",
            )
            return result

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

        result = SilverIngestionResult(
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
        observer.succeed(
            status=result.status,
            rows_read=result.rows_read,
            rows_written=result.rows_merged,
            metadata=result.as_dict(),
            watermark_type="bar_date",
            watermark_column="bar_date",
        )
        return result


def run_silver_ecb_fx_ref_rates_daily(
    spark: Any,
    raw_quote_currencies: str,
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
    source: EcbSource | None = None,
) -> EcbSilverIngestionResult:
    """Run the validated Silver ECB FX flow from package code."""

    from delta.tables import DeltaTable  # type: ignore import-not-found
    from pyspark.sql import functions as F  # type: ignore import-not-found
    from pyspark.sql.window import Window  # type: ignore import-not-found

    source = source or EcbSource()
    quote_currencies = source.parse_quote_currencies(raw_quote_currencies)
    latest_complete_date = datetime.now(source.local_timezone).date() - timedelta(
        days=1
    )
    start_date, end_date = resolve_date_window(
        mode,
        start_date_raw,
        end_date_raw,
        lookback_days_raw,
        latest_complete_date=latest_complete_date,
        latest_complete_timezone_label="Europe/Berlin",
    )
    source_table = source_table or f"{catalog}.brz_macro.raw_ecb_fx_ref_rates_daily"
    target_table = target_table or f"{catalog}.slv_macro.ecb_fx_ref_rates_daily"
    quarantine_table = (
        quarantine_table or f"{catalog}.slv_macro.ecb_fx_ref_rates_daily_quarantine"
    )

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
    observer = PipelineObserver(
        spark=spark,
        catalog=catalog,
        pipeline_name="silver_macro_ecb_fx_ref_rates_daily",
        layer="silver",
        source_name="ecb",
        target_table=target_table,
        run_id=run_id,
        start_metadata={
            "mode": mode,
            "quote_currencies": quote_currencies,
            "source_table": source_table,
            "quarantine_table": quarantine_table,
            "start_date": start_date.isoformat(),
            "end_date": end_date.isoformat(),
        },
    )

    with observer:
        bronze_df = (
            spark.table(source_table)
            .select(
                F.upper(F.col("base_currency")).alias("base_currency"),
                F.upper(F.col("quote_currency")).alias("quote_currency"),
                F.col("rate_date"),
                F.col("rate"),
                F.col("ingested_at").alias("source_ingested_at"),
                F.col("run_id").alias("source_run_id"),
                F.col("payload_hash"),
            )
            .filter(F.col("quote_currency").isin(quote_currencies))
            .filter(
                (F.col("rate_date") >= F.lit(start_date))
                & (F.col("rate_date") <= F.lit(end_date))
            )
        )

        rows_read = bronze_df.count()
        observer.update_progress(rows_read=rows_read)
        per_currency_rows_read = (
            collect_counts(bronze_df, "quote_currency", "rows_read")
            if rows_read
            else {}
        )

        if rows_read == 0:
            result = EcbSilverIngestionResult(
                status="success_empty",
                source_system="ecb",
                mode=mode,
                quote_currencies=quote_currencies,
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
                per_currency_rows_read=per_currency_rows_read,
            )
            observer.succeed(
                status=result.status,
                rows_read=result.rows_read,
                rows_written=result.rows_merged,
                metadata=result.as_dict(),
                watermark_type="rate_date",
                watermark_column="rate_date",
            )
            return result

        dedup_window = Window.partitionBy(
            "base_currency", "quote_currency", "rate_date"
        ).orderBy(
            F.col("source_ingested_at").desc(),
            F.col("payload_hash").desc(),
        )

        bronze_latest_df = (
            bronze_df.withColumn("_row_number", F.row_number().over(dedup_window))
            .filter(F.col("_row_number") == 1)
            .drop("_row_number")
        )

        rows_after_dedup = bronze_latest_df.count()
        per_currency_rows_after_dedup = collect_counts(
            bronze_latest_df,
            "quote_currency",
            "rows_after_dedup",
        )

        structural_invalid_df = bronze_latest_df.filter(
            F.col("base_currency").isNull()
            | F.col("quote_currency").isNull()
            | F.col("rate_date").isNull()
            | (~F.col("base_currency").rlike("^[A-Z]{3}$"))
            | (~F.col("quote_currency").rlike("^[A-Z]{3}$"))
            | (F.col("base_currency") != F.lit("EUR"))
        )

        rows_structural_invalid = structural_invalid_df.count()
        if rows_structural_invalid > 0:
            if display_fn is not None:
                display_fn(structural_invalid_df.orderBy("quote_currency", "rate_date"))
            raise RuntimeError(
                f"Detected {rows_structural_invalid} structural-invalid ECB rows in Bronze. Silver load aborted."
            )

        silver_ingested_at = datetime.now(UTC)

        dq_reason = F.when(F.col("rate").isNull(), F.lit("rate_null")).when(
            F.col("rate") <= F.lit(0),
            F.lit("rate_non_positive"),
        )

        assessed_df = bronze_latest_df.withColumn("dq_reason", dq_reason)
        rejected_df = assessed_df.filter(F.col("dq_reason").isNotNull())
        rows_rejected = rejected_df.count()
        per_currency_rows_rejected = (
            collect_counts(rejected_df, "quote_currency", "rows_rejected")
            if rows_rejected
            else {}
        )

        quarantine_df = rejected_df.select(
            F.lit(run_id).alias("run_id"),
            "base_currency",
            "quote_currency",
            "rate_date",
            "rate",
            "dq_reason",
            "source_ingested_at",
            "source_run_id",
            "payload_hash",
            F.lit(silver_ingested_at).alias("quarantined_at"),
        )

        rows_quarantined = quarantine_df.count()
        if rows_quarantined > 0:
            quarantine_df.write.format("delta").mode("append").saveAsTable(
                quarantine_table
            )

        valid_df = (
            assessed_df.filter(F.col("dq_reason").isNull())
            .select(
                "base_currency",
                "quote_currency",
                "rate_date",
                "rate",
            )
            .withColumn("ingested_at", F.lit(silver_ingested_at))
            .withColumn("run_id", F.lit(run_id))
        )

        rows_valid = valid_df.count()
        per_currency_rows_merged = (
            collect_counts(valid_df, "quote_currency", "rows_merged")
            if rows_valid
            else {}
        )

        if rows_valid == 0:
            if rows_quarantined > 0 and display_fn is not None:
                display_fn(
                    quarantine_df.orderBy("quote_currency", "rate_date", "dq_reason")
                )

            result = EcbSilverIngestionResult(
                status="success_empty_valid",
                source_system="ecb",
                mode=mode,
                quote_currencies=quote_currencies,
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
                per_currency_rows_read=per_currency_rows_read,
                per_currency_rows_after_dedup=per_currency_rows_after_dedup,
                per_currency_rows_rejected=per_currency_rows_rejected,
            )
            observer.succeed(
                status=result.status,
                rows_read=result.rows_read,
                rows_written=result.rows_merged,
                metadata=result.as_dict(),
                watermark_type="rate_date",
                watermark_column="rate_date",
            )
            return result

        existing_key_count = (
            valid_df.select("base_currency", "quote_currency", "rate_date")
            .join(
                spark.table(target_table).select(
                    "base_currency", "quote_currency", "rate_date"
                ),
                on=["base_currency", "quote_currency", "rate_date"],
                how="inner",
            )
            .count()
        )

        DeltaTable.forName(spark, target_table).alias("tgt").merge(
            valid_df.alias("src"),
            "tgt.base_currency = src.base_currency "
            "AND tgt.quote_currency = src.quote_currency "
            "AND tgt.rate_date = src.rate_date",
        ).whenMatchedUpdate(
            set={
                "rate": "src.rate",
                "ingested_at": "src.ingested_at",
                "run_id": "src.run_id",
            }
        ).whenNotMatchedInsertAll().execute()

        if display_fn is not None:
            display_fn(valid_df.orderBy("quote_currency", "rate_date"))
            if rows_quarantined > 0:
                display_fn(
                    quarantine_df.orderBy("quote_currency", "rate_date", "dq_reason")
                )

        result = EcbSilverIngestionResult(
            status="success",
            source_system="ecb",
            mode=mode,
            quote_currencies=quote_currencies,
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
            per_currency_rows_read=per_currency_rows_read,
            per_currency_rows_after_dedup=per_currency_rows_after_dedup,
            per_currency_rows_rejected=per_currency_rows_rejected,
            per_currency_rows_merged=per_currency_rows_merged,
        )
        observer.succeed(
            status=result.status,
            rows_read=result.rows_read,
            rows_written=result.rows_merged,
            metadata=result.as_dict(),
            watermark_type="rate_date",
            watermark_column="rate_date",
        )
        return result


def run_silver_fred_series_clean(
    spark: Any,
    raw_series_ids: str,
    mode: str,
    start_date_raw: str,
    end_date_raw: str,
    lookback_days_raw: str,
    run_id: str,
    catalog: str = "market_macro",
    source_table: str | None = None,
    metadata_table: str | None = None,
    target_table: str | None = None,
    quarantine_table: str | None = None,
    display_fn: Callable[[Any], None] | None = None,
) -> FredSilverIngestionResult:
    """Run the validated Silver FRED series flow from package code."""

    from delta.tables import DeltaTable  # type: ignore import-not-found
    from pyspark.sql import functions as F  # type: ignore import-not-found
    from pyspark.sql.window import Window  # type: ignore import-not-found

    dedup_strategy = "technical_duplicate_elimination_on_full_revision_key"

    series_ids = parse_series_ids(raw_series_ids)
    start_date, end_date = resolve_date_window(
        mode, start_date_raw, end_date_raw, lookback_days_raw
    )
    source_table = source_table or f"{catalog}.brz_macro.raw_fred_series"
    metadata_table = metadata_table or f"{catalog}.brz_macro.raw_fred_series_metadata"
    target_table = target_table or f"{catalog}.slv_macro.fred_series_clean"
    quarantine_table = (
        quarantine_table or f"{catalog}.slv_macro.fred_series_clean_quarantine"
    )

    for table_name in [source_table, metadata_table, target_table, quarantine_table]:
        if not spark.catalog.tableExists(table_name):
            raise RuntimeError(
                f"Required table {table_name} does not exist. Run 00_platform_setup_catalog_schema.ipynb first."
            )
    observer = PipelineObserver(
        spark=spark,
        catalog=catalog,
        pipeline_name="silver_macro_fred_series_clean",
        layer="silver",
        source_name="fred",
        target_table=target_table,
        run_id=run_id,
        start_metadata={
            "mode": mode,
            "series_ids": series_ids,
            "source_table": source_table,
            "metadata_table": metadata_table,
            "quarantine_table": quarantine_table,
            "dedup_strategy": dedup_strategy,
            "start_date": start_date.isoformat(),
            "end_date": end_date.isoformat(),
        },
    )

    with observer:
        bronze_df = (
            spark.table(source_table)
            .select(
                "series_id",
                "observation_date",
                "realtime_start",
                "realtime_end",
                "value_raw",
                "value",
                F.col("ingested_at").alias("source_ingested_at"),
                F.col("run_id").alias("source_run_id"),
                "payload_hash",
            )
            .filter(F.col("series_id").isin(series_ids))
            .filter(
                (F.col("observation_date") >= F.lit(start_date))
                & (F.col("observation_date") <= F.lit(end_date))
            )
        )

        available_metadata_ids = {
            row["series_id"]
            for row in (
                spark.table(metadata_table)
                .select("series_id")
                .filter(F.col("series_id").isin(series_ids))
                .dropDuplicates()
                .collect()
            )
        }
        missing_metadata_ids = sorted(set(series_ids) - available_metadata_ids)
        metadata_complete = len(missing_metadata_ids) == 0

        rows_read = bronze_df.count()
        observer.update_progress(rows_read=rows_read)
        per_series_rows_read = (
            collect_counts(bronze_df, "series_id", "rows_read") if rows_read else {}
        )

        if rows_read == 0:
            result = FredSilverIngestionResult(
                status="success_empty",
                source_system="fred",
                mode=mode,
                dedup_strategy=dedup_strategy,
                series_ids=series_ids,
                series_ids_missing_metadata=missing_metadata_ids,
                metadata_complete=metadata_complete,
                start_date=start_date.isoformat(),
                end_date=end_date.isoformat(),
                source_table=source_table,
                metadata_table=metadata_table,
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
                per_series_rows_read=per_series_rows_read,
            )
            observer.succeed(
                status=result.status,
                rows_read=result.rows_read,
                rows_written=result.rows_merged,
                metadata=result.as_dict(),
                watermark_type="observation_date",
                watermark_column="observation_date",
            )
            return result

        dedup_window = Window.partitionBy(
            "series_id",
            "observation_date",
            "realtime_start",
            "realtime_end",
        ).orderBy(
            F.col("source_ingested_at").desc(),
            F.col("payload_hash").desc(),
        )

        bronze_latest_df = (
            bronze_df.withColumn("_row_number", F.row_number().over(dedup_window))
            .filter(F.col("_row_number") == 1)
            .drop("_row_number")
        )

        rows_after_dedup = bronze_latest_df.count()
        per_series_rows_after_dedup = collect_counts(
            bronze_latest_df,
            "series_id",
            "rows_after_dedup",
        )

        if available_metadata_ids:
            metadata_flags_df = (
                spark.table(metadata_table)
                .select("series_id")
                .filter(F.col("series_id").isin(sorted(available_metadata_ids)))
                .dropDuplicates()
                .withColumn("metadata_present", F.lit(True))
            )
            assessed_df = bronze_latest_df.join(
                metadata_flags_df, on="series_id", how="left"
            )
        else:
            assessed_df = bronze_latest_df.withColumn(
                "metadata_present", F.lit(None).cast("boolean")
            )

        assessed_df = assessed_df.withColumn("dq_reason", build_fred_dq_reason_expr(F))
        rows_structural_invalid = assessed_df.filter(
            F.col("dq_reason").isin(FRED_STRUCTURAL_DQ_REASONS)
        ).count()

        rejected_df = assessed_df.filter(F.col("dq_reason").isNotNull())
        rows_rejected = rejected_df.count()
        per_series_rows_rejected = (
            collect_counts(rejected_df, "series_id", "rows_rejected")
            if rows_rejected
            else {}
        )

        silver_ingested_at = datetime.now(UTC)
        quarantine_df = rejected_df.select(
            F.lit(run_id).alias("run_id"),
            "series_id",
            "observation_date",
            "realtime_start",
            "realtime_end",
            "value_raw",
            "value",
            "dq_reason",
            "source_ingested_at",
            "source_run_id",
            "payload_hash",
            F.lit(silver_ingested_at).alias("quarantined_at"),
        )

        rows_quarantined = quarantine_df.count()
        if rows_quarantined > 0:
            quarantine_df.write.format("delta").mode("append").saveAsTable(
                quarantine_table
            )

        valid_df = (
            assessed_df.filter(F.col("dq_reason").isNull())
            .select(
                "series_id",
                "observation_date",
                "realtime_start",
                "realtime_end",
                "value",
            )
            .withColumn("ingested_at", F.lit(silver_ingested_at))
            .withColumn("run_id", F.lit(run_id))
        )

        rows_valid = valid_df.count()
        per_series_rows_merged = (
            collect_counts(valid_df, "series_id", "rows_merged") if rows_valid else {}
        )

        if rows_valid == 0:
            if rows_quarantined > 0 and display_fn is not None:
                display_fn(
                    quarantine_df.orderBy(
                        "series_id", "observation_date", "realtime_start", "dq_reason"
                    )
                )

            result = FredSilverIngestionResult(
                status="success_empty_valid",
                source_system="fred",
                mode=mode,
                dedup_strategy=dedup_strategy,
                series_ids=series_ids,
                series_ids_missing_metadata=missing_metadata_ids,
                metadata_complete=metadata_complete,
                start_date=start_date.isoformat(),
                end_date=end_date.isoformat(),
                source_table=source_table,
                metadata_table=metadata_table,
                target_table=target_table,
                quarantine_table=quarantine_table,
                rows_read=rows_read,
                rows_after_dedup=rows_after_dedup,
                rows_structural_invalid=rows_structural_invalid,
                rows_rejected=rows_rejected,
                rows_quarantined=rows_quarantined,
                rows_to_update=0,
                rows_to_insert=0,
                rows_merged=0,
                run_id=run_id,
                per_series_rows_read=per_series_rows_read,
                per_series_rows_after_dedup=per_series_rows_after_dedup,
                per_series_rows_rejected=per_series_rows_rejected,
                per_series_rows_merged=per_series_rows_merged,
            )
            observer.succeed(
                status=result.status,
                rows_read=result.rows_read,
                rows_written=result.rows_merged,
                metadata=result.as_dict(),
                watermark_type="observation_date",
                watermark_column="observation_date",
            )
            return result

        existing_key_count = (
            valid_df.select(
                "series_id", "observation_date", "realtime_start", "realtime_end"
            )
            .join(
                spark.table(target_table).select(
                    "series_id",
                    "observation_date",
                    "realtime_start",
                    "realtime_end",
                ),
                on=["series_id", "observation_date", "realtime_start", "realtime_end"],
                how="inner",
            )
            .count()
        )

        DeltaTable.forName(spark, target_table).alias("tgt").merge(
            valid_df.alias("src"),
            "tgt.series_id = src.series_id "
            "AND tgt.observation_date = src.observation_date "
            "AND tgt.realtime_start = src.realtime_start "
            "AND tgt.realtime_end = src.realtime_end",
        ).whenMatchedUpdate(
            set={
                "value": "src.value",
                "ingested_at": "src.ingested_at",
                "run_id": "src.run_id",
            }
        ).whenNotMatchedInsertAll().execute()

        if display_fn is not None:
            display_fn(
                valid_df.orderBy(
                    "series_id", "observation_date", "realtime_start", "realtime_end"
                )
            )
            if rows_quarantined > 0:
                display_fn(
                    quarantine_df.orderBy(
                        "series_id", "observation_date", "realtime_start", "dq_reason"
                    )
                )

        result = FredSilverIngestionResult(
            status="success",
            source_system="fred",
            mode=mode,
            dedup_strategy=dedup_strategy,
            series_ids=series_ids,
            series_ids_missing_metadata=missing_metadata_ids,
            metadata_complete=metadata_complete,
            start_date=start_date.isoformat(),
            end_date=end_date.isoformat(),
            source_table=source_table,
            metadata_table=metadata_table,
            target_table=target_table,
            quarantine_table=quarantine_table,
            rows_read=rows_read,
            rows_after_dedup=rows_after_dedup,
            rows_structural_invalid=rows_structural_invalid,
            rows_rejected=rows_rejected,
            rows_quarantined=rows_quarantined,
            rows_to_update=existing_key_count,
            rows_to_insert=rows_valid - existing_key_count,
            rows_merged=rows_valid,
            run_id=run_id,
            per_series_rows_read=per_series_rows_read,
            per_series_rows_after_dedup=per_series_rows_after_dedup,
            per_series_rows_rejected=per_series_rows_rejected,
            per_series_rows_merged=per_series_rows_merged,
        )
        observer.succeed(
            status=result.status,
            rows_read=result.rows_read,
            rows_written=result.rows_merged,
            metadata=result.as_dict(),
            watermark_type="observation_date",
            watermark_column="observation_date",
        )
        return result
