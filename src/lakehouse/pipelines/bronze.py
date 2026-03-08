"""Bronze ingestion orchestration."""

from __future__ import annotations

from datetime import datetime, timedelta
from typing import Any, Callable

import requests

from lakehouse.common.models import BronzeIngestionResult, EcbBronzeIngestionResult, LoadResult
from lakehouse.common.runtime import parse_product_ids, resolve_date_window
from lakehouse.sources.base import SourceAdapter
from lakehouse.sources.coinbase import CoinbaseSource
from lakehouse.sources.ecb import EcbSource


def run_bronze_ingestion(
    adapter: SourceAdapter,
    run_id: str,
    rows_written: int = 0,
) -> LoadResult:
    """Return standardized Bronze load metadata for notebook entrypoints."""

    return LoadResult(
        status="success",
        table_name=adapter.bronze_table,
        rows_written=rows_written,
        run_id=run_id,
        metadata={
            **adapter.describe(),
            "layer": "bronze",
        },
    )


def build_coinbase_bronze_schema():
    """Build the Spark schema for the Coinbase Bronze target table."""

    from pyspark.sql.types import (  # type: ignore import-not-found
        DateType,
        DoubleType,
        StringType,
        StructField,
        StructType,
        TimestampType,
    )

    return StructType(
        [
            StructField("product_id", StringType(), False),
            StructField("bar_date", DateType(), False),
            StructField("open", DoubleType(), True),
            StructField("high", DoubleType(), True),
            StructField("low", DoubleType(), True),
            StructField("close", DoubleType(), True),
            StructField("volume", DoubleType(), True),
            StructField("source_window_start", TimestampType(), False),
            StructField("source_window_end", TimestampType(), False),
            StructField("ingested_at", TimestampType(), False),
            StructField("run_id", StringType(), False),
            StructField("payload_hash", StringType(), False),
        ]
    )


def build_ecb_bronze_schema():
    """Build the Spark schema for the ECB Bronze target table."""

    from pyspark.sql.types import (  # type: ignore import-not-found
        DateType,
        DoubleType,
        StringType,
        StructField,
        StructType,
        TimestampType,
    )

    return StructType(
        [
            StructField("base_currency", StringType(), False),
            StructField("quote_currency", StringType(), False),
            StructField("rate_date", DateType(), False),
            StructField("rate", DoubleType(), True),
            StructField("ingested_at", TimestampType(), False),
            StructField("run_id", StringType(), False),
            StructField("payload_hash", StringType(), True),
        ]
    )


def run_coinbase_bronze_ingestion(
    spark: Any,
    raw_product_ids: str,
    mode: str,
    start_date_raw: str,
    end_date_raw: str,
    lookback_days_raw: str,
    run_id: str,
    target_table: str | None = None,
    display_fn: Callable[[Any], None] | None = None,
    source: CoinbaseSource | None = None,
) -> BronzeIngestionResult:
    """Run the validated Coinbase Bronze notebook flow from package code."""

    from delta.tables import DeltaTable  # type: ignore import-not-found
    from pyspark.sql import functions as F  # type: ignore import-not-found
    from pyspark.sql.window import Window  # type: ignore import-not-found

    source = source or CoinbaseSource()
    target_table = target_table or source.bronze_table

    if not spark.catalog.tableExists(target_table):
        raise RuntimeError(
            f"Target table {target_table} does not exist. Run 00_platform_setup_catalog_schema.ipynb first."
        )

    product_ids = parse_product_ids(raw_product_ids)
    start_date, end_date = resolve_date_window(mode, start_date_raw, end_date_raw, lookback_days_raw)
    schema = build_coinbase_bronze_schema()

    all_records: list[dict[str, Any]] = []
    per_product_stats = {}
    session = requests.Session()

    try:
        for product_id in product_ids:
            print(
                f"Fetching {product_id} from {start_date.isoformat()} "
                f"to {end_date.isoformat()} in {mode} mode"
            )
            records, stats = source.fetch_daily_candles(
                session=session,
                product_id=product_id,
                start_date=start_date,
                end_date=end_date,
                run_id=run_id,
            )
            all_records.extend(records)
            per_product_stats[product_id] = stats
            print(
                f"{product_id}: fetched={stats.api_rows_fetched} "
                f"in_range={stats.rows_in_requested_range} "
                f"windows={stats.request_windows}"
            )
    finally:
        session.close()

    rows_fetched = sum(stats.api_rows_fetched for stats in per_product_stats.values())
    rows_after_filter = len(all_records)

    if not all_records:
        return BronzeIngestionResult(
            status="success_empty",
            mode=mode,
            product_ids=product_ids,
            start_date=start_date.isoformat(),
            end_date=end_date.isoformat(),
            rows_fetched=rows_fetched,
            rows_after_filter=0,
            rows_after_dedup=0,
            rows_to_update=0,
            rows_to_insert=0,
            rows_merged=0,
            run_id=run_id,
            target_table=target_table,
            per_product_stats=per_product_stats,
        )

    raw_df = spark.createDataFrame(all_records, schema=schema)
    date_filtered_df = raw_df.filter(
        (F.col("bar_date") >= F.lit(start_date)) & (F.col("bar_date") <= F.lit(end_date))
    )

    dedup_window = Window.partitionBy("product_id", "bar_date").orderBy(
        F.col("source_window_end").desc(),
        F.col("ingested_at").desc(),
        F.col("payload_hash").desc(),
    )

    deduped_df = (
        date_filtered_df
        .withColumn("_row_number", F.row_number().over(dedup_window))
        .filter(F.col("_row_number") == 1)
        .drop("_row_number")
    )

    rows_after_dedup = deduped_df.count()
    existing_key_count = (
        deduped_df.select("product_id", "bar_date")
        .join(
            spark.table(target_table).select("product_id", "bar_date"),
            on=["product_id", "bar_date"],
            how="inner",
        )
        .count()
    )

    DeltaTable.forName(spark, target_table).alias("tgt").merge(
        deduped_df.alias("src"),
        "tgt.product_id = src.product_id AND tgt.bar_date = src.bar_date",
    ).whenMatchedUpdate(
        set={
            "open": "src.open",
            "high": "src.high",
            "low": "src.low",
            "close": "src.close",
            "volume": "src.volume",
            "source_window_start": "src.source_window_start",
            "source_window_end": "src.source_window_end",
            "ingested_at": "src.ingested_at",
            "run_id": "src.run_id",
            "payload_hash": "src.payload_hash",
        }
    ).whenNotMatchedInsertAll().execute()

    if display_fn is not None:
        display_fn(deduped_df.orderBy("product_id", "bar_date"))

    return BronzeIngestionResult(
        status="success",
        mode=mode,
        product_ids=product_ids,
        start_date=start_date.isoformat(),
        end_date=end_date.isoformat(),
        rows_fetched=rows_fetched,
        rows_after_filter=rows_after_filter,
        rows_after_dedup=rows_after_dedup,
        rows_to_update=existing_key_count,
        rows_to_insert=rows_after_dedup - existing_key_count,
        rows_merged=rows_after_dedup,
        run_id=run_id,
        target_table=target_table,
        per_product_stats=per_product_stats,
    )


def run_ecb_bronze_ingestion(
    spark: Any,
    raw_quote_currencies: str,
    mode: str,
    start_date_raw: str,
    end_date_raw: str,
    lookback_days_raw: str,
    run_id: str,
    target_table: str | None = None,
    display_fn: Callable[[Any], None] | None = None,
    source: EcbSource | None = None,
) -> EcbBronzeIngestionResult:
    """Run the validated ECB Bronze notebook flow from package code."""

    from delta.tables import DeltaTable  # type: ignore import-not-found
    from pyspark.sql import functions as F  # type: ignore import-not-found
    from pyspark.sql.window import Window  # type: ignore import-not-found

    source = source or EcbSource()
    target_table = target_table or source.bronze_table

    if not spark.catalog.tableExists(target_table):
        raise RuntimeError(
            f"Target table {target_table} does not exist. Run 00_platform_setup_catalog_schema.ipynb first."
        )

    quote_currencies = source.parse_quote_currencies(raw_quote_currencies)
    latest_complete_date = datetime.now(source.local_timezone).date() - timedelta(days=1)
    start_date, end_date = resolve_date_window(
        mode,
        start_date_raw,
        end_date_raw,
        lookback_days_raw,
        latest_complete_date=latest_complete_date,
        latest_complete_timezone_label="Europe/Berlin",
    )
    schema = build_ecb_bronze_schema()

    print(
        f"Fetching ECB FX reference rates for {','.join(quote_currencies)} "
        f"from {start_date.isoformat()} to {end_date.isoformat()} in {mode} mode"
    )

    session = requests.Session()
    try:
        records, per_currency_stats, request_url, series_key = source.fetch_reference_rates(
            session=session,
            quote_currencies=quote_currencies,
            start_date=start_date,
            end_date=end_date,
            run_id=run_id,
        )
    finally:
        session.close()

    rows_fetched = sum(stats.api_rows_fetched for stats in per_currency_stats.values())
    rows_after_filter = len(records)
    print(
        f"ECB: fetched={rows_fetched} "
        f"currencies={len(quote_currencies)} "
        f"series_key={series_key}"
    )

    if not records:
        return EcbBronzeIngestionResult(
            status="success_empty",
            mode=mode,
            quote_currencies=quote_currencies,
            start_date=start_date.isoformat(),
            end_date=end_date.isoformat(),
            request_url=request_url,
            series_key=series_key,
            rows_fetched=0,
            rows_after_filter=0,
            rows_after_dedup=0,
            rows_to_update=0,
            rows_to_insert=0,
            rows_merged=0,
            run_id=run_id,
            target_table=target_table,
            per_currency_stats=per_currency_stats,
        )

    raw_df = spark.createDataFrame(records, schema=schema)
    date_filtered_df = raw_df.filter(
        (F.col("rate_date") >= F.lit(start_date)) & (F.col("rate_date") <= F.lit(end_date))
    )

    dedup_window = Window.partitionBy("base_currency", "quote_currency", "rate_date").orderBy(
        F.col("ingested_at").desc(),
        F.col("payload_hash").desc(),
    )

    deduped_df = (
        date_filtered_df
        .withColumn("_row_number", F.row_number().over(dedup_window))
        .filter(F.col("_row_number") == 1)
        .drop("_row_number")
    )

    rows_after_dedup = deduped_df.count()
    existing_key_count = (
        deduped_df.select("base_currency", "quote_currency", "rate_date")
        .join(
            spark.table(target_table).select("base_currency", "quote_currency", "rate_date"),
            on=["base_currency", "quote_currency", "rate_date"],
            how="inner",
        )
        .count()
    )

    DeltaTable.forName(spark, target_table).alias("tgt").merge(
        deduped_df.alias("src"),
        "tgt.base_currency = src.base_currency "
        "AND tgt.quote_currency = src.quote_currency "
        "AND tgt.rate_date = src.rate_date",
    ).whenMatchedUpdate(
        set={
            "rate": "src.rate",
            "ingested_at": "src.ingested_at",
            "run_id": "src.run_id",
            "payload_hash": "src.payload_hash",
        }
    ).whenNotMatchedInsertAll().execute()

    if display_fn is not None:
        display_fn(deduped_df.orderBy("quote_currency", "rate_date"))

    return EcbBronzeIngestionResult(
        status="success",
        mode=mode,
        quote_currencies=quote_currencies,
        start_date=start_date.isoformat(),
        end_date=end_date.isoformat(),
        request_url=request_url,
        series_key=series_key,
        rows_fetched=rows_fetched,
        rows_after_filter=rows_after_filter,
        rows_after_dedup=rows_after_dedup,
        rows_to_update=existing_key_count,
        rows_to_insert=rows_after_dedup - existing_key_count,
        rows_merged=rows_after_dedup,
        run_id=run_id,
        target_table=target_table,
        per_currency_stats=per_currency_stats,
    )
