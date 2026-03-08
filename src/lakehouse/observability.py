"""Shared observability helpers for pipeline runtime logging and ingestion state."""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any

from lakehouse.common.runtime import UTC


def _build_metadata_json(payload: dict[str, Any] | None) -> str | None:
    if payload is None:
        return None
    return json.dumps(payload, sort_keys=True, default=str)


def _ensure_obs_tables(spark: Any, state_table: str, run_log_table: str):
    for table_name in [state_table, run_log_table]:
        if not spark.catalog.tableExists(table_name):
            raise RuntimeError(
                f"Required observability table {table_name} does not exist. "
                "Run 00_platform_setup_catalog_schema.ipynb first."
            )


def _serialize_watermark_value(value: Any) -> str | None:
    if value is None:
        return None
    if hasattr(value, "isoformat"):
        return value.isoformat()
    return str(value)


def read_table_watermark(
    spark: Any,
    table_name: str,
    watermark_column: str,
    filter_sql: str | None = None,
) -> str | None:
    """Read the current max watermark from a target table."""

    from pyspark.sql import functions as F  # type: ignore import-not-found

    observed_df = spark.table(table_name)
    if filter_sql:
        observed_df = observed_df.filter(F.expr(filter_sql))

    row = observed_df.agg(F.max(F.col(watermark_column)).alias("max_watermark")).collect()[0]
    return _serialize_watermark_value(row["max_watermark"])


@dataclass(slots=True)
class PipelineObserver:
    """Track pipeline execution into obs tables with started/success/failed semantics."""

    spark: Any
    catalog: str
    pipeline_name: str
    layer: str
    source_name: str
    target_table: str
    run_id: str
    start_metadata: dict[str, Any] = field(default_factory=dict)
    state_table: str | None = None
    run_log_table: str | None = None
    rows_read: int | None = None
    rows_written: int | None = None
    started_at: datetime = field(default_factory=lambda: datetime.now(UTC))
    _finalized: bool = field(default=False, init=False)

    def __enter__(self) -> "PipelineObserver":
        self.state_table = self.state_table or f"{self.catalog}.obs.obs_ingestion_state"
        self.run_log_table = self.run_log_table or f"{self.catalog}.obs.obs_pipeline_run_log"
        _ensure_obs_tables(self.spark, self.state_table, self.run_log_table)
        self._upsert_run_log(
            status="started",
            completed_at=None,
            error_message=None,
            metadata=self.start_metadata,
        )
        return self

    def __exit__(self, exc_type, exc, tb) -> bool:
        if exc is not None and not self._finalized:
            self.fail(exc)
        return False

    def update_progress(
        self,
        *,
        rows_read: int | None = None,
        rows_written: int | None = None,
    ):
        if rows_read is not None:
            self.rows_read = rows_read
        if rows_written is not None:
            self.rows_written = rows_written

    def succeed(
        self,
        *,
        status: str,
        watermark_type: str,
        metadata: dict[str, Any] | None = None,
        rows_read: int | None = None,
        rows_written: int | None = None,
        watermark_column: str | None = None,
        watermark_value: str | None = None,
        state_filter_sql: str | None = None,
    ):
        if rows_read is not None:
            self.rows_read = rows_read
        if rows_written is not None:
            self.rows_written = rows_written

        if watermark_value is None and watermark_column is not None:
            watermark_value = read_table_watermark(
                self.spark,
                self.target_table,
                watermark_column,
                filter_sql=state_filter_sql,
            )

        completed_at = datetime.now(UTC)
        self._upsert_run_log(
            status=status,
            completed_at=completed_at,
            error_message=None,
            metadata=metadata,
        )
        self._upsert_ingestion_state(
            watermark_value=watermark_value,
            watermark_type=watermark_type,
            status=status,
            completed_at=completed_at,
        )
        self._finalized = True

    def fail(
        self,
        exc: BaseException,
        *,
        metadata: dict[str, Any] | None = None,
        rows_read: int | None = None,
        rows_written: int | None = None,
    ):
        if rows_read is not None:
            self.rows_read = rows_read
        if rows_written is not None:
            self.rows_written = rows_written

        completed_at = datetime.now(UTC)
        self._upsert_run_log(
            status="failed",
            completed_at=completed_at,
            error_message=str(exc),
            metadata=metadata,
        )
        self._finalized = True

    def _upsert_run_log(
        self,
        *,
        status: str,
        completed_at: datetime | None,
        error_message: str | None,
        metadata: dict[str, Any] | None,
    ):
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

        log_df = self.spark.createDataFrame(
            [
                (
                    self.run_id,
                    self.pipeline_name,
                    self.layer,
                    self.source_name,
                    self.target_table,
                    status,
                    self.rows_read,
                    self.rows_written,
                    self.started_at,
                    completed_at,
                    error_message,
                    _build_metadata_json(metadata),
                )
            ],
            schema=schema,
        )

        DeltaTable.forName(self.spark, self.run_log_table).alias("tgt").merge(
            log_df.alias("src"),
            "tgt.run_id = src.run_id AND tgt.pipeline_name = src.pipeline_name AND tgt.target_table = src.target_table",
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

    def _upsert_ingestion_state(
        self,
        *,
        watermark_value: str | None,
        watermark_type: str,
        status: str,
        completed_at: datetime,
    ):
        from delta.tables import DeltaTable  # type: ignore import-not-found
        from pyspark.sql.types import (  # type: ignore import-not-found
            StringType,
            StructField,
            StructType,
            TimestampType,
        )

        schema = StructType(
            [
                StructField("pipeline_name", StringType(), False),
                StructField("source_name", StringType(), True),
                StructField("target_table", StringType(), False),
                StructField("watermark_value", StringType(), True),
                StructField("watermark_type", StringType(), True),
                StructField("last_success_at", TimestampType(), True),
                StructField("last_run_id", StringType(), True),
                StructField("status", StringType(), True),
                StructField("updated_at", TimestampType(), False),
            ]
        )

        state_df = self.spark.createDataFrame(
            [
                (
                    self.pipeline_name,
                    self.source_name,
                    self.target_table,
                    watermark_value,
                    watermark_type,
                    completed_at,
                    self.run_id,
                    status,
                    completed_at,
                )
            ],
            schema=schema,
        )

        DeltaTable.forName(self.spark, self.state_table).alias("tgt").merge(
            state_df.alias("src"),
            "tgt.pipeline_name = src.pipeline_name AND tgt.target_table = src.target_table",
        ).whenMatchedUpdate(
            set={
                "source_name": "src.source_name",
                "watermark_value": "src.watermark_value",
                "watermark_type": "src.watermark_type",
                "last_success_at": "src.last_success_at",
                "last_run_id": "src.last_run_id",
                "status": "src.status",
                "updated_at": "src.updated_at",
            }
        ).whenNotMatchedInsertAll().execute()
