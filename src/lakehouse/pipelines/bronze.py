"""Bronze ingestion orchestration."""

from lakehouse.common.models import LoadResult
from lakehouse.sources.base import SourceAdapter


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
