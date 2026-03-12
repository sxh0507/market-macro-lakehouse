from __future__ import annotations

import re
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[2]
PIPELINES_DIR = REPO_ROOT / "src" / "lakehouse" / "pipelines"


def read_pipeline_source(name: str) -> str:
    return (PIPELINES_DIR / name).read_text()


def test_serverless_pipeline_modules_do_not_construct_dataframe_from_tuple_literals() -> None:
    forbidden_pattern = re.compile(r"createDataFrame\s*\(\s*\[\s*\(", re.MULTILINE)

    for filename in ["silver.py", "gold.py", "obs.py"]:
        source = read_pipeline_source(filename)
        assert forbidden_pattern.search(source) is None, filename


def test_fred_silver_expands_structural_dq_tuple_for_isin() -> None:
    source = read_pipeline_source("silver.py")

    assert "isin(*FRED_STRUCTURAL_DQ_REASONS)" in source
    assert "isin(FRED_STRUCTURAL_DQ_REASONS)" not in source


def test_cross_gold_uses_pure_spark_indicator_id_construction() -> None:
    source = read_pipeline_source("gold.py")

    assert "spark.range(1).select(" in source
    assert "[(indicator_id,) for indicator_id in macro_indicator_ids]" not in source


def test_fred_silver_uses_metadata_table_join_instead_of_local_tuple_dataframe() -> None:
    source = read_pipeline_source("silver.py")

    assert '.withColumn("metadata_present", F.lit(True))' in source
    assert "[(series_id,) for series_id in sorted(available_metadata_ids)]" not in source
