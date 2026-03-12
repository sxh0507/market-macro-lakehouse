from __future__ import annotations

import re
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[2]


def test_databricks_bundle_includes_resource_files_and_required_variables() -> None:
    bundle_text = (REPO_ROOT / "databricks.yml").read_text()

    assert "include:" in bundle_text
    assert "- resources/*.yml" in bundle_text
    for variable_name in [
        "catalog:",
        "fred_secret_scope:",
        "fred_secret_key:",
        "sql_warehouse_id:",
        "daily_job_cron:",
    ]:
        assert variable_name in bundle_text


def test_jobs_yaml_contains_expected_task_keys() -> None:
    jobs_text = (REPO_ROOT / "resources" / "jobs.yml").read_text()

    task_keys = set(re.findall(r"task_key:\s*([A-Za-z0-9_]+)", jobs_text))
    assert task_keys == {
        "t10_bronze_market_coinbase",
        "t11_bronze_ecb",
        "t12_bronze_fred",
        "t20_silver_market",
        "t22_silver_ecb",
        "t22_silver_fred",
        "t30_gold_market",
        "t32_gold_ecb",
        "t32_gold_fred",
        "t40_gold_cross",
        "t70_observability",
        "t80_refresh_dashboard",
    }


def test_jobs_yaml_keeps_expected_dag_and_dashboard_refresh_contract() -> None:
    jobs_text = (REPO_ROOT / "resources" / "jobs.yml").read_text()

    assert "run_if: ALL_DONE" in jobs_text
    assert "dashboard_id: ${resources.dashboards.market_macro_overview.id}" in jobs_text
    assert "warehouse_id: ${var.sql_warehouse_id}" in jobs_text
    assert "task_key: t70_observability" in jobs_text
    assert "task_key: t80_refresh_dashboard" in jobs_text
    assert "task_key: t40_gold_cross" in jobs_text


def test_dashboard_resource_yaml_points_to_exported_lvdash_file() -> None:
    dashboard_text = (REPO_ROOT / "resources" / "dashboards.yml").read_text()

    assert "market_macro_overview:" in dashboard_text
    assert "display_name: Market Macro Overview" in dashboard_text
    assert "warehouse_id: ${var.sql_warehouse_id}" in dashboard_text
    assert "file_path: ../dashboards/market_macro_overview.lvdash.json" in dashboard_text
