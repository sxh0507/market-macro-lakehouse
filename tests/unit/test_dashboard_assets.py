from __future__ import annotations

import json
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[2]
DASHBOARD_PATH = REPO_ROOT / "dashboards" / "market_macro_overview.lvdash.json"


def test_dashboard_export_is_valid_json() -> None:
    parsed = json.loads(DASHBOARD_PATH.read_text())

    assert set(parsed) >= {"datasets", "pages", "uiSettings"}


def test_dashboard_export_contains_expected_datasets() -> None:
    parsed = json.loads(DASHBOARD_PATH.read_text())
    dataset_names = {dataset["displayName"] for dataset in parsed["datasets"]}

    assert dataset_names == {
        "Pipeline Freshness",
        "Recent Pipeline Runs",
        "Crypto Market Snapshot",
        "Macro Indicator Latest",
        "Cross Feature Snapshot",
    }


def test_dashboard_export_contains_expected_titled_widgets() -> None:
    parsed = json.loads(DASHBOARD_PATH.read_text())
    titled_widgets = {
        widget["spec"]["frame"]["title"]
        for page in parsed["pages"]
        for item in page.get("layout", [])
        for widget in [item.get("widget", {})]
        if widget.get("spec", {}).get("frame", {}).get("title")
    }

    assert titled_widgets >= {
        "Pipeline Freshness",
        "Recent Pipeline Runs",
        "Crypto Market Snapshot",
        "Cross Feature Snapshot",
        "Macro Indicator Latest",
    }


def test_dashboard_query_sql_assets_exist_for_all_ui_datasets() -> None:
    query_dir = REPO_ROOT / "dashboards" / "queries"

    assert {path.name for path in query_dir.glob("*.sql")} == {
        "01_pipeline_freshness.sql",
        "02_pipeline_recent_runs.sql",
        "03_crypto_market_snapshot.sql",
        "04_macro_indicator_latest.sql",
        "05_cross_feature_snapshot.sql",
    }
