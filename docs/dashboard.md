# Dashboard Guide

This document describes the current dashboard assets, how they are built in the Databricks UI, and how the exported dashboard is promoted through the repository and bundle.

## Current Assets

Repository-managed dashboard files:

- `dashboards/market_macro_overview.lvdash.json`
- `dashboards/queries/01_pipeline_freshness.sql`
- `dashboards/queries/02_pipeline_recent_runs.sql`
- `dashboards/queries/03_crypto_market_snapshot.sql`
- `dashboards/queries/04_macro_indicator_latest.sql`
- `dashboards/queries/05_cross_feature_snapshot.sql`

Bundle resources:

- `resources/dashboards.yml`
- `resources/jobs.yml`

The bundle-managed dashboard resource name is `market_macro_overview`.

## Current Dashboard Layout

The current AI/BI dashboard is `Market Macro Overview`.

Primary widgets:

- `Pipeline Freshness`
- `Recent Pipeline Runs`
- `Crypto Market Snapshot`
- `Cross Feature Snapshot`
- `Macro Indicator Latest`

Data sources:

- `market_macro.obs.obs_ingestion_state`
- `market_macro.obs.obs_pipeline_run_log`
- `market_macro.gld_market.dp_crypto_returns_1d`
- `market_macro.gld_market.dp_crypto_volatility_1d`
- `market_macro.gld_macro.dp_macro_indicators`
- `market_macro.gld_cross.dp_crypto_macro_features_1d`

## SQL Dataset Mapping

Recommended query-to-widget mapping:

- `01_pipeline_freshness.sql`
  - widget: `Pipeline Freshness`
- `02_pipeline_recent_runs.sql`
  - widget: `Recent Pipeline Runs`
- `03_crypto_market_snapshot.sql`
  - widget: `Crypto Market Snapshot`
- `04_macro_indicator_latest.sql`
  - widget: `Macro Indicator Latest`
- `05_cross_feature_snapshot.sql`
  - widget: `Cross Feature Snapshot`

## UI Editing Workflow

Use this when you want to change the dashboard layout or widget configuration.

1. Open the AI/BI dashboard in Databricks UI.
2. Keep the warehouse set to the existing serverless SQL warehouse.
3. Edit datasets in the `Data` tab only if the SQL itself must change.
4. Edit tables and charts in the page canvas.
5. Prefer getting one widget correct before adjusting layout.
6. Publish the dashboard after visual checks pass.
7. Export the dashboard through `... -> File actions -> Export`.

Export format:

- Databricks exports AI/BI dashboards as `.lvdash.json`

## Promotion Workflow

Use this when a UI-edited dashboard should become the repository version.

1. Export the latest `.lvdash.json` from Databricks UI.
2. Replace `dashboards/market_macro_overview.lvdash.json` with the export.
3. Commit and push the new exported file.
4. Run:

```bash
databricks bundle deploy --target dev
```

5. Optionally run the daily job or allow `t80_refresh_dashboard` to refresh the dashboard on the next scheduled run.

## Bundle and Job Integration

The dashboard is managed through:

- `resources/dashboards.yml`
- `resources/jobs.yml`

The refresh task is:

- `t80_refresh_dashboard`

It depends on:

- `t70_observability`

Current refresh contract:

- warehouse id comes from `${var.sql_warehouse_id}`
- refresh runs after pipeline completion
- `run_if: ALL_DONE`

## Important Operational Notes

- The dashboard object you edit manually in the UI is not guaranteed to be the same object as the bundle-managed dashboard resource.
- The repository export is the source of truth for bundle deployment.
- If the bundle-managed dashboard looks stale, re-export from UI, replace the repository file, and redeploy.
- If the deployed dashboard loads blank widgets, the exported `.lvdash.json` likely contains placeholder widgets without dataset field bindings.

## Validation Checklist

Before exporting a new dashboard version, confirm:

- all 5 datasets run successfully
- all 5 widgets render correctly
- `Pipeline Freshness` shows 11 monitored pipelines
- `Recent Pipeline Runs` includes Bronze, Silver, Gold, Cross, and Obs runs
- `Cross Feature Snapshot` shows non-empty `macro_features`
- widget titles are enabled
- layout is finalized

## Free Edition Notes

This workspace currently uses:

- `Serverless Starter Warehouse`

Do not assume a new warehouse can be created. Reuse the existing serverless warehouse unless the workspace plan changes.
