# Job Operations

This document is the operational reference for the Databricks Asset Bundle job that runs the phase-1 lakehouse DAG.

## Primary Files

- `databricks.yml`
- `resources/jobs.yml`
- `resources/dashboards.yml`

## Job Resource

Bundle job resource name:

- `market_macro_daily`

Rendered Databricks job name pattern:

- `${bundle.name}-${bundle.target}-daily`

For the current repository, this becomes a `market-macro-lakehouse-dev-daily` style job in the `dev` target.

## DAG Overview

Task order:

- `t10_bronze_market_coinbase`
- `t11_bronze_ecb`
- `t12_bronze_fred`
- `t20_silver_market`
- `t22_silver_ecb`
- `t22_silver_fred`
- `t30_gold_market`
- `t32_gold_ecb`
- `t32_gold_fred`
- `t40_gold_cross`
- `t70_observability`
- `t80_refresh_dashboard`

Dependency model:

- Bronze tasks start first
- Silver depends on Bronze
- Gold depends on Silver
- Cross depends on market Gold plus macro Gold
- Observability runs after the business DAG with `ALL_DONE`
- Dashboard refresh runs after observability with `ALL_DONE`

## Key Runtime Parameters

Configured in `databricks.yml`:

- `catalog`
- `market_product_ids`
- `ecb_quote_currencies`
- `fred_series_ids`
- `cross_macro_indicator_ids`
- `fred_secret_scope`
- `fred_secret_key`
- `daily_job_cron`
- `daily_job_timezone`
- `daily_job_pause_status`
- `sql_warehouse_id`

Current defaults assume:

- catalog: `market_macro`
- products: `BTC-USD,ETH-USD,SOL-USD,LINK-USD,DOGE-USD`
- ECB currencies: `USD`
- FRED series: `CPIAUCSL,FEDFUNDS,GDP`

## Core Commands

Validate bundle:

```bash
databricks bundle validate --target dev
```

Deploy bundle:

```bash
databricks bundle deploy --target dev
```

Run the full DAG manually:

```bash
databricks bundle run market_macro_daily --target dev
```

## Standard Operating Workflow

For a normal release:

1. Pull the latest repository code.
2. Run local tests.
3. Deploy the bundle.
4. Trigger a manual run.
5. Check Job timeline.
6. Check `obs.obs_pipeline_run_log`.
7. Check `obs.obs_ingestion_state`.
8. Confirm dashboard refresh completed.

## What Success Looks Like

Expected success signals:

- all Bronze, Silver, Gold, Cross tasks complete
- `t70_observability` completes
- `t80_refresh_dashboard` completes
- `obs.obs_ingestion_state` contains 11 monitored pipelines
- `obs.obs_pipeline_run_log` contains current run records across layers

## Repair Strategy

If a run fails:

1. Identify the first failed task in the timeline.
2. Open the failed task output.
3. Determine whether it is:
   - source/API issue
   - serverless/runtime compatibility issue
   - secret/configuration issue
   - schema/data issue
4. Fix the cause in code or config.
5. Redeploy the bundle if code changed.
6. Prefer a fresh `bundle run` after a code fix.
7. Use `Repair run` only when the code version and task parameters are already correct.

## Common Failure Modes

### FRED secret lookup failure

Symptom:

- `Secret does not exist with scope ...`

Fix:

- confirm Databricks secret scope exists
- confirm `fred_secret_scope` matches the scope name
- confirm `fred_secret_key` matches the stored secret key name

### Serverless tuple compatibility errors

Symptom:

- `UNSUPPORTED_DATA_TYPE Unsupported DataType 'tuple'`

Typical cause:

- local Python tuple objects passed into Spark Connect / serverless expressions

Fix:

- patch `src` code
- deploy bundle
- rerun the DAG

### Stale Repos or import cache

Symptom:

- notebook import error after recent refactor

Fix:

- pull latest code in Databricks Repos
- restart Python session
- rerun the notebook or job

### Dashboard looks stale after deploy

Symptom:

- bundle-managed dashboard does not match the latest UI edits

Fix:

- export latest `.lvdash.json`
- replace `dashboards/market_macro_overview.lvdash.json`
- commit and deploy again

## Operational Tables

Use these for diagnosis:

- `market_macro.obs.obs_pipeline_run_log`
- `market_macro.obs.obs_ingestion_state`
- `market_macro.obs.obs_dq_metrics`

Useful questions:

- Which task failed first?
- Which pipelines have stale watermarks?
- Are any quarantine tables growing unexpectedly?

## Scheduling Notes

Current schedule variables:

- cron: `0 30 18 * * ?`
- timezone: `Europe/Berlin`
- initial pause status: `PAUSED`

Recommended rollout:

1. Deploy with schedule paused.
2. Run manually.
3. Validate output tables and observability.
4. Enable schedule only after a clean manual run.
