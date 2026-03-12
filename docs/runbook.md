# Runbook

This document is the end-to-end operating guide for the current phase-1 lakehouse implementation.

Use it as the main entrypoint. Detailed operational sections live in:

- `docs/dashboard.md`
- `docs/job-operations.md`

## Scope

The current system includes:

- Bronze ingestion for Coinbase, ECB, and FRED
- Silver normalization for market and macro
- Gold market and macro products
- cross-domain Gold feature assembly
- observability aggregation
- bundle-managed job orchestration
- bundle-managed dashboard refresh

## Main Assets

Notebooks:

- `notebooks/00_platform_setup_catalog_schema.ipynb`
- `notebooks/10_direct_bronze_market_crypto_ingest.ipynb`
- `notebooks/11_bronze_ecb_ingest.ipynb`
- `notebooks/12_bronze_fred_ingest.ipynb`
- `notebooks/20_direct_silver_market_crypto_ohlc_1d.ipynb`
- `notebooks/22_silver_macro_transform.ipynb`
- `notebooks/30_direct_gold_market_crypto_returns_volatility_1d.ipynb`
- `notebooks/32_gold_macro_indicators.ipynb`
- `notebooks/40_direct_gold_cross_crypto_macro_features_1d.ipynb`
- `notebooks/70_pipeline_observability_metrics.ipynb`

Bundle resources:

- `databricks.yml`
- `resources/jobs.yml`
- `resources/dashboards.yml`

## One-Time Environment Setup

1. Make sure the Databricks catalog already exists:
   - `market_macro`
2. Make sure the FRED API key is stored in Databricks secrets:
   - scope: `market_macro`
   - key: `fred_api_key`
3. Run:
   - `notebooks/00_platform_setup_catalog_schema.ipynb`

This creates the schemas, Delta tables, quarantine tables, and observability tables required by the pipelines.

## Local Validation Before Deploy

Run:

```bash
python3 -m pytest tests/unit -q
python3 -m ruff check src tests
databricks bundle validate --target dev
```

Expected current baseline:

- unit tests pass
- ruff check passes
- bundle validate succeeds

## Deploy and Run

Deploy:

```bash
databricks bundle deploy --target dev
```

Run the full DAG:

```bash
databricks bundle run market_macro_daily --target dev
```

## Historical Backfill Checklist

Use this once before enabling the daily schedule. The goal is to load a stable baseline history without clearing existing tables.

Pre-checks:

- keep the daily job paused
- do not truncate tables before the first controlled backfill
- use `2026-03-12` as the current backfill end date, not the still-open `2026-03-13`
- do not rerun `00_platform_setup_catalog_schema.ipynb` unless the schema changed

Recommended backfill windows:

- market: `2023-10-03` to `2026-03-12`
- ECB: `2024-01-01` to `2026-03-12`
- FRED: `2020-01-01` to `2026-03-12`
- cross-domain product: `2024-01-01` to `2026-03-12`

Execution order:

1. `notebooks/10_direct_bronze_market_crypto_ingest.ipynb`
   - `product_ids=BTC-USD,ETH-USD,SOL-USD,LINK-USD,DOGE-USD`
   - `mode=backfill`
   - `start_date=2023-10-03`
   - `end_date=2026-03-12`
2. `notebooks/11_bronze_ecb_ingest.ipynb`
   - `quote_currencies=USD`
   - `mode=backfill`
   - `start_date=2024-01-01`
   - `end_date=2026-03-12`
3. `notebooks/12_bronze_fred_ingest.ipynb`
   - `series_ids=CPIAUCSL,FEDFUNDS,GDP`
   - `mode=backfill`
   - `start_date=2020-01-01`
   - `end_date=2026-03-12`
   - `secret_scope=market_macro`
   - `secret_key=fred_api_key`
4. `notebooks/20_direct_silver_market_crypto_ohlc_1d.ipynb`
   - `product_ids=BTC-USD,ETH-USD,SOL-USD,LINK-USD,DOGE-USD`
   - `mode=backfill`
   - `start_date=2023-10-03`
   - `end_date=2026-03-12`
5. `notebooks/22_silver_macro_transform.ipynb`
   - `source_system=ecb`
   - `quote_currencies=USD`
   - `mode=backfill`
   - `start_date=2024-01-01`
   - `end_date=2026-03-12`
6. `notebooks/22_silver_macro_transform.ipynb`
   - `source_system=fred`
   - `series_ids=CPIAUCSL,FEDFUNDS,GDP`
   - `mode=backfill`
   - `start_date=2020-01-01`
   - `end_date=2026-03-12`
7. `notebooks/30_direct_gold_market_crypto_returns_volatility_1d.ipynb`
   - `product_ids=BTC-USD,ETH-USD,SOL-USD,LINK-USD,DOGE-USD`
   - `mode=backfill`
   - `start_date=2023-10-03`
   - `end_date=2026-03-12`
8. `notebooks/32_gold_macro_indicators.ipynb`
   - `source_system=ecb`
   - `quote_currencies=USD`
   - `mode=backfill`
   - `start_date=2024-01-01`
   - `end_date=2026-03-12`
9. `notebooks/32_gold_macro_indicators.ipynb`
   - `source_system=fred`
   - `series_ids=CPIAUCSL,FEDFUNDS,GDP`
   - `mode=backfill`
   - `start_date=2020-01-01`
   - `end_date=2026-03-12`
10. `notebooks/40_direct_gold_cross_crypto_macro_features_1d.ipynb`
   - `product_ids=BTC-USD,ETH-USD,SOL-USD,LINK-USD,DOGE-USD`
   - `macro_indicator_ids=ECB_FX_REF_EUR_USD,FRED_CPIAUCSL,FRED_FEDFUNDS,FRED_GDP`
   - `mode=backfill`
   - `start_date=2024-01-01`
   - `end_date=2026-03-12`
11. `notebooks/70_pipeline_observability_metrics.ipynb`
   - default parameters

Before enabling the schedule, run one manual incremental DAG:

```bash
databricks bundle run market_macro_daily --target dev
```

Enable the schedule only after the manual incremental run is clean. This is controlled by `daily_job_pause_status` in `databricks.yml`.

## End-to-End Data Flow

1. Bronze
   - Coinbase OHLC
   - ECB FX reference rates
   - FRED observations and metadata
2. Silver
   - market daily OHLC clean table
   - ECB FX clean table
   - FRED clean observation table
3. Gold
   - crypto returns
   - crypto volatility
   - unified macro indicators
4. Cross
   - crypto + macro feature table
5. Observability
   - run log
   - ingestion state
   - DQ metrics
6. Dashboard refresh

## Verification Checklist

After a successful run, verify:

- `obs.obs_pipeline_run_log` contains current run records
- `obs.obs_ingestion_state` contains all monitored pipelines
- `gld_cross.dp_crypto_macro_features_1d` has unique `(feature_date, product_id)`
- dashboard refresh task completed

After the historical backfill specifically, also verify:

- market, macro, and cross tables contain the expected historical range
- all 11 monitored pipelines appear in `obs.obs_ingestion_state`
- no duplicate keys exist in market Gold, macro Gold, or cross Gold
- dashboard widgets render full history instead of only recent data

## Recommended Verification SQL

Latest ingestion state:

```sql
SELECT pipeline_name, source_name, target_table, watermark_value, status, last_success_at
FROM market_macro.obs.obs_ingestion_state
ORDER BY source_name, pipeline_name;
```

Recent run log:

```sql
SELECT run_id, pipeline_name, layer, source_name, status, rows_read, rows_written, started_at, completed_at
FROM market_macro.obs.obs_pipeline_run_log
ORDER BY started_at DESC
LIMIT 50;
```

Duplicate cross keys:

```sql
SELECT feature_date, product_id, COUNT(*) AS row_count
FROM market_macro.gld_cross.dp_crypto_macro_features_1d
GROUP BY feature_date, product_id
HAVING COUNT(*) > 1;
```

Duplicate market Gold keys:

```sql
SELECT product_id, bar_date, COUNT(*) AS row_count
FROM market_macro.gld_market.dp_crypto_returns_1d
GROUP BY product_id, bar_date
HAVING COUNT(*) > 1;
```

Duplicate macro Gold keys:

```sql
SELECT indicator_id, observation_date, COUNT(*) AS row_count
FROM market_macro.gld_macro.dp_macro_indicators
GROUP BY indicator_id, observation_date
HAVING COUNT(*) > 1;
```

Cross historical range by product:

```sql
SELECT product_id, MIN(feature_date) AS min_date, MAX(feature_date) AS max_date, COUNT(*) AS row_count
FROM market_macro.gld_cross.dp_crypto_macro_features_1d
GROUP BY product_id
ORDER BY product_id;
```

Current DQ metrics:

```sql
SELECT *
FROM market_macro.obs.obs_dq_metrics
ORDER BY measured_at DESC, pipeline_name, dq_reason;
```

## Dashboard Workflow

For dashboard-specific operations, use `docs/dashboard.md`.

Short version:

1. edit in Databricks UI
2. export `.lvdash.json`
3. replace repository export
4. deploy bundle
5. rerun dashboard refresh

## Job Workflow

For DAG repair and failure handling, use `docs/job-operations.md`.

Short version:

1. inspect failed task
2. fix code or configuration
3. deploy bundle again if code changed
4. rerun the DAG

## Known Constraints

- serverless/Spark Connect compatibility matters for notebook jobs
- FRED incremental mode is still observation-window re-pull for phase-1 correctness
- cross-domain macro alignment uses `observation_date` as-of fill
- current cross-domain product is suitable for analytics and dashboarding, not declared backtest-safe
- free edition SQL warehouse availability is limited to the existing serverless starter warehouse

## Change Management

When changing pipeline logic:

1. update `src`
2. run local tests
3. deploy bundle
4. run the DAG manually
5. verify observability tables
6. refresh dashboard if output contracts changed

When changing only dashboard layout:

1. edit in UI
2. export `.lvdash.json`
3. replace repository file
4. deploy bundle

## Handoff Note

The repository source of truth is:

- pipeline code in `src/`
- orchestration in `resources/`
- dashboard export in `dashboards/market_macro_overview.lvdash.json`

Databricks UI edits are not final until the exported dashboard file is committed back into the repository.
