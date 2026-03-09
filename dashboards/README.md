# Dashboard Assets

This folder keeps repository-managed SQL assets for a first Databricks SQL dashboard.

Recommended dashboard sections:

- Pipeline health
- Crypto market performance
- Macro indicator monitoring
- Cross-domain feature inspection

Suggested query-to-tile mapping:

- `queries/01_pipeline_freshness.sql`
  - table tile for latest pipeline watermark and freshness state
- `queries/02_pipeline_recent_runs.sql`
  - table tile for recent run status across Bronze / Silver / Gold / Obs
- `queries/03_crypto_market_snapshot.sql`
  - table tile for latest return / volatility snapshot by product
- `queries/04_macro_indicator_latest.sql`
  - table tile for latest macro indicator values by source
- `queries/05_cross_feature_snapshot.sql`
  - table tile for the latest cross-domain feature payload by product

These queries intentionally avoid Databricks-SQL-specific parameter syntax so they can be pasted directly into SQL editor queries or embedded into dashboards with minor adaptation.
