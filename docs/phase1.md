# Phase 1 Scope

This document describes the current implementation status of the repository.

The project goal remains the full Medallion structure described in the README. The codebase is temporarily narrowed to one Bronze path so the first Databricks flow can be proven end-to-end before more layers are added.

## Current Implementation

The repository currently keeps a single path:

```text
Coinbase -> Bronze notebook -> Python package orchestration
```

Files kept for this stage:

- `notebooks/00_platform_setup_catalog_schema.ipynb`
- `notebooks/10_direct_bronze_market_crypto_ingest.ipynb`
- `src/lakehouse/sources/coinbase.py`
- `src/lakehouse/pipelines/bronze.py`
- `src/lakehouse/common/models.py`

## Purpose

This phase is only meant to verify:

- the setup notebook can bootstrap the initial catalog schemas and Bronze tables
- the repo can be imported into Databricks Repos
- the notebook can import code from `src/lakehouse`
- the Bronze entrypoint returns a stable payload shape

## Deferred Until Later Phases

These parts are intentionally postponed:

- real Coinbase API ingestion and Delta write logic
- Silver and Gold layers
- observability rules
- unit tests and CI
- Databricks job resources
- dashboard queries and dashboard assets

## Next Step

The next implementation target should be making the Bronze notebook perform a real Bronze write while keeping the notebook thin and pushing reusable logic into `src/lakehouse`.
