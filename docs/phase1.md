# Phase 1 Scope

This document describes the current implementation status of the repository.

The project goal remains the full Medallion structure described in the README. The codebase is temporarily narrowed to one Bronze path so the first Databricks flow can be proven end-to-end before more layers are added.

## Current Implementation

The repository currently keeps a single path:

```text
Coinbase API -> Thin Bronze notebook -> src/lakehouse package -> Bronze Delta table
```

Files kept for this stage:

- `notebooks/00_platform_setup_catalog_schema.ipynb`
- `notebooks/10_direct_bronze_market_crypto_ingest.ipynb`
- `src/lakehouse/sources/coinbase.py`
- `src/lakehouse/pipelines/bronze.py`
- `src/lakehouse/common/models.py`

## Purpose

This phase is currently meant to verify:

- the setup notebook can bootstrap the initial catalog schemas and Bronze tables
- the repo can be imported into Databricks Repos
- the notebook can import code from `src/lakehouse`
- the Coinbase Bronze flow can fetch real API data and merge it into Delta
- the Bronze entrypoint returns a stable payload shape

## Deferred Until Later Phases

These parts are intentionally postponed:

- Silver and Gold layers
- observability rules
- unit tests and CI
- Databricks job resources
- dashboard queries and dashboard assets

## Next Step

The next implementation target should be building the Silver and Gold layers on top of the validated Coinbase Bronze path and then expanding macro sources and observability.
