# Architecture Overview

This repository follows a Medallion architecture:

- Bronze ingests raw source payloads.
- Silver normalizes and deduplicates business entities.
- Gold publishes analytics-ready products.
- Observability records quality and runtime metadata.

## Code Layout

- `src/lakehouse/sources`: source-specific adapters and request metadata.
- `src/lakehouse/common`: shared models and utility functions.
- `src/lakehouse/transforms`: reusable row-level transformation logic.
- `src/lakehouse/pipelines`: orchestration modules used by thin notebooks.
- `src/lakehouse/*_rules.py`: data quality and observability rules.

## Execution Model

Notebooks should stay thin:

1. Read Databricks widgets or job parameters.
2. Create the relevant source adapter.
3. Call Python pipeline functions.
4. Persist output and return lightweight run metadata.
