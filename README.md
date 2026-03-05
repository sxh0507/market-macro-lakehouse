# Market Macro Lakehouse

Databricks Lakehouse project for building scalable market and macroeconomic data pipelines.

This repository demonstrates production-style data engineering practices, including modular pipeline design, CI testing, and Databricks job orchestration.

## Architecture

The platform implements a Medallion Lakehouse Architecture on Databricks.

Data flows through Bronze → Silver → Gold → Observability layers.

                External Data Sources
          ┌───────────────────────────────┐
          │ Coinbase │ ECB │ FRED APIs    │
          └───────────────┬───────────────┘
                          │
                          ▼
                    Bronze Layer
             Raw ingestion (API → Delta)

                          │
                          ▼
                    Silver Layer
          Cleaning, normalization, deduplication

                          │
                          ▼
                     Gold Layer
           Analytics-ready data products

                          │
                          ▼
                  Observability Layer
              Data quality & pipeline metrics

## Project Overview

This project separates pipeline orchestration from business logic.

### Notebook Layer

Responsible for:

- runtime parameters
- 
- pipeline orchestration
- 
- Databricks job entrypoints

### Python Package (src/lakehouse)

Responsible for:

- API clients
- parsing logic
- transformations
- reusable pipeline utilities
- data quality rules

This design follows the **Thin Notebook + Heavy Python Module** pattern commonly used in production Databricks projects.

## Data Sources

The project currently integrates multiple data domains.

### Crypto Market Data

Source: Coinbase API

Example data:

- BTC / ETH OHLC
- trade volumes
- market activity signals

### ECB Exchange Rates

Source: European Central Bank

Example data:

- EUR reference FX rates
- cross-currency reference values

### FRED Macroeconomic Indicators

Source: Federal Reserve Economic Data

Example data:

- interest rates
- inflation indicators
- macroeconomic series

## Tech Stack

Core technologies used in this project:

### Data Platform

- Databricks
- Apache Spark
- Delta Lake

### Languages

- Python
- SQL

### Engineering Tools

- Git
- GitHub
- GitHub Actions

### Code Quality

- Ruff (lint + formatting)
- Pytest (unit tests)

## Repository Structure

```text
.
├── src/lakehouse/
│   ├── sources/                 # API source adapters (coinbase / ecb / fred)
│   ├── common/                  # shared utilities (time windowing, ingestion utils)
│   ├── pipelines/               # bronze / silver / gold orchestration modules
│   ├── transforms/              # reusable transformation logic
│   └── *_rules.py               # data quality & observability rules
├── tests/unit/                  # unit tests aligned with src layout
├── docs/                        # architecture and pipeline documentation
├── .github/workflows/ci.yml     # lint + unit test CI
├── databricks.yml               # Databricks bundle/job definition
└── notebooks/                   # thin entry notebooks
```

## Pipeline Execution Order

Pipeline notebooks follow the sequence below.

**1. Platform initialization**

      00_platform_setup_catalog_schema.ipynb

**2. Bronze ingestion**

      10_direct_bronze_market_crypto_ingest.ipynb
      11_bronze_ecb_ingest.ipynb
      12_bronze_fred_ingest.ipynb

**3. Silver transformation**

      20_silver_crypto_transform.ipynb
      22_silver_macro_transform.ipynb

**4. Gold analytics**

      30_gold_market_metrics.ipynb
      32_gold_macro_indicators.ipynb

**5. Observability**

      70_pipeline_observability_metrics.ipynb

## Local Development

### Prerequisites

- Python 3.10+
- Git
- PySpark for Spark tests

### Install Dependencies

- python3 -m pip install --upgrade pip
- python3 -m pip install -e ".[dev]"

### Run Quality Checks

- python3 -m ruff check src tests
- python3 -m ruff format --check src tests
- python3 -m pytest -q tests/unit -m "not spark"

## Continuous Integration

The CI pipeline runs automatically on GitHub.

### CI tasks include:

- ruff check src/ tests/
- ruff format --check src/ tests/
- pytest tests/unit -m "not spark"

### CI ensures that:

- code style is consistent
- unit tests pass
- pipeline logic remains stable

## Databricks Deployment

### Import Repository

- Go to Databricks Repos
- Click Add Repo
- Connect to this GitHub repository

### Install Python Package

In the first notebook cell:

      %pip install .
      dbutils.library.restartPython()

This installs the lakehouse package into the cluster environment.

### Validate Bronze Pipeline

After running:

      10_direct_bronze_market_crypto_ingest.ipynb

Verify:

      result["status"] == "success"
      result["rows_written"] > 0

Data should appear in the **Bronze Delta table** for the generated run_id.

## Engineering Conventions

Key design principles used in this project:

### Thin Notebook

- notebooks contain only orchestration logic

### Heavy Python Modules

- business logic lives in reusable Python modules

### Source Isolation

- each data source has its own client and parser module

### Reusable Utilities

- shared logic lives in common

### Incremental Migration

- legacy compatibility layers are removed once new modules are adopted

## Future Improvements

Planned extensions for the project:

- Historical macroeconomic analytics
- Market volatility regime modeling
- Feature engineering for ML pipelines
- Gold data products for financial analytics

## Summary

This project demonstrates a **production-style data engineering workflow**, including:

- modular pipeline architecture
- Databricks Lakehouse design
- CI-enabled code quality
- testable data pipelines
- scalable data ingestion patterns

