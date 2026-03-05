# Market Macro Lakehouse

Databricks Lakehouse project for building scalable market and macroeconomic data pipelines.

This repository demonstrates production-style data engineering practices, including modular pipeline design, CI testing, and Databricks job orchestration.

# Architecture

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

# Project Overview

This project separates pipeline orchestration from business logic.

# Notebook Layer

Responsible for:

runtime parameters

pipeline orchestration

Databricks job entrypoints

Python Package (src/lakehouse)

Responsible for:

API clients

parsing logic

transformations

reusable pipeline utilities

data quality rules

This design follows the Thin Notebook + Heavy Python Module pattern commonly used in production Databricks projects.

Data Sources

The project currently integrates multiple data domains.

Crypto Market Data

Source: Coinbase API

Example data:

BTC / ETH OHLC

trade volumes

market activity signals

ECB Exchange Rates

Source: European Central Bank

Example data:

EUR reference FX rates

cross-currency reference values

FRED Macroeconomic Indicators

Source: Federal Reserve Economic Data

Example data:

interest rates

inflation indicators

macroeconomic series

Tech Stack

Core technologies used in this project:

Data Platform

Databricks

Apache Spark

Delta Lake

Languages

Python

SQL

Engineering Tools

Git

GitHub

GitHub Actions

Code Quality

Ruff (lint + formatting)

Pytest (unit tests)
