"""Pipeline orchestration modules."""

from lakehouse.pipelines.bronze import (
    run_bronze_ingestion,
    run_coinbase_bronze_ingestion,
    run_ecb_bronze_ingestion,
    run_fred_bronze_ingestion,
)
from lakehouse.pipelines.gold import (
    run_gold_cross_crypto_macro_features,
    run_gold_crypto_returns_and_volatility,
    run_gold_macro_indicators,
)
from lakehouse.pipelines.silver import (
    run_silver_crypto_ohlc_1d,
    run_silver_ecb_fx_ref_rates_daily,
    run_silver_fred_series_clean,
)

__all__ = [
    "run_bronze_ingestion",
    "run_coinbase_bronze_ingestion",
    "run_ecb_bronze_ingestion",
    "run_fred_bronze_ingestion",
    "run_silver_crypto_ohlc_1d",
    "run_silver_ecb_fx_ref_rates_daily",
    "run_silver_fred_series_clean",
    "run_gold_crypto_returns_and_volatility",
    "run_gold_macro_indicators",
    "run_gold_cross_crypto_macro_features",
]
