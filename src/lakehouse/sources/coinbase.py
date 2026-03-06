"""Coinbase source adapter definitions."""

from lakehouse.sources.base import SourceAdapter


class CoinbaseSource(SourceAdapter):
    """Adapter for crypto market data."""

    def __init__(self, dataset: str = "market_crypto") -> None:
        super().__init__(
            source_name="coinbase",
            dataset=dataset,
            base_url="https://api.exchange.coinbase.com",
            bronze_table="market_macro.brz_market.raw_coinbase_ohlc_1d",
        )
