"""External source adapters."""

from lakehouse.sources.coinbase import CoinbaseSource
from lakehouse.sources.ecb import EcbSource
from lakehouse.sources.fred import FredSource

__all__ = ["CoinbaseSource", "EcbSource", "FredSource"]
