"""Market Macro Lakehouse package."""

from lakehouse.common.models import (
    BronzeIngestionResult,
    GoldIngestionResult,
    LoadResult,
    ProductIngestionStats,
    SilverIngestionResult,
)

__all__ = [
    "LoadResult",
    "ProductIngestionStats",
    "BronzeIngestionResult",
    "SilverIngestionResult",
    "GoldIngestionResult",
]

__version__ = "0.1.0"
