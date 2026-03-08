"""Market Macro Lakehouse package."""

from lakehouse.common.models import (
    BronzeIngestionResult,
    EcbBronzeIngestionResult,
    GoldIngestionResult,
    LoadResult,
    ProductIngestionStats,
    SilverIngestionResult,
)

__all__ = [
    "LoadResult",
    "ProductIngestionStats",
    "BronzeIngestionResult",
    "EcbBronzeIngestionResult",
    "SilverIngestionResult",
    "GoldIngestionResult",
]

__version__ = "0.1.0"
