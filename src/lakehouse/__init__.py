"""Market Macro Lakehouse package."""

from lakehouse.common.models import (
    BronzeIngestionResult,
    EcbBronzeIngestionResult,
    EcbSilverIngestionResult,
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
    "EcbSilverIngestionResult",
    "GoldIngestionResult",
]

__version__ = "0.1.0"
