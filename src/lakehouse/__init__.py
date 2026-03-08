"""Market Macro Lakehouse package."""

from lakehouse.common.models import (
    BronzeIngestionResult,
    EcbBronzeIngestionResult,
    EcbSilverIngestionResult,
    FredBronzeIngestionResult,
    FredSilverIngestionResult,
    FredSeriesIngestionStats,
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
    "FredSeriesIngestionStats",
    "FredBronzeIngestionResult",
    "SilverIngestionResult",
    "EcbSilverIngestionResult",
    "FredSilverIngestionResult",
    "GoldIngestionResult",
]

__version__ = "0.1.0"
