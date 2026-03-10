"""Market Macro Lakehouse package."""

from lakehouse.common.models import (
    BronzeIngestionResult,
    CrossGoldIngestionResult,
    EcbBronzeIngestionResult,
    EcbSilverIngestionResult,
    FredBronzeIngestionResult,
    FredSilverIngestionResult,
    FredSeriesIngestionStats,
    GoldIngestionResult,
    LoadResult,
    ObservabilityIngestionResult,
    MacroGoldIngestionResult,
    ProductIngestionStats,
    SilverIngestionResult,
)

__all__ = [
    "LoadResult",
    "ProductIngestionStats",
    "BronzeIngestionResult",
    "CrossGoldIngestionResult",
    "EcbBronzeIngestionResult",
    "FredSeriesIngestionStats",
    "FredBronzeIngestionResult",
    "SilverIngestionResult",
    "EcbSilverIngestionResult",
    "FredSilverIngestionResult",
    "GoldIngestionResult",
    "ObservabilityIngestionResult",
    "MacroGoldIngestionResult",
]

__version__ = "0.1.0"
