"""
Backtesting package (Phase 1: Data Loading).

This package is intentionally small at the moment. Phase 1 focuses on loading
historical OHLCV data from a few supported sources into a normalized
pandas.DataFrame with:
  - a DatetimeIndex
  - standardized OHLCV columns: open, high, low, close, volume

Example (placeholders):
    from backtesting.data_loading import load_ohlcv

    df = load_ohlcv(
        SAMPLE_DATA_PATH,
        timestamp_col=SAMPLE_TIMESTAMP_COL,
        column_map=SAMPLE_COLUMN_MAP,
        tz=None,
    )
"""

from .data_loading import OhlcvSchemaError, load_ohlcv, normalize_column_names, validate_ohlcv_frame

__all__ = [
    "OhlcvSchemaError",
    "load_ohlcv",
    "normalize_column_names",
    "validate_ohlcv_frame",
]

