"""
Compatibility module for the Phase 1 file layout.

Some tooling or notes may refer to `backtesting/init.py`. The canonical package
initializer is `backtesting/__init__.py`, but this file re-exports the same
public symbols for convenience.
"""

from backtesting.data_loading import OhlcvSchemaError, load_ohlcv, normalize_column_names, validate_ohlcv_frame

__all__ = [
    "OhlcvSchemaError",
    "load_ohlcv",
    "normalize_column_names",
    "validate_ohlcv_frame",
]

