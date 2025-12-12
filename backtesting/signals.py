"""
Phase 2: Signal Generation Logic

This module converts price/indicator data into standardized trading signals.

Signal convention (strict):
    1  = long / buy
    0  = flat / hold
    -1 = short / sell

The main entry point is `generate_signals(data, strategy, ...)` which supports:
    - Function-based strategies: Callable[[pd.DataFrame], Union[pd.Series, list]]
    - Class-based strategies: objects with generate_signals(data) -> pd.Series

Example (placeholders):
    # callable strategy example
    def SAMPLE_STRATEGY_FUNCTION(df: pd.DataFrame) -> pd.Series:
        signals = pd.Series(0, index=df.index)
        signals[df[SAMPLE_PRICE_COL].pct_change() > 0] = 1
        signals[df[SAMPLE_PRICE_COL].pct_change() < 0] = -1
        return signals

    signals = generate_signals(data, SAMPLE_STRATEGY_FUNCTION, price_col=SAMPLE_PRICE_COL)

    # strategy factory example
    crossover = moving_average_crossover_strategy(
        short_window=SAMPLE_WINDOW,
        long_window=SAMPLE_WINDOW,
        price_col=SAMPLE_PRICE_COL,
    )
    signals = generate_signals(data, crossover, price_col=SAMPLE_PRICE_COL)
"""

from __future__ import annotations

from typing import Any, Callable, Optional, Protocol, Sequence, Union, runtime_checkable

import pandas as pd

# Allowed signals in the normalized output.
ALLOWED_SIGNALS = (-1, 0, 1)


@runtime_checkable
class StrategyProtocol(Protocol):
    """
    Minimal interface for class-based strategies.

    Any object implementing this method is accepted by `generate_signals`.
    """

    def generate_signals(self, data: pd.DataFrame) -> pd.Series:
        ...


def sma(series: pd.Series, window: int) -> pd.Series:
    """
    Simple moving average (SMA).

    Args:
        series: Input series (typically a price series).
        window: Rolling window size (must be >= 1).

    Returns:
        A pandas.Series of SMA values. The first (window-1) values are NaN.
    """

    if window < 1:
        raise ValueError("window must be >= 1")
    return series.rolling(window=window, min_periods=window).mean()


def ema(series: pd.Series, span: int) -> pd.Series:
    """
    Exponential moving average (EMA).

    Args:
        series: Input series (typically a price series).
        span: EMA span (must be >= 1).

    Returns:
        A pandas.Series of EMA values.
    """

    if span < 1:
        raise ValueError("span must be >= 1")
    return series.ewm(span=span, adjust=False).mean()


def moving_average_crossover_strategy(
    short_window: int,
    long_window: int,
    price_col: str = "close",
) -> Callable[[pd.DataFrame], pd.Series]:
    """
    Example strategy factory: moving-average crossover.

    Produces:
        1  when short SMA > long SMA
        -1 when short SMA < long SMA
        0  otherwise (including warm-up NaNs)

    Args:
        short_window: SMA window for the fast MA.
        long_window: SMA window for the slow MA.
        price_col: Price column to use (placeholder: SAMPLE_PRICE_COL).
    """

    if short_window < 1 or long_window < 1:
        raise ValueError("short_window and long_window must be >= 1")
    if short_window >= long_window:
        # Keep this strict: avoids confusing "crossover" semantics.
        raise ValueError("short_window must be < long_window")

    def _strategy(data: pd.DataFrame) -> pd.Series:
        if price_col not in data.columns:
            raise KeyError(f"Missing required price column: {price_col}")

        price = pd.to_numeric(data[price_col], errors="coerce")
        fast = sma(price, short_window)
        slow = sma(price, long_window)

        signals = pd.Series(0, index=data.index, dtype="int64")
        signals[fast > slow] = 1
        signals[fast < slow] = -1
        signals.name = "signal"
        return signals

    return _strategy


def _is_range_index_like(index: pd.Index) -> bool:
    """
    Return True if the index is a default RangeIndex(0..n-1).

    This is treated as "positional" output and will be aligned to the input
    data index by assignment.
    """

    if not isinstance(index, pd.RangeIndex):
        return False
    return index.start == 0 and index.step == 1


def _standardize_signal_value(value: Any) -> Optional[int]:
    """
    Convert one raw signal value to an integer in {-1, 0, 1}, or None for missing.

    Supported inputs:
      - ints / floats (must be exactly -1/0/1)
      - bools (True->1, False->0)
      - strings: buy/sell/hold/flat/long/short, or "-1"/"0"/"1"
    """

    if value is None:
        return None
    if pd.isna(value):
        return None

    # bool is a subclass of int, so check it first.
    if isinstance(value, bool):
        return 1 if value else 0

    # Allow exact numeric signals only (anything else is an error).
    if isinstance(value, (int,)):
        if value in ALLOWED_SIGNALS:
            return int(value)
        raise ValueError(f"Invalid numeric signal (must be -1/0/1): {value!r}")

    if isinstance(value, (float,)):
        if value in (-1.0, 0.0, 1.0):
            return int(value)
        raise ValueError(f"Invalid numeric signal (must be -1/0/1): {value!r}")

    if isinstance(value, str):
        token = value.strip().lower()
        if token == "":
            return None
        mapping = {
            "buy": 1,
            "long": 1,
            "1": 1,
            "+1": 1,
            "sell": -1,
            "short": -1,
            "-1": -1,
            "hold": 0,
            "flat": 0,
            "0": 0,
        }
        if token in mapping:
            return mapping[token]
        # Support common numeric string variants like "1.0" / "-1.0" / "0.0".
        if token in {"1.0", "+1.0"}:
            return 1
        if token == "-1.0":
            return -1
        if token == "0.0":
            return 0
        raise ValueError(f"Unrecognized signal token: {value!r}")

    # Unknown type: let validation fail with a clear message.
    raise ValueError(f"Unsupported signal value type: {type(value)!r}")


def _coerce_and_validate_signals(raw: pd.Series, fill_value: int) -> pd.Series:
    """
    Coerce raw outputs into strict {-1, 0, 1} integer signals.

    - Converts booleans and common strings into numeric signals.
    - Preserves NaNs for later fill with `fill_value`.
    - Ensures only values in {-1, 0, 1} remain after fill.
    """

    if isinstance(fill_value, bool):
        # bool is an int subclass; treat explicitly for clarity.
        fill_value_int = 1 if fill_value else 0
    else:
        try:
            fill_value_int = int(fill_value)
        except Exception as exc:
            raise TypeError("fill_value must be coercible to int") from exc

    if fill_value_int not in ALLOWED_SIGNALS:
        raise ValueError(f"fill_value must be one of {ALLOWED_SIGNALS}, got: {fill_value!r}")

    # Standardize element-wise to handle mixed/object dtype safely.
    coerced = raw.map(_standardize_signal_value)

    # Replace None with NaN for pandas operations, then fill.
    coerced = coerced.astype("float64").fillna(float(fill_value_int))

    # Validate strict allowed set.
    unique_vals = sorted(set(coerced.unique().tolist()))
    if not set(unique_vals).issubset(set([float(v) for v in ALLOWED_SIGNALS])):
        raise ValueError(f"Signals must be in {ALLOWED_SIGNALS}; got values: {unique_vals}")

    # Return as int series, standardized name.
    out = coerced.astype("int64")
    out.name = "signal"
    return out


def generate_signals(
    data: pd.DataFrame,
    strategy: Union[
        Callable[[pd.DataFrame], Union[pd.Series, Sequence[Any]]],
        StrategyProtocol,
        Any,
    ],
    *,
    price_col: str = "close",
    fill_value: int = 0,
) -> pd.Series:
    """
    Generate standardized trading signals from `data` using a provided strategy.

    Args:
        data: Input OHLCV/feature DataFrame (index is preserved in the output).
        strategy:
            - Callable strategy: strategy(data) -> pd.Series or list-like
            - Class strategy: object with generate_signals(data) -> pd.Series
        price_col: Column name used by many strategies (placeholder: SAMPLE_PRICE_COL).
        fill_value: Value to use for NaNs in signals (must be one of -1/0/1).

    Returns:
        pd.Series of integer signals in {-1, 0, 1} aligned to `data.index`.
    """

    if not isinstance(data, pd.DataFrame):
        raise TypeError(f"data must be a pandas.DataFrame, got: {type(data)!r}")

    if price_col and price_col not in data.columns:
        # This validation is intentionally lightweight; strategies can use other
        # columns, but `price_col` is the common baseline.
        raise KeyError(f"Missing required price column: {price_col}")

    # Prefer an explicit generate_signals method when present.
    if hasattr(strategy, "generate_signals") and callable(getattr(strategy, "generate_signals")):
        raw_out = getattr(strategy, "generate_signals")(data)
    elif callable(strategy):
        raw_out = strategy(data)
    else:
        raise TypeError("strategy must be callable or implement generate_signals(data) -> pd.Series")

    # Convert output to a Series.
    if isinstance(raw_out, pd.Series):
        raw_series = raw_out.copy()
    else:
        # Accept list/tuple/numpy arrays/Index-like. Reject raw strings.
        if isinstance(raw_out, (str, bytes)):
            raise TypeError("strategy output must be a Series or list-like, not a string")
        raw_series = pd.Series(raw_out)

    # Validate length matches data length.
    if len(raw_series) != len(data):
        raise ValueError(f"Signal length {len(raw_series)} does not match data length {len(data)}")

    # Align to the input index.
    if raw_series.index.equals(data.index):
        aligned = raw_series
    else:
        # Allow positional outputs with a default RangeIndex.
        if _is_range_index_like(raw_series.index):
            aligned = raw_series.copy()
            aligned.index = data.index
        else:
            raise ValueError("Signal index must match data.index (or be a default RangeIndex for positional output)")

    # Coerce + validate strict {-1,0,1}.
    return _coerce_and_validate_signals(aligned, fill_value=fill_value)
