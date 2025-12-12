"""
Phase 5: Integration and Usage Example

End-to-end example wiring Phase 1-4 modules:
  1) Load OHLCV data (CSV path or synthetic fallback)
  2) Generate signals from a sample strategy
  3) Run the execution simulator
  4) Compute and print summary metrics

Run:
    python3 example_backtest_run.py

Notes:
  - Replace placeholders (SAMPLE_DATA_PATH, SAMPLE_STRATEGY_PARAMS, SAMPLE_INITIAL_CASH)
    to point to your own data and strategy configuration.
  - If SAMPLE_DATA_PATH is missing/unreadable, a deterministic synthetic dataset is used.
"""

from __future__ import annotations

import math
from pathlib import Path
from typing import Any, Dict

import pandas as pd

from backtesting.data_loading import OhlcvSchemaError, load_ohlcv
from backtesting.execution import ExecutionConfig, run_backtest
from backtesting.reporting import summarize_backtest
from backtesting.signals import generate_signals, moving_average_crossover_strategy


# --- User-editable placeholders ------------------------------------------------

# 1) Point this to your OHLCV CSV file.
SAMPLE_DATA_PATH = "SAMPLE_DATA_PATH"

# 2) Configure the sample strategy (moving-average crossover).
#    Example: {"short_window": 5, "long_window": 20, "price_col": "close"}
SAMPLE_STRATEGY_PARAMS: Dict[str, Any] = {
    "short_window": 5,
    "long_window": 20,
    "price_col": "close",  # placeholder: SAMPLE_PRICE_COL
}

# 3) Backtest starting cash.
SAMPLE_INITIAL_CASH = 100000.0


def _synthetic_ohlcv(periods: int = 120, freq: str = "D") -> pd.DataFrame:
    """
    Generate a deterministic synthetic OHLCV DataFrame (no randomness).

    This is intentionally simple, but produces enough structure for a moving
    average crossover to generate trades.
    """

    idx = pd.date_range("2020-01-01", periods=periods, freq=freq)

    # A smooth-ish upward trend with a sinusoidal component.
    close_vals = [100.0 + i * 0.2 + 2.0 * math.sin(i / 3.0) for i in range(periods)]
    close = pd.Series(close_vals, index=idx, dtype="float64")

    open_ = close.shift(1).fillna(close.iloc[0])
    high = pd.concat([open_, close], axis=1).max(axis=1) + 0.5
    low = pd.concat([open_, close], axis=1).min(axis=1) - 0.5
    volume = pd.Series([1000 + (i % 20) * 5 for i in range(periods)], index=idx, dtype="float64")

    df = pd.DataFrame({"open": open_, "high": high, "low": low, "close": close, "volume": volume})
    df.index.name = "timestamp"
    return df


def _load_or_synthetic(path_text: str) -> pd.DataFrame:
    """
    Try loading from CSV; fall back to synthetic data if unavailable/unreadable.
    """

    path = Path(path_text)
    if not path_text or "SAMPLE_DATA_PATH" in str(path_text) or not path.exists():
        print(f"[data] Using synthetic OHLCV (path missing): {path_text!r}")
        return _synthetic_ohlcv()

    try:
        print(f"[data] Loading OHLCV from CSV: {path}")
        # Assumes CSV has a 'timestamp' column or a DatetimeIndex compatible layout.
        # If your CSV uses a different timestamp column, change timestamp_col here.
        return load_ohlcv(path, timestamp_col="timestamp", column_map=None, tz=None)
    except (FileNotFoundError, OhlcvSchemaError, ValueError) as exc:
        print(f"[data] Failed to load CSV ({exc}); using synthetic OHLCV instead.")
        return _synthetic_ohlcv()


def _pretty_print_metrics(metrics: Dict[str, float]) -> None:
    """
    Print metrics in a stable, readable order.
    """

    print("\n=== Backtest Summary Metrics ===")
    for key in sorted(metrics.keys()):
        val = metrics[key]
        if val == float("inf"):
            text = "inf"
        else:
            text = f"{val:.6f}"
        print(f"{key:>14s}: {text}")


def main() -> None:
    # 1) Load OHLCV data
    prices = _load_or_synthetic(SAMPLE_DATA_PATH)
    print("\n[data] OHLCV head:")
    print(prices.head())

    # 2) Build a sample strategy (moving average crossover) and generate signals
    short_window = int(SAMPLE_STRATEGY_PARAMS.get("short_window", 5))
    long_window = int(SAMPLE_STRATEGY_PARAMS.get("long_window", 20))
    price_col = str(SAMPLE_STRATEGY_PARAMS.get("price_col", "close"))

    strategy = moving_average_crossover_strategy(
        short_window=short_window,
        long_window=long_window,
        price_col=price_col,
    )

    signals = generate_signals(prices, strategy, price_col=price_col, fill_value=0)
    print("\n[signals] value counts:")
    print(signals.value_counts(dropna=False).sort_index())

    # 3) Execute backtest
    config = ExecutionConfig(
        initial_cash=float(SAMPLE_INITIAL_CASH),
        allow_short=True,
        slippage_bps=0.0,  # placeholder: SAMPLE_SLIPPAGE_BPS
        fee_per_trade=0.0,  # placeholder: SAMPLE_FEE_PER_TRADE
        fill_method="next_open",  # change to "close" to fill on same bar close
    )
    result = run_backtest(prices, signals, config)

    print("\n[result] equity curve head:")
    print(result.equity_curve_df.head())

    print("\n[result] trades head:")
    print(result.trades_df.head())

    # 4) Summarize and report metrics
    metrics = summarize_backtest(
        result,
        periods_per_year=252,  # placeholder: SAMPLE_PERIODS_PER_YEAR
        risk_free_rate=0.0,  # placeholder: SAMPLE_RISK_FREE_RATE
    )
    _pretty_print_metrics(metrics)


if __name__ == "__main__":
    main()
