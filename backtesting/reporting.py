"""
Phase 4: Result Aggregation and Reporting

This module computes performance metrics and summary outputs from a backtest.

Inputs:
  - equity series (portfolio equity over time)
  - trades DataFrame (one row per completed trade)

Outputs:
  - drawdown DataFrame (peak + drawdown series)
  - metrics dictionaries for equity and trades

Placeholders:
  - SAMPLE_RISK_FREE_RATE
  - SAMPLE_PERIODS_PER_YEAR
"""

from __future__ import annotations

import math
from typing import Any, Dict, Mapping, Optional, Union, cast

import pandas as pd


def compute_drawdown(equity_series: pd.Series) -> pd.DataFrame:
    """
    Compute running peak and drawdown from an equity series.

    Definitions:
      peak_t = max(equity_0..t)
      drawdown_t = equity_t / peak_t - 1

    Args:
        equity_series: Portfolio equity indexed by timestamp.

    Returns:
        DataFrame indexed like equity_series with columns:
            - equity
            - peak
            - drawdown
    """

    if not isinstance(equity_series, pd.Series):
        raise TypeError(f"equity_series must be a pandas.Series, got: {type(equity_series)!r}")

    equity = pd.to_numeric(equity_series, errors="coerce").astype(float)
    if equity.empty:
        return pd.DataFrame(index=equity_series.index, columns=["equity", "peak", "drawdown"])

    peak = equity.cummax()
    drawdown = equity / peak - 1.0

    out = pd.DataFrame({"equity": equity, "peak": peak, "drawdown": drawdown}, index=equity.index)
    return out


def compute_equity_metrics(
    equity_series: pd.Series,
    periods_per_year: int = 252,
    risk_free_rate: float = 0.0,
) -> Dict[str, float]:
    """
    Compute basic equity-curve performance metrics.

    Metric definitions (high-level):
      - total_return = final / initial - 1
      - max_drawdown = min(drawdown_t)
      - volatility = std(returns) * sqrt(periods_per_year)
      - sharpe = (mean(returns) * periods_per_year - risk_free_rate) / volatility

    Assumptions:
      - equity_series is sampled at a constant frequency corresponding to
        `periods_per_year` (placeholder: SAMPLE_PERIODS_PER_YEAR).
      - risk_free_rate is annualized (placeholder: SAMPLE_RISK_FREE_RATE).

    Edge cases:
      - empty/all-NaN equity -> zeros for all metrics
      - zero/NaN volatility -> sharpe = 0.0

    Returns:
        Dict with keys: total_return, max_drawdown, volatility, sharpe
    """

    if periods_per_year <= 0:
        raise ValueError("periods_per_year must be > 0")

    equity = pd.to_numeric(equity_series, errors="coerce").astype(float).dropna()
    if equity.empty:
        return {
            "total_return": 0.0,
            "max_drawdown": 0.0,
            "volatility": 0.0,
            "sharpe": 0.0,
        }

    initial = float(equity.iloc[0])
    final = float(equity.iloc[-1])
    if initial <= 0:
        total_return = 0.0
    else:
        total_return = final / initial - 1.0

    dd = compute_drawdown(equity)
    max_drawdown = float(dd["drawdown"].min()) if not dd.empty else 0.0
    if math.isnan(max_drawdown):
        max_drawdown = 0.0

    returns = equity.pct_change().dropna()
    if returns.empty:
        vol = 0.0
        sharpe = 0.0
    else:
        vol = float(returns.std(ddof=0)) * math.sqrt(float(periods_per_year))
        if not math.isfinite(vol) or vol == 0.0:
            vol = 0.0
            sharpe = 0.0
        else:
            mean_return = float(returns.mean()) * float(periods_per_year)
            sharpe = (mean_return - float(risk_free_rate)) / vol
            if not math.isfinite(sharpe):
                sharpe = 0.0

    return {
        "total_return": float(total_return),
        "max_drawdown": float(max_drawdown),
        "volatility": float(vol),
        "sharpe": float(sharpe),
    }


def compute_trade_metrics(trades_df: pd.DataFrame) -> Dict[str, float]:
    """
    Compute basic trade-level performance metrics.

    Required input:
      - trades_df with a numeric 'net_pnl' column (one row per completed trade)

    Metrics:
      - num_trades
      - win_rate
      - avg_win (mean net_pnl of wins)
      - avg_loss (mean net_pnl of losses; negative or 0)
      - profit_factor = sum(wins) / abs(sum(losses))

    Edge cases:
      - empty trades or missing net_pnl -> zeros
      - no losses and some wins -> profit_factor = inf
      - no wins and no losses -> profit_factor = 0
    """

    if not isinstance(trades_df, pd.DataFrame):
        raise TypeError(f"trades_df must be a pandas.DataFrame, got: {type(trades_df)!r}")

    if trades_df.empty or "net_pnl" not in trades_df.columns:
        return {
            "num_trades": 0.0,
            "win_rate": 0.0,
            "avg_win": 0.0,
            "avg_loss": 0.0,
            "profit_factor": 0.0,
        }

    net = pd.to_numeric(trades_df["net_pnl"], errors="coerce").astype(float).dropna()
    num = int(len(net))
    if num == 0:
        return {
            "num_trades": 0.0,
            "win_rate": 0.0,
            "avg_win": 0.0,
            "avg_loss": 0.0,
            "profit_factor": 0.0,
        }

    wins = net[net > 0]
    losses = net[net < 0]

    win_rate = float(len(wins)) / float(num) if num > 0 else 0.0
    avg_win = float(wins.mean()) if not wins.empty else 0.0
    avg_loss = float(losses.mean()) if not losses.empty else 0.0

    gross_profit = float(wins.sum()) if not wins.empty else 0.0
    gross_loss = float(abs(losses.sum())) if not losses.empty else 0.0

    if gross_loss == 0.0:
        profit_factor = float("inf") if gross_profit > 0.0 else 0.0
    else:
        profit_factor = gross_profit / gross_loss

    return {
        "num_trades": float(num),
        "win_rate": float(win_rate),
        "avg_win": float(avg_win),
        "avg_loss": float(avg_loss),
        "profit_factor": float(profit_factor),
    }


def summarize_backtest(
    result: Any,
    periods_per_year: int = 252,
    risk_free_rate: float = 0.0,
) -> Dict[str, float]:
    """
    Summarize a BacktestResult into a single metrics dictionary.

    Expected `result` shape:
      - result.equity_curve_df: DataFrame containing an 'equity' column
      - result.trades_df: DataFrame with 'net_pnl' column (optional/empty ok)

    Returns:
        dict combining equity and trade metrics.
    """

    equity_curve_df = getattr(result, "equity_curve_df", None)
    trades_df = getattr(result, "trades_df", None)

    if not isinstance(equity_curve_df, pd.DataFrame):
        raise TypeError("result.equity_curve_df must be a pandas.DataFrame")
    if "equity" not in equity_curve_df.columns:
        raise ValueError("result.equity_curve_df must contain an 'equity' column")

    if trades_df is None:
        trades_df = pd.DataFrame()
    if not isinstance(trades_df, pd.DataFrame):
        raise TypeError("result.trades_df must be a pandas.DataFrame")

    equity_metrics = compute_equity_metrics(
        cast(pd.Series, equity_curve_df["equity"]),
        periods_per_year=periods_per_year,
        risk_free_rate=risk_free_rate,
    )
    trade_metrics = compute_trade_metrics(trades_df)

    summary: Dict[str, float] = {}
    summary.update(equity_metrics)
    summary.update(trade_metrics)
    return summary

