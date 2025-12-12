"""
Phase 3: Trade Execution and Portfolio Logic

This module simulates trade execution from target-position signals and maintains
portfolio state over time.

Core assumptions (deterministic and testable):
  - Signals represent the *desired target position* in {-1, 0, 1}.
  - Position size is fixed to 1 unit (qty=1) for now; future phases can extend
    sizing logic without changing the execution bookkeeping.
  - Execution occurs either:
      * "close":     execute on the same bar's close
      * "next_open": execute on the next bar's open (signal at t executes at t+1 open)
  - Adverse slippage is applied to fills:
      * buys pay higher:  fill_price * (1 + s)
      * sells receive lower: fill_price * (1 - s)
  - Fees are charged per execution action (enter/exit). A flip (long->short or
    short->long) implies two actions on the same bar (exit + enter), hence two
    fees.

Outputs:
  - equity_curve_df indexed like prices_df with: cash, position, holdings_value, equity
  - trades_df with one row per completed trade (entry/exit filled)

Placeholders:
  - SAMPLE_INITIAL_CASH
  - SAMPLE_SLIPPAGE_BPS
  - SAMPLE_FEE_PER_TRADE
"""

from __future__ import annotations

from dataclasses import asdict, dataclass
from typing import Any, Dict, List, Optional, Sequence, Union, cast

import pandas as pd

ALLOWED_POSITIONS: Sequence[int] = (-1, 0, 1)


@dataclass(frozen=True)
class ExecutionConfig:
    """
    Configuration for the execution model.

    Attributes:
        initial_cash: Starting cash for the backtest (placeholder: SAMPLE_INITIAL_CASH).
        allow_short: If False, target -1 signals are treated as 0 (flat).
        slippage_bps: Optional slippage in basis points (1 bps = 0.01%).
        slippage_frac: Optional slippage as a fraction (e.g., 0.001 == 0.1%).
        fee_per_trade: Fixed fee applied per execution action (placeholder: SAMPLE_FEE_PER_TRADE).
        fill_method: "next_open" or "close".
    """

    initial_cash: float
    allow_short: bool = True
    slippage_bps: Optional[float] = None
    slippage_frac: Optional[float] = None
    fee_per_trade: float = 0.0
    fill_method: str = "next_open"

    def __post_init__(self) -> None:
        if self.initial_cash < 0:
            raise ValueError("initial_cash must be >= 0")
        if self.fill_method not in {"next_open", "close"}:
            raise ValueError("fill_method must be 'next_open' or 'close'")
        if self.slippage_bps is not None and self.slippage_frac is not None:
            raise ValueError("Provide only one of slippage_bps or slippage_frac")
        if self.slippage_bps is not None and self.slippage_bps < 0:
            raise ValueError("slippage_bps must be >= 0")
        if self.slippage_frac is not None and self.slippage_frac < 0:
            raise ValueError("slippage_frac must be >= 0")
        if self.fee_per_trade < 0:
            raise ValueError("fee_per_trade must be >= 0")

    @property
    def slippage(self) -> float:
        """Return slippage as a fraction in [0, inf)."""

        if self.slippage_frac is not None:
            return float(self.slippage_frac)
        if self.slippage_bps is not None:
            return float(self.slippage_bps) / 10000.0
        return 0.0


@dataclass(frozen=True)
class Trade:
    """
    A completed trade (entry and exit both filled).

    side:
        "long"  for buys followed by sells
        "short" for sells followed by buys
    """

    entry_time: pd.Timestamp
    exit_time: pd.Timestamp
    side: str
    qty: float
    entry_price: float
    exit_price: float
    gross_pnl: float
    fees: float
    net_pnl: float


@dataclass(frozen=True)
class BacktestResult:
    """Container for backtest outputs."""

    equity_curve_df: pd.DataFrame
    trades_df: pd.DataFrame
    config: ExecutionConfig


def _coerce_signals(prices_index: pd.Index, signals: Union[pd.Series, Sequence[Any]]) -> pd.Series:
    """
    Coerce signals into a pandas.Series aligned to `prices_index`.

    Accepted:
      - pd.Series with matching index
      - pd.Series with default RangeIndex (positional)
      - list-like length == len(prices_index) (positional)
    """

    if isinstance(signals, pd.Series):
        s = signals.copy()
    else:
        s = pd.Series(list(signals))

    if len(s) != len(prices_index):
        raise ValueError(f"signals length {len(s)} does not match prices length {len(prices_index)}")

    if s.index.equals(prices_index):
        aligned = s
    else:
        # Treat default RangeIndex as positional.
        if isinstance(s.index, pd.RangeIndex) and s.index.start == 0 and s.index.step == 1:
            aligned = s.copy()
            aligned.index = prices_index
        else:
            raise ValueError("signals index must match prices_df.index (or be default RangeIndex for positional signals)")

    # Replace missing/NaN with flat (0) deterministically.
    aligned = aligned.fillna(0)

    # Coerce to int if possible; invalid values are handled in validation below.
    try:
        aligned = aligned.astype(int)
    except Exception:
        # Keep as-is; validation will raise a clearer error.
        pass
    aligned.name = "signal"
    return aligned


def _validate_prices(prices_df: pd.DataFrame, *, require_open: bool) -> None:
    if not isinstance(prices_df, pd.DataFrame):
        raise TypeError(f"prices_df must be a pandas.DataFrame, got: {type(prices_df)!r}")

    required = ["close"] + (["open"] if require_open else [])
    missing = [c for c in required if c not in prices_df.columns]
    if missing:
        raise ValueError(f"prices_df missing required column(s): {missing}")

    # We only validate NaNs when needed during execution/marking to keep the
    # error messages contextual, but "close" is always needed for equity.
    if pd.to_numeric(prices_df["close"], errors="coerce").isna().any():
        raise ValueError("prices_df['close'] contains NaN/non-numeric values")
    if require_open:
        if pd.to_numeric(prices_df["open"], errors="coerce").isna().any():
            raise ValueError("prices_df['open'] contains NaN/non-numeric values")


def _apply_slippage(price: float, *, action: str, slippage: float) -> float:
    """
    Apply adverse slippage to a base fill price.

    action:
        "buy"  -> pay higher (1 + s)
        "sell" -> receive lower (1 - s)
    """

    if slippage <= 0:
        return float(price)
    if action == "buy":
        return float(price) * (1.0 + float(slippage))
    if action == "sell":
        return float(price) * (1.0 - float(slippage))
    raise ValueError(f"Unknown action: {action!r}")


def run_backtest(prices_df: pd.DataFrame, signals: Union[pd.Series, Sequence[Any]], config: ExecutionConfig) -> BacktestResult:
    """
    Run a simple deterministic backtest with target-position signals.

    Args:
        prices_df: Price data indexed by time. Requires:
            - "close" for marking to market
            - "open" if fill_method == "next_open"
        signals: Desired target position in {-1, 0, 1} per bar.
        config: ExecutionConfig

    Returns:
        BacktestResult containing equity curve and trades.
    """

    require_open = config.fill_method == "next_open"
    _validate_prices(prices_df, require_open=require_open)

    s = _coerce_signals(prices_df.index, signals)

    # Validate and optionally clamp short signals.
    valid_set = set(ALLOWED_POSITIONS)
    for raw in s.tolist():
        try:
            value = int(raw)
        except Exception as exc:
            raise ValueError(f"Invalid signal value (must be -1/0/1): {raw!r}") from exc
        if value not in valid_set:
            raise ValueError(f"Invalid signal value (must be -1/0/1): {raw!r}")

    if not config.allow_short:
        s = s.replace(-1, 0)

    # Internal state.
    cash = float(config.initial_cash)
    position = 0  # -1, 0, 1 (one unit)
    qty = 1.0

    # Track the currently open trade (if any).
    open_trade: Optional[Dict[str, Any]] = None

    trades: List[Trade] = []
    equity_rows: List[Dict[str, Any]] = []

    pending_target: Optional[int] = None  # used only for "next_open"

    index = prices_df.index
    close_series = pd.to_numeric(prices_df["close"], errors="coerce")
    open_series = pd.to_numeric(prices_df["open"], errors="coerce") if require_open else None

    def _execute_transition(ts: pd.Timestamp, *, target: int, fill_price: float) -> None:
        """
        Execute a transition from current `position` to `target` at `fill_price`.

        This function mutates `cash`, `position`, and may append to `trades`.
        """

        nonlocal cash, position, open_trade, trades

        if target == position:
            return

        if target not in valid_set:
            raise ValueError(f"Invalid target position: {target}")

        # Helper to close an existing trade (position != 0).
        def _close_trade(*, exit_time: pd.Timestamp, exit_price: float, exit_fee: float) -> None:
            nonlocal open_trade, trades
            if open_trade is None:
                return
            side = cast(str, open_trade["side"])
            entry_price = float(open_trade["entry_price"])
            entry_time = cast(pd.Timestamp, open_trade["entry_time"])
            entry_fee = float(open_trade["entry_fee"])
            entry_qty = float(open_trade["qty"])

            if side == "long":
                gross = (exit_price - entry_price) * entry_qty
            elif side == "short":
                gross = (entry_price - exit_price) * entry_qty
            else:
                raise ValueError(f"Unknown open trade side: {side!r}")

            fees_total = entry_fee + exit_fee
            net = gross - fees_total
            trades.append(
                Trade(
                    entry_time=entry_time,
                    exit_time=exit_time,
                    side=side,
                    qty=entry_qty,
                    entry_price=entry_price,
                    exit_price=exit_price,
                    gross_pnl=float(gross),
                    fees=float(fees_total),
                    net_pnl=float(net),
                )
            )
            open_trade = None

        # Helper to open a new trade (target != 0).
        def _open_trade(*, entry_time: pd.Timestamp, side: str, entry_price: float, entry_fee: float) -> None:
            nonlocal open_trade
            open_trade = {
                "entry_time": entry_time,
                "side": side,
                "qty": qty,
                "entry_price": float(entry_price),
                "entry_fee": float(entry_fee),
            }

        fee = float(config.fee_per_trade)
        slip = float(config.slippage)

        # Determine required actions based on current and target positions.
        # Transitions:
        #   0 -> 1   : buy (open long)
        #   0 -> -1  : sell (open short)
        #   1 -> 0   : sell (close long)
        #  -1 -> 0   : buy (close short)
        #   1 -> -1  : sell (close long) + sell (open short)
        #  -1 -> 1   : buy (close short) + buy (open long)
        actions: List[Dict[str, Any]] = []
        if position == 0 and target == 1:
            actions = [{"action": "buy", "new_position": 1}]
        elif position == 0 and target == -1:
            actions = [{"action": "sell", "new_position": -1}]
        elif position == 1 and target == 0:
            actions = [{"action": "sell", "new_position": 0}]
        elif position == -1 and target == 0:
            actions = [{"action": "buy", "new_position": 0}]
        elif position == 1 and target == -1:
            actions = [{"action": "sell", "new_position": 0}, {"action": "sell", "new_position": -1}]
        elif position == -1 and target == 1:
            actions = [{"action": "buy", "new_position": 0}, {"action": "buy", "new_position": 1}]
        else:
            # Should be unreachable with allowed positions.
            raise ValueError(f"Unsupported transition: {position} -> {target}")

        for action_info in actions:
            action = cast(str, action_info["action"])
            new_pos = cast(int, action_info["new_position"])
            exec_price = _apply_slippage(float(fill_price), action=action, slippage=slip)

            if action == "buy":
                cash -= exec_price * qty
                cash -= fee
                # If we just bought to close a short, record the exit.
                if position == -1 and new_pos == 0:
                    _close_trade(exit_time=ts, exit_price=exec_price, exit_fee=fee)
                # If we just bought to open a long, record the entry.
                if new_pos == 1:
                    _open_trade(entry_time=ts, side="long", entry_price=exec_price, entry_fee=fee)
            elif action == "sell":
                cash += exec_price * qty
                cash -= fee
                # If we just sold to close a long, record the exit.
                if position == 1 and new_pos == 0:
                    _close_trade(exit_time=ts, exit_price=exec_price, exit_fee=fee)
                # If we just sold to open a short, record the entry.
                if new_pos == -1:
                    _open_trade(entry_time=ts, side="short", entry_price=exec_price, entry_fee=fee)
            else:
                raise ValueError(f"Unknown action: {action!r}")

            position = new_pos

    # Iterate over bars.
    for i, ts_raw in enumerate(index):
        ts = cast(pd.Timestamp, pd.Timestamp(ts_raw))

        # 1) Execute pending order at the current bar open for "next_open".
        if config.fill_method == "next_open" and pending_target is not None:
            # Fill at this bar's open.
            open_price = float(cast(pd.Series, open_series).iloc[i])
            if pd.isna(open_price):
                raise ValueError(f"Open price is NaN at {ts}")
            _execute_transition(ts, target=int(pending_target), fill_price=open_price)

        # 2) For "close", execute transition immediately at this bar's close.
        if config.fill_method == "close":
            target_now = int(s.iloc[i])
            close_price = float(close_series.iloc[i])
            if pd.isna(close_price):
                raise ValueError(f"Close price is NaN at {ts}")
            _execute_transition(ts, target=target_now, fill_price=close_price)

        # 3) Mark-to-market at bar close (end-of-bar state).
        close_price_mark = float(close_series.iloc[i])
        if pd.isna(close_price_mark):
            raise ValueError(f"Close price is NaN at {ts}")

        holdings_value = float(position) * qty * close_price_mark
        equity = float(cash) + float(holdings_value)
        equity_rows.append(
            {
                "timestamp": ts,
                "cash": float(cash),
                "position": int(position),
                "holdings_value": float(holdings_value),
                "equity": float(equity),
            }
        )

        # 4) For "next_open", schedule the desired target for the *next* bar.
        if config.fill_method == "next_open":
            target = int(s.iloc[i])
            if target == position:
                pending_target = None
            else:
                # If this is the last bar, there is no next open; skip per spec.
                if i == len(index) - 1:
                    pending_target = None
                else:
                    pending_target = target

    equity_curve_df = pd.DataFrame(equity_rows).set_index("timestamp")

    # Completed trades only; open positions at the end are intentionally not forced-closed.
    trade_dicts = [asdict(t) for t in trades]
    if trade_dicts:
        trades_df = pd.DataFrame(trade_dicts)
    else:
        trades_df = pd.DataFrame(
            columns=[
                "entry_time",
                "exit_time",
                "side",
                "qty",
                "entry_price",
                "exit_price",
                "gross_pnl",
                "fees",
                "net_pnl",
            ]
        )

    return BacktestResult(equity_curve_df=equity_curve_df, trades_df=trades_df, config=config)

