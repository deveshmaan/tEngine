from __future__ import annotations

import datetime as dt
from collections import deque
from dataclasses import dataclass, field
from typing import Deque, Dict, Iterable, Optional, Tuple

from engine.fees import FeeConfig, fees_for_execution
from engine.time_machine import now as engine_now
from persistence import SQLiteStore


@dataclass(frozen=True)
class Execution:
    exec_id: str
    order_id: str
    symbol: str
    side: str
    qty: int
    price: float
    ts: dt.datetime
    expiry: Optional[str] = None
    strike: Optional[float] = None
    opt_type: Optional[str] = None
    venue: str = "SIM"


@dataclass
class Lot:
    qty: int
    price: float
    ts: dt.datetime


@dataclass
class PositionState:
    symbol: str
    expiry: Optional[str]
    strike: Optional[float]
    opt_type: Optional[str]
    lots: Deque[Lot] = field(default_factory=deque)
    realized: float = 0.0
    fees: float = 0.0
    unrealized: float = 0.0
    last_price: Optional[float] = None
    opened_at: Optional[dt.datetime] = None
    closed_at: Optional[dt.datetime] = None

    @property
    def net_qty(self) -> int:
        return sum(lot.qty for lot in self.lots)

    @property
    def avg_price(self) -> float:
        total = 0.0
        qty = 0
        for lot in self.lots:
            total += abs(lot.qty) * lot.price
            qty += abs(lot.qty)
        return total / qty if qty else 0.0


class PnLCalculator:
    def __init__(self, store: SQLiteStore, fee_config: Optional[FeeConfig] = None, currency: str = "INR"):
        self.store = store
        self.fee_config = fee_config or FeeConfig()
        self.currency = currency
        self._positions: Dict[Tuple[str, Optional[str], Optional[float], Optional[str]], PositionState] = {}
        self._total_fees = 0.0
        self._total_realized = 0.0
        self._brokerage_charged_orders: set[str] = set()

    def on_execution(self, exec: Execution) -> None:
        key = (exec.symbol, exec.expiry, exec.strike, exec.opt_type)
        state = self._positions.setdefault(
            key,
            PositionState(symbol=exec.symbol, expiry=exec.expiry, strike=exec.strike, opt_type=exec.opt_type),
        )
        state.opened_at = state.opened_at or exec.ts
        include_brokerage = exec.order_id not in self._brokerage_charged_orders
        fees = fees_for_execution(exec, self.fee_config, include_brokerage=include_brokerage)
        if include_brokerage:
            self._brokerage_charged_orders.add(exec.order_id)
        fee_total = sum(row.amount for row in fees)
        for row in fees:
            self.store.record_cost(exec.exec_id, category=row.category, amount=row.amount, currency=row.currency, note=row.note, ts=exec.ts)
        state.fees += fee_total
        self._total_fees += fee_total
        realized = self._apply_fifo(state, exec)
        state.realized += realized
        self._total_realized += realized
        state.last_price = exec.price
        if state.net_qty == 0:
            state.closed_at = exec.ts
        else:
            state.closed_at = None
        self.store.upsert_position(
            symbol=state.symbol,
            expiry=state.expiry,
            strike=state.strike,
            opt_type=state.opt_type,
            qty=state.net_qty,
            avg_price=state.avg_price,
            opened_at=state.opened_at or exec.ts,
            closed_at=state.closed_at,
        )

    def mark_to_market(self, quotes: Dict[str, float]) -> None:
        for state in self._positions.values():
            price = quotes.get(state.symbol)
            if price is None:
                price = quotes.get(self._state_key(state))
            if price is None:
                continue
            state.last_price = price
            state.unrealized = self._compute_unrealized(state, price)

    def snapshot(self, ts: Optional[dt.datetime] = None) -> None:
        ts = ts or engine_now()
        total_unrealized = sum(state.unrealized for state in self._positions.values())
        per_symbol = {}
        for state in self._positions.values():
            per_symbol[self._state_key(state)] = {
                "qty": state.net_qty,
                "avg_price": state.avg_price,
                "realized": state.realized,
                "unrealized": state.unrealized,
                "fees": state.fees,
            }
        self.store.record_pnl_snapshot(
            ts=ts,
            realized=self._total_realized,
            unrealized=total_unrealized,
            fees=self._total_fees,
            per_symbol=per_symbol,
        )

    def _apply_fifo(self, state: PositionState, exec: Execution) -> float:
        signed_qty = exec.qty if exec.side.upper() == "BUY" else -exec.qty
        realized = 0.0
        qty_remaining = abs(signed_qty)
        incoming_sign = 1 if signed_qty > 0 else -1
        while qty_remaining > 0 and state.lots:
            head = state.lots[0]
            if head.qty * signed_qty >= 0:
                break
            lot_qty = min(abs(head.qty), qty_remaining)
            direction = 1 if head.qty > 0 else -1
            realized += lot_qty * (exec.price - head.price) * direction
            head.qty -= lot_qty * direction
            qty_remaining -= lot_qty
            if head.qty == 0:
                state.lots.popleft()
        if qty_remaining > 0:
            if not state.lots:
                state.opened_at = exec.ts
                state.closed_at = None
            state.lots.append(Lot(qty=qty_remaining * incoming_sign, price=exec.price, ts=exec.ts))
        return realized

    def _compute_unrealized(self, state: PositionState, price: float) -> float:
        unrealized = 0.0
        for lot in state.lots:
            direction = 1 if lot.qty > 0 else -1
            unrealized += (price - lot.price) * abs(lot.qty) * direction
        return unrealized

    def _state_key(self, state: PositionState) -> str:
        parts = [state.symbol]
        if state.expiry:
            parts.append(state.expiry)
        if state.strike is not None:
            parts.append(str(state.strike))
        if state.opt_type:
            parts.append(state.opt_type)
        return "|".join(parts)

    def totals(self) -> tuple[float, float, float]:
        total_unrealized = sum(state.unrealized for state in self._positions.values())
        return self._total_realized, total_unrealized, self._total_fees


__all__ = ["Execution", "PnLCalculator"]
