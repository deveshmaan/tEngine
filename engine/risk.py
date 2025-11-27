from __future__ import annotations

import datetime as dt
import logging
from collections import deque
from dataclasses import dataclass, field
from typing import Callable, Deque, Dict, Optional

from engine.config import IST, RiskLimits
from engine.data import is_market_data_stale
from engine.logging_utils import get_logger
try:
    from engine.metrics import inc_market_data_stale_blocked_entries, set_risk_dials
except Exception:  # pragma: no cover
    def set_risk_dials(**kwargs): ...
    def inc_market_data_stale_blocked_entries(*args, **kwargs): ...
from engine.time_machine import now as engine_now
from persistence import SQLiteStore

# Legacy imports kept for backwards compatibility with RiskGates
from engine.events import OrderIntent, OrderSignal, RiskReject
from market.instrument_cache import InstrumentCache, InstrumentMeta


@dataclass(frozen=True)
class OrderBudget:
    symbol: str
    qty: int
    price: float
    lot_size: int
    side: str = "BUY"


@dataclass
class PositionState:
    lot_size: int = 1
    net_qty: int = 0
    avg_price: float = 0.0
    realized_pnl: float = 0.0
    last_price: Optional[float] = None

    def mark_to_market(self) -> float:
        if self.net_qty == 0:
            return 0.0
        reference = self.last_price if self.last_price is not None else self.avg_price
        return (reference - self.avg_price) * self.net_qty


@dataclass
class RiskEvent:
    ts: dt.datetime
    code: str
    message: str
    symbol: Optional[str] = None
    context: Dict[str, float | int | str] = field(default_factory=dict)


class SessionGuard:
    """Encapsulate time-of-day guardrails for session bootstrap."""

    MARKET_START = (9, 15, 0)
    MARKET_CLOSE = (15, 30, 0)

    def __init__(self, config: RiskLimits):
        self._cfg = config

    @staticmethod
    def _parse_tod(hhmmss: str) -> tuple[int, int, int]:
        parts = [int(part) for part in hhmmss.split(":") if part]
        while len(parts) < 3:
            parts.append(0)
        return parts[0], parts[1], parts[2]

    @staticmethod
    def _now_ist() -> dt.datetime:
        return engine_now(IST)

    @staticmethod
    def _is_market_open(now_ist: dt.datetime) -> bool:
        start = now_ist.replace(hour=SessionGuard.MARKET_START[0], minute=SessionGuard.MARKET_START[1], second=0, microsecond=0)
        close = now_ist.replace(hour=SessionGuard.MARKET_CLOSE[0], minute=SessionGuard.MARKET_CLOSE[1], second=0, microsecond=0)
        return start <= now_ist < close

    def _combine(self, now_ist: dt.datetime, target: dt.time) -> dt.datetime:
        h, m, s = self._parse_tod(target.strftime("%H:%M:%S"))
        return now_ist.replace(hour=h, minute=m, second=s, microsecond=0)

    def evaluate_boot_state(self, now_ist: dt.datetime, has_positions: bool) -> dict[str, str]:
        cutoff = self._combine(now_ist, self._cfg.no_new_entries_after)
        square_off = self._combine(now_ist, self._cfg.square_off_by)
        if now_ist < cutoff:
            market_reason = "PREOPEN" if now_ist < now_ist.replace(hour=self.MARKET_START[0], minute=self.MARKET_START[1], second=0, microsecond=0) else "LIVE"
            return {"action": "continue", "reason": market_reason}
        if now_ist < square_off:
            return {"action": "halt", "reason": "AFTER_CUTOFF"}
        if has_positions:
            return {"action": "squareoff_then_halt", "reason": "POST_CLOSE"}
        behavior = (self._cfg.post_close_behavior or "halt_if_flat").lower()
        if behavior == "shutdown":
            return {"action": "shutdown", "reason": "POST_CLOSE"}
        return {"action": "halt", "reason": "POST_CLOSE"}

    def now(self) -> dt.datetime:
        return self._now_ist()


class RiskManager:
    """Production-ready risk controller for the BUY-only strategy."""

    def __init__(self, config: RiskLimits, store: SQLiteStore, *, clock: Optional[Callable[[], dt.datetime]] = None, logger: Optional[logging.Logger] = None):
        self.cfg = config
        self.store = store
        self._clock = clock or (lambda: engine_now(IST))
        self._positions: Dict[str, PositionState] = {}
        self._order_timestamps: Deque[dt.datetime] = deque()
        self._halt_reason: Optional[str] = None
        self._kill_switch = False
        self._logger = logger if logger else get_logger("RiskManager")
        self._session_guard = SessionGuard(config)
        self._update_risk_metrics()

    # ----------------------------------------------------------------- updates
    def on_fill(self, *, symbol: str, side: str, qty: int, price: float, lot_size: int) -> None:
        state = self._positions.setdefault(symbol, PositionState(lot_size=max(lot_size, 1)))
        signed_qty = qty if side.upper() == "BUY" else -qty
        prev_qty = state.net_qty
        state.lot_size = max(lot_size, 1)
        state.last_price = price

        if prev_qty == 0 or prev_qty * signed_qty >= 0:
            new_qty = prev_qty + signed_qty
            if new_qty != 0:
                weighted = (state.avg_price * prev_qty) + (price * signed_qty)
                state.avg_price = weighted / new_qty
            else:
                state.avg_price = 0.0
            state.net_qty = new_qty
        else:
            closing = min(abs(prev_qty), abs(signed_qty))
            direction = 1 if prev_qty > 0 else -1
            pnl_move = (price - state.avg_price) * closing * direction
            state.realized_pnl += pnl_move
            remainder = prev_qty + signed_qty
            state.net_qty = remainder
            if remainder == 0:
                state.avg_price = 0.0
            elif remainder * prev_qty < 0:
                # flipped direction – treat leftover as fresh exposure
                state.avg_price = price

        self._positions[symbol] = state
        self._evaluate_limits(symbol)
        self._update_risk_metrics()

    def on_tick(self, symbol: str, ltp: float) -> None:
        state = self._positions.get(symbol)
        if not state:
            return
        state.last_price = ltp
        self._positions[symbol] = state
        self._evaluate_limits(symbol)
        self._update_risk_metrics()

    # ----------------------------------------------------------------- checks
    def budget_ok_for(self, order: OrderBudget) -> bool:
        side = order.side.upper()
        now_dt = self._now()
        now_time = now_dt.time().replace(tzinfo=None)
        if side == "BUY" and now_time >= self.cfg.no_new_entries_after:
            self._emit_event("ENTRY_WINDOW", f"Entry window closed at {self.cfg.no_new_entries_after}", order.symbol)
            return False

        if side == "BUY" and self.should_halt():
            self._emit_event("HALT", "Kill switch engaged", order.symbol)
            return False

        if order.qty <= 0 or order.price <= 0:
            self._emit_event("ORDER_INVALID", "Order requires positive qty and price", order.symbol)
            return False

        if order.qty % max(order.lot_size, 1) != 0:
            self._emit_event("LOT_MISMATCH", "Quantity not aligned to lot size", order.symbol)
            return False

        if side == "BUY" and not self._order_rate_ok(now_dt):
            self._emit_event("RATE_LIMIT", "Order rate exceeded", order.symbol)
            return False

        if side == "BUY" and not self._lots_available(order):
            self._emit_event("OPEN_LOTS", "Max open lots reached", order.symbol)
            return False

        if side == "BUY" and not self._premium_budget_ok(order):
            self._emit_event("NOTIONAL", "Notional premium budget exhausted", order.symbol)
            return False

        return True

    def _update_risk_metrics(self) -> None:
        """Update exposure gauges for open lots, premium, and square-off timer."""

        try:
            open_lots = 0.0
            notional = 0.0
            for state in self._positions.values():
                lot = max(state.lot_size, 1)
                open_lots += abs(state.net_qty) / lot
                ref_price = state.last_price if state.last_price is not None else state.avg_price or 0.0
                notional += abs(state.net_qty) * ref_price
            now = self._now()
            now_ist = now.astimezone(IST) if now.tzinfo else now
            square_off_dt = now_ist.replace(
                hour=self.cfg.square_off_by.hour,
                minute=self.cfg.square_off_by.minute,
                second=self.cfg.square_off_by.second,
                microsecond=0,
            )
            minutes_to_sqoff = max(0, int((square_off_dt - now_ist).total_seconds() // 60)) if square_off_dt > now_ist else 0
            set_risk_dials(
                daily_stop_rupees=self.cfg.daily_pnl_stop,
                open_lots=open_lots,
                notional_rupees=notional,
                minutes_to_sqoff=minutes_to_sqoff,
            )
        except Exception:
            return

    def should_halt(self) -> bool:
        if self._now().time().replace(tzinfo=None) >= self.cfg.square_off_by:
            self._halt_reason = self._halt_reason or "SQUARE_OFF_TIME"
            self._kill_switch = True
        return self._kill_switch

    def halt_reason(self) -> Optional[str]:
        return self._halt_reason

    def trigger_kill(self, reason: str) -> None:
        if not self._kill_switch:
            self._emit_event("KILL", reason, None)
        self._kill_switch = True
        if not self._halt_reason:
            self._halt_reason = reason

    def halt_new_entries(self, reason: str) -> None:
        self._emit_event("HALT", reason, None)
        self._kill_switch = True
        self._halt_reason = reason

    def block_if_stale(self, instrument: str, *, threshold: float) -> bool:
        """Return True if market data is stale and the entry should be blocked."""

        if threshold <= 0:
            return False
        if not instrument:
            return False
        if not is_market_data_stale(instrument, threshold=threshold):
            return False
        self.store.record_risk_event(
            code="MD_STALE",
            message=f"Market data stale for {instrument}",
            symbol=instrument,
            ts=self._clock(),
            context={"threshold": threshold},
        )
        try:
            inc_market_data_stale_blocked_entries(instrument)
        except Exception:
            pass
        self._logger.log_event(30, "md_stale_block", instrument=instrument, threshold=threshold)
        return True

    def square_off_all(self, reason: str) -> list[OrderBudget]:
        budgets: list[OrderBudget] = []
        for symbol, state in self._positions.items():
            if state.net_qty <= 0:
                continue
            px = state.last_price if state.last_price is not None else state.avg_price
            budgets.append(
                OrderBudget(
                    symbol=symbol,
                    qty=state.net_qty,
                    price=px,
                    lot_size=max(state.lot_size, 1),
                    side="SELL",
                )
            )
        if budgets:
            self.store.record_incident("SQUARE_OFF", {"reason": reason, "count": len(budgets)}, ts=self._clock())
        return budgets

    def open_position_count(self) -> int:
        runtime = sum(1 for state in self._positions.values() if state.net_qty > 0)
        if runtime:
            return runtime
        conn = getattr(self.store, "_conn", None)
        lock = getattr(self.store, "_lock", None)
        if conn is None or lock is None:
            return 0
        try:
            with lock:
                cur = conn.execute(
                    "SELECT qty FROM positions WHERE run_id=? AND (closed_at IS NULL OR closed_at='') AND ABS(qty) > 0",
                    (self.store.run_id,),
                )
                rows = cur.fetchall()
        except Exception:
            return 0
        count = 0
        for row in rows:
            qty = row[0]
            if qty:
                count += 1
        return count

    def has_open_positions(self) -> bool:
        return self.open_position_count() > 0

    def evaluate_boot_state(self, now_ist: Optional[dt.datetime] = None, has_positions: Optional[bool] = None) -> dict[str, str]:
        now = now_ist or self._session_guard.now()
        snapshot_has_positions = has_positions if has_positions is not None else self.has_open_positions()
        return self._session_guard.evaluate_boot_state(now, snapshot_has_positions)

    def session_guard_now(self) -> dt.datetime:
        return self._session_guard.now()

    # ------------------------------------------------------------------ helpers
    def _order_rate_ok(self, now: dt.datetime) -> bool:
        self._purge_old_orders(now)
        if len(self._order_timestamps) >= self.cfg.max_order_rate:
            return False
        self._order_timestamps.append(now)
        return True

    def _purge_old_orders(self, now: dt.datetime) -> None:
        # Enforce rate as a per-second ceiling to avoid overly conservative minute-long windows.
        horizon = now - dt.timedelta(seconds=1)
        while self._order_timestamps and self._order_timestamps[0] < horizon:
            self._order_timestamps.popleft()

    def _lots_available(self, order: OrderBudget) -> bool:
        open_lots = self._total_open_lots()
        pending = order.qty / max(order.lot_size, 1)
        return open_lots + pending <= self.cfg.max_open_lots

    def _total_open_lots(self) -> float:
        lots = 0.0
        for state in self._positions.values():
            if state.net_qty > 0:
                lots += state.net_qty / max(state.lot_size, 1)
        return lots

    def _premium_budget_ok(self, order: OrderBudget) -> bool:
        exposure = self._premium_exposure()
        incoming = order.qty * order.price
        return exposure + incoming <= self.cfg.notional_premium_cap

    def _premium_exposure(self) -> float:
        exposure = 0.0
        for state in self._positions.values():
            mark = state.last_price if state.last_price is not None else state.avg_price
            exposure += abs(state.net_qty) * mark
        return exposure

    def _evaluate_limits(self, symbol: str) -> None:
        total_pnl = self._total_pnl()
        # daily_pnl_stop is expected to be negative (loss stop). Trigger when total PnL breaches it.
        if self.cfg.daily_pnl_stop < 0 and total_pnl <= self.cfg.daily_pnl_stop:
            self.trigger_kill("DAILY_STOP")
        state = self._positions.get(symbol)
        if not state:
            return
        symbol_pnl = state.realized_pnl + state.mark_to_market()
        if symbol_pnl <= -self.cfg.per_symbol_loss_stop:
            self.trigger_kill(f"SYMBOL_STOP:{symbol}")

    def _total_pnl(self) -> float:
        pnl = 0.0
        for state in self._positions.values():
            pnl += state.realized_pnl + state.mark_to_market()
        return pnl

    def _emit_event(self, code: str, message: str, symbol: Optional[str], context: Optional[Dict[str, str | int | float]] = None) -> None:
        ts = self._clock()
        event = RiskEvent(ts=ts, code=code, message=message, symbol=symbol, context=context or {})
        self.store.record_risk_event(code=event.code, message=event.message, symbol=event.symbol, context=event.context, ts=event.ts)
        self._logger.log_event(logging.INFO, "risk_event", code=code, message=message, symbol=symbol, context=context or {})

    def _now(self) -> dt.datetime:
        return self._clock().astimezone(IST)


# ---------------------------------------------------------------------------
# Legacy BUY-only risk gates kept for compatibility with existing notebooks/tests


@dataclass
class RiskConfig:
    max_order_notional: float = 250000.0
    max_symbol_notional: float = 500000.0
    max_global_notional: float = 800000.0
    max_open_positions: int = 3
    strict_price_bands: bool = False
    default_lot_size: int = 50
    default_tick_size: float = 0.05


@dataclass
class RiskState:
    exposures: Dict[str, float] = field(default_factory=dict)
    open_positions: Dict[str, int] = field(default_factory=dict)
    realized_pnl: float = 0.0


class InstrumentMetaProvider:
    def __init__(self, cache: InstrumentCache):
        self._cache = cache

    def get(self, instrument_key: str) -> Optional[InstrumentMeta]:
        return self._cache.get_meta(instrument_key)


class RiskGates:
    """BUY-only risk checks before hitting the broker (legacy)."""

    def __init__(self, config: RiskConfig, meta_provider: InstrumentMetaProvider):
        self.cfg = config
        self.meta_provider = meta_provider
        self.state = RiskState()
        self.logger = logging.getLogger("RiskGates")

    def evaluate(self, signal: OrderSignal) -> OrderIntent | RiskReject:
        ts = dt.datetime.utcnow()
        if signal.side.upper() != "BUY":
            return RiskReject(signal=signal, reason="SELL not allowed", code="side", ts=ts)
        price = signal.limit_price or (signal.meta or {}).get("price")
        if price is None or price <= 0:
            return RiskReject(signal=signal, reason="Price required", code="price", ts=ts)
        meta = self.meta_provider.get(signal.instrument)
        lot = int(meta.lot_size) if (meta and meta.lot_size) else self.cfg.default_lot_size
        if signal.qty % max(lot, 1) != 0:
            return RiskReject(signal=signal, reason="Qty not aligned to lot", code="lot", ts=ts)
        if meta and meta.freeze_qty and signal.qty > meta.freeze_qty:
            return RiskReject(signal=signal, reason="Freeze qty breached", code="freeze", ts=ts)
        tick = float(meta.tick_size) if (meta and meta.tick_size) else self.cfg.default_tick_size
        price = self._round_price(price, tick)
        notional = price * signal.qty
        if notional > self.cfg.max_order_notional:
            return RiskReject(signal=signal, reason="Order notional too high", code="order_notional", ts=ts)
        symbol_exposure = self.state.exposures.get(signal.instrument, 0.0)
        if symbol_exposure + notional > self.cfg.max_symbol_notional:
            return RiskReject(signal=signal, reason="Symbol exposure limit", code="symbol_exposure", ts=ts)
        total_exposure = sum(self.state.exposures.values()) + notional
        if total_exposure > self.cfg.max_global_notional:
            return RiskReject(signal=signal, reason="Global exposure limit", code="global_exposure", ts=ts)
        open_positions = sum(1 for qty in self.state.open_positions.values() if qty > 0)
        if open_positions >= self.cfg.max_open_positions and self.state.open_positions.get(signal.instrument, 0) <= 0:
            return RiskReject(signal=signal, reason="Max positions", code="max_positions", ts=ts)
        if self.cfg.strict_price_bands and meta:
            if meta.price_band_low and price < meta.price_band_low:
                return RiskReject(signal=signal, reason="Below price band", code="band_low", ts=ts)
            if meta.price_band_high and price > meta.price_band_high:
                return RiskReject(signal=signal, reason="Above price band", code="band_high", ts=ts)
        tag = (signal.meta or {}).get("tag") or f"BUY-{signal.instrument}"
        return OrderIntent(
            instrument=signal.instrument,
            side="BUY",
            qty=signal.qty,
            order_type=signal.order_type,
            price=price,
            tag=tag,
        )

    def record_fill(self, intent: OrderIntent, price: float) -> None:
        notional = price * intent.qty
        self.state.exposures[intent.instrument] = self.state.exposures.get(intent.instrument, 0.0) + notional
        self.state.open_positions[intent.instrument] = self.state.open_positions.get(intent.instrument, 0) + intent.qty

    def record_exit(self, instrument: str, qty: int, price: float) -> None:
        notional = price * qty
        self.state.exposures[instrument] = max(0.0, self.state.exposures.get(instrument, 0.0) - notional)
        remaining = max(0, self.state.open_positions.get(instrument, 0) - qty)
        if remaining == 0:
            self.state.open_positions.pop(instrument, None)
        else:
            self.state.open_positions[instrument] = remaining

    @staticmethod
    def _round_price(price: float, tick: float) -> float:
        if tick <= 0:
            return price
        return round(price / tick) * tick
