from __future__ import annotations

import datetime as dt
from typing import Any, Dict, Optional

from engine.config import ExitConfig, IST
from engine.logging_utils import get_logger
from engine.oms import OMS
from engine.risk import RiskManager
from persistence import SQLiteStore

try:  # pragma: no cover - optional metrics
    from engine.metrics import EngineMetrics
except Exception:  # pragma: no cover
    EngineMetrics = None  # type: ignore


class ExitEngine:
    """Rule-based exit planner that drives automated exits for open BUY positions."""

    def __init__(
        self,
        *,
        config: ExitConfig,
        risk: RiskManager,
        oms: OMS,
        store: SQLiteStore,
        tick_size: float,
        metrics: Optional["EngineMetrics"] = None,
    ) -> None:
        self.cfg = config
        self.risk = risk
        self.oms = oms
        self.store = store
        self._tick = max(float(tick_size or 0.0), 0.0)
        self._metrics = metrics
        self._logger = get_logger("ExitEngine")
        self._risk_halt_executed = False

    # ----------------------------------------------------------------- lifecycle
    def on_fill(self, *, symbol: str, side: str, qty: int, price: float, ts: dt.datetime) -> None:
        """Seed or clear exit plans when fills occur."""

        ts = self._parse_ts(ts)
        side_upper = side.upper()
        if side_upper == "BUY":
            if not self._exits_enabled():
                return
            stop_price = self._stop_price(float(price))
            plan = {
                "entry_price": float(price),
                "entry_ts": ts.isoformat(),
                "stop_price": stop_price,
                "target1_price": self._target_price(float(price)),
                "trailing_stop": None,
                "trail_anchor_price": None,
                "trailing_active": False,
                "partial_filled_qty": 0,
                "pending_exit_reason": None,
                "pending_exit_ts": None,
                "highest_price": float(price),
                "risk_per_unit": self._risk_per_unit(float(price), stop_price),
            }
            self.store.upsert_exit_plan(symbol, plan, ts=ts)
            return

        plan = self.store.load_exit_plan(symbol)
        position = self.store.load_open_position(symbol)
        open_qty = int(position["qty"]) if position else 0
        if not position or open_qty <= 0:
            self.store.delete_exit_plan(symbol)
            return
        if not plan:
            return
        plan["pending_exit_reason"] = None
        plan["pending_exit_ts"] = None
        self.store.upsert_exit_plan(symbol, plan, ts=ts)

    async def on_tick(self, instrument_key: str, ltp: float, ts: dt.datetime) -> None:
        """Evaluate exit rules on each tick for instruments with open positions."""

        if not self._exits_enabled() or self._risk_halt_executed:
            return
        plan = self.store.load_exit_plan(instrument_key)
        if not plan:
            return
        ts_dt = self._parse_ts(ts)
        position = self.store.load_open_position(instrument_key)
        if not position or int(position["qty"]) <= 0:
            self.store.delete_exit_plan(instrument_key)
            return
        qty = int(position["qty"])
        entry_price = float(plan.get("entry_price") or position.get("avg_price") or 0.0)
        if entry_price <= 0:
            return
        entry_ts = self._parse_ts(plan.get("entry_ts")) or position.get("opened_at") or ts_dt
        highest_price = float(plan.get("highest_price") or entry_price)
        highest_price = max(highest_price, float(ltp))
        plan["highest_price"] = highest_price
        stop_price = float(plan["stop_price"]) if plan.get("stop_price") else None
        target1_price = float(plan["target1_price"]) if plan.get("target1_price") else None
        trailing_stop = float(plan["trailing_stop"]) if plan.get("trailing_stop") else None
        trail_anchor = float(plan.get("trail_anchor_price") or entry_price)
        trailing_active = bool(plan.get("trailing_active", False))
        pending_reason = plan.get("pending_exit_reason")
        pending_ts = self._parse_ts(plan.get("pending_exit_ts"))
        if pending_reason and pending_ts:
            if (ts_dt - pending_ts).total_seconds() < 5.0:
                return
            plan["pending_exit_reason"] = None
            plan["pending_exit_ts"] = None

        reason: Optional[str] = None
        exit_qty = qty

        # 1) Hard stop
        if stop_price and ltp <= stop_price and self.cfg.stop_pct > 0:
            reason = "STOP"

        # 2) Target / partial
        elif target1_price and ltp >= target1_price and not trailing_active:
            partial_qty = self._partial_exit_qty(qty - int(plan.get("partial_filled_qty") or 0))
            trail_anchor = max(trail_anchor, ltp)
            plan["trailing_active"] = True
            plan["trail_anchor_price"] = trail_anchor
            plan["trailing_stop"] = self._compute_trailing_stop(entry_price, trail_anchor, stop_price, trailing_stop, highest_price)
            if partial_qty > 0:
                reason = "PARTIAL_TARGET"
                exit_qty = partial_qty
                plan["partial_filled_qty"] = plan.get("partial_filled_qty", 0) + partial_qty
            else:
                self.store.upsert_exit_plan(instrument_key, plan, ts=ts_dt)
                return

        # 3) Trailing stop
        elif trailing_active:
            trail_anchor = max(trail_anchor, ltp)
            plan["trail_anchor_price"] = trail_anchor
            plan["trailing_stop"] = self._compute_trailing_stop(entry_price, trail_anchor, stop_price, trailing_stop, highest_price)
            trailing_stop = float(plan["trailing_stop"]) if plan.get("trailing_stop") else None
            if trailing_stop and ltp <= trailing_stop:
                reason = "TRAIL"

        # 4) Time stop
        time_stop = self.cfg.time_stop_minutes or self.cfg.max_holding_minutes
        if not reason and time_stop > 0:
            age_minutes = (ts_dt - entry_ts).total_seconds() / 60.0
            if age_minutes >= time_stop:
                above_stop = stop_price is None or ltp > stop_price
                below_target = target1_price is None or ltp < target1_price
                if above_stop and below_target:
                    reason = "TIME"

        if not reason:
            self.store.upsert_exit_plan(instrument_key, plan, ts=ts_dt)
            return

        await self._submit_exit_order(instrument_key, exit_qty, ltp, ts_dt, reason)
        plan["pending_exit_reason"] = reason
        plan["pending_exit_ts"] = ts_dt.isoformat()
        self.store.upsert_exit_plan(instrument_key, plan, ts=ts_dt)

    async def handle_risk_halt(self, reason: Optional[str] = None) -> None:
        """Fire a flat-all exit once when the risk manager halts trading."""

        if self._risk_halt_executed:
            return
        self._risk_halt_executed = True
        reason_text = reason or self.risk.halt_reason() or "RISK_HALT"
        budgets = self.risk.square_off_all(reason_text)
        if not budgets:
            budgets = []
            for row in self.store.list_open_positions():
                qty = int(row.get("qty") or 0)
                if qty <= 0:
                    continue
                budgets.append(
                    {
                        "symbol": row["symbol"],
                        "qty": qty,
                        "price": float(row.get("avg_price") or 0.0),
                    }
                )
        now = dt.datetime.now(IST)
        for budget in budgets:
            try:
                symbol = budget.symbol if hasattr(budget, "symbol") else budget.get("symbol")  # type: ignore[union-attr]
                qty = budget.qty if hasattr(budget, "qty") else budget.get("qty")  # type: ignore[union-attr]
                price = budget.price if hasattr(budget, "price") else budget.get("price")  # type: ignore[union-attr]
                if not symbol or not qty:
                    continue
                await self.oms.submit(
                    strategy=self.risk.__class__.__name__.lower(),
                    symbol=symbol,
                    side="SELL",
                    qty=int(abs(qty)),
                    order_type="MARKET",
                    limit_price=price,
                    ts=now,
                )
            except Exception as exc:  # pragma: no cover - best effort
                self._logger.log_event(30, "risk_halt_exit_failed", symbol=getattr(budget, "symbol", None), error=str(exc))
        try:
            if self._metrics:
                self._metrics.exit_events_total.labels(reason="RISK_HALT").inc()
        except Exception:
            pass
        self.store.clear_exit_plans()

    def clear_all_plans(self) -> None:
        self.store.clear_exit_plans()

    # ----------------------------------------------------------------- helpers
    async def _submit_exit_order(self, instrument: str, qty: int, ltp: float, ts: dt.datetime, reason: str) -> None:
        qty = int(abs(qty))
        if qty <= 0:
            return
        try:
            await self.oms.submit(
                strategy="exit",
                symbol=instrument,
                side="SELL",
                qty=qty,
                order_type="MARKET",
                limit_price=ltp,
                ts=ts,
            )
            if self._metrics:
                self._metrics.exit_events_total.labels(reason=reason).inc()
            self._logger.log_event(20, "exit_submit", symbol=instrument, qty=qty, price=ltp, reason=reason)
        except Exception as exc:  # pragma: no cover - fail-safe
            self._logger.log_event(30, "exit_submit_failed", symbol=instrument, qty=qty, price=ltp, reason=reason, error=str(exc))

    def _stop_price(self, entry: float) -> Optional[float]:
        if self.cfg.stop_pct <= 0:
            return None
        stop = entry * (1 - self.cfg.stop_pct)
        return max(stop, 0.0)

    def _target_price(self, entry: float) -> Optional[float]:
        risk_unit = self._risk_per_unit(entry, self._stop_price(entry))
        if self.cfg.partial_target_multiplier > 0 and risk_unit > 0:
            target = entry + risk_unit * self.cfg.partial_target_multiplier
            return max(target, 0.0)
        if self.cfg.target1_pct <= 0:
            return None
        target = entry * (1 + self.cfg.target1_pct)
        return max(target, 0.0)

    def _partial_exit_qty(self, open_qty: int) -> int:
        fraction = max(min(float(self.cfg.partial_fraction), 1.0), 0.0)
        qty = int(open_qty * fraction)
        return qty if qty > 0 else (1 if open_qty > 0 and fraction > 0 else 0)

    def _compute_trailing_stop(
        self,
        entry_price: float,
        anchor: float,
        floor: Optional[float],
        existing: Optional[float],
        highest: float,
    ) -> Optional[float]:
        if self.cfg.trailing_stop_pct > 0:
            candidate = highest * (1 - self.cfg.trailing_stop_pct)
            if floor is not None:
                candidate = max(candidate, floor)
            if existing is not None:
                candidate = max(candidate, existing)
            return max(candidate, 0.0)
        profit = anchor - entry_price
        min_move = self.cfg.min_trail_ticks * self._tick if self._tick > 0 else float(self.cfg.min_trail_ticks or 0)
        if profit <= max(min_move, 0.0):
            return existing or floor
        lock_level = entry_price + profit * self.cfg.trail_lock_pct
        giveback_level = anchor - profit * self.cfg.trail_giveback_pct
        candidate = max(lock_level, giveback_level)
        if floor is not None:
            candidate = max(candidate, floor)
        if min_move > 0 and anchor - candidate < min_move:
            candidate = anchor - min_move
        return max(candidate, 0.0)

    def _parse_ts(self, value: Any) -> dt.datetime:
        if isinstance(value, dt.datetime):
            return value
        if isinstance(value, str):
            try:
                parsed = dt.datetime.fromisoformat(value)
            except ValueError:
                parsed = dt.datetime.now(IST)
            if parsed.tzinfo is None:
                parsed = parsed.replace(tzinfo=IST)
            return parsed
        return dt.datetime.now(IST)

    def _exits_enabled(self) -> bool:
        return any(
            [
                self.cfg.stop_pct > 0,
                self.cfg.target1_pct > 0,
                self.cfg.partial_target_multiplier > 0,
                (self.cfg.time_stop_minutes or 0) > 0 or self.cfg.max_holding_minutes > 0,
                self.cfg.trailing_stop_pct > 0,
            ]
        )

    def _risk_per_unit(self, entry: float, stop_price: Optional[float]) -> float:
        if stop_price and entry > stop_price:
            return entry - stop_price
        return entry * max(self.cfg.stop_pct, 0.0)


__all__ = ["ExitEngine"]
