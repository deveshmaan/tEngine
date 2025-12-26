from __future__ import annotations

import datetime as dt
from dataclasses import dataclass, field
from typing import Any, Dict, Optional

from engine.backtest.expiry_resolver import resolve_expiry
from engine.backtest.strategy_spec import BacktestRunSpec, LegRiskRuleSpec
from engine.backtest.strike_resolver import resolve_atm_strike, resolve_strike_offset, resolve_target_premium
from engine.config import IST
from strategy.base import BaseStrategy


STRATEGY_TAG_PREFIX = "optbt"


def _as_naive_time(value: dt.time) -> dt.time:
    return value.replace(tzinfo=None, second=0, microsecond=0)


def _strategy_tag(*parts: str) -> str:
    clean = [STRATEGY_TAG_PREFIX]
    for part in parts:
        text = str(part or "").replace("|", "/").strip()
        clean.append(text)
    return "|".join(clean)


def parse_strategy_tag(text: str) -> Dict[str, str]:
    """
    Parse `OMS.strategy` tags emitted by OptionsBacktestRunner.

    Format:
      optbt|{strategy_trade_id}|{leg_id}|{ENTRY|EXIT}|{reason?}
    """

    raw = str(text or "")
    parts = raw.split("|")
    if not parts or parts[0] != STRATEGY_TAG_PREFIX:
        return {}
    out: Dict[str, str] = {"raw": raw}
    if len(parts) >= 2:
        out["strategy_trade_id"] = parts[1]
    if len(parts) >= 3:
        out["leg_id"] = parts[2]
    if len(parts) >= 4:
        out["action"] = parts[3]
    if len(parts) >= 5:
        out["reason"] = parts[4]
    return out


@dataclass
class _LegRuntime:
    leg_id: str
    side: str
    opt_type: str
    qty_lots: int
    risk: Optional[LegRiskRuleSpec]
    reentries_done: int = 0
    cooldown_until: Optional[dt.datetime] = None
    last_exit_reason: Optional[str] = None
    last_entry_price: Optional[float] = None
    trail_best: Optional[float] = None
    profit_lock_triggered: bool = False
    active_trade_id: Optional[str] = None
    symbol: Optional[str] = None
    instrument_key: Optional[str] = None
    expiry: Optional[str] = None
    entry_order_id: Optional[str] = None
    exit_order_id: Optional[str] = None
    entry_started_at: Optional[dt.datetime] = None
    exit_started_at: Optional[dt.datetime] = None

    def reset_for_day(self) -> None:
        self.reentries_done = 0
        self.cooldown_until = None
        self.last_exit_reason = None
        self.last_entry_price = None
        self.trail_best = None
        self.profit_lock_triggered = False
        self.active_trade_id = None
        self.symbol = None
        self.instrument_key = None
        self.expiry = None
        self.entry_order_id = None
        self.exit_order_id = None
        self.entry_started_at = None
        self.exit_started_at = None


class OptionsBacktestRunner(BaseStrategy):
    """
    Options backtest runner driven by `BacktestRunSpec`.

    Responsibilities (StockMock-style, best-effort):
    - Enter legs at `entry_time`
    - Enforce per-leg SL/TP/trailing/profit-lock
    - Enforce strategy-level daily max loss/profit
    - Re-entry with cooldown + cap (per-leg)
    - Exit open positions at `exit_time` / `force_exit_time`
    - Label orders with a `strategy_trade_id` for grouping in results
    """

    def __init__(self, *args, **kwargs) -> None:  # type: ignore[no-untyped-def]
        super().__init__(*args, **kwargs)
        self.spec: Optional[BacktestRunSpec] = None
        self._day: Optional[dt.date] = None
        self._day_entered: bool = False
        self._day_trade_seq: int = 0
        self._legs: Dict[str, _LegRuntime] = {}
        self._order_index: Dict[str, Dict[str, str]] = {}
        self._day_start_realized: float = 0.0
        self._day_start_fees: float = 0.0
        self._halted_for_day: bool = False

    async def init(self, app: Any) -> None:
        await super().init(app)

        spec: Optional[BacktestRunSpec] = None
        raw = getattr(app, "strategy_spec", None)
        if isinstance(raw, BacktestRunSpec):
            spec = raw
        if spec is None:
            raw_json = getattr(app, "strategy_spec_json", None)
            if raw_json:
                spec = BacktestRunSpec.from_json(str(raw_json))
        if not isinstance(spec, BacktestRunSpec):
            raise RuntimeError("OptionsBacktestRunner requires BacktestingEngine.strategy_spec (BacktestRunSpec)")
        self.spec = spec

        # Initialize leg runtime state.
        risk_by_leg = {r.leg_id: r for r in (spec.risk.per_leg or ())}
        legs: Dict[str, _LegRuntime] = {}
        for leg in spec.legs:
            leg_id = str(leg.leg_id)
            legs[leg_id] = _LegRuntime(
                leg_id=leg_id,
                side=str(leg.side).upper(),
                opt_type=str(leg.option_type).upper(),
                qty_lots=int(leg.qty),
                risk=risk_by_leg.get(leg_id),
            )
        self._legs = legs

    async def on_tick(self, event: dict) -> None:
        if self.spec is None:
            return
        payload = event.get("payload") or {}
        symbol = str(payload.get("symbol") or "")

        # Drive from underlying ticks only (engine also emits option ticks).
        underlying_symbol = str(getattr(self._app, "_bt_underlying_symbol", "") or "").upper()
        if not underlying_symbol:
            underlying_symbol = str(self.cfg.data.index_symbol or "NIFTY").upper()
        if symbol and symbol.upper() != underlying_symbol.upper():
            return

        spot = self._extract_price(payload)
        if spot is None:
            return

        ts = self._event_ts(event.get("ts")).astimezone(IST)
        trade_date = ts.date()
        now_time = _as_naive_time(ts.time())

        if self._day is None or self._day != trade_date:
            self._start_new_day(trade_date)

        entry_time = _as_naive_time(self.spec.config.entry_time)
        exit_time = _as_naive_time(self.spec.config.exit_time)
        strat_risk = self.spec.risk.strategy
        force_exit_time = _as_naive_time(strat_risk.force_exit_time) if strat_risk and strat_risk.force_exit_time else exit_time
        disable_entries_after = (
            _as_naive_time(strat_risk.disable_entries_after_time)
            if strat_risk and strat_risk.disable_entries_after_time
            else exit_time
        )

        # Hard stop for the day after a strategy-level breach.
        if self._halted_for_day and now_time >= force_exit_time:
            await self._exit_all(ts, underlying_symbol, reason="FORCE_EXIT")
            return

        if now_time >= force_exit_time:
            await self._exit_all(ts, underlying_symbol, reason="TIME_EXIT")
            return

        # Enforce strategy-level daily limits.
        await self._enforce_strategy_limits(ts, underlying_symbol)
        if self._halted_for_day:
            await self._exit_all(ts, underlying_symbol, reason="RISK_HALT")
            return

        # Entry / re-entry is disabled after cutoff.
        if now_time >= disable_entries_after:
            return

        # Initial entry: enter all legs once per day at/after entry_time.
        if not self._day_entered and now_time >= entry_time and not self._has_any_open_positions():
            await self._enter_legs(ts, underlying_symbol, float(spot), reentry=False)
            self._day_entered = True
            return

        # During session: per-leg risk management + re-entry.
        await self._manage_open_legs(ts, underlying_symbol)
        await self._maybe_reenter(ts, underlying_symbol, float(spot))

    async def on_fill(self, fill: dict) -> None:
        if self.spec is None:
            return
        order_id = str(fill.get("order_id") or "")
        if not order_id:
            return
        meta = self._order_index.get(order_id)
        if not meta:
            return
        leg_id = meta.get("leg_id")
        if not leg_id or leg_id not in self._legs:
            return
        leg = self._legs[leg_id]
        try:
            ts = dt.datetime.fromisoformat(str(fill.get("ts") or "")).astimezone(IST)
        except Exception:
            ts = dt.datetime.now(IST)
        action = meta.get("action") or ""
        if action == "ENTRY":
            leg.entry_order_id = order_id
            leg.entry_started_at = leg.entry_started_at or ts
            # Update last entry price from store once a position is visible.
            if leg.symbol:
                pos = getattr(self._app, "store", None).load_open_position(leg.symbol) if getattr(self._app, "store", None) else None
                if pos and float(pos.get("avg_price") or 0.0) > 0:
                    leg.last_entry_price = float(pos["avg_price"])
        elif action == "EXIT":
            leg.exit_order_id = order_id
            leg.exit_started_at = leg.exit_started_at or ts
            # If the position is closed, arm cooldown for any future re-entry.
            if leg.symbol:
                pos = getattr(self._app, "store", None).load_open_position(leg.symbol) if getattr(self._app, "store", None) else None
                if not pos or int(pos.get("qty") or 0) == 0:
                    leg.last_exit_reason = meta.get("reason") or leg.last_exit_reason
                    leg.exit_order_id = None
                    leg.exit_started_at = None
                    leg.trail_best = None
                    leg.profit_lock_triggered = False

                    if leg.risk and leg.risk.reentry_enabled:
                        cooldown = int(leg.risk.cool_down_minutes or 0)
                        leg.cooldown_until = ts + dt.timedelta(minutes=max(cooldown, 0))

    # ----------------------------------------------------------------- internals
    def _start_new_day(self, trade_date: dt.date) -> None:
        self._day = trade_date
        self._day_entered = False
        self._day_trade_seq = 0
        self._halted_for_day = False
        for leg in self._legs.values():
            leg.reset_for_day()
        try:
            realized, _unrealized, fees = getattr(self._app, "pnl").totals()
            self._day_start_realized = float(realized or 0.0)
            self._day_start_fees = float(fees or 0.0)
        except Exception:
            self._day_start_realized = 0.0
            self._day_start_fees = 0.0

    def _has_any_open_positions(self) -> bool:
        store = getattr(self._app, "store", None)
        if store is None:
            return False
        try:
            return bool(store.list_open_positions())
        except Exception:
            return False

    async def _enter_legs(self, ts: dt.datetime, underlying_symbol: str, spot: float, *, reentry: bool) -> None:
        if self.spec is None:
            return
        strat_risk = self.spec.risk.strategy
        if strat_risk and strat_risk.max_trades_per_day is not None:
            if self._day_trade_seq >= int(strat_risk.max_trades_per_day):
                return

        self._day_trade_seq += 1
        trade_id = f"{ts.date().isoformat()}-{self._day_trade_seq:03d}"

        step = int(getattr(self._app, "_strike_step")(underlying_symbol))  # type: ignore[misc]
        atm = resolve_atm_strike(spot, step)
        expiry_weekday = getattr(self.cfg.data, "weekly_expiry_weekday", None)
        holidays = getattr(self.cfg.data, "holidays", None)

        for leg_spec in self.spec.legs:
            leg_id = str(leg_spec.leg_id)
            runtime = self._legs.get(leg_id)
            if runtime is None:
                continue
            await self._enter_leg(
                ts,
                underlying_symbol,
                trade_id=trade_id,
                leg_spec=leg_spec,
                runtime=runtime,
                atm=atm,
                step=step,
                expiry_weekday=expiry_weekday,
                holidays=holidays,
                reentry=reentry,
            )

    async def _enter_leg(
        self,
        ts: dt.datetime,
        underlying_symbol: str,
        *,
        trade_id: str,
        leg_spec: Any,
        runtime: _LegRuntime,
        atm: int,
        step: int,
        expiry_weekday: Optional[int],
        holidays: Any,
        reentry: bool,
    ) -> None:
        if runtime.exit_order_id:
            return
        if reentry:
            if not (runtime.risk and runtime.risk.reentry_enabled):
                return
            if runtime.reentries_done >= int(runtime.risk.max_reentries or 0):
                return

        expiry_date = (
            leg_spec.expiry_selector.expiry_date
            if leg_spec.expiry_selector.expiry_date is not None
            else resolve_expiry(
                ts.date(),
                leg_spec.expiry_selector.mode,
                weekly_expiry_weekday=expiry_weekday,
                holiday_calendar=holidays,
            )
        )
        strike = await self._resolve_strike(
            ts,
            underlying_symbol,
            atm=atm,
            step=step,
            expiry_date=expiry_date,
            leg_id=str(runtime.leg_id),
        )
        expiry = expiry_date.isoformat()
        symbol = f"{underlying_symbol.upper()}-{expiry}-{int(strike)}{runtime.opt_type}"
        qty_units = int(runtime.qty_lots) * int(getattr(self.cfg.data, "lot_step", 1) or 1)
        if qty_units <= 0:
            return

        runtime.active_trade_id = trade_id
        runtime.symbol = symbol
        runtime.expiry = expiry
        runtime.entry_order_id = None
        runtime.exit_order_id = None
        runtime.last_exit_reason = None
        runtime.entry_started_at = ts
        runtime.exit_started_at = None
        runtime.trail_best = None
        runtime.profit_lock_triggered = False

        try:
            instrument_key = getattr(self._app, "_resolve_option_instrument_key")(  # type: ignore[misc]
                underlying=underlying_symbol,
                expiry=expiry,
                strike=int(strike),
                opt_type=runtime.opt_type,
            )
        except Exception as exc:
            try:
                self._logger.warning("option_key_missing: %s (%s)", symbol, exc)
            except Exception:
                pass
            return
        runtime.instrument_key = str(instrument_key)

        # Prefetch option candles into the shared history cache so fills/MTM use real data.
        try:
            getattr(self._app, "_load_series")(  # type: ignore[misc]
                str(instrument_key),
                str(self.spec.config.interval),
                is_option=True,
                expiry=expiry,
            )
        except Exception as exc:
            try:
                self._logger.warning("option_candles_prefetch_failed: %s (%s)", symbol, exc)
            except Exception:
                pass

        strategy_label = _strategy_tag(trade_id, runtime.leg_id, "ENTRY", "REENTRY" if reentry else "DAY_ENTRY")
        try:
            order = await self.oms.submit(
                strategy=strategy_label,
                symbol=symbol,
                side=runtime.side,
                qty=qty_units,
                order_type="MARKET",
                ts=ts,
            )
            self._order_index[str(order.client_order_id)] = {
                "leg_id": runtime.leg_id,
                "action": "ENTRY",
                "strategy_trade_id": trade_id,
                "reason": "REENTRY" if reentry else "DAY_ENTRY",
            }
            runtime.entry_order_id = str(order.client_order_id)
            if reentry:
                runtime.reentries_done += 1
        except Exception as exc:
            try:
                self._logger.warning("entry_submit_failed: %s (%s)", symbol, exc)
            except Exception:
                pass

    async def _resolve_strike(
        self,
        entry_ts: dt.datetime,
        underlying_symbol: str,
        *,
        atm: int,
        step: int,
        expiry_date: dt.date,
        leg_id: str,
    ) -> int:
        assert self.spec is not None
        leg_spec = next((l for l in self.spec.legs if str(l.leg_id) == str(leg_id)), None)
        if leg_spec is None:
            raise RuntimeError("leg_not_found")
        mode = str(leg_spec.strike_selector.mode or "").upper()
        if mode == "ATM":
            return int(atm)
        if mode == "ATM_OFFSET":
            return int(resolve_strike_offset(atm, leg_spec.option_type, int(leg_spec.strike_selector.offset_points or 0)))
        if mode == "TARGET_PREMIUM":
            inst_master = getattr(self._app, "_instrument_master", None)
            if inst_master is None:
                raise RuntimeError("no_instrument_master")
            underlying_key = str(self.spec.config.underlying_instrument_key)
            grid_all = inst_master.strikes_for(underlying_key=underlying_key, expiry=expiry_date.isoformat(), opt_type=leg_spec.option_type)
            if not grid_all:
                raise RuntimeError("empty_strike_grid")
            window_steps = 20
            lo = int(atm - window_steps * step)
            hi = int(atm + window_steps * step)
            grid = [s for s in grid_all if lo <= int(s) <= hi]
            if not grid:
                grid = list(grid_all)

            def _price_fn(ts: dt.datetime, exp: dt.date, opt_type: str, strike: int) -> Optional[float]:
                return self._option_price_for_target(entry_ts=ts, expiry_date=exp, opt_type=opt_type, strike=strike, underlying=underlying_symbol)

            return int(
                resolve_target_premium(
                    entry_ts,
                    expiry_date,
                    leg_spec.option_type,
                    float(leg_spec.strike_selector.target_premium or 0.0),
                    grid,
                    _price_fn,
                )
            )
        raise ValueError(f"Unsupported strike_selector.mode: {leg_spec.strike_selector.mode!r}")

    def _option_price_for_target(
        self,
        *,
        entry_ts: dt.datetime,
        expiry_date: dt.date,
        opt_type: str,
        strike: int,
        underlying: str,
    ) -> Optional[float]:
        """
        Price helper for TARGET_PREMIUM strike selection.

        Uses candle open for `fill_model="same_tick"` and candle close for `fill_model="next_tick"`.
        """

        assert self.spec is not None
        app = self._app
        if app is None:
            return None
        expiry = expiry_date.isoformat()
        symbol = f"{underlying.upper()}-{expiry}-{int(strike)}{str(opt_type).upper()}"
        try:
            key = getattr(app, "_resolve_option_instrument_key")(underlying=underlying, expiry=expiry, strike=int(strike), opt_type=str(opt_type).upper())  # type: ignore[misc]
            series = getattr(app, "_load_series")(  # type: ignore[misc]
                key,
                getattr(app, "_bt_interval") or self.spec.config.interval,
                is_option=True,
                expiry=expiry,
            )
            bar = getattr(app, "_bar_for_ts")(series, entry_ts)  # type: ignore[misc]
        except Exception:
            return None
        if bar is None:
            return None
        fill_model = str(self.spec.execution_model.fill_model or "next_tick").lower()
        if fill_model == "same_tick":
            return float(getattr(bar, "open", 0.0) or 0.0)
        return float(getattr(bar, "close", 0.0) or 0.0)

    async def _manage_open_legs(self, ts: dt.datetime, underlying_symbol: str) -> None:
        store = getattr(self._app, "store", None)
        if store is None:
            return
        for leg_id, runtime in self._legs.items():
            if not runtime.symbol:
                continue
            pos = store.load_open_position(runtime.symbol)
            if not pos or int(pos.get("qty") or 0) == 0:
                continue
            if runtime.exit_order_id:
                continue
            entry_price = float(pos.get("avg_price") or 0.0)
            if entry_price <= 0:
                continue
            runtime.last_entry_price = runtime.last_entry_price or entry_price
            ltp = getattr(self._app, "_fill_price")(runtime.symbol, ts, underlying_symbol)  # type: ignore[misc]
            if ltp is None:
                continue
            qty = int(pos.get("qty") or 0)
            pnl_per_unit = (float(ltp) - entry_price) * (1.0 if qty > 0 else -1.0)
            profit_points = float(pnl_per_unit)
            loss_points = float(-pnl_per_unit)
            mtm = pnl_per_unit * abs(qty)

            reason = self._evaluate_leg_risk(runtime, entry_price, profit_points, loss_points, mtm)
            if reason:
                await self._exit_leg(ts, underlying_symbol, runtime, int(qty), reason=reason)

    def _evaluate_leg_risk(
        self,
        leg: _LegRuntime,
        entry_price: float,
        profit_points: float,
        loss_points: float,
        mtm: float,
    ) -> Optional[str]:
        risk = leg.risk
        if risk is None:
            return None

        # Stoploss
        if risk.stoploss_type and risk.stoploss_value is not None:
            stype = str(risk.stoploss_type).strip().upper()
            sval = float(risk.stoploss_value)
            if stype == "MTM":
                if float(mtm) <= -abs(float(sval)):
                    return "STOPLOSS"
            else:
                thresh = self._threshold_from_risk(stype, float(sval), entry_price=entry_price)
                if float(loss_points) >= float(thresh):
                    return "STOPLOSS"

        # Profit target
        if risk.profit_target_type and risk.profit_target_value is not None:
            ptype = str(risk.profit_target_type).strip().upper()
            pval = float(risk.profit_target_value)
            if ptype == "MTM":
                if float(mtm) >= abs(float(pval)):
                    return "PROFIT_TARGET"
            else:
                thresh = self._threshold_from_risk(ptype, float(pval), entry_price=entry_price)
                if float(profit_points) >= float(thresh):
                    return "PROFIT_TARGET"

        # Trailing (profit metric based)
        if risk.trailing_enabled and risk.trailing_type and risk.trailing_trigger is not None and risk.trailing_step is not None:
            ttype = str(risk.trailing_type).strip().upper()
            if ttype == "PREMIUM_PCT":
                metric = (profit_points / entry_price) if entry_price > 0 else 0.0
                trigger = float(risk.trailing_trigger)
                step = float(risk.trailing_step)
            elif ttype == "POINTS":
                metric = float(profit_points)
                trigger = float(risk.trailing_trigger)
                step = float(risk.trailing_step)
            elif ttype == "MTM":
                metric = float(mtm)
                trigger = float(risk.trailing_trigger)
                step = float(risk.trailing_step)
            else:
                metric = float(profit_points)
                trigger = float(risk.trailing_trigger)
                step = float(risk.trailing_step)
            if metric >= trigger:
                leg.trail_best = max(float(leg.trail_best or metric), float(metric))
                if leg.trail_best is not None and metric <= float(leg.trail_best) - float(step):
                    return "TRAILING"

        # Profit lock (legacy templates): trigger at profit_lock.trigger_pct then exit if profit <= lock_to_pct
        if risk.profit_lock is not None:
            profit_pct = (profit_points / entry_price) if entry_price > 0 else 0.0
            if profit_pct >= float(risk.profit_lock.trigger_pct):
                leg.profit_lock_triggered = True
            if leg.profit_lock_triggered and profit_pct <= float(risk.profit_lock.lock_to_pct):
                return "PROFIT_LOCK"

        return None

    def _threshold_from_risk(self, risk_type: str, value: float, *, entry_price: float) -> float:
        t = str(risk_type).strip().upper()
        if t in {"PREMIUM_PCT", "PCT", "%"}:
            return float(entry_price) * float(value)
        if t == "POINTS":
            return float(value)
        return float(value)

    async def _enforce_strategy_limits(self, ts: dt.datetime, underlying_symbol: str) -> None:
        if self.spec is None:
            return
        strat = self.spec.risk.strategy
        if strat is None:
            return
        max_loss = float(getattr(strat, "max_daily_loss_mtm", 0.0) or 0.0)
        max_profit = float(getattr(strat, "max_daily_profit_mtm", 0.0) or 0.0)
        if max_loss <= 0 and max_profit <= 0:
            return

        store = getattr(self._app, "store", None)
        if store is None:
            return

        open_positions = []
        try:
            open_positions = store.list_open_positions()
        except Exception:
            open_positions = []

        open_pnl = 0.0
        for pos in open_positions:
            sym = str(pos.get("symbol") or "")
            qty = int(pos.get("qty") or 0)
            avg = float(pos.get("avg_price") or 0.0)
            if not sym or qty == 0 or avg <= 0:
                continue
            ltp = getattr(self._app, "_fill_price")(sym, ts, underlying_symbol)  # type: ignore[misc]
            if ltp is None:
                continue
            open_pnl += (float(ltp) - avg) * qty

        try:
            realized, _unrealized, fees = getattr(self._app, "pnl").totals()
            realized_delta = float(realized or 0.0) - float(self._day_start_realized)
            fees_delta = float(fees or 0.0) - float(self._day_start_fees)
        except Exception:
            realized_delta = 0.0
            fees_delta = 0.0

        daily_mtm = realized_delta + open_pnl - fees_delta
        if max_loss > 0 and daily_mtm <= -abs(max_loss):
            self._halted_for_day = True
        if max_profit > 0 and daily_mtm >= abs(max_profit):
            self._halted_for_day = True

    async def _exit_leg(self, ts: dt.datetime, underlying_symbol: str, leg: _LegRuntime, qty: int, *, reason: str) -> None:
        if qty == 0 or leg.exit_order_id or not leg.symbol:
            return
        side = "SELL" if qty > 0 else "BUY"
        close_qty = abs(int(qty))
        if close_qty <= 0:
            return
        trade_id = leg.active_trade_id or f"{ts.date().isoformat()}-{self._day_trade_seq:03d}"
        strategy_label = _strategy_tag(trade_id, leg.leg_id, "EXIT", str(reason))
        try:
            order = await self.oms.submit(
                strategy=strategy_label,
                symbol=leg.symbol,
                side=side,
                qty=close_qty,
                order_type="MARKET",
                ts=ts,
            )
            self._order_index[str(order.client_order_id)] = {
                "leg_id": leg.leg_id,
                "action": "EXIT",
                "strategy_trade_id": trade_id,
                "reason": str(reason),
            }
            leg.exit_order_id = str(order.client_order_id)
            leg.exit_started_at = ts
            leg.last_exit_reason = str(reason)
        except Exception as exc:
            try:
                self._logger.warning("exit_submit_failed: %s (%s)", leg.symbol, exc)
            except Exception:
                pass

    async def _exit_all(self, ts: dt.datetime, underlying_symbol: str, *, reason: str) -> None:
        store = getattr(self._app, "store", None)
        if store is None:
            return
        try:
            positions = store.list_open_positions()
        except Exception:
            positions = []
        for pos in positions:
            sym = str(pos.get("symbol") or "")
            qty = int(pos.get("qty") or 0)
            if not sym or qty == 0:
                continue
            leg_id = None
            for cand in self._legs.values():
                if cand.symbol == sym:
                    leg_id = cand.leg_id
                    runtime = cand
                    break
            else:
                runtime = None
            if runtime is None:
                continue
            await self._exit_leg(ts, underlying_symbol, runtime, qty, reason=reason)

    async def _maybe_reenter(self, ts: dt.datetime, underlying_symbol: str, spot: float) -> None:
        if self.spec is None:
            return
        entry_time = _as_naive_time(self.spec.config.entry_time)
        now_time = _as_naive_time(ts.time())
        if now_time < entry_time:
            return
        strat_risk = self.spec.risk.strategy
        if strat_risk and strat_risk.max_trades_per_day is not None:
            if self._day_trade_seq >= int(strat_risk.max_trades_per_day):
                return

        step = int(getattr(self._app, "_strike_step")(underlying_symbol))  # type: ignore[misc]
        atm = resolve_atm_strike(float(spot), step)
        expiry_weekday = getattr(self.cfg.data, "weekly_expiry_weekday", None)
        holidays = getattr(self.cfg.data, "holidays", None)

        for leg_id, runtime in self._legs.items():
            risk = runtime.risk
            if risk is None or not risk.reentry_enabled:
                continue
            if runtime.reentries_done >= int(risk.max_reentries or 0):
                continue
            if runtime.exit_order_id:
                continue
            if runtime.symbol:
                # still open
                store = getattr(self._app, "store", None)
                if store and store.load_open_position(runtime.symbol):
                    continue
            if runtime.cooldown_until and ts < runtime.cooldown_until:
                continue
            cond = str(risk.reentry_condition or "premium_returns_to_entry_zone").strip().lower()
            if cond == "after_stoploss" and runtime.last_exit_reason != "STOPLOSS":
                continue
            if cond == "after_profit_target" and runtime.last_exit_reason != "PROFIT_TARGET":
                continue
            if cond == "premium_returns_to_entry_zone":
                if runtime.last_entry_price and runtime.symbol:
                    ltp = getattr(self._app, "_fill_price")(runtime.symbol, ts, underlying_symbol)  # type: ignore[misc]
                    if ltp is None:
                        continue
                    ref = float(runtime.last_entry_price)
                    if ref > 0:
                        band = 0.05  # +/-5% band; deterministic default
                        if abs(float(ltp) - ref) / ref > band:
                            continue
            leg_spec = next((l for l in self.spec.legs if str(l.leg_id) == str(leg_id)), None)
            if leg_spec is None:
                continue
            self._day_trade_seq += 1
            trade_id = f"{ts.date().isoformat()}-{self._day_trade_seq:03d}"
            await self._enter_leg(
                ts,
                underlying_symbol,
                trade_id=trade_id,
                leg_spec=leg_spec,
                runtime=runtime,
                atm=atm,
                step=step,
                expiry_weekday=expiry_weekday,
                holidays=holidays,
                reentry=True,
            )
            return
