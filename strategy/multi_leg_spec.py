from __future__ import annotations

import datetime as dt
from dataclasses import dataclass
from typing import Any, Dict, Optional

from engine.backtest.expiry_resolver import resolve_expiry
from engine.backtest.strategy_spec import LegSpec, StrategySpec
from engine.backtest.strike_resolver import resolve_atm_strike, resolve_strike_offset, resolve_target_premium
from engine.config import IST
from strategy.base import BaseStrategy


@dataclass
class _DayState:
    date: dt.date
    entered: bool = False
    exited: bool = False
    entry_notes: Optional[str] = None


class MultiLegSpecStrategy(BaseStrategy):
    """
    Backtest-first multi-leg strategy driven by a `StrategySpec`.

    Current behaviour (intentionally minimal, StockMock-like):
    - Enters all legs once per trading day at/after `entry_time`
    - Exits all open positions at/after `exit_time`

    Not implemented yet:
    - per-leg stoploss/profit-target/trailing/re-entry automation (values are stored in `StrategySpec` but not enforced)
    """

    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self.spec: Optional[StrategySpec] = None
        self._day: Optional[_DayState] = None

    async def init(self, app: Any) -> None:
        await super().init(app)
        spec = getattr(app, "strategy_spec", None)
        if spec is None:
            raw = getattr(app, "strategy_spec_json", None)
            if raw:
                spec = StrategySpec.from_json(str(raw))
        if not isinstance(spec, StrategySpec):
            raise RuntimeError("MultiLegSpecStrategy requires BacktestingEngine.strategy_spec (StrategySpec)")
        self.spec = spec

    async def on_tick(self, event: dict) -> None:
        if self.spec is None:
            return
        payload = event.get("payload") or {}
        instrument_key = str(payload.get("instrument_key") or "")
        if instrument_key and instrument_key != self.spec.underlying_instrument_key:
            return
        spot = self._extract_price(payload)
        if spot is None:
            return

        ts = self._event_ts(event.get("ts")).astimezone(IST)
        trade_date = ts.date()
        now_time = ts.time().replace(tzinfo=None)

        if self._day is None or self._day.date != trade_date:
            self._day = _DayState(date=trade_date)

        if not self._day.entered and now_time >= self.spec.entry_time and now_time < self.spec.exit_time:
            await self._enter(ts, float(spot))
            self._day.entered = True

        if self._day.entered and not self._day.exited and now_time >= self.spec.exit_time:
            await self._exit(ts)
            self._day.exited = True

    async def on_fill(self, fill: dict) -> None:
        return

    async def _enter(self, ts: dt.datetime, spot: float) -> None:
        assert self.spec is not None
        app = self._app
        if app is None:
            return

        underlying_symbol = str(getattr(app, "_bt_underlying_symbol", "") or "").upper()
        if not underlying_symbol:
            underlying_symbol = str(payload_symbol_from_key(self.spec.underlying_instrument_key) or "NIFTY")

        try:
            step = int(getattr(app, "_strike_step")(underlying_symbol))  # type: ignore[misc]
        except Exception:
            step = int(getattr(self.cfg.data, "lot_step", 50) or 50)
        atm = resolve_atm_strike(spot, step)

        expiry_weekday = getattr(self.cfg.data, "weekly_expiry_weekday", None)
        holidays = getattr(self.cfg.data, "holidays", None)

        for idx, leg in enumerate(self.spec.legs):
            expiry_date = resolve_expiry(ts.date(), leg.expiry_mode, weekly_expiry_weekday=expiry_weekday, holiday_calendar=holidays)
            expiry = expiry_date.isoformat()
            strike = await self._resolve_strike(ts, underlying_symbol, atm=atm, step=step, expiry_date=expiry_date, leg=leg)
            symbol = f"{underlying_symbol}-{expiry}-{int(strike)}{leg.opt_type}"
            qty = int(leg.qty_lots) * int(getattr(self.cfg.data, "lot_step", 1) or 1)
            if qty <= 0:
                continue
            try:
                await self.oms.submit(
                    strategy=f"{self.spec.name}:{idx+1}",
                    symbol=symbol,
                    side=leg.side,
                    qty=qty,
                    order_type="MARKET",
                    ts=ts,
                )
            except Exception as exc:
                try:
                    self._logger.warning("spec_enter_failed: %s (%s)", symbol, exc)
                except Exception:
                    pass

    async def _exit(self, ts: dt.datetime) -> None:
        app = self._app
        if app is None:
            return
        store = getattr(app, "store", None)
        if store is None:
            return
        try:
            positions = store.list_open_positions()
        except Exception:
            positions = []
        for pos in positions:
            symbol = str(pos.get("symbol") or "")
            qty = int(pos.get("qty") or 0)
            if not symbol or qty == 0:
                continue
            side = "SELL" if qty > 0 else "BUY"
            close_qty = abs(qty)
            try:
                await self.oms.submit(
                    strategy="spec_exit",
                    symbol=symbol,
                    side=side,
                    qty=close_qty,
                    order_type="MARKET",
                    ts=ts,
                )
            except Exception as exc:
                try:
                    self._logger.warning("spec_exit_failed: %s (%s)", symbol, exc)
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
        leg: LegSpec,
    ) -> int:
        mode = str(leg.strike_mode or "").upper()
        if mode == "ATM":
            return int(atm)
        if mode == "ATM_OFFSET":
            return int(resolve_strike_offset(atm, leg.opt_type, int(leg.strike_offset_points or 0)))
        if mode == "TARGET_PREMIUM":
            app = self._app
            if app is None:
                raise RuntimeError("no_app")
            inst_master = getattr(app, "_instrument_master", None)
            if inst_master is None:
                raise RuntimeError("no_instrument_master")
            underlying_key = str(self.spec.underlying_instrument_key)
            grid_all = inst_master.strikes_for(underlying_key=underlying_key, expiry=expiry_date.isoformat(), opt_type=leg.opt_type)
            if not grid_all:
                raise RuntimeError("empty_strike_grid")
            # Keep target-premium scanning deterministic + bounded.
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
                    leg.opt_type,
                    float(leg.target_premium or 0.0),
                    grid,
                    _price_fn,
                )
            )
        raise ValueError(f"Unsupported strike_mode: {leg.strike_mode!r}")

    def _option_price_for_target(self, *, entry_ts: dt.datetime, expiry_date: dt.date, opt_type: str, strike: int, underlying: str) -> Optional[float]:
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
                getattr(app, "_bt_interval") or self.spec.candle_interval,
                is_option=True,
                expiry=expiry,
            )
            bar = getattr(app, "_bar_for_ts")(series, entry_ts)  # type: ignore[misc]
        except Exception:
            return None
        if bar is None:
            return None
        if str(self.spec.fill_model).lower() == "next_tick":
            return float(bar.close)
        return float(bar.open)


def payload_symbol_from_key(underlying_instrument_key: str) -> Optional[str]:
    key = str(underlying_instrument_key or "")
    from brokerage.upstox_client import INDEX_INSTRUMENT_KEYS

    for sym, ik in INDEX_INSTRUMENT_KEYS.items():
        if ik == key:
            return sym
    return None


__all__ = ["MultiLegSpecStrategy"]
