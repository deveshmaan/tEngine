from __future__ import annotations

import datetime as dt
import math
import time
from collections import deque
from typing import Callable, Optional

from engine.config import EngineConfig
from engine.data import pick_strike_from_spot, pick_subscription_expiry, resolve_next_expiry, resolve_weekly_expiry
from engine.events import EventBus, OrderSignal
from engine.exit import ExitEngine
from engine.metrics import EngineMetrics
from engine.oms import OMS, OrderValidationError
from engine.risk import OrderBudget, RiskManager
from market.instrument_cache import InstrumentCache
from strategy.base import BaseStrategy

_OPTION_MAX_AGE_SECONDS = 5.0


class _AtrTracker:
    def __init__(self, period: int) -> None:
        self.period = max(int(period), 1)
        self._prev_close: Optional[float] = None
        self._atr: Optional[float] = None
        self._seed: deque[float] = deque(maxlen=self.period)

    def update(self, high: float, low: float, close: float) -> None:
        if high is None or low is None or close is None:
            return
        prev_close = self._prev_close if self._prev_close is not None else close
        tr = max(high - low, abs(high - prev_close), abs(low - prev_close))
        if self._atr is None:
            self._seed.append(tr)
            if len(self._seed) == self.period:
                self._atr = sum(self._seed) / float(self.period)
        else:
            self._atr = ((self._atr * (self.period - 1)) + tr) / float(self.period)
        self._prev_close = close

    def ready(self) -> bool:
        return self._atr is not None

    @property
    def value(self) -> Optional[float]:
        return self._atr


def _atr_baseline(atr_value: float, opening_range_minutes: int) -> float:
    return atr_value * math.sqrt(opening_range_minutes)


def _orb_width_bounds(atr_value: float, opening_range_minutes: int, min_mult: float, max_mult: float) -> tuple[float, float]:
    baseline = _atr_baseline(atr_value, opening_range_minutes)
    return min_mult * baseline, max_mult * baseline


def _dynamic_breakout_buffer(fixed: float, atr_value: Optional[float], atr_mult: float, use_atr_filter: bool) -> float:
    if use_atr_filter and atr_value is not None:
        return max(fixed, atr_value * atr_mult)
    return fixed


class OpeningRangeBreakoutStrategy(BaseStrategy):
    """Opening Range Breakout strategy for the configured index."""

    def __init__(
        self,
        config: EngineConfig,
        risk: RiskManager,
        oms: OMS,
        bus: EventBus,
        exit_engine: ExitEngine,
        instrument_cache: InstrumentCache,
        metrics: EngineMetrics,
        subscription_expiry_provider: Optional[Callable[[str], str]] = None,
    ) -> None:
        super().__init__(config, risk, oms, bus, exit_engine, instrument_cache, metrics, subscription_expiry_provider)
        try:
            self.opening_range_minutes = max(int(getattr(config.strategy, "opening_range_minutes", 15)), 1)
        except (TypeError, ValueError):
            self.opening_range_minutes = 15
        try:
            self.volume_surge_ratio = max(float(getattr(config.strategy, "volume_surge_ratio", 1.5)), 0.0)
        except (TypeError, ValueError):
            self.volume_surge_ratio = 1.5
        self.use_vwap_filter = bool(getattr(config.strategy, "vwap_confirmation", True))
        try:
            self.orb_atr_period = max(int(getattr(config.strategy, "orb_atr_period", 14)), 1)
        except (TypeError, ValueError):
            self.orb_atr_period = 14
        try:
            self.min_or_atr_mult = max(float(getattr(config.strategy, "min_or_atr_mult", 0.55)), 0.0)
        except (TypeError, ValueError):
            self.min_or_atr_mult = 0.55
        try:
            self.max_or_atr_mult = max(float(getattr(config.strategy, "max_or_atr_mult", 2.5)), 0.0)
        except (TypeError, ValueError):
            self.max_or_atr_mult = 2.5
        try:
            self.buffer_atr_mult = max(float(getattr(config.strategy, "buffer_atr_mult", 0.35)), 0.0)
        except (TypeError, ValueError):
            self.buffer_atr_mult = 0.35
        try:
            self.breakout_buffer_pts = max(float(getattr(config.strategy, "breakout_buffer_pts", 5.0)), 0.0)
        except (TypeError, ValueError):
            self.breakout_buffer_pts = 5.0
        self.use_atr_filter = bool(getattr(config.strategy, "use_atr_filter", True))

        self._orb_high: Optional[float] = None
        self._orb_low: Optional[float] = None
        self._orb_start_time: Optional[dt.datetime] = None
        self._orb_end_time: Optional[dt.datetime] = None
        self._orb_established = False
        self._session_date: Optional[dt.date] = None
        self._orb_tradeable = True
        self._above_range_triggered = False
        self._below_range_triggered = False

        self._cum_volume = 0.0
        self._cum_price_volume = 0.0
        self._last_underlying_volume: Optional[float] = None
        self._current_minute_volume = 0.0
        self._current_minute_start: Optional[dt.datetime] = None
        self._orb_volume = 0.0
        self._atr = _AtrTracker(self.orb_atr_period)
        self._atr_bar_bucket: Optional[dt.datetime] = None
        self._atr_bar: Optional[dict[str, float]] = None
        self._atr_uses_bars = False

        self._last_option_prices: dict[str, float] = {}
        self._last_option_ts: dict[str, dt.datetime] = {}

    def _reset_session(self, ts: dt.datetime) -> None:
        """Reset ORB state for a new trading date."""

        self._session_date = ts.date()
        self._orb_high = None
        self._orb_low = None
        self._orb_start_time = None
        self._orb_end_time = None
        self._orb_established = False
        self._orb_tradeable = True
        self._above_range_triggered = False
        self._below_range_triggered = False
        self._cum_volume = 0.0
        self._cum_price_volume = 0.0
        self._last_underlying_volume = None
        self._current_minute_volume = 0.0
        self._current_minute_start = None
        self._orb_volume = 0.0
        self._atr = _AtrTracker(self.orb_atr_period)
        self._atr_bar_bucket = None
        self._atr_bar = None
        self._atr_uses_bars = False
        self._last_option_prices.clear()
        self._last_option_ts.clear()

    async def on_tick(self, event: dict) -> None:
        evt_type = event.get("type")
        if evt_type not in {"tick", "quote", "bar"}:
            return
        payload = event.get("payload") or {}
        ts = self._event_ts(event.get("ts"))
        price = self._extract_price(payload)
        if price is None:
            return
        self._record_eval_metrics()

        symbol_hint = payload.get("symbol") or payload.get("underlying") or ""
        opt_type = str(payload.get("opt_type") or "").upper()
        index_symbol = self.cfg.data.index_symbol.upper()
        if self._is_option_tick(symbol_hint, opt_type, index_symbol):
            option_symbol = symbol_hint or payload.get("instrument") or payload.get("instrument_key") or ""
            self._record_option_price(option_symbol, price, ts)
            await self._handle_option_tick(option_symbol, price, ts, payload)
            return

        underlying = (payload.get("symbol") or payload.get("underlying") or index_symbol).upper()
        if underlying != index_symbol:
            return
        await self._handle_underlying_tick(underlying, price, ts, payload, evt_type)

    async def on_fill(self, fill: dict) -> None:
        return

    async def _handle_underlying_tick(self, symbol: str, price: float, ts: dt.datetime, payload: dict, evt_type: str) -> None:
        if ts.date() != self._session_date:
            self._reset_session(ts)
        if self._orb_start_time is None:
            market_open = ts.replace(hour=9, minute=15, second=0, microsecond=0)
            start_time = market_open if ts < market_open else ts.replace(second=0, microsecond=0)
            self._orb_start_time = start_time
            self._orb_end_time = start_time + dt.timedelta(minutes=self.opening_range_minutes)
            self._logger.log_event(
                20,
                "orb_start",
                start=self._orb_start_time.isoformat(),
                end=self._orb_end_time.isoformat(),
            )
            if start_time > market_open:
                self._logger.log_event(20, "orb_late_bootstrap", start=self._orb_start_time.isoformat())

        if self._orb_start_time and ts < self._orb_start_time:
            return

        self._update_volume_state(price, ts, payload)
        self._update_atr_state(price, ts, payload, evt_type)

        if not self._orb_established:
            if self._orb_end_time and ts < self._orb_end_time:
                if self._orb_high is None or price > self._orb_high:
                    self._orb_high = price
                if self._orb_low is None or price < self._orb_low:
                    self._orb_low = price
                return
            if self._orb_end_time and ts >= self._orb_end_time:
                if self._orb_high is None or self._orb_low is None:
                    self._orb_high = price
                    self._orb_low = price
                self._orb_established = True
                self._orb_volume = self._cum_volume
                self._logger.log_event(
                    20,
                    "orb_established",
                    high=self._orb_high,
                    low=self._orb_low,
                    ts=ts.isoformat(),
                )
                self._orb_tradeable = True
                if self.use_atr_filter and self._orb_high is not None and self._orb_low is not None:
                    atr_val = self._atr.value if self._atr.ready() else None
                    if atr_val is None:
                        self._logger.log_event(20, "orb_atr_not_ready")
                    else:
                        or_width = self._orb_high - self._orb_low
                        min_ok, max_ok = _orb_width_bounds(
                            atr_val,
                            self.opening_range_minutes,
                            self.min_or_atr_mult,
                            self.max_or_atr_mult,
                        )
                        baseline = _atr_baseline(atr_val, self.opening_range_minutes)
                        if or_width < min_ok or or_width > max_ok:
                            self._orb_tradeable = False
                            self._logger.log_event(
                                20,
                                "orb_orwidth_atr_block",
                                or_width=or_width,
                                atr=atr_val,
                                baseline=baseline,
                                min_ok=min_ok,
                                max_ok=max_ok,
                            )

        if not self._orb_established or self._orb_high is None or self._orb_low is None:
            return

        if not self._orb_tradeable:
            return

        has_open = self.risk.has_open_positions()
        atr_val = self._atr.value if self._atr.ready() else None
        dyn_buffer = _dynamic_breakout_buffer(self.breakout_buffer_pts, atr_val, self.buffer_atr_mult, self.use_atr_filter)
        breakout_up = price > (self._orb_high + dyn_buffer)
        breakout_down = price < (self._orb_low - dyn_buffer)

        if breakout_up and not self._above_range_triggered and not has_open:
            if not self._volume_ok(direction="up"):
                breakout_up = False
            if breakout_up and not self._vwap_ok(price, direction="up"):
                breakout_up = False
            if breakout_up:
                placed = await self._place_order(symbol, price, ts, opt_type="CE")
                if placed:
                    self._above_range_triggered = True

        if breakout_down and not self._below_range_triggered and not has_open:
            if not self._volume_ok(direction="down"):
                breakout_down = False
            if breakout_down and not self._vwap_ok(price, direction="down"):
                breakout_down = False
            if breakout_down:
                placed = await self._place_order(symbol, price, ts, opt_type="PE")
                if placed:
                    self._below_range_triggered = True

        if self._above_range_triggered and price <= self._orb_high:
            self._above_range_triggered = False
        if self._below_range_triggered and price >= self._orb_low:
            self._below_range_triggered = False

    def _record_eval_metrics(self) -> None:
        try:
            self.metrics.strategy_last_eval_ts.set(time.time())
            self.metrics.strategy_evals_total.inc()
        except Exception:
            pass

    def _update_atr_state(self, price: float, ts: dt.datetime, payload: dict, evt_type: str) -> None:
        if evt_type == "bar":
            raw_high = payload.get("high")
            raw_low = payload.get("low")
            raw_close = payload.get("close")
            if raw_high is not None and raw_low is not None and raw_close is not None:
                try:
                    high = float(raw_high)
                    low = float(raw_low)
                    close = float(raw_close)
                except (TypeError, ValueError):
                    high = low = close = None
                if high is not None and low is not None and close is not None:
                    self._atr.update(high, low, close)
                    self._atr_uses_bars = True
                    self._atr_bar_bucket = None
                    self._atr_bar = None
                    return
        if self._atr_uses_bars:
            return
        bucket = ts.replace(second=0, microsecond=0)
        if self._atr_bar_bucket is None:
            self._atr_bar_bucket = bucket
            self._atr_bar = {"open": price, "high": price, "low": price, "close": price}
            return
        if bucket == self._atr_bar_bucket:
            bar = self._atr_bar
            if bar is None:
                bar = {"open": price, "high": price, "low": price, "close": price}
                self._atr_bar = bar
            bar["close"] = price
            if price > bar["high"]:
                bar["high"] = price
            if price < bar["low"]:
                bar["low"] = price
            return
        bar = self._atr_bar
        if bar is not None:
            self._atr.update(bar["high"], bar["low"], bar["close"])
        self._atr_bar_bucket = bucket
        self._atr_bar = {"open": price, "high": price, "low": price, "close": price}

    def _is_option_tick(self, symbol_hint: str, opt_type: str, index_symbol: str) -> bool:
        if opt_type in {"CE", "PE"}:
            return True
        symbol_upper = str(symbol_hint).upper()
        if not symbol_upper or symbol_upper == index_symbol:
            return False
        if "-" not in symbol_upper:
            return False
        tail = symbol_upper.split("-")[-1]
        return tail.endswith(("CE", "PE"))

    def _update_volume_state(self, price: float, ts: dt.datetime, payload: dict) -> None:
        raw_vol = payload.get("volume")
        if raw_vol is None:
            raw_vol = payload.get("vol_traded_today") or payload.get("volume_traded")
        if raw_vol is None:
            return
        try:
            vol = float(raw_vol)
        except (TypeError, ValueError):
            return
        bucket = ts.replace(second=0, microsecond=0)
        if self._current_minute_start is None or bucket != self._current_minute_start:
            self._current_minute_start = bucket
            self._current_minute_volume = 0.0

        vol_diff = vol
        if self._last_underlying_volume is not None:
            vol_diff = vol - self._last_underlying_volume
            if vol_diff < 0:
                vol_diff = vol
        self._last_underlying_volume = vol
        if vol_diff <= 0:
            return
        self._cum_volume += vol_diff
        self._cum_price_volume += vol_diff * price
        self._current_minute_volume += vol_diff

    def _volume_ok(self, *, direction: str) -> bool:
        if self.volume_surge_ratio <= 0:
            return True
        if self._orb_volume <= 0 or self._last_underlying_volume is None:
            return True
        avg_or_vol = self._orb_volume / float(self.opening_range_minutes)
        if avg_or_vol <= 0 or self._current_minute_volume <= 0:
            return True
        threshold = avg_or_vol * self.volume_surge_ratio
        if self._current_minute_volume < threshold:
            self._logger.log_event(
                20,
                "orb_volume_block",
                direction=direction,
                current=self._current_minute_volume,
                threshold=threshold,
            )
            return False
        return True

    def _vwap_ok(self, price: float, *, direction: str) -> bool:
        if not self.use_vwap_filter or self._cum_volume <= 0:
            return True
        vwap = self._cum_price_volume / self._cum_volume
        if direction == "up" and price < vwap:
            self._logger.log_event(20, "orb_vwap_block", direction=direction, price=price, vwap=vwap)
            return False
        if direction == "down" and price > vwap:
            self._logger.log_event(20, "orb_vwap_block", direction=direction, price=price, vwap=vwap)
            return False
        return True

    async def _handle_option_tick(self, symbol: str, price: float, ts: dt.datetime, payload: Optional[dict] = None) -> None:
        if symbol:
            try:
                self.risk.on_tick(symbol, price)
            except Exception:
                pass
        try:
            oi_val = (payload or {}).get("oi")
            iv_val = (payload or {}).get("iv")
            await self.exit_engine.on_tick(symbol, price, ts, oi=oi_val, iv=iv_val)
        except Exception:
            self._logger.log_event(30, "exit_tick_failed", symbol=symbol)

    async def _place_order(self, underlying: str, spot_price: float, ts: dt.datetime, opt_type: str) -> bool:
        if self.risk.should_halt():
            await self.exit_engine.handle_risk_halt()
            return False
        threshold = getattr(self.cfg.market_data, "max_tick_age_seconds", 0.0)
        if self.risk.block_if_stale(underlying, threshold=threshold):
            return False
        if not self._static_ip_ok():
            self._logger.log_event(30, "static_ip_gate_block", symbol=underlying)
            return False
        if not self._broker_live():
            self._logger.log_event(30, "broker_stream_block", symbol=underlying)
            return False

        expiry_str = self._resolve_expiry(ts, underlying)
        strike = pick_strike_from_spot(spot_price, step=self._strike_step(underlying))
        symbol = f"{underlying}-{expiry_str}-{int(strike)}{opt_type}"
        premium = self._option_price_for(symbol, ts, threshold or _OPTION_MAX_AGE_SECONDS)
        if premium is None:
            self._logger.log_event(20, "premium_missing_block", symbol=symbol)
            return False
        lot_size = self._resolve_lot_size(expiry_str, underlying)
        qty = self.risk.position_size(premium=premium, lot_size=lot_size)
        try:
            if self.metrics and hasattr(self.metrics, "strategy_position_size"):
                self.metrics.strategy_position_size.labels(instrument=symbol).set(qty)
        except Exception:
            pass
        if qty <= 0:
            return False
        budget = OrderBudget(symbol=symbol, qty=qty, price=premium, lot_size=lot_size, side="BUY")
        self.risk.on_tick(symbol, premium)
        if not self.risk.budget_ok_for(budget):
            return False

        await self._publish_signal(symbol, qty, premium, ts)
        try:
            await self.oms.submit(
                strategy=self.cfg.strategy_tag,
                symbol=symbol,
                side="BUY",
                qty=budget.qty,
                order_type="MARKET",
                limit_price=budget.price,
                ts=ts,
            )
        except OrderValidationError as exc:
            self._logger.log_event(30, "order_validation_failed", symbol=symbol, code=exc.code, message=str(exc))
            return False
        except Exception as exc:
            self._logger.log_event(40, "order_submit_failed", symbol=symbol, error=str(exc))
            return False
        self._logger.log_event(20, "submitted", symbol=symbol, price=budget.price, qty=budget.qty, opt=opt_type)
        return True

    def _resolve_expiry(self, ts: dt.datetime, symbol: str) -> str:
        preference = getattr(self.cfg.data, "subscription_expiry_preference", "current")
        target_symbol = symbol.upper()
        if target_symbol == "BANKNIFTY":
            try:
                return resolve_next_expiry(target_symbol, ts, kind="monthly", weekly_weekday=self.cfg.data.weekly_expiry_weekday)
            except Exception:
                fallback = resolve_weekly_expiry(
                    target_symbol,
                    ts,
                    self.cfg.data.holidays,
                    weekly_weekday=self.cfg.data.weekly_expiry_weekday,
                )
                return fallback.isoformat()
        if self._subscription_expiry_provider:
            try:
                expiry = self._subscription_expiry_provider(target_symbol)
                if expiry:
                    return expiry
            except Exception:
                pass
        try:
            return pick_subscription_expiry(target_symbol, preference)
        except Exception:
            fallback = resolve_weekly_expiry(
                target_symbol,
                ts,
                self.cfg.data.holidays,
                weekly_weekday=self.cfg.data.weekly_expiry_weekday,
            )
            return fallback.isoformat()

    def _strike_step(self, symbol: str) -> int:
        try:
            step_map = getattr(self.cfg.data, "strike_steps", {}) or {}
            return max(int(step_map.get(symbol.upper(), 50)), 1)
        except Exception:
            return 50

    def _resolve_lot_size(self, expiry: str, symbol: str) -> int:
        lot = max(int(self.cfg.data.lot_step), 1)
        cache = self.instrument_cache
        try:
            meta = cache.get_meta(symbol, expiry)
        except Exception:
            meta = None
        if isinstance(meta, tuple) and len(meta) >= 2 and meta[1]:
            try:
                lot = max(int(meta[1]), 1)
            except (TypeError, ValueError):
                pass
        return lot

    def _option_price_for(self, symbol: str, ts: dt.datetime, threshold: float) -> Optional[float]:
        price = self._last_option_prices.get(symbol)
        seen_ts = self._last_option_ts.get(symbol)
        if price is None:
            return None
        if threshold <= 0 or seen_ts is None:
            return price
        age = abs((ts - seen_ts).total_seconds())
        return price if age <= threshold else None

    def _record_option_price(self, symbol: str, price: float, ts: dt.datetime) -> None:
        if not symbol:
            return
        self._last_option_prices[symbol] = price
        self._last_option_ts[symbol] = ts

    async def _publish_signal(self, symbol: str, qty: int, price: float, ts: dt.datetime) -> None:
        signal = OrderSignal(
            instrument=symbol,
            side="BUY",
            qty=qty,
            order_type="MARKET",
            limit_price=price,
            meta={"strategy": self.cfg.strategy_tag},
        )
        try:
            await self.bus.publish(
                "orders/signal",
                {
                    "ts": ts.isoformat(),
                    "type": "signal",
                    "payload": {
                        "instrument": signal.instrument,
                        "side": signal.side,
                        "qty": signal.qty,
                        "order_type": signal.order_type,
                        "limit_price": signal.limit_price,
                        "meta": signal.meta,
                    },
                },
            )
            if self.metrics and hasattr(self.metrics, "strategy_entry_signals_total"):
                self.metrics.strategy_entry_signals_total.inc()
        except Exception:
            return

    def _static_ip_ok(self) -> bool:
        broker = getattr(self._app, "broker", None)
        if broker is None:
            return True
        for attr in ("static_ip_ok", "_static_ip_ok"):
            flag = getattr(broker, attr, None)
            if isinstance(flag, bool):
                return flag
        checker = getattr(broker, "is_static_ip_valid", None)
        if callable(checker):
            try:
                return bool(checker())
            except Exception:
                return False
        return True

    def _broker_live(self) -> bool:
        broker = getattr(self._app, "broker", None)
        if broker is None:
            return True
        probe = getattr(broker, "is_streaming_alive", None)
        if callable(probe):
            try:
                return bool(probe())
            except Exception:
                return False
        return True


__all__ = ["OpeningRangeBreakoutStrategy"]
