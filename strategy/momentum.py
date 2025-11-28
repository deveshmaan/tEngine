from __future__ import annotations

import datetime as dt
import time
from collections import deque
from typing import Callable, Optional

from engine.config import EngineConfig
from engine.data import pick_strike_from_spot, pick_subscription_expiry, resolve_weekly_expiry
from engine.events import EventBus, OrderSignal
from engine.logging_utils import get_logger
from engine.metrics import EngineMetrics
from engine.oms import OMS, OrderValidationError
from engine.risk import OrderBudget, RiskManager
from strategy.base import BaseStrategy
from engine.exit import ExitEngine
from market.instrument_cache import InstrumentCache

STRATEGY_LOGGER = get_logger("strategy")


class SimpleMomentumStrategy(BaseStrategy):
    """Entry based on short/long MA cross on the index spot."""

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
        self._short_lookback = max(int(getattr(config.strategy, "short_ma", 5)), 1)
        self._long_lookback = max(int(getattr(config.strategy, "long_ma", 20)), self._short_lookback + 1)
        try:
            self._iv_threshold = max(float(getattr(config.strategy, "iv_threshold", 0.0)), 0.0)
        except (TypeError, ValueError):
            self._iv_threshold = 0.0
        self._short_window: deque[float] = deque(maxlen=self._short_lookback)
        self._long_window: deque[float] = deque(maxlen=self._long_lookback)
        self._prev_short: Optional[float] = None
        self._prev_long: Optional[float] = None
        self._last_iv_by_symbol: dict[str, float] = {}

    async def on_tick(self, event: dict) -> None:
        evt_type = event.get("type")
        if evt_type not in {"tick", "quote", "bar"}:
            return
        payload = event.get("payload") or {}
        instrument_key = payload.get("instrument_key") or payload.get("instrument") or payload.get("token")
        price = self._extract_price(payload)
        if price is None:
            return
        ts = self._event_ts(event.get("ts"))
        try:
            self.metrics.strategy_last_eval_ts.set(time.time())
            self.metrics.strategy_evals_total.inc()
        except Exception:
            pass
        symbol_hint = payload.get("symbol") or payload.get("underlying") or ""
        index_sym = self.cfg.data.index_symbol.upper()
        opt_type = str(payload.get("opt_type") or "").upper()
        is_option = opt_type in {"CE", "PE"} or (symbol_hint and str(symbol_hint).upper() != index_sym)

        if is_option:
            self._record_iv(symbol_hint, payload.get("iv"))
            await self._handle_option_tick(symbol_hint, price, ts)
            STRATEGY_LOGGER.info(
                "strategy_heartbeat: run=%s underlying=%s instruments=%d has_order_intent=%s",
                getattr(self.cfg, "run_id", "n/a"),
                index_sym,
                1,
                False,
            )
            return

        if not symbol_hint or str(symbol_hint).upper() != index_sym:
            return

        crossed = self._update_moving_averages(price)
        if not crossed:
            return
        if self.risk.has_open_positions():
            return
        has_intent = await self._enter_from_signal(price, ts, instrument_key=instrument_key)
        STRATEGY_LOGGER.info(
            "strategy_heartbeat: run=%s underlying=%s instruments=%d has_order_intent=%s",
            getattr(self.cfg, "run_id", "n/a"),
            index_sym,
            1,
            bool(has_intent),
        )

    async def on_fill(self, fill: dict) -> None:
        # Entry sizing is handled at signal time; no additional bookkeeping required here.
        return

    # ------------------------------------------------------------------ internals
    def _update_moving_averages(self, price: float) -> bool:
        self._short_window.append(price)
        self._long_window.append(price)
        if len(self._short_window) < self._short_lookback or len(self._long_window) < self._long_lookback:
            return False
        curr_short = sum(self._short_window) / len(self._short_window)
        curr_long = sum(self._long_window) / len(self._long_window)
        prev_short = self._prev_short
        prev_long = self._prev_long
        self._prev_short = curr_short
        self._prev_long = curr_long
        if prev_short is None or prev_long is None:
            return False
        return prev_short <= prev_long and curr_short > curr_long

    async def _handle_option_tick(self, symbol: str, price: float, ts: dt.datetime) -> None:
        if symbol:
            try:
                self.risk.on_tick(symbol, price)
            except Exception:
                pass
        try:
            await self.exit_engine.on_tick(symbol, price, ts)
        except Exception:
            self._logger.log_event(30, "exit_tick_failed", symbol=symbol)

    async def _enter_from_signal(self, spot: float, ts: dt.datetime, *, instrument_key: Optional[str] = None) -> bool:
        if self.risk.should_halt():
            await self.exit_engine.handle_risk_halt()
            return False
        threshold = getattr(self.cfg.market_data, "max_tick_age_seconds", 0.0)
        identifier = instrument_key or self.cfg.data.index_symbol
        if self.risk.block_if_stale(identifier, threshold=threshold):
            return False
        if self.risk.block_if_stale(self.cfg.data.index_symbol, threshold=threshold):
            return False

        expiry_str = self._resolve_expiry(ts)
        strike = pick_strike_from_spot(
            spot,
            step=self.cfg.data.lot_step,
        )
        option_type = "CE"
        lot_size = self._resolve_lot_size(expiry_str)
        symbol = f"{self.cfg.data.index_symbol}-{expiry_str}-{int(strike)}{option_type}"

        if self._iv_threshold > 0:
            iv = self._last_iv_by_symbol.get(symbol)
            if iv is None:
                return False
            if iv > self._iv_threshold:
                self._logger.log_event(20, "iv_gate_block", symbol=symbol, iv=iv, threshold=self._iv_threshold)
                return False

        qty = self.risk.position_size(premium=spot, lot_size=lot_size)
        if qty <= 0:
            return False
        budget = OrderBudget(symbol=symbol, qty=qty, price=spot, lot_size=lot_size, side="BUY")
        self.risk.on_tick(symbol, spot)
        if not self.risk.budget_ok_for(budget):
            return False

        await self._publish_signal(symbol, qty, spot, ts)
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
        self._logger.log_event(20, "submitted", symbol=symbol, price=budget.price, qty=budget.qty)
        return True

    def _resolve_expiry(self, ts: dt.datetime) -> str:
        preference = getattr(self.cfg.data, "subscription_expiry_preference", "current")
        if self._subscription_expiry_provider:
            try:
                expiry = self._subscription_expiry_provider(self.cfg.data.index_symbol)
                if expiry:
                    return expiry
            except Exception:
                pass
        try:
            return pick_subscription_expiry(self.cfg.data.index_symbol, preference)
        except Exception:
            fallback = resolve_weekly_expiry(
                self.cfg.data.index_symbol,
                ts,
                self.cfg.data.holidays,
                weekly_weekday=self.cfg.data.weekly_expiry_weekday,
            )
            return fallback.isoformat()

    def _resolve_lot_size(self, expiry: str) -> int:
        lot = max(int(self.cfg.data.lot_step), 1)
        cache = self.instrument_cache
        try:
            meta = cache.get_meta(self.cfg.data.index_symbol, expiry)
        except Exception:
            meta = None
        if isinstance(meta, tuple) and len(meta) >= 2 and meta[1]:
            try:
                lot = max(int(meta[1]), 1)
            except (TypeError, ValueError):
                pass
        return lot

    def _record_iv(self, symbol: Optional[str], value: Optional[float]) -> None:
        if symbol is None or value is None:
            return
        try:
            iv_val = float(value)
        except (TypeError, ValueError):
            return
        self._last_iv_by_symbol[str(symbol)] = iv_val

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
            try:
                if self.metrics and hasattr(self.metrics, "strategy_entry_signals_total"):
                    self.metrics.strategy_entry_signals_total.inc()
            except Exception:
                pass
        except Exception:
            return


class IntradayBuyStrategy(SimpleMomentumStrategy):
    """Backward-compatible alias for the intraday buy strategy."""


__all__ = ["SimpleMomentumStrategy", "IntradayBuyStrategy"]
