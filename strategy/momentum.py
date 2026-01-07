from __future__ import annotations

import datetime as dt
import statistics
import time
from collections import deque
from typing import Callable, Optional

from engine.config import EngineConfig
from engine.alerts import notify_incident
from engine.data import pick_strike_from_spot, pick_subscription_expiry, resolve_next_expiry, resolve_weekly_expiry
from engine.events import EventBus, OrderSignal
from engine.logging_utils import get_logger
from engine.metrics import EngineMetrics
from engine.oms import OMS, OrderValidationError
from engine.risk import OrderBudget, RiskManager
from strategy.base import BaseStrategy
from engine.exit import ExitEngine
from market.instrument_cache import InstrumentCache

STRATEGY_LOGGER = get_logger("strategy")
_OPTION_MAX_AGE_SECONDS = 5.0


class SimpleMomentumStrategy(BaseStrategy):
    """Momentum + volatility breakout entries for ATM weekly/monthly calls."""

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
        self._short_alpha = 2.0 / (self._short_lookback + 1)
        self._long_alpha = 2.0 / (self._long_lookback + 1)
        try:
            self._iv_threshold = max(float(getattr(config.strategy, "iv_threshold", 0.0)), 0.0)
        except (TypeError, ValueError):
            self._iv_threshold = 0.0
        try:
            self._vol_breakout_mult = max(float(getattr(config.strategy, "vol_breakout_mult", 1.5)), 0.0)
        except (TypeError, ValueError):
            self._vol_breakout_mult = 1.5
        try:
            self._banknifty_vol_mult = max(float(getattr(config.banknifty, "vol_breakout_mult", 1.2)), 0.0)
        except Exception:
            self._banknifty_vol_mult = 1.2
        self._vol_avg_window = 20
        self._minute_returns: dict[str, deque[float]] = {}
        self._realized_vol: dict[str, float] = {}
        self._realized_vol_avg: dict[str, float] = {}
        self._realized_vol_history: dict[str, deque[float]] = {}
        self._ema_short: dict[str, float] = {}
        self._ema_long: dict[str, float] = {}
        self._prev_ema_short: dict[str, float] = {}
        self._prev_ema_long: dict[str, float] = {}
        self._current_minute: dict[str, dt.datetime] = {}
        self._minute_close: dict[str, float] = {}
        self._prev_minute_close: dict[str, float] = {}
        self._last_option_prices: dict[str, float] = {}
        self._last_option_ts: dict[str, dt.datetime] = {}
        self._lot_size_fallbacks: set[tuple[str, str]] = set()
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
        self._record_eval_metrics()
        symbol_hint = payload.get("symbol") or payload.get("underlying") or ""
        index_sym = self.cfg.data.index_symbol.upper()
        opt_type = str(payload.get("opt_type") or "").upper()
        is_option = opt_type in {"CE", "PE"} or (symbol_hint and "-" in str(symbol_hint) and str(symbol_hint).upper() != index_sym)

        if is_option:
            option_symbol = symbol_hint or payload.get("instrument") or ""
            self._record_option_price(option_symbol, price, ts)
            self._record_iv(symbol_hint, payload.get("iv"))
            await self._handle_option_tick(option_symbol, price, ts)
            STRATEGY_LOGGER.info(
                "strategy_heartbeat: run=%s underlying=%s instruments=%d has_order_intent=%s",
                getattr(self.cfg, "run_id", "n/a"),
                index_sym,
                1,
                False,
            )
            return

        underlying = (payload.get("symbol") or payload.get("underlying") or index_sym).upper()
        if underlying != index_sym:
            return

        updated = self._update_time_series(underlying, price, ts)
        if not updated:
            return

        breakout = self._is_vol_breakout(underlying)
        crossover = self._crossed_up(underlying)
        self._publish_runtime_metrics(underlying, breakout, crossover)
        if self.risk.has_open_positions():
            STRATEGY_LOGGER.info(
                "strategy_heartbeat: run=%s underlying=%s breakout=%s crossover=%s has_order_intent=%s",
                getattr(self.cfg, "run_id", "n/a"),
                underlying,
                breakout,
                crossover,
                False,
            )
            return

        has_intent = False
        if breakout and crossover:
            has_intent = await self._enter_from_signal(price, ts, instrument_key=instrument_key, underlying=underlying)
        STRATEGY_LOGGER.info(
            "strategy_heartbeat: run=%s underlying=%s breakout=%s crossover=%s has_order_intent=%s",
            getattr(self.cfg, "run_id", "n/a"),
            underlying,
            breakout,
            crossover,
            bool(has_intent),
        )

    async def on_fill(self, fill: dict) -> None:
        # Entry sizing is handled at signal time; no additional bookkeeping required here.
        return

    # ------------------------------------------------------------------ internals
    def _record_eval_metrics(self) -> None:
        try:
            self.metrics.strategy_last_eval_ts.set(time.time())
            self.metrics.strategy_evals_total.inc()
        except Exception:
            pass

    def _update_time_series(self, symbol: str, price: float, ts: dt.datetime) -> bool:
        bucket = ts.replace(second=0, microsecond=0)
        current_bucket = self._current_minute.get(symbol)
        if current_bucket is None:
            self._current_minute[symbol] = bucket
            self._minute_close[symbol] = price
            return False
        if bucket == current_bucket:
            self._minute_close[symbol] = price
            return False

        last_close = self._minute_close.get(symbol, price)
        prev_close = self._prev_minute_close.get(symbol)
        self._prev_minute_close[symbol] = last_close
        self._current_minute[symbol] = bucket
        self._minute_close[symbol] = price
        self._update_emas(symbol, last_close)
        self._update_realized_vol(symbol, last_close, prev_close)
        return True

    def _update_emas(self, symbol: str, close: float) -> None:
        prev_short = self._ema_short.get(symbol)
        prev_long = self._ema_long.get(symbol)
        if prev_short is not None:
            self._prev_ema_short[symbol] = prev_short
        if prev_long is not None:
            self._prev_ema_long[symbol] = prev_long
        self._ema_short[symbol] = close if prev_short is None else prev_short + self._short_alpha * (close - prev_short)
        self._ema_long[symbol] = close if prev_long is None else prev_long + self._long_alpha * (close - prev_long)

    def _update_realized_vol(self, symbol: str, close: float, prev_close: Optional[float]) -> None:
        returns_q = self._minute_returns.setdefault(symbol, deque(maxlen=self._vol_avg_window * 3))
        if prev_close and prev_close > 0:
            try:
                returns_q.append((close / prev_close) - 1.0)
            except ZeroDivisionError:
                pass
        if len(returns_q) >= 2:
            rv = statistics.pstdev(returns_q)
            self._realized_vol[symbol] = rv
            history = self._realized_vol_history.setdefault(symbol, deque(maxlen=self._vol_avg_window * 3))
            history.append(rv)
            if len(history) >= min(self._vol_avg_window, 3):
                try:
                    avg = statistics.mean(list(history)[-self._vol_avg_window :])
                    self._realized_vol_avg[symbol] = avg
                except statistics.StatisticsError:
                    return

    def _crossed_up(self, symbol: str) -> bool:
        curr_short = self._ema_short.get(symbol)
        curr_long = self._ema_long.get(symbol)
        prev_short = self._prev_ema_short.get(symbol)
        prev_long = self._prev_ema_long.get(symbol)
        if curr_short is None or curr_long is None or prev_short is None or prev_long is None:
            return False
        return prev_short <= prev_long and curr_short > curr_long

    def _vol_multiplier(self, symbol: str) -> float:
        if symbol.upper() == "BANKNIFTY":
            return self._banknifty_vol_mult or self._vol_breakout_mult
        return self._vol_breakout_mult

    def _is_vol_breakout(self, symbol: str) -> bool:
        rv = self._realized_vol.get(symbol)
        avg = self._realized_vol_avg.get(symbol)
        if rv is None or avg is None or avg <= 0:
            return False
        return rv > avg * self._vol_multiplier(symbol)

    def _publish_runtime_metrics(self, symbol: str, breakout: bool, crossover: bool) -> None:
        try:
            if hasattr(self.metrics, "strategy_realized_vol") and symbol:
                if symbol in self._realized_vol:
                    self.metrics.strategy_realized_vol.labels(symbol=symbol).set(self._realized_vol[symbol])
                if symbol in self._realized_vol_avg:
                    self.metrics.strategy_realized_vol_avg.labels(symbol=symbol).set(self._realized_vol_avg[symbol])
                self.metrics.strategy_vol_breakout_state.labels(symbol=symbol).set(1 if breakout else 0)
                self.metrics.strategy_ma_crossover_state.labels(symbol=symbol).set(1 if (self._ema_short.get(symbol, 0) > self._ema_long.get(symbol, 0)) else 0)
                if symbol in self._ema_short:
                    self.metrics.strategy_short_ema.labels(symbol=symbol).set(self._ema_short[symbol])
                if symbol in self._ema_long:
                    self.metrics.strategy_long_ema.labels(symbol=symbol).set(self._ema_long[symbol])
        except Exception:
            pass

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

    async def _enter_from_signal(self, spot: float, ts: dt.datetime, *, instrument_key: Optional[str] = None, underlying: Optional[str] = None) -> bool:
        underlier = (underlying or self.cfg.data.index_symbol).upper()
        if self.risk.should_halt():
            await self.exit_engine.handle_risk_halt()
            return False
        threshold = getattr(self.cfg.market_data, "max_tick_age_seconds", 0.0)
        identifier = instrument_key or underlier
        if self.risk.block_if_stale(identifier, threshold=threshold):
            return False
        if self.risk.block_if_stale(underlier, threshold=threshold):
            return False
        if not self._static_ip_ok():
            self._logger.log_event(30, "static_ip_gate_block", symbol=underlier)
            return False
        if not self._broker_live():
            self._logger.log_event(30, "broker_stream_block", symbol=underlier)
            return False

        expiry_str = self._resolve_expiry(ts, underlier)
        strike = pick_strike_from_spot(
            spot,
            step=self._strike_step(underlier),
        )
        option_type = "CE"
        lot_size = self._resolve_lot_size(expiry_str, underlier)
        symbol = f"{underlier}-{expiry_str}-{int(strike)}{option_type}"
        premium = self._option_price_for(symbol, ts, threshold or _OPTION_MAX_AGE_SECONDS)
        if premium is None:
            self._logger.log_event(20, "premium_missing_block", symbol=symbol)
            return False

        if self._iv_threshold > 0:
            iv = self._last_iv_by_symbol.get(symbol)
            if iv is None:
                return False
            if iv > self._iv_threshold:
                self._logger.log_event(20, "iv_gate_block", symbol=symbol, iv=iv, threshold=self._iv_threshold)
                return False

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
        self._logger.log_event(20, "submitted", symbol=symbol, price=budget.price, qty=budget.qty)
        return True

    def _strike_step(self, symbol: str) -> int:
        try:
            step_map = getattr(self.cfg.data, "strike_steps", {}) or {}
            return max(int(step_map.get(symbol.upper(), 50)), 1)
        except Exception:
            return 50

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

    def _resolve_lot_size(self, expiry: str, symbol: str) -> int:
        fallback = max(int(self.cfg.data.lot_step), 1)
        cache = self.instrument_cache
        try:
            meta = cache.get_meta(symbol, expiry)
        except Exception:
            meta = None
        if isinstance(meta, tuple) and len(meta) >= 2 and meta[1]:
            try:
                return max(int(meta[1]), 1)
            except (TypeError, ValueError):
                pass
        key = (symbol.upper(), expiry)
        if key not in self._lot_size_fallbacks:
            self._lot_size_fallbacks.add(key)
            self._logger.log_event(30, "lot_size_fallback", symbol=symbol, expiry=expiry, fallback=fallback)
            notify_incident("WARN", "Lot size fallback", f"symbol={symbol} expiry={expiry} lot_step={fallback}", tags=["lot_size_fallback"])
        return fallback

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
