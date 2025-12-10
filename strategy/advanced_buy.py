from __future__ import annotations

import asyncio
import datetime as dt
import json
import math
import time
from collections import defaultdict, deque
from pathlib import Path
from typing import Any, Callable, Optional

import yaml

from engine.config import EngineConfig
from engine.data import pick_strike_from_spot, pick_subscription_expiry, resolve_next_expiry
from engine.events import EventBus, OrderSignal
from engine.logging_utils import get_logger
from engine.metrics import EngineMetrics
from engine.oms import OMS, OrderValidationError
from engine.risk import OrderBudget, RiskManager
from strategy.base import BaseStrategy
from engine.exit import ExitEngine
from market.instrument_cache import InstrumentCache


LOG = get_logger("AdvancedBuyStrategy")
CHAIN_REFRESH_SECONDS = 300
IV_HISTORY_LEN = 120  # ~last 120 samples across days
IMI_DEFAULT_PERIOD = 14


class AdvancedBuyStrategy(BaseStrategy):
    """Momentum + vol breakout with IMI, PCR, IV percentile, and event guards."""

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
        self.short_ma = max(int(getattr(config.strategy, "short_ma", 5)), 1)
        self.long_ma = max(int(getattr(config.strategy, "long_ma", 20)), self.short_ma + 1)
        self.vol_mult = max(float(getattr(config.strategy, "vol_breakout_mult", 1.5)), 0.0)
        self.imi_period = max(int(getattr(config.strategy, "imi_period", IMI_DEFAULT_PERIOD)), 1)
        self.pcr_hi = max(float(getattr(config.strategy, "pcr_extreme_high", 1.3)), 0.0)
        self.pcr_lo = max(float(getattr(config.strategy, "pcr_extreme_low", 0.7)), 0.0)
        self.iv_entry_threshold = max(float(getattr(config.strategy, "iv_percentile_threshold", 0.6)), 0.0)
        self.iv_exit_percentile = max(float(getattr(config.strategy, "iv_exit_percentile", 0.9)), 0.0)
        self.oi_volume_min = max(float(getattr(config.strategy, "oi_volume_min_threshold", 0.0)), 0.0)
        self.gamma_threshold = max(float(getattr(config.strategy, "gamma_threshold", 0.0)), 0.0)
        self.min_minutes_to_expiry = max(int(getattr(config.strategy, "min_minutes_to_expiry", 0)), 0)
        self.event_halt_minutes = max(int(getattr(config.strategy, "event_halt_minutes", 0)), 0)
        event_path = getattr(config.strategy, "event_file_path", None)
        self._event_path = Path(event_path) if event_path else None
        self._event_windows: list[tuple[dt.datetime, dt.datetime]] = []
        self.enable_call_entries = bool(getattr(config.strategy, "enable_call_entries", True))
        self.enable_put_entries = bool(getattr(config.strategy, "enable_put_entries", False))

        self._ema_short: dict[str, float] = {}
        self._ema_long: dict[str, float] = {}
        self._prev_ema_short: dict[str, float] = {}
        self._prev_ema_long: dict[str, float] = {}
        self._vol_returns: dict[str, deque[float]] = defaultdict(lambda: deque(maxlen=120))
        self._vol_avg: dict[str, float] = {}
        self._vol_history: dict[str, deque[float]] = defaultdict(lambda: deque(maxlen=120))
        self._minute_bar: dict[str, dict[str, Any]] = {}
        self._imi_bars: dict[str, deque[tuple[float, float]]] = defaultdict(lambda: deque(maxlen=self.imi_period * 2))
        self._iv_history: dict[str, deque[float]] = defaultdict(lambda: deque(maxlen=IV_HISTORY_LEN))
        self._chain_cache: dict[str, dict[str, Any]] = {}
        self._chain_ts: dict[str, float] = {}
        self._chain_oi_prev: dict[str, dict[int, float]] = {}
        self._oi_history: dict[str, deque[float]] = defaultdict(lambda: deque(maxlen=4))
        self._last_option_price: dict[str, float] = {}
        self._last_option_ts: dict[str, dt.datetime] = {}
        self._last_underlying_price: dict[str, float] = {}
        self._prev_underlying_price: dict[str, float] = {}

    async def init(self, app: Any) -> None:
        await super().init(app)
        self._load_event_calendar()

    async def on_tick(self, event: dict) -> None:
        evt_type = event.get("type")
        if evt_type not in {"tick", "quote"}:
            return
        payload = event.get("payload") or {}
        ts = self._event_ts(event.get("ts"))
        self._record_eval_metrics()
        opt_type = str(payload.get("opt_type") or "").upper()
        symbol_hint = payload.get("symbol") or payload.get("underlying") or ""
        underlying = (payload.get("underlying") or self.cfg.data.index_symbol).upper()
        price = self._extract_price(payload)
        if price is None:
            return

        if opt_type in {"CE", "PE"} or ("-" in str(symbol_hint)):
            instrument = symbol_hint or payload.get("instrument_key") or payload.get("instrument")
            self._record_option_observations(instrument, payload, price, ts)
            await self._handle_option_tick(instrument, price, ts, payload)
            return

        if underlying not in {"NIFTY", "BANKNIFTY"}:
            return
        await self._process_underlying_tick(underlying, price, ts)

    async def on_fill(self, fill: dict) -> None:
        return

    # ------------------------------------------------------------------ processing
    async def _process_underlying_tick(self, symbol: str, price: float, ts: dt.datetime) -> None:
        if symbol in self._last_underlying_price:
            self._prev_underlying_price[symbol] = self._last_underlying_price[symbol]
        self._last_underlying_price[symbol] = price
        new_bar = self._update_minute_bar(symbol, price, ts)
        if new_bar:
            self._update_imi(symbol)
        self._update_emas(symbol, price)
        breakout = self._vol_breakout(symbol)
        crossover_up = self._crossed_up(symbol)
        crossover_down = self._crossed_down(symbol)
        imi = self._current_imi(symbol)
        iv_pct = self._iv_percentile(symbol)
        pcr = await self._maybe_refresh_chain(symbol, ts)
        event_block = self._event_guard(ts)

        self._publish_metrics(symbol, imi=imi, pcr=pcr, iv_pct=iv_pct, breakout=breakout, crossover=crossover_up)
        # Always push latest OI trend based on chain snapshots, independent of entry signals.
        self._update_oi_trend_metric(symbol, price, ts)

        if self.risk.has_open_positions():
            return
        if event_block:
            return

        if self.enable_call_entries:
            if imi is not None and imi < 30 and iv_pct is not None and iv_pct < self.iv_entry_threshold and pcr is not None and self.pcr_lo <= pcr <= self.pcr_hi and breakout and crossover_up:
                await self._enter(symbol, price, ts, iv_pct, pcr, opt_type="CE")

        if self.enable_put_entries and not self.risk.has_open_positions():
            if imi is not None and imi > 70 and iv_pct is not None and iv_pct < self.iv_entry_threshold and pcr is not None and self.pcr_lo <= pcr <= self.pcr_hi and breakout and crossover_down:
                await self._enter(symbol, price, ts, iv_pct, pcr, opt_type="PE")

    async def _enter(self, symbol: str, spot: float, ts: dt.datetime, iv_pct: float, pcr: float, opt_type: str = "CE") -> None:
        if self.risk.should_halt():
            await self.exit_engine.handle_risk_halt()
            return
        threshold = getattr(self.cfg.market_data, "max_tick_age_seconds", 0.0)
        identifier = symbol
        if self.risk.block_if_stale(identifier, threshold=threshold):
            return

        expiry = self._resolve_expiry(symbol, ts)
        step = self._strike_step(symbol)
        atm = pick_strike_from_spot(spot, step=step)
        otm = atm + step if opt_type == "CE" else atm - step
        chain = self._chain_cache.get(symbol, {})
        candidates = [atm, otm]
        best = None
        for strike in candidates:
            entry = chain.get(opt_type, {}).get(strike)
            if not entry:
                continue
            oi = float(entry.get("oi") or 0.0)
            vol = float(entry.get("volume") or 0.0)
            # allow either OI or volume to clear the bar; skip only if both are thin
            if self.oi_volume_min > 0 and (oi < self.oi_volume_min and vol < self.oi_volume_min):
                continue
            prev_price = self._prev_underlying_price.get(symbol)
            prev_chain = self._chain_oi_prev.get(symbol, {})
            prev_oi = {}
            if isinstance(prev_chain, dict):
                prev_oi = prev_chain.get(opt_type, {}) if opt_type in prev_chain else {}
            prev_val = prev_oi.get(strike) if isinstance(prev_oi, dict) else None
            if prev_price is not None and spot > prev_price and prev_val is not None and oi < prev_val:
                continue  # weakening OI trend on rising price
            score = oi + vol
            if best is None or score > best["score"]:
                trend = 0
                if prev_val is not None:
                    trend = 1 if oi > prev_val else -1
                best = {"strike": strike, "entry": entry, "score": score, "trend": trend}
        if best is None:
            return
        strike = best["strike"]
        entry = best["entry"]
        instrument_key = entry.get("instrument_key") or entry.get("token") or ""
        instrument = f"{symbol}-{expiry}-{int(strike)}{opt_type}"
        try:
            trend = float(best.get("trend", 0))
            self.metrics.strategy_oi_trend.labels(instrument=instrument).set(trend)
        except Exception:
            pass
        premium = self._option_price_for(instrument, entry, ts) or spot * 0.01
        if premium <= 0:
            return
        try:
            self.risk.record_expected_price(instrument, premium, best.get("spread_pct"))
        except Exception:
            pass
        lot_size = self._resolve_lot_size(expiry, symbol)
        risk_pct = getattr(self.cfg.risk, "risk_percent_per_trade", 0.0)
        risk_pct_norm = risk_pct if risk_pct <= 1 else risk_pct / 100.0
        risk_adjust = 1.0
        if iv_pct >= 0.8:
            risk_adjust *= 0.5
        if self._event_guard(ts):
            risk_adjust *= 0.5
        qty = math.floor((self.cfg.capital_base * risk_pct_norm * risk_adjust) / (premium * lot_size))
        qty = int(qty * lot_size)
        if qty <= 0:
            return

        # Clamp position size to respect max_open_lots to avoid RiskManager rejections.
        max_lots = getattr(self.cfg.risk, "max_open_lots", None)
        if max_lots is not None and max_lots > 0:
            max_qty = int(max_lots) * lot_size
            if qty > max_qty:
                qty = max_qty
                if qty <= 0:
                    return

        if not self._gamma_ok(entry, expiry, ts):
            return

        budget = OrderBudget(symbol=instrument, qty=qty, price=premium, lot_size=lot_size, side="BUY")
        self.risk.on_tick(instrument, premium)
        if not self.risk.budget_ok_for(budget):
            return

        await self._publish_signal(instrument, qty, premium, ts)
        try:
            await self.oms.submit(
                strategy=self.cfg.strategy_tag,
                symbol=instrument,
                side="BUY",
                qty=qty,
                order_type="MARKET",
                limit_price=premium,
                ts=ts,
            )
        except OrderValidationError as exc:
            self._logger.log_event(30, "order_validation_failed", symbol=instrument, code=exc.code, message=str(exc))
            return
        except Exception as exc:
            self._logger.log_event(40, "order_submit_failed", symbol=instrument, error=str(exc))
            return
        self._logger.log_event(20, "submitted", symbol=instrument, price=premium, qty=qty, pcr=pcr, iv_pct=iv_pct)
        try:
            self.metrics.strategy_selected_strike.labels(
                instrument=instrument,
                symbol=symbol,
                expiry=expiry,
                opt="CE",
                reason="entry",
            ).set(1)
            self.metrics.strategy_position_size.labels(instrument=instrument).set(qty)
        except Exception:
            pass

    def _record_eval_metrics(self) -> None:
        try:
            self.metrics.strategy_last_eval_ts.set(time.time())
            self.metrics.strategy_evals_total.inc()
        except Exception:
            pass

    def _update_minute_bar(self, symbol: str, price: float, ts: dt.datetime) -> bool:
        bucket = ts.replace(second=0, microsecond=0)
        bar = self._minute_bar.get(symbol)
        if bar is None or bar["bucket"] != bucket:
            if bar:
                self._finalize_bar(symbol, bar)
            self._minute_bar[symbol] = {"bucket": bucket, "open": price, "high": price, "low": price, "close": price}
            return bar is not None
        bar["close"] = price
        bar["high"] = max(bar["high"], price)
        bar["low"] = min(bar["low"], price)
        return False

    def _finalize_bar(self, symbol: str, bar: dict[str, Any]) -> None:
        open_p = bar["open"]
        close_p = bar["close"]
        if close_p > 0 and open_p > 0:
            returns = (close_p / open_p) - 1.0
            self._vol_returns[symbol].append(returns)
            self._vol_history[symbol].append(abs(returns))
        self._imi_bars[symbol].append((open_p, close_p))
        try:
            if len(self._vol_returns[symbol]) >= 2:
                import statistics

                rv = statistics.pstdev(self._vol_returns[symbol])
                self._vol_history[symbol].append(rv)
                self._vol_avg[symbol] = statistics.mean(list(self._vol_history[symbol])[-20:])
        except Exception:
            pass

    def _update_imi(self, symbol: str) -> None:
        bars = list(self._imi_bars[symbol])[-self.imi_period :]
        up = 0.0
        down = 0.0
        for o, c in bars:
            diff = c - o
            if diff > 0:
                up += diff
            else:
                down += abs(diff)
        denom = up + down
        if denom <= 0:
            imi = None
        else:
            imi = 100.0 * up / denom
        self._imi_value = getattr(self, "_imi_value", {})
        self._imi_value[symbol] = imi

    def _current_imi(self, symbol: str) -> Optional[float]:
        return getattr(self, "_imi_value", {}).get(symbol)

    def _update_emas(self, symbol: str, price: float) -> None:
        vol_pct = self._vol_percentile(symbol)
        short_look = self.short_ma * (0.5 if vol_pct is not None and vol_pct > 0.8 else 1.0)
        long_look = self.long_ma * (0.5 if vol_pct is not None and vol_pct > 0.8 else 1.0)
        alpha_s = 2.0 / (short_look + 1)
        alpha_l = 2.0 / (long_look + 1)
        prev_s = self._ema_short.get(symbol)
        prev_l = self._ema_long.get(symbol)
        if prev_s is not None:
            self._prev_ema_short[symbol] = prev_s
        if prev_l is not None:
            self._prev_ema_long[symbol] = prev_l
        self._ema_short[symbol] = price if prev_s is None else prev_s + alpha_s * (price - prev_s)
        self._ema_long[symbol] = price if prev_l is None else prev_l + alpha_l * (price - prev_l)

    def _crossed_up(self, symbol: str) -> bool:
        curr_s = self._ema_short.get(symbol)
        curr_l = self._ema_long.get(symbol)
        prev_s = self._prev_ema_short.get(symbol)
        prev_l = self._prev_ema_long.get(symbol)
        if None in (curr_s, curr_l, prev_s, prev_l):
            return False
        return prev_s <= prev_l and curr_s > curr_l

    def _crossed_down(self, symbol: str) -> bool:
        curr_s = self._ema_short.get(symbol)
        curr_l = self._ema_long.get(symbol)
        prev_s = self._prev_ema_short.get(symbol)
        prev_l = self._prev_ema_long.get(symbol)
        if None in (curr_s, curr_l, prev_s, prev_l):
            return False
        return prev_s >= prev_l and curr_s < curr_l

    def _vol_breakout(self, symbol: str) -> bool:
        if symbol not in self._vol_returns or len(self._vol_returns[symbol]) < 5:
            return False
        try:
            import statistics

            rv = statistics.pstdev(self._vol_returns[symbol])
            avg = self._vol_avg.get(symbol) or statistics.mean(list(self._vol_history[symbol])[-20:]) if self._vol_history[symbol] else None
            if avg is None or avg <= 0:
                return False
            return rv > avg * self.vol_mult
        except Exception:
            return False

    def _vol_percentile(self, symbol: str) -> Optional[float]:
        history = list(self._vol_history.get(symbol, ()))
        if not history:
            return None
        current = history[-1]
        smaller = sum(1 for v in history if v <= current)
        return smaller / len(history)

    async def _maybe_refresh_chain(self, symbol: str, ts: dt.datetime) -> Optional[float]:
        now = ts.timestamp()
        if now - self._chain_ts.get(symbol, 0) < CHAIN_REFRESH_SECONDS and symbol in self._chain_cache:
            return self._pcr_from_cache(symbol)
        try:
            await self._fetch_option_chain(symbol)
            self._chain_ts[symbol] = now
        except Exception as exc:
            LOG.debug("chain_fetch_failed: %s", exc)
        return self._pcr_from_cache(symbol)

    async def _fetch_option_chain(self, symbol: str) -> None:
        cache = self.instrument_cache
        session = cache.upstox_session()
        expiry = self._resolve_expiry(symbol, dt.datetime.now(dt.timezone.utc))
        key = cache.resolve_index_key(symbol)

        def _call() -> Any:
            return session.get_option_chain(key, expiry)

        payload = await asyncio.to_thread(_call)
        data = getattr(payload, "data", None) or (payload.get("data") if isinstance(payload, dict) else None)
        raw_entries = data or []
        if isinstance(raw_entries, dict):
            flat_entries: list[dict[str, Any]] = []

            def _collect(seq: Any, opt: str) -> None:
                rows = seq.get("data") if isinstance(seq, dict) else seq
                if not isinstance(rows, list):
                    return
                for item in rows:
                    if isinstance(item, dict):
                        merged = dict(item)
                        merged["option_type"] = opt
                        flat_entries.append(merged)

            _collect(raw_entries.get("call") or raw_entries.get("CALL") or raw_entries.get("ce") or raw_entries.get("CE"), "CE")
            _collect(raw_entries.get("put") or raw_entries.get("PUT") or raw_entries.get("pe") or raw_entries.get("PE"), "PE")
            raw_entries = flat_entries
        # capture previous OI snapshot for trend detection
        prev_chain = self._chain_cache.get(symbol, {})
        prev_snapshot: dict[str, dict[int, float]] = {}
        for opt_key in ("CE", "PE"):
            prev_snapshot[opt_key] = {}
            prev_opt = prev_chain.get(opt_key, {}) if isinstance(prev_chain, dict) else {}
            for k, v in prev_opt.items():
                if isinstance(k, int) and isinstance(v, dict) and "oi" in v:
                    try:
                        prev_snapshot[opt_key][k] = float(v.get("oi") or 0.0)
                    except Exception:
                        continue
        self._chain_oi_prev[symbol] = prev_snapshot
        chain: dict[str, dict[int, dict[str, Any]]] = {"CE": {}, "PE": {}}
        call_vol = 0.0
        put_vol = 0.0
        iv_samples: list[float] = []

        def _ingest(opt_type: str, strike_raw: Any, node: Any, expiry_hint: str) -> None:
            nonlocal call_vol, put_vol
            if opt_type not in {"CE", "PE"} or not isinstance(node, dict):
                return
            if strike_raw is None:
                return
            try:
                strike_val = int(float(strike_raw))
            except (TypeError, ValueError):
                return
            market = node.get("market_data") or node.get("marketData") or node
            greeks = node.get("option_greeks") or node.get("optionGreeks") or {}
            try:
                vol = float(market.get("volume") or market.get("vol_traded_today") or market.get("volume_traded") or 0.0)
            except Exception:
                vol = 0.0
            try:
                oi_val = float(market.get("oi") or node.get("oi") or node.get("open_interest") or 0.0)
            except Exception:
                oi_val = market.get("oi") or node.get("oi") or node.get("open_interest")
            entry = {
                "instrument_key": node.get("instrument_key") or node.get("instrumentKey"),
                "oi": oi_val,
                "volume": vol,
                "ltp": market.get("ltp") or market.get("close_price") or node.get("ltp") or node.get("last_price"),
                "iv": greeks.get("iv") or node.get("iv") or node.get("implied_volatility"),
                "gamma": greeks.get("gamma") or node.get("gamma"),
                "expiry": node.get("expiry") or expiry_hint or expiry,
            }
            chain.setdefault(opt_type, {})[strike_val] = entry
            if opt_type == "CE":
                call_vol += vol
            else:
                put_vol += vol
            if entry.get("iv") is not None:
                iv_samples.append(entry["iv"])

        is_strike_shape = isinstance(raw_entries, list) and raw_entries and isinstance(raw_entries[0], dict) and (
            "call_options" in raw_entries[0] or "put_options" in raw_entries[0] or "callOptions" in raw_entries[0] or "putOptions" in raw_entries[0]
        )
        if is_strike_shape:
            for row in raw_entries:
                if not isinstance(row, dict):
                    continue
                strike_val = row.get("strike_price") or row.get("strike") or row.get("strikePrice")
                exp_val = row.get("expiry") or expiry
                call_node = row.get("call_options") or row.get("callOptions") or row.get("call")
                put_node = row.get("put_options") or row.get("putOptions") or row.get("put")
                _ingest("CE", strike_val, call_node, exp_val)
                _ingest("PE", strike_val, put_node, exp_val)
        else:
            for row in raw_entries if isinstance(raw_entries, list) else []:
                if not isinstance(row, dict):
                    continue
                strike = row.get("strike") or row.get("strike_price") or row.get("strikePrice")
                opt_type = str(row.get("option_type") or row.get("optionType") or row.get("type") or "").upper()
                _ingest(opt_type, strike, row, expiry)

        # derive PCR from puts if available
        if any(chain.get(k) for k in ("CE", "PE")):
            self._chain_cache[symbol] = chain
            if call_vol > 0:
                self._chain_cache[symbol]["__pcr__"] = put_vol / call_vol if call_vol else None
        if self.iv_entry_threshold > 0 and iv_samples:
            for iv_val in iv_samples:
                try:
                    self._iv_history[symbol].append(float(iv_val))
                except Exception:
                    continue

    def _pcr_from_cache(self, symbol: str) -> Optional[float]:
        chain = self._chain_cache.get(symbol, {})
        val = chain.get("__pcr__")
        if isinstance(val, (int, float)):
            return float(val)
        return None

    def _resolve_expiry(self, symbol: str, ts: dt.datetime) -> str:
        if symbol.upper() == "BANKNIFTY":
            try:
                return resolve_next_expiry(symbol, ts, kind="monthly", weekly_weekday=self.cfg.data.weekly_expiry_weekday)
            except Exception:
                pass
        preference = getattr(self.cfg.data, "subscription_expiry_preference", "current")
        if self._subscription_expiry_provider:
            try:
                expiry = self._subscription_expiry_provider(symbol)
                if expiry:
                    return expiry
            except Exception:
                pass
        try:
            return pick_subscription_expiry(symbol, preference)
        except Exception:
            return dt.date.today().isoformat()

    def _strike_step(self, symbol: str) -> int:
        try:
            step_map = getattr(self.cfg.data, "strike_steps", {}) or {}
            return max(int(step_map.get(symbol.upper(), self.cfg.data.lot_step)), 1)
        except Exception:
            return max(int(self.cfg.data.lot_step), 1)

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

    def _record_option_observations(self, instrument: str, payload: dict, price: float, ts: dt.datetime) -> None:
        if not instrument:
            return
        self._last_option_price[instrument] = price
        self._last_option_ts[instrument] = ts
        if "oi" in payload:
            try:
                self._oi_history[instrument].append(float(payload["oi"]))
            except Exception:
                pass
        if "iv" in payload:
            try:
                iv_val = float(payload["iv"])
                underlying = str(payload.get("underlying") or "").upper()
                if underlying:
                    self._iv_history[underlying].append(iv_val)
            except Exception:
                pass

    def _option_price_for(self, instrument: str, entry: dict[str, Any], ts: dt.datetime) -> Optional[float]:
        price = entry.get("ltp")
        if price is None:
            price = self._last_option_price.get(instrument)
        seen_ts = self._last_option_ts.get(instrument)
        if price is not None and seen_ts:
            age = abs((ts - seen_ts).total_seconds())
            if age > 5.0:
                price = entry.get("ltp")
        try:
            return float(price) if price is not None else None
        except (TypeError, ValueError):
            return None

    def _gamma_ok(self, entry: dict[str, Any], expiry: str, ts: dt.datetime) -> bool:
        if self.min_minutes_to_expiry > 0:
            try:
                exp_dt = dt.date.fromisoformat(expiry)
                deadline = dt.datetime.combine(exp_dt, dt.time(15, 30), tzinfo=ts.tzinfo)
                mins_left = (deadline - ts).total_seconds() / 60.0
                if mins_left < self.min_minutes_to_expiry:
                    return False
            except Exception:
                pass
        if self.gamma_threshold > 0:
            gamma = entry.get("gamma")
            try:
                if gamma is not None and float(gamma) > self.gamma_threshold:
                    return False
            except Exception:
                pass
        return True

    def _iv_percentile(self, symbol: str) -> Optional[float]:
        history = list(self._iv_history.get(symbol, ()))
        if not history:
            return None
        current = history[-1]
        below = sum(1 for v in history if v <= current)
        return below / len(history)

    def _event_guard(self, ts: dt.datetime) -> bool:
        if not self._event_windows:
            return False
        for start, end in self._event_windows:
            window_start = start - dt.timedelta(minutes=self.event_halt_minutes)
            window_end = end + dt.timedelta(minutes=self.event_halt_minutes)
            if window_start <= ts <= window_end:
                return True
        return False

    def _load_event_calendar(self) -> None:
        if not self._event_path or not self._event_path.exists():
            return
        try:
            raw = self._event_path.read_text(encoding="utf-8")
            if self._event_path.suffix.lower() in {".yml", ".yaml"}:
                events = yaml.safe_load(raw) or []
            else:
                events = json.loads(raw)
        except Exception as exc:
            LOG.warning("event_calendar_load_failed: %s", exc)
            return
        windows: list[tuple[dt.datetime, dt.datetime]] = []
        for evt in events:
            if not isinstance(evt, dict):
                continue
            start = evt.get("start") or evt.get("from")
            end = evt.get("end") or evt.get("to")
            try:
                start_dt = dt.datetime.fromisoformat(str(start))
                end_dt = dt.datetime.fromisoformat(str(end))
            except Exception:
                continue
            if start_dt.tzinfo is None:
                start_dt = start_dt.replace(tzinfo=dt.timezone.utc)
            if end_dt.tzinfo is None:
                end_dt = end_dt.replace(tzinfo=dt.timezone.utc)
            windows.append((start_dt, end_dt))
        self._event_windows = windows

    def _publish_metrics(
        self,
        symbol: str,
        *,
        imi: Optional[float],
        pcr: Optional[float],
        iv_pct: Optional[float],
        breakout: bool,
        crossover: bool,
    ) -> None:
        try:
            self.metrics.strategy_imi.labels(symbol=symbol).set(imi if imi is not None else 0.0)
            self.metrics.strategy_pcr.labels(symbol=symbol).set(pcr if pcr is not None else 0.0)
            self.metrics.strategy_iv_percentile.labels(symbol=symbol).set(iv_pct if iv_pct is not None else 0.0)
            self.metrics.strategy_vol_breakout_state.labels(symbol=symbol).set(1 if breakout else 0)
            self.metrics.strategy_ma_crossover_state.labels(symbol=symbol).set(1 if crossover else 0)
            if symbol in self._ema_short:
                self.metrics.strategy_short_ema.labels(symbol=symbol).set(self._ema_short[symbol])
            if symbol in self._ema_long:
                self.metrics.strategy_long_ema.labels(symbol=symbol).set(self._ema_long[symbol])
            # Ensure OI trend gauge is always present, even if chain is missing.
            self.metrics.strategy_oi_trend.labels(instrument=f"{symbol}-NA").set(0)
        except Exception:
            pass

    def _update_oi_trend_metric(self, symbol: str, spot: float, ts: dt.datetime) -> None:
        """
        Compute and publish OI trend for both legs using the latest chain snapshot.
        Picks the top 3 strikes per side by (oi+vol) within the current chain.
        This runs even when no entry signal is fired so the dashboard reflects changes.
        """
        expiry = self._resolve_expiry(symbol, ts)
        step = self._strike_step(symbol)
        chain = self._chain_cache.get(symbol, {})
        for opt_type in ("CE", "PE"):
            entries = []
            bucket = chain.get(opt_type, {}) if isinstance(chain, dict) else {}
            for strike, entry in bucket.items():
                if not isinstance(strike, (int, float)):
                    continue
                oi = float(entry.get("oi") or 0.0)
                vol = float(entry.get("volume") or 0.0)
                if self.oi_volume_min > 0 and (oi < self.oi_volume_min and vol < self.oi_volume_min):
                    continue
                prev_chain = self._chain_oi_prev.get(symbol, {})
                prev_oi = prev_chain.get(opt_type, {}) if isinstance(prev_chain, dict) else {}
                prev_val = prev_oi.get(strike) if isinstance(prev_oi, dict) else None
                trend = 0
                if prev_val is not None:
                    try:
                        trend = 1 if oi > float(prev_val) else -1 if oi < float(prev_val) else 0
                    except Exception:
                        trend = 0
                score = oi + vol
                entries.append(
                    {
                        "strike": strike,
                        "entry": entry,
                        "trend": trend,
                        "score": score,
                    }
                )
            if not entries:
                continue
            top = sorted(entries, key=lambda x: x["score"], reverse=True)[:3]
            for item in top:
                instrument_key = item["entry"].get("instrument_key") or item["entry"].get("token") or ""
                instrument = f"{symbol}-{expiry}-{int(item['strike'])}{opt_type}"
                try:
                    self.metrics.strategy_oi_trend.labels(instrument=instrument).set(float(item.get("trend", 0)))
                except Exception:
                    continue

    async def _publish_signal(self, instrument: str, qty: int, price: float, ts: dt.datetime) -> None:
        signal = OrderSignal(
            instrument=instrument,
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

    async def _handle_option_tick(self, symbol: str, price: float, ts: dt.datetime, payload: Optional[dict] = None) -> None:
        if symbol:
            try:
                self.risk.on_tick(symbol, price)
            except Exception:
                pass
        try:
            oi_val = (payload or {}).get("oi") if payload else None
            iv_val = (payload or {}).get("iv") if payload else None
            await self.exit_engine.on_tick(symbol, price, ts, oi=oi_val, iv=iv_val)
        except Exception:
            self._logger.log_event(30, "exit_tick_failed", symbol=symbol)


__all__ = ["AdvancedBuyStrategy"]
