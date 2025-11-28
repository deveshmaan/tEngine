from __future__ import annotations

import asyncio
import datetime as dt
import json
import math
import time
from collections import deque
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


LOG = get_logger("ScalpingBuyStrategy")
CHAIN_REFRESH_SECONDS = 300


class ScalpingBuyStrategy(BaseStrategy):
    """High-frequency breakout scalper with volume/IMI/PCR/OI filters."""

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
        self.breakout_window = max(int(getattr(config.strategy, "breakout_window", 5)), 1)
        self.breakout_margin = max(float(getattr(config.strategy, "breakout_margin", 0.0)), 0.0)
        self.volume_mult = max(float(getattr(config.strategy, "volume_mult", 1.5)), 0.0)
        self.imi_period = max(int(getattr(config.strategy, "imi_period", 14)), 1)
        pcr_range = getattr(config.strategy, "pcr_range", (0.7, 1.3))
        try:
            self.pcr_low, self.pcr_high = float(pcr_range[0]), float(pcr_range[1])
        except Exception:
            self.pcr_low, self.pcr_high = 0.7, 1.3
        self.event_halt_minutes = max(int(getattr(config.strategy, "event_halt_minutes", 0)), 0)
        self.spread_max_pct = max(float(getattr(config.strategy, "spread_max_pct", 0.05)), 0.0)
        self.oi_volume_min = max(float(getattr(config.strategy, "oi_volume_min_threshold", 0.0)), 0.0)
        self.scalp_risk_pct = max(float(getattr(config.risk, "scalping_risk_pct", 0.5)), 0.0)
        self._event_path = Path(getattr(config.strategy, "event_file_path", "") or "") if getattr(config.strategy, "event_file_path", None) else None
        self._event_windows: list[tuple[dt.datetime, dt.datetime]] = []
        self._chain_cache: dict[str, dict[int, dict[str, Any]]] = {}
        self._chain_ts: dict[str, float] = {}
        self._bar_history: dict[str, deque[dict[str, float]]] = {}
        self._imi_vals: dict[str, Optional[float]] = {}
        self._volume_history: dict[str, deque[float]] = {}
        self._highest_lookback: dict[str, deque[float]] = {}
        self._scalp_start: dict[str, float] = {}
        self._scalp_durations: deque[float] = deque(maxlen=100)

    async def init(self, app: Any) -> None:
        await super().init(app)
        self._load_event_calendar()

    async def on_tick(self, event: dict) -> None:
        if event.get("type") not in {"tick", "quote"}:
            return
        payload = event.get("payload") or {}
        symbol = (payload.get("symbol") or payload.get("underlying") or "").upper()
        if symbol not in {"NIFTY", "BANKNIFTY"}:
            return
        ltp = self._extract_price(payload)
        if ltp is None:
            return
        ts = self._event_ts(event.get("ts"))
        if self._event_guard(ts):
            return
        self._record_eval_metrics()
        new_bar = self._update_bar(symbol, ltp, payload.get("volume") or payload.get("vol_traded_today") or 0.0, ts)
        if new_bar:
            self._update_imi(symbol)
        breakout = self._is_breakout(symbol, ltp)
        vol_spike = self._volume_spike(symbol)
        imi_ok = self._imi_ok(symbol)
        pcr = await self._maybe_refresh_chain(symbol, ts)
        oi_ok = self._oi_trending(symbol)
        spread_ok = True  # refined at strike selection
        if not (breakout and vol_spike and imi_ok and oi_ok and pcr is not None and self.pcr_low <= pcr <= self.pcr_high):
            return
        await self._enter(symbol, ltp, ts, pcr, spread_ok)

    async def on_fill(self, fill: dict) -> None:
        if fill.get("side", "").upper() == "BUY":
            self._scalp_start[fill["order_id"]] = dt.datetime.fromisoformat(fill["ts"]).timestamp()
            try:
                self.metrics.scalping_trades_total.inc()
            except Exception:
                pass
        return

    # ------------------------------------------------------------------ helpers
    def _record_eval_metrics(self) -> None:
        try:
            self.metrics.strategy_last_eval_ts.set(time.time())
            self.metrics.strategy_evals_total.inc()
        except Exception:
            pass

    def _update_bar(self, symbol: str, price: float, volume: float, ts: dt.datetime) -> bool:
        bucket = ts.replace(second=0, microsecond=0)
        bars = self._bar_history.setdefault(symbol, deque(maxlen=200))
        if bars and bars[-1]["ts"] == bucket:
            bar = bars[-1]
            bar["close"] = price
            bar["high"] = max(bar["high"], price)
            bar["low"] = min(bar["low"], price)
            bar["volume"] += float(volume or 0.0)
            return False
        bars.append({"ts": bucket, "open": price, "high": price, "low": price, "close": price, "volume": float(volume or 0.0)})
        self._volume_history.setdefault(symbol, deque(maxlen=100)).append(float(volume or 0.0))
        self._highest_lookback.setdefault(symbol, deque(maxlen=self.breakout_window)).append(price)
        return True

    def _update_imi(self, symbol: str) -> None:
        bars = list(self._bar_history.get(symbol, ()))
        if len(bars) < self.imi_period:
            self._imi_vals[symbol] = None
            return
        window = bars[-self.imi_period :]
        up = 0.0
        down = 0.0
        for bar in window:
            diff = bar["close"] - bar["open"]
            if diff > 0:
                up += diff
            else:
                down += abs(diff)
        total = up + down
        imi = None if total <= 0 else (up / total) * 100.0
        self._imi_vals[symbol] = imi
        try:
            self.metrics.strategy_imi.labels(symbol=symbol).set(imi or 0.0)
        except Exception:
            pass

    def _imi_ok(self, symbol: str) -> bool:
        imi = self._imi_vals.get(symbol)
        return imi is not None and imi < 30.0

    def _is_breakout(self, symbol: str, price: float) -> bool:
        highs = self._highest_lookback.get(symbol, deque())
        if len(highs) < self.breakout_window:
            return False
        recent_high = max(highs)
        return price >= recent_high + self.breakout_margin

    def _volume_spike(self, symbol: str) -> bool:
        vols = self._volume_history.get(symbol, deque())
        if len(vols) < self.breakout_window + 1:
            return False
        current = vols[-1]
        avg = sum(list(vols)[-self.breakout_window - 1 : -1]) / self.breakout_window
        return current > avg * self.volume_mult

    async def _enter(self, symbol: str, spot: float, ts: dt.datetime, pcr: float, spread_ok: bool) -> None:
        if self.risk.should_halt():
            return
        threshold = getattr(self.cfg.market_data, "max_tick_age_seconds", 0.0)
        if self.risk.block_if_stale(symbol, threshold=threshold):
            return
        expiry = self._resolve_expiry(symbol, ts)
        step = self._strike_step(symbol)
        atm = pick_strike_from_spot(spot, step=step)
        otm = atm + step
        chain = self._chain_cache.get(symbol, {})
        best = None
        for strike in (atm, otm):
            row = chain.get(strike)
            if not row:
                continue
            oi = float(row.get("oi") or 0.0)
            vol = float(row.get("volume") or 0.0)
            if self.oi_volume_min and (oi < self.oi_volume_min or vol < self.oi_volume_min):
                continue
            bid = row.get("bid") or row.get("ltp")
            ask = row.get("ask") or row.get("ltp")
            ltp = row.get("ltp") or row.get("ask") or row.get("bid")
            spread_pct = 0.0
            try:
                if bid and ask and ltp:
                    spread_pct = (float(ask) - float(bid)) / float(ltp)
            except Exception:
                spread_pct = 0.0
            if spread_pct > self.spread_max_pct:
                continue
            score = oi + vol
            if best is None or score > best["score"]:
                best = {"strike": strike, "row": row, "spread_pct": spread_pct}
        if not best:
            return
        strike = best["strike"]
        row = best["row"]
        instrument = f"{symbol}-{expiry}-{int(strike)}CE"
        premium = row.get("ltp") or spot * 0.01
        try:
            premium = float(premium)
        except (TypeError, ValueError):
            return
        lot_size = self._resolve_lot_size(expiry, symbol)
        qty = self._compute_qty(premium, lot_size)
        if qty <= 0:
            return
        budget = OrderBudget(symbol=instrument, qty=qty, price=premium, lot_size=lot_size, side="BUY")
        self.risk.on_tick(instrument, premium)
        if not self.risk.budget_ok_for(budget):
            return
        await self._publish_signal(instrument, qty, premium, ts, meta={"pcr": pcr, "spread_pct": best["spread_pct"]})
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
            try:
                self.metrics.scalping_trades_total.inc()
            except Exception:
                pass
        except OrderValidationError as exc:
            self._logger.log_event(30, "order_validation_failed", symbol=instrument, code=exc.code, message=str(exc))
            return
        except Exception as exc:
            self._logger.log_event(40, "order_submit_failed", symbol=instrument, error=str(exc))
            return
        self._logger.log_event(20, "submitted_scalp", symbol=instrument, price=premium, qty=qty, pcr=pcr)

    def _compute_qty(self, premium: float, lot_size: int) -> int:
        try:
            pct = self.scalp_risk_pct if self.scalp_risk_pct <= 1 else self.scalp_risk_pct / 100.0
            lots = math.floor((self.cfg.capital_base * pct) / (premium * lot_size))
            lots = max(lots, 0)
            qty = lots * lot_size
            return qty
        except Exception:
            return 0

    def _resolve_expiry(self, symbol: str, ts: dt.datetime) -> str:
        if symbol == "BANKNIFTY":
            try:
                return resolve_next_expiry(symbol, ts, kind="monthly", weekly_weekday=self.cfg.data.weekly_expiry_weekday)
            except Exception:
                pass
        preference = getattr(self.cfg.data, "subscription_expiry_preference", "current")
        try:
            return pick_subscription_expiry(symbol, preference)
        except Exception:
            return dt.date.today().isoformat()

    def _strike_step(self, symbol: str) -> int:
        try:
            return int(getattr(self.cfg.data, "strike_steps", {}).get(symbol.upper(), self.cfg.data.lot_step))
        except Exception:
            return int(self.cfg.data.lot_step)

    def _resolve_lot_size(self, expiry: str, symbol: str) -> int:
        lot = max(int(self.cfg.data.lot_step), 1)
        try:
            meta = self.instrument_cache.get_meta(symbol, expiry)
            if isinstance(meta, tuple) and len(meta) >= 2 and meta[1]:
                lot = max(int(meta[1]), 1)
        except Exception:
            pass
        return lot

    async def _publish_signal(self, instrument: str, qty: int, price: float, ts: dt.datetime, meta: Optional[dict[str, Any]] = None) -> None:
        signal = OrderSignal(
            instrument=instrument,
            side="BUY",
            qty=qty,
            order_type="MARKET",
            limit_price=price,
            meta={"strategy": self.cfg.strategy_tag, **(meta or {})},
        )
        try:
            await self.bus.publish(
                "orders/signal",
                {"ts": ts.isoformat(), "type": "signal", "payload": signal.__dict__},
            )
            self.metrics.scalping_signals_total.inc()
            self.metrics.strategy_entry_signals_total.inc()
        except Exception:
            return

    async def _maybe_refresh_chain(self, symbol: str, ts: dt.datetime) -> Optional[float]:
        now = ts.timestamp()
        if symbol in self._chain_cache and (now - self._chain_ts.get(symbol, 0) < CHAIN_REFRESH_SECONDS):
            return self._pcr(symbol)
        try:
            await self._fetch_chain(symbol, ts)
        except Exception as exc:
            LOG.debug("chain_fetch_failed: %s", exc)
        return self._pcr(symbol)

    async def _fetch_chain(self, symbol: str, ts: dt.datetime) -> None:
        cache = self.instrument_cache
        session = cache.upstox_session()
        expiry = self._resolve_expiry(symbol, ts)
        key = cache.resolve_index_key(symbol)

        def _call() -> Any:
            return session.get_option_chain(key, expiry)

        payload = await asyncio.to_thread(_call)
        data = getattr(payload, "data", None) or (payload.get("data") if isinstance(payload, dict) else None) or []
        chain: dict[int, dict[str, Any]] = {}
        call_vol = 0.0
        put_vol = 0.0
        for row in data:
            opt_type = str(row.get("option_type") or row.get("optionType") or row.get("type") or "").upper()
            strike = row.get("strike") or row.get("strike_price") or row.get("strikePrice")
            if strike is None:
                continue
            try:
                strike_val = int(float(strike))
            except Exception:
                continue
            vol = float(row.get("volume") or row.get("vol_traded_today") or row.get("volume_traded") or 0.0)
            if opt_type == "CE":
                call_vol += vol
            elif opt_type == "PE":
                put_vol += vol
            if opt_type != "CE":
                continue
            chain[strike_val] = {
                "instrument_key": row.get("instrument_key") or row.get("instrumentKey"),
                "oi": row.get("oi") or row.get("open_interest"),
                "volume": vol,
                "ltp": row.get("ltp") or row.get("last_price"),
                "bid": row.get("bid") or row.get("best_bid_price"),
                "ask": row.get("ask") or row.get("best_ask_price"),
                "expiry": expiry,
            }
        self._chain_cache[symbol] = chain
        self._chain_ts[symbol] = ts.timestamp()
        if call_vol > 0:
            self._chain_cache[symbol]["__pcr__"] = put_vol / call_vol if call_vol else None

    def _pcr(self, symbol: str) -> Optional[float]:
        chain = self._chain_cache.get(symbol, {})
        val = chain.get("__pcr__")
        try:
            return float(val) if val is not None else None
        except Exception:
            return None

    def _oi_trending(self, symbol: str) -> bool:
        chain = self._chain_cache.get(symbol, {})
        oi_vals = [v.get("oi") for k, v in chain.items() if isinstance(k, int)]
        if not oi_vals:
            return True
        try:
            return all(float(oi or 0.0) >= 0 for oi in oi_vals)
        except Exception:
            return True

    def _event_guard(self, ts: dt.datetime) -> bool:
        if not self._event_windows:
            return False
        for start, end in self._event_windows:
            if start - dt.timedelta(minutes=self.event_halt_minutes) <= ts <= end + dt.timedelta(minutes=self.event_halt_minutes):
                return True
        return False

    def _load_event_calendar(self) -> None:
        if not self._event_path or not self._event_path.exists():
            return
        try:
            raw = self._event_path.read_text(encoding="utf-8")
            events = yaml.safe_load(raw) if self._event_path.suffix.lower() in {".yml", ".yaml"} else json.loads(raw)
        except Exception as exc:
            LOG.warning("event_calendar_load_failed: %s", exc)
            return
        windows: list[tuple[dt.datetime, dt.datetime]] = []
        for evt in events or []:
            if not isinstance(evt, dict):
                continue
            start = evt.get("start") or evt.get("from")
            end = evt.get("end") or evt.get("to")
            try:
                s_dt = dt.datetime.fromisoformat(str(start))
                e_dt = dt.datetime.fromisoformat(str(end))
                if s_dt.tzinfo is None:
                    s_dt = s_dt.replace(tzinfo=dt.timezone.utc)
                if e_dt.tzinfo is None:
                    e_dt = e_dt.replace(tzinfo=dt.timezone.utc)
                windows.append((s_dt, e_dt))
            except Exception:
                continue
        self._event_windows = windows


__all__ = ["ScalpingBuyStrategy"]
