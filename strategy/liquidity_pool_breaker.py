from __future__ import annotations

import asyncio
import datetime as dt
import time
from dataclasses import dataclass
from typing import Any, Optional

from engine.alerts import notify_incident
from engine.config import EngineConfig, IST
from engine.data import pick_subscription_expiry, resolve_weekly_expiry
from engine.events import EventBus, OrderSignal
from engine.exit import ExitEngine
from engine.logging_utils import get_logger
from engine.metrics import EngineMetrics
from engine.oms import OMS, OrderValidationError
from engine.risk import OrderBudget, RiskManager, compute_position_size
from engine.time_machine import now as engine_now
from market.instrument_cache import InstrumentCache
from strategy.base import BaseStrategy


@dataclass
class LiquidityZone:
    strike: int
    score: float
    ce_oi: float
    pe_oi: float
    ce_vol: float
    pe_vol: float
    dominant_side: str
    last_chain_ts: float


@dataclass
class PendingBreach:
    strike: int
    direction: str
    breach_ts: dt.datetime
    breach_price: float
    opt_type: str
    baseline_oi: Optional[float] = None
    baseline_vol: Optional[float] = None
    baseline_chain_ts: float = 0.0
    stage: str = "awaiting_chain"
    absorb_deadline: Optional[dt.datetime] = None


@dataclass
class EntryIntent:
    strike: int
    direction: str
    trade_type: str
    opt_type: str
    underlying_price: float
    ts: dt.datetime


@dataclass
class OpenTrade:
    symbol: str
    strike: int
    direction: str
    trade_type: str
    opt_type: str
    entry_underlying: float
    entry_ts: dt.datetime


def detect_breach(prev_price: float, price: float, strike: int) -> Optional[str]:
    if prev_price < strike <= price:
        return "up"
    if prev_price > strike >= price:
        return "down"
    return None


def classify_liquidity_event(
    oi_change_pct: float,
    vol_delta: float,
    *,
    oi_drop_pct_trigger: float,
    absorption_oi_rise_pct: float,
    min_vol_delta: float,
) -> str:
    if vol_delta < min_vol_delta:
        return "none"
    if oi_change_pct <= -oi_drop_pct_trigger:
        return "stop_grab"
    if oi_change_pct >= absorption_oi_rise_pct or oi_change_pct > -oi_drop_pct_trigger:
        return "absorption"
    return "none"


def select_liquid_contract(
    chain: dict[str, dict[int, dict[str, Any]]],
    strikes: list[int],
    opt_type: str,
    *,
    min_oi: float,
    min_vol: float,
) -> Optional[tuple[int, dict[str, Any]]]:
    best_strike = None
    best_entry: Optional[dict[str, Any]] = None
    best_score = -1.0
    for strike in strikes:
        entry = (chain.get(opt_type, {}) or {}).get(strike)
        if not isinstance(entry, dict):
            continue
        try:
            oi = float(entry.get("oi") or 0.0)
        except Exception:
            oi = 0.0
        try:
            vol = float(entry.get("volume") or 0.0)
        except Exception:
            vol = 0.0
        if min_oi > 0 and oi < min_oi:
            continue
        if min_vol > 0 and vol < min_vol:
            continue
        score = oi + vol
        if score > best_score:
            best_score = score
            best_strike = strike
            best_entry = entry
    if best_strike is None or best_entry is None:
        return None
    return best_strike, best_entry


class LiquidityPoolBreakerStrategy(BaseStrategy):
    """Liquidity pool breaker using option OI/volume around strike crossings."""

    def __init__(
        self,
        config: EngineConfig,
        risk: RiskManager,
        oms: OMS,
        bus: EventBus,
        exit_engine: ExitEngine,
        instrument_cache: InstrumentCache,
        metrics: EngineMetrics,
        subscription_expiry_provider: Optional[Any] = None,
    ) -> None:
        super().__init__(config, risk, oms, bus, exit_engine, instrument_cache, metrics, subscription_expiry_provider)
        self._logger = get_logger("LiquidityPoolBreakerStrategy")
        self._enabled = str(self.cfg.data.index_symbol).upper() == "NIFTY"
        self._underlying = "NIFTY"
        self._prev_price: Optional[float] = None
        self._pending_breach: Optional[PendingBreach] = None
        self._entry_intents: dict[str, EntryIntent] = {}
        self._open_trades: dict[str, OpenTrade] = {}
        self._exit_cooldown: dict[str, float] = {}
        self._chain: dict[str, dict[int, dict[str, Any]]] = {"CE": {}, "PE": {}}
        self._chain_prev: dict[str, dict[int, dict[str, Any]]] = {"CE": {}, "PE": {}}
        self._chain_ts: float = 0.0
        self._last_chain_fetch: float = 0.0
        self._zones: list[LiquidityZone] = []
        self._zones_ts: float = 0.0
        self._lot_size_fallbacks: set[tuple[str, str]] = set()

        strat_cfg = self.cfg.strategy
        self.top_n_zones = max(int(getattr(strat_cfg, "top_n_zones", 3)), 1)
        self.chain_refresh_seconds = max(int(getattr(strat_cfg, "chain_refresh_seconds", 300)), 1)
        self.chain_min_cooldown_seconds = max(int(getattr(strat_cfg, "chain_min_cooldown_seconds", 30)), 1)
        self.liquidity_weight = max(float(getattr(strat_cfg, "liquidity_weight", 1.0)), 0.0)
        self.min_oi = max(float(getattr(strat_cfg, "min_oi", 0.0)), 0.0)
        self.min_vol = max(float(getattr(strat_cfg, "min_vol", 0.0)), 0.0)
        self.oi_drop_pct_trigger = max(float(getattr(strat_cfg, "oi_drop_pct_trigger", 0.15)), 0.0)
        self.absorption_oi_rise_pct = max(float(getattr(strat_cfg, "absorption_oi_rise_pct", 0.1)), 0.0)
        self.min_vol_delta = max(float(getattr(strat_cfg, "min_vol_delta", 100.0)), 0.0)
        self.absorption_confirm_seconds = max(int(getattr(strat_cfg, "absorption_confirm_seconds", 45)), 1)
        self.reclaim_buffer_points = max(float(getattr(strat_cfg, "reclaim_buffer_points", 2.0)), 0.0)
        self.stop_buffer_points = max(float(getattr(strat_cfg, "stop_buffer_points", 3.0)), 0.0)
        self.risk_pct = max(float(getattr(strat_cfg, "risk_pct", 0.0)), 0.0)
        self.max_hold_minutes = max(int(getattr(strat_cfg, "max_hold_minutes", 0)), 0)
        self.no_new_entries_after = getattr(strat_cfg, "no_new_entries_after_time", None)

        if not self._enabled:
            self._logger.log_event(30, "lpb_disabled", reason="unsupported_underlying", symbol=self.cfg.data.index_symbol)
            notify_incident("WARN", "LPB disabled", f"index_symbol={self.cfg.data.index_symbol}", tags=["strategy_guard"])

    async def on_tick(self, event: dict) -> None:
        evt_type = event.get("type")
        if evt_type not in {"tick", "quote"}:
            return
        payload = event.get("payload") or {}
        ts = self._event_ts(event.get("ts"))
        price = self._extract_price(payload)
        if price is None:
            return
        symbol_hint = payload.get("symbol") or payload.get("underlying") or ""
        opt_type = str(payload.get("opt_type") or "").upper()
        if opt_type in {"CE", "PE"} or ("-" in str(symbol_hint) and str(symbol_hint).upper() != self._underlying):
            option_symbol = symbol_hint or payload.get("instrument") or payload.get("instrument_key") or ""
            await self._handle_option_tick(option_symbol, price, ts, payload)
            return
        symbol = str(symbol_hint).upper() if symbol_hint else self._underlying
        if symbol != self._underlying:
            return
        if not self._enabled:
            return
        self._record_eval_metrics()
        await self._maybe_refresh_chain(ts, force=False)
        self._maybe_refresh_zones(price, ts)
        await self._handle_pending_breach(price, ts)
        await self._evaluate_open_trades(price, ts)
        await self._detect_new_breach(price, ts)
        self._prev_price = price

    async def on_fill(self, fill: dict) -> None:
        if fill.get("symbol") is None:
            return
        side = str(fill.get("side") or "").upper()
        symbol = str(fill.get("symbol") or "")
        if side == "BUY":
            order_id = str(fill.get("order_id") or "")
            intent = self._entry_intents.pop(order_id, None)
            strike = self._parse_strike(symbol) or (intent.strike if intent else 0)
            if intent:
                trade = OpenTrade(
                    symbol=symbol,
                    strike=strike,
                    direction=intent.direction,
                    trade_type=intent.trade_type,
                    opt_type=intent.opt_type,
                    entry_underlying=intent.underlying_price,
                    entry_ts=intent.ts,
                )
                self._open_trades[symbol] = trade
                self._logger.log_event(20, "lpb_fill_buy", symbol=symbol, strike=strike, reason=intent.trade_type)
            return
        if side == "SELL":
            self._open_trades.pop(symbol, None)
        return

    def _record_eval_metrics(self) -> None:
        try:
            self.metrics.strategy_last_eval_ts.set(time.time())
            self.metrics.strategy_evals_total.inc()
        except Exception:
            pass

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

    async def _maybe_refresh_chain(self, ts: dt.datetime, *, force: bool) -> None:
        now = ts.timestamp()
        if not force and (now - self._chain_ts) < self.chain_refresh_seconds and self._chain:
            return
        if (now - self._last_chain_fetch) < self.chain_min_cooldown_seconds:
            return
        self._last_chain_fetch = now
        try:
            await self._fetch_chain(self._underlying, ts)
            self._chain_ts = now
            self._zones_ts = 0.0
        except Exception as exc:
            self._logger.log_event(30, "lpb_chain_fetch_failed", error=str(exc))

    def _maybe_refresh_zones(self, price: float, ts: dt.datetime) -> None:
        now = ts.timestamp()
        if self._zones and (now - self._zones_ts) < self.chain_refresh_seconds:
            if self._zones_far_from_price(price):
                self._zones_ts = 0.0
            else:
                return
        if not self._chain.get("CE") and not self._chain.get("PE"):
            return
        self._zones = self._select_zones(now)
        self._zones_ts = now
        self._publish_zone_metrics()

    def _zones_far_from_price(self, price: float) -> bool:
        if not self._zones:
            return True
        step = self._strike_step(self._underlying)
        strikes = [z.strike for z in self._zones]
        lower = min(strikes) - (2 * step)
        upper = max(strikes) + (2 * step)
        return price < lower or price > upper

    def _select_zones(self, chain_ts: float) -> list[LiquidityZone]:
        chain = self._chain
        strikes = set(chain.get("CE", {}).keys()) | set(chain.get("PE", {}).keys())
        zones: list[LiquidityZone] = []
        for strike in strikes:
            ce_entry = chain.get("CE", {}).get(strike) or {}
            pe_entry = chain.get("PE", {}).get(strike) or {}
            ce_oi = float(ce_entry.get("oi") or 0.0)
            pe_oi = float(pe_entry.get("oi") or 0.0)
            ce_vol = float(ce_entry.get("volume") or 0.0)
            pe_vol = float(pe_entry.get("volume") or 0.0)
            sum_oi = ce_oi + pe_oi
            sum_vol = ce_vol + pe_vol
            if self.min_oi > 0 and sum_oi < self.min_oi:
                continue
            if self.min_vol > 0 and sum_vol < self.min_vol:
                continue
            score = sum_oi + (self.liquidity_weight * sum_vol)
            dominant_side = "CE" if ce_oi >= pe_oi else "PE"
            zones.append(
                LiquidityZone(
                    strike=int(strike),
                    score=float(score),
                    ce_oi=ce_oi,
                    pe_oi=pe_oi,
                    ce_vol=ce_vol,
                    pe_vol=pe_vol,
                    dominant_side=dominant_side,
                    last_chain_ts=chain_ts,
                )
            )
        zones.sort(key=lambda z: z.score, reverse=True)
        return zones[: self.top_n_zones]

    def _publish_zone_metrics(self) -> None:
        if not self.metrics or not hasattr(self.metrics, "lpbreaker_zone_strike"):
            return
        for idx in range(self.top_n_zones):
            if idx < len(self._zones):
                zone = self._zones[idx]
                try:
                    self.metrics.lpbreaker_zone_strike.labels(rank=str(idx + 1), side=zone.dominant_side).set(zone.strike)
                except Exception:
                    continue
            else:
                try:
                    self.metrics.lpbreaker_zone_strike.labels(rank=str(idx + 1), side="NA").set(0)
                except Exception:
                    continue

    async def _detect_new_breach(self, price: float, ts: dt.datetime) -> None:
        if self._pending_breach or self.risk.has_open_positions():
            return
        if not self._zones or self._prev_price is None:
            return
        if self.no_new_entries_after and ts.time() >= self.no_new_entries_after:
            return
        breaches: list[tuple[LiquidityZone, str]] = []
        for zone in self._zones:
            direction = detect_breach(self._prev_price, price, zone.strike)
            if direction:
                breaches.append((zone, direction))
        if not breaches:
            return
        zone, direction = sorted(breaches, key=lambda x: x[0].score, reverse=True)[0]
        opt_type = "CE" if direction == "up" else "PE"
        baseline = self._chain.get(opt_type, {}).get(zone.strike) or {}
        self._pending_breach = PendingBreach(
            strike=zone.strike,
            direction=direction,
            breach_ts=ts,
            breach_price=price,
            opt_type=opt_type,
            baseline_oi=baseline.get("oi"),
            baseline_vol=baseline.get("volume"),
            baseline_chain_ts=self._chain_ts,
        )
        if self.metrics and hasattr(self.metrics, "lpbreaker_pending_breach"):
            try:
                self.metrics.lpbreaker_pending_breach.labels(strike=str(zone.strike), direction=direction).set(1)
            except Exception:
                pass
        await self._maybe_refresh_chain(ts, force=True)

    async def _handle_pending_breach(self, price: float, ts: dt.datetime) -> None:
        if not self._pending_breach:
            return
        pending = self._pending_breach
        if pending.stage == "awaiting_failure":
            if pending.absorb_deadline and ts > pending.absorb_deadline:
                self._clear_pending_breach()
                return
            if self._failure_confirmed(pending, price):
                await self._enter_from_breach(pending, ts, price, trade_type="absorption")
                self._clear_pending_breach()
            return
        if self._chain_ts <= pending.baseline_chain_ts:
            await self._maybe_refresh_chain(ts, force=True)
            if self._chain_ts <= pending.baseline_chain_ts:
                return
        entry = self._chain.get(pending.opt_type, {}).get(pending.strike)
        if not entry:
            self._clear_pending_breach()
            return
        if pending.baseline_oi is None:
            pending.baseline_oi = entry.get("oi")
        if pending.baseline_vol is None:
            pending.baseline_vol = entry.get("volume")
        try:
            new_oi = float(entry.get("oi") or 0.0)
            old_oi = float(pending.baseline_oi or 0.0)
            new_vol = float(entry.get("volume") or 0.0)
            old_vol = float(pending.baseline_vol or 0.0)
        except Exception:
            self._clear_pending_breach()
            return
        oi_change_pct = (new_oi - old_oi) / max(old_oi, 1.0)
        vol_delta = new_vol - old_vol
        if self.metrics and hasattr(self.metrics, "lpbreaker_oi_change_pct"):
            try:
                self.metrics.lpbreaker_oi_change_pct.labels(strike=str(pending.strike), opt_type=pending.opt_type).set(oi_change_pct)
                self.metrics.lpbreaker_vol_delta.labels(strike=str(pending.strike), opt_type=pending.opt_type).set(vol_delta)
            except Exception:
                pass
        classification = classify_liquidity_event(
            oi_change_pct,
            vol_delta,
            oi_drop_pct_trigger=self.oi_drop_pct_trigger,
            absorption_oi_rise_pct=self.absorption_oi_rise_pct,
            min_vol_delta=self.min_vol_delta,
        )
        if classification == "stop_grab":
            await self._enter_from_breach(pending, ts, price, trade_type="stop_grab")
            self._clear_pending_breach()
            return
        if classification == "absorption":
            pending.stage = "awaiting_failure"
            pending.absorb_deadline = ts + dt.timedelta(seconds=self.absorption_confirm_seconds)
            return
        self._clear_pending_breach()

    async def _enter_from_breach(self, pending: PendingBreach, ts: dt.datetime, price: float, *, trade_type: str) -> None:
        opt_type = pending.opt_type
        direction = pending.direction
        if trade_type == "absorption":
            opt_type = "PE" if pending.direction == "up" else "CE"
            direction = "down" if pending.direction == "up" else "up"
        await self._enter_trade(opt_type, pending.strike, ts, price, direction, trade_type)

    async def _enter_trade(
        self,
        opt_type: str,
        breach_strike: int,
        ts: dt.datetime,
        spot_price: float,
        direction: str,
        trade_type: str,
    ) -> None:
        if self.risk.should_halt():
            await self.exit_engine.handle_risk_halt()
            return
        threshold = getattr(self.cfg.market_data, "max_tick_age_seconds", 0.0)
        if self.risk.block_if_stale(self._underlying, threshold=threshold):
            return
        if not self._static_ip_ok():
            self._logger.log_event(30, "static_ip_gate_block", symbol=self._underlying)
            return
        if not self._broker_live():
            self._logger.log_event(30, "broker_stream_block", symbol=self._underlying)
            return
        expiry = self._resolve_expiry(ts)
        step = self._strike_step(self._underlying)
        atm = self._round_strike(spot_price, step)
        candidates = [breach_strike, atm, atm + step, atm - step]
        candidates = [s for s in dict.fromkeys(candidates) if s > 0]
        selected = select_liquid_contract(self._chain, candidates, opt_type, min_oi=self.min_oi, min_vol=self.min_vol)
        if not selected:
            return
        strike, entry = selected
        symbol = f"{self._underlying}-{expiry}-{int(strike)}{opt_type}"
        premium = self._option_ltp(entry)
        if premium is None or premium <= 0:
            return
        lot_size = self._resolve_lot_size(expiry, self._underlying)
        qty = self._position_size(premium, lot_size)
        if qty <= 0:
            return
        budget = OrderBudget(symbol=symbol, qty=qty, price=premium, lot_size=lot_size, side="BUY")
        try:
            self.risk.on_tick(symbol, premium)
        except Exception:
            pass
        if not self.risk.budget_ok_for(budget):
            return
        if self.metrics and hasattr(self.metrics, "strategy_position_size"):
            try:
                self.metrics.strategy_position_size.labels(instrument=symbol).set(qty)
            except Exception:
                pass
        await self._publish_signal(symbol, qty, premium, ts, trade_type=trade_type)
        try:
            order = await self.oms.submit(
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
            return
        except Exception as exc:
            self._logger.log_event(40, "order_submit_failed", symbol=symbol, error=str(exc))
            return
        self._entry_intents[order.client_order_id] = EntryIntent(
            strike=strike,
            direction=direction,
            trade_type=trade_type,
            opt_type=opt_type,
            underlying_price=spot_price,
            ts=ts,
        )
        if self.metrics and hasattr(self.metrics, "lpbreaker_signals_total"):
            try:
                self.metrics.lpbreaker_signals_total.labels(type=trade_type).inc()
            except Exception:
                pass
        self._logger.log_event(20, "lpb_submitted", symbol=symbol, strike=strike, opt=opt_type, reason=trade_type)

    async def _evaluate_open_trades(self, price: float, ts: dt.datetime) -> None:
        if not self._open_trades:
            return
        for symbol, trade in list(self._open_trades.items()):
            if self._time_stop_hit(trade, ts):
                await self._exit_position(symbol, ts, reason="TIME_STOP")
                continue
            if self._underlying_invalidation(trade, price):
                await self._exit_position(symbol, ts, reason="UNDERLYING_INVALIDATION")

    def _time_stop_hit(self, trade: OpenTrade, ts: dt.datetime) -> bool:
        if self.max_hold_minutes <= 0:
            return False
        age = (ts - trade.entry_ts).total_seconds() / 60.0
        return age >= self.max_hold_minutes

    def _underlying_invalidation(self, trade: OpenTrade, price: float) -> bool:
        if trade.trade_type == "stop_grab":
            if trade.opt_type == "CE":
                return price <= trade.strike - self.stop_buffer_points
            return price >= trade.strike + self.stop_buffer_points
        if trade.opt_type == "PE":
            return price >= trade.strike + self.stop_buffer_points
        return price <= trade.strike - self.stop_buffer_points

    async def _exit_position(self, symbol: str, ts: dt.datetime, *, reason: str) -> None:
        cooldown = self._exit_cooldown.get(symbol, 0.0)
        if time.time() - cooldown < 2.0:
            return
        plan = self._app.store.load_exit_plan(symbol) if self._app and hasattr(self._app, "store") else None
        if plan and plan.get("pending_exit_reason"):
            return
        position = self._app.store.load_open_position(symbol) if self._app and hasattr(self._app, "store") else None
        if not position:
            return
        qty = int(position.get("qty") or 0)
        if qty <= 0:
            return
        price = float(position.get("avg_price") or 0.0)
        try:
            await self.oms.submit(
                strategy="exit",
                symbol=symbol,
                side="SELL",
                qty=int(abs(qty)),
                order_type="MARKET",
                limit_price=price,
                ts=ts,
            )
        except Exception as exc:
            self._logger.log_event(30, "lpb_exit_submit_failed", symbol=symbol, reason=reason, error=str(exc))
            return
        self._exit_cooldown[symbol] = time.time()
        self._logger.log_event(20, "lpb_exit_submit", symbol=symbol, reason=reason)

    def _clear_pending_breach(self) -> None:
        pending = self._pending_breach
        self._pending_breach = None
        if pending and self.metrics and hasattr(self.metrics, "lpbreaker_pending_breach"):
            try:
                self.metrics.lpbreaker_pending_breach.labels(strike=str(pending.strike), direction=pending.direction).set(0)
            except Exception:
                pass

    def _failure_confirmed(self, pending: PendingBreach, price: float) -> bool:
        if pending.direction == "up":
            return price <= pending.strike - self.reclaim_buffer_points
        return price >= pending.strike + self.reclaim_buffer_points

    async def _fetch_chain(self, symbol: str, ts: dt.datetime) -> None:
        cache = self.instrument_cache
        session = cache.upstox_session()
        expiry = self._resolve_expiry(ts)
        key = cache.resolve_index_key(symbol)

        def _call() -> Any:
            return session.get_option_chain(key, expiry)

        payload = await asyncio.to_thread(_call)
        data = getattr(payload, "data", None) or (payload.get("data") if isinstance(payload, dict) else None) or []
        raw_entries = data
        if isinstance(raw_entries, dict):
            flattened: list[dict[str, Any]] = []

            def _collect(seq: Any, opt: str) -> None:
                rows = seq.get("data") if isinstance(seq, dict) else seq
                if not isinstance(rows, list):
                    return
                for item in rows:
                    if isinstance(item, dict):
                        merged = dict(item)
                        merged["option_type"] = opt
                        flattened.append(merged)

            _collect(raw_entries.get("call") or raw_entries.get("CALL") or raw_entries.get("ce") or raw_entries.get("CE"), "CE")
            _collect(raw_entries.get("put") or raw_entries.get("PUT") or raw_entries.get("pe") or raw_entries.get("PE"), "PE")
            raw_entries = flattened

        self._chain_prev = {"CE": dict(self._chain.get("CE", {})), "PE": dict(self._chain.get("PE", {}))}
        chain: dict[str, dict[int, dict[str, Any]]] = {"CE": {}, "PE": {}}

        def _ingest(opt_type: str, strike_raw: Any, node: Any, expiry_hint: str) -> None:
            if opt_type not in {"CE", "PE"} or not isinstance(node, dict):
                return
            if strike_raw is None:
                return
            try:
                strike_val = int(float(strike_raw))
            except Exception:
                return
            market = node.get("market_data") or node.get("marketData") or node
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
                "bid": market.get("bid_price") or node.get("bid") or node.get("best_bid_price"),
                "ask": market.get("ask_price") or node.get("ask") or node.get("best_ask_price"),
                "expiry": node.get("expiry") or expiry_hint or expiry,
            }
            chain.setdefault(opt_type, {})[strike_val] = entry

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

        if chain.get("CE") or chain.get("PE"):
            self._chain = chain
        self._chain_ts = ts.timestamp()

    def _resolve_expiry(self, ts: dt.datetime) -> str:
        preference = getattr(self.cfg.data, "subscription_expiry_preference", "current")
        if self._subscription_expiry_provider:
            try:
                expiry = self._subscription_expiry_provider(self._underlying)
                if expiry:
                    return expiry
            except Exception:
                pass
        try:
            return pick_subscription_expiry(self._underlying, preference)
        except Exception:
            fallback = resolve_weekly_expiry(
                self._underlying,
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

    def _round_strike(self, price: float, step: int) -> int:
        return int(round(float(price) / float(step)) * step)

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

    def _option_ltp(self, entry: dict[str, Any]) -> Optional[float]:
        raw = entry.get("ltp")
        try:
            val = float(raw) if raw is not None else None
        except Exception:
            val = None
        if val is not None and val > 0:
            return val
        key = entry.get("instrument_key")
        broker = getattr(self._app, "broker", None)
        if key and broker and hasattr(broker, "cached_option_quote"):
            try:
                quote = broker.cached_option_quote(key)
                raw = (quote or {}).get("ltp") or (quote or {}).get("close") or (quote or {}).get("price")
                val = float(raw) if raw is not None else None
            except Exception:
                val = None
        return val if val and val > 0 else None

    def _position_size(self, premium: float, lot_size: int) -> int:
        stop_pct = getattr(self.cfg.exit, "stop_pct", 0.0)
        open_lots = self.risk._total_open_lots(include_pending=True)
        if self.risk_pct > 0:
            qty = compute_position_size(
                self.cfg.capital_base,
                self.risk_pct,
                premium,
                lot_size,
                stop_loss_pct=stop_pct,
                max_open_lots=getattr(self.risk.cfg, "max_open_lots", None),
                open_lots=open_lots,
            )
            if qty > 0:
                try:
                    lot = max(int(lot_size), 1)
                    pending_lots = qty / lot
                    exposure = self.risk._premium_exposure() + (qty * premium)
                    open_lots = open_lots + pending_lots
                    from engine.metrics import set_risk_dials

                    set_risk_dials(open_lots=open_lots, notional_rupees=exposure, daily_stop_rupees=self.risk.cfg.daily_pnl_stop)
                except Exception:
                    pass
            return qty
        return self.risk.position_size(premium=premium, lot_size=lot_size, stop_loss_pct=stop_pct)

    def _parse_strike(self, symbol: str) -> Optional[int]:
        parts = symbol.split("-")
        if len(parts) < 3:
            return None
        tail = parts[-1]
        if tail.endswith(("CE", "PE")):
            tail = tail[:-2]
        try:
            return int(float(tail))
        except Exception:
            return None

    async def _publish_signal(self, symbol: str, qty: int, price: float, ts: dt.datetime, *, trade_type: str) -> None:
        signal = OrderSignal(
            instrument=symbol,
            side="BUY",
            qty=qty,
            order_type="MARKET",
            limit_price=price,
            meta={"strategy": self.cfg.strategy_tag, "signal": trade_type},
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


__all__ = ["LiquidityPoolBreakerStrategy", "detect_breach", "classify_liquidity_event", "select_liquid_contract"]
