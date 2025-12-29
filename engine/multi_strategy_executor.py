from __future__ import annotations

import asyncio
import datetime as dt
from concurrent.futures import ThreadPoolExecutor
from typing import Any, Callable, Dict, List, Optional, Sequence

from engine.config import EngineConfig, IST
from engine.data import get_app_config
from engine.events import EventBus
from engine.exit import ExitEngine
from engine.intent_router import IntentRouter
from engine.logging_utils import get_logger
from engine.metrics import EngineMetrics
from engine.oms import OMS, OrderValidationError
from engine.risk import RiskManager
from engine.signal_engine import SignalEngine
from engine.sizing import SizingInputs, compute_qty, risk_fraction
from engine.strategy_manager import StrategyManager
from market.instrument_cache import InstrumentCache
from strategy.base import BaseStrategy
from strategy.contracts import StrategyContext, StrategyDecision, StrategyState, TradeIntent
from strategy.market_snapshot import MarketSnapshot, OptionQuote


class _ExecContext(StrategyContext):
    def __init__(self, *, cfg: EngineConfig, clock: Callable[[], dt.datetime], state_lookup: Callable[[str], StrategyState]):
        self.cfg = cfg
        self._clock = clock
        self._state_lookup = state_lookup

    def now(self) -> dt.datetime:
        return self._clock()

    def state_for(self, strategy_id: str) -> StrategyState:
        return self._state_lookup(strategy_id)


class MultiStrategyExecutor(BaseStrategy):
    """Runs multiple intent-only strategies and routes them through centralized gates."""

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
        *,
        app_config: Optional[dict] = None,
    ) -> None:
        super().__init__(config, risk, oms, bus, exit_engine, instrument_cache, metrics, subscription_expiry_provider)
        self._logger = get_logger("MultiStrategyExecutor")
        self._raw_cfg = dict(app_config or get_app_config())
        ms = self._raw_cfg.get("multi_strategy") or {}
        self._parallel = bool(getattr(ms, "get", lambda *_: False)("parallel", False))
        self._max_workers = int(getattr(ms, "get", lambda *_: 0)("max_workers", 0) or 0)
        self._allow_straddle = bool(getattr(ms, "get", lambda *_: False)("allow_straddle", False))
        self._max_global_positions = int(getattr(ms, "get", lambda *_: 0)("max_global_positions", 0) or 0)

        gates = ms.get("signal_gates") if isinstance(ms, dict) else {}
        gates = gates if isinstance(gates, dict) else {}
        spread_default = float(getattr(config.strategy, "spread_max_pct", 0.0) or 0.0)
        self._signal = SignalEngine(
            delta_min=float(gates.get("delta_min", 0.25)),
            delta_max=float(gates.get("delta_max", 0.55)),
            spread_pct_max=float(gates.get("spread_pct_max", spread_default)),
            iv_zscore_max=float(gates.get("iv_zscore_max", 0.0)),
            iv_percentile_min=float(gates.get("iv_percentile_min", 0.0)),
            oi_percentile_min=float(gates.get("oi_percentile_min", 0.0)),
        )
        self._strategies = StrategyManager.from_app_config(cfg=config, app_config=self._raw_cfg)
        self._router = IntentRouter(
            risk=risk,
            signal_engine=self._signal,
            strategies=self._strategies,
            allow_straddle=self._allow_straddle,
            max_global_positions=self._max_global_positions,
        )

        self._spot: Dict[str, float] = {}
        self._options: Dict[str, OptionQuote] = {}  # key: instrument_key or symbol
        self._pool: Optional[ThreadPoolExecutor] = None

    async def init(self, app: Any) -> None:
        await super().init(app)
        self._pool = ThreadPoolExecutor(max_workers=self._max_workers or None) if self._parallel else None
        self._logger.log_event(
            20,
            "multi_strategy_init",
            strategies=len(self._strategies.enabled_strategy_ids()),
            parallel=int(self._parallel),
            max_workers=int(self._max_workers or 0),
        )

    async def run(self, stop_event: asyncio.Event) -> None:
        await self._ensure_initialized()
        market_q = await self.bus.subscribe("market/events", maxsize=2000)
        fills_q = await self.bus.subscribe("orders/fill", maxsize=1000)
        tasks = [
            asyncio.create_task(self._consume_ticks(stop_event, market_q), name="multistrat-ticks"),
            asyncio.create_task(self._consume_fills(stop_event, fills_q), name="multistrat-fills"),
        ]
        try:
            await stop_event.wait()
        finally:
            for task in tasks:
                task.cancel()
            for task in tasks:
                try:
                    await task
                except asyncio.CancelledError:
                    continue
            await self._shutdown()

    async def _shutdown(self) -> None:
        # Best-effort hook for strategies to reset state or flush telemetry.
        try:
            ctx = _ExecContext(cfg=self.cfg, clock=lambda: dt.datetime.now(IST), state_lookup=self._strategies.state_for)
            for sid in self._strategies.enabled_strategy_ids():
                strat = self._strategies.strategy(sid)
                if strat:
                    strat.end_of_day_flatten(ctx)
        except Exception:
            pass
        if self._pool:
            try:
                self._pool.shutdown(wait=False, cancel_futures=True)
            except Exception:
                pass
            self._pool = None

    async def on_tick(self, event: dict) -> None:
        evt_type = event.get("type")
        if evt_type != "tick":
            return
        payload = event.get("payload") or {}
        ts = MarketSnapshot.coerce_ts(event.get("ts") or payload.get("ts"), default_tz=IST)
        symbol = str(payload.get("symbol") or payload.get("underlying") or "").upper()
        ltp_val = payload.get("ltp") if payload.get("ltp") is not None else payload.get("price")
        try:
            ltp = float(ltp_val)
        except Exception:
            return
        opt_type = MarketSnapshot.normalize_option_type(payload.get("opt_type") or payload.get("option_type"))
        is_option = opt_type in {"CE", "PE"} or ("-" in str(payload.get("symbol") or "") and str(payload.get("symbol") or "").endswith(("CE", "PE")))
        if is_option:
            self._ingest_option_tick(payload, ts, ltp)
            option_symbol = str(payload.get("symbol") or payload.get("instrument_key") or payload.get("instrument") or "")
            try:
                self.risk.on_tick(option_symbol, ltp)
            except Exception:
                pass
            try:
                await self.exit_engine.on_tick(option_symbol, ltp, ts, oi=payload.get("oi"), iv=payload.get("iv"))
            except Exception:
                pass
            return

        underlying = (payload.get("underlying") or payload.get("symbol") or self.cfg.data.index_symbol).upper()
        self._spot[underlying] = ltp
        await self._run_strategies(ts, underlying, ltp)

    async def on_fill(self, fill: dict) -> None:
        try:
            self._strategies.on_fill(fill)
        except Exception:
            return

    # ---------------------------------------------------------------- internals
    def _ingest_option_tick(self, payload: dict, ts: dt.datetime, ltp: float) -> None:
        underlying = str(payload.get("underlying") or "").upper()
        symbol = str(payload.get("symbol") or "")
        instrument_key = str(payload.get("instrument_key") or payload.get("instrument") or "")
        expiry = str(payload.get("expiry") or "")
        strike = payload.get("strike")
        try:
            strike_int = int(float(strike)) if strike is not None else 0
        except Exception:
            strike_int = 0
        opt_type = MarketSnapshot.normalize_option_type(payload.get("opt_type") or payload.get("option_type"))
        if not opt_type or not underlying or not expiry or strike_int <= 0:
            return
        quote = OptionQuote(
            instrument_key=instrument_key,
            symbol=symbol or instrument_key,
            underlying=underlying,
            expiry=expiry,
            strike=strike_int,
            opt_type=opt_type,
            ltp=float(ltp),
            bid=_coerce_float(payload.get("bid")),
            ask=_coerce_float(payload.get("ask")),
            iv=_coerce_float(payload.get("iv")),
            oi=_coerce_float(payload.get("oi")),
            delta=_coerce_float(payload.get("delta")),
            gamma=_coerce_float(payload.get("gamma")),
            theta=_coerce_float(payload.get("theta")),
            vega=_coerce_float(payload.get("vega")),
            ts=ts,
        )
        key = instrument_key or symbol
        if key:
            self._options[key] = quote

    async def _run_strategies(self, ts: dt.datetime, underlying: str, spot: float) -> None:
        expiry = None
        if self._subscription_expiry_provider:
            try:
                expiry = self._subscription_expiry_provider(underlying)
            except Exception:
                expiry = None
        option_quotes = [q for q in self._options.values() if q.underlying == underlying and (expiry is None or q.expiry == expiry)]
        snapshot = MarketSnapshot.from_tick(ts=ts, underlying=underlying, spot=spot, option_quotes=option_quotes, meta={"expiry": expiry or ""})
        self._signal.observe(snapshot)
        ctx = _ExecContext(cfg=self.cfg, clock=lambda: ts, state_lookup=self._strategies.state_for)

        intents: list[TradeIntent] = []
        strategies = [(sid, self._strategies.strategy(sid)) for sid in self._strategies.enabled_strategy_ids()]
        strategies = [(sid, strat) for sid, strat in strategies if strat is not None]

        if self._parallel and strategies:
            loop = asyncio.get_running_loop()
            tasks = [
                loop.run_in_executor(self._pool, _safe_on_tick, strat, snapshot, ctx)  # type: ignore[arg-type]
                for _, strat in strategies
            ]
            results = await asyncio.gather(*tasks, return_exceptions=True)
            for (sid, _), result in zip(strategies, results):
                if isinstance(result, Exception):
                    self._logger.log_event(30, "strategy_tick_failed", strategy_id=sid, error=str(result))
                    continue
                intents.extend(_normalize_decision(sid, result).intents)
        else:
            for sid, strat in strategies:
                if not self._strategies.can_run(sid, now=ts):
                    continue
                try:
                    decision = strat.on_tick(snapshot, ctx)
                except Exception as exc:
                    self._logger.log_event(30, "strategy_tick_failed", strategy_id=sid, error=str(exc))
                    continue
                intents.extend(_normalize_decision(sid, decision).intents)

        for intent in intents:
            try:
                self.metrics.multi_strategy_intents_total.labels(strategy_id=intent.strategy_id, status="emitted").inc()
            except Exception:
                pass
            self._strategies.apply_post_decision(intent, now=ts)

        routed = self._router.route(intents, snapshot, now=ts)
        for intent, code, reason in routed.rejected:
            try:
                self.metrics.multi_strategy_intents_total.labels(strategy_id=intent.strategy_id, status="rejected").inc()
            except Exception:
                pass
            self._logger.log_event(20, "intent_rejected", strategy_id=intent.strategy_id, code=code, reason=reason, symbol=intent.symbol)

        for intent in routed.accepted:
            await self._submit_intent(intent, snapshot, ts)

    async def _submit_intent(self, intent: TradeIntent, snapshot: MarketSnapshot, ts: dt.datetime) -> None:
        quote = snapshot.by_symbol().get(intent.symbol)
        if quote is None:
            self._logger.log_event(30, "intent_no_quote", strategy_id=intent.strategy_id, symbol=intent.symbol)
            return
        premium = _first_float(quote.ask, quote.ltp, quote.bid)
        if premium is None or premium <= 0:
            self._logger.log_event(30, "intent_no_premium", strategy_id=intent.strategy_id, symbol=intent.symbol)
            return
        lot_size = _resolve_lot_size(self.instrument_cache, quote.instrument_key, quote.underlying, quote.expiry, default=int(self.cfg.data.lot_step))
        budget = self._strategies.budget_for(intent.strategy_id)
        qty = intent.qty
        if qty <= 0:
            rf = risk_fraction(budget.risk_percent_per_trade or getattr(self.cfg.risk, "risk_percent_per_trade", 0.0))
            portfolio_cap = float(getattr(self.cfg.risk, "notional_premium_cap", 0.0) or 0.0)
            try:
                portfolio_open = float(self.risk.premium_exposure())
            except Exception:
                portfolio_open = 0.0
            qty = compute_qty(
                SizingInputs(
                    capital_base=float(getattr(self.cfg, "capital_base", 0.0) or 0.0),
                    risk_fraction=rf,
                    premium=float(premium),
                    lot_size=int(lot_size),
                    max_premium_per_trade=float(budget.max_premium_per_trade or 0.0),
                    portfolio_cap=portfolio_cap,
                    portfolio_open_premium=portfolio_open,
                )
            )
        if qty <= 0:
            self._logger.log_event(20, "intent_zero_qty", strategy_id=intent.strategy_id, symbol=intent.symbol)
            return

        max_lots = getattr(self.cfg.risk, "max_open_lots", None)
        if max_lots is not None and max_lots > 0:
            try:
                max_qty = int(max_lots) * int(lot_size)
                qty = min(int(qty), max_qty)
            except Exception:
                pass
        if qty <= 0 or qty % max(lot_size, 1) != 0:
            self._logger.log_event(30, "intent_qty_invalid", strategy_id=intent.strategy_id, qty=qty, lot_size=lot_size, symbol=intent.symbol)
            return

        from engine.risk import OrderBudget

        ob = OrderBudget(symbol=intent.symbol, qty=qty, price=premium, lot_size=lot_size, side="BUY")
        try:
            spread_pct = quote.spread_pct()
            self.risk.record_expected_price(intent.symbol, premium, spread_pct)
        except Exception:
            pass
        self.risk.on_tick(intent.symbol, premium)
        if not self.risk.budget_ok_for(ob, now=ts):
            self._logger.log_event(20, "intent_risk_reject", strategy_id=intent.strategy_id, symbol=intent.symbol)
            return

        order_type = "MARKET"
        limit_price = premium
        if intent.entry_style == "LIMIT":
            order_type = "IOC_LIMIT"
            limit_price = float(intent.limit_price or premium)

        try:
            await self.bus.publish(
                "orders/signal",
                {
                    "ts": ts.isoformat(),
                    "type": "signal",
                    "payload": {
                        "instrument": intent.symbol,
                        "side": "BUY",
                        "qty": qty,
                        "order_type": order_type,
                        "limit_price": limit_price,
                        "meta": {"strategy_id": intent.strategy_id, "reason": intent.reason, "score": intent.signal_score},
                    },
                },
            )
        except Exception:
            pass

        try:
            order = await self.oms.submit(
                strategy=intent.strategy_id,
                symbol=intent.symbol,
                side="BUY",
                qty=qty,
                order_type=order_type,
                limit_price=limit_price,
                ts=ts,
            )
        except OrderValidationError as exc:
            self._logger.log_event(30, "order_validation_failed", strategy_id=intent.strategy_id, symbol=intent.symbol, code=exc.code, message=str(exc))
            try:
                self.metrics.multi_strategy_intents_total.labels(strategy_id=intent.strategy_id, status="order_validation_reject").inc()
            except Exception:
                pass
            return
        except Exception as exc:
            self._logger.log_event(40, "order_submit_failed", strategy_id=intent.strategy_id, symbol=intent.symbol, error=str(exc))
            return

        self._strategies.record_order_submission(client_order_id=order.client_order_id, strategy_id=intent.strategy_id, symbol=intent.symbol)
        try:
            self.metrics.multi_strategy_intents_total.labels(strategy_id=intent.strategy_id, status="submitted").inc()
        except Exception:
            pass
        self._logger.log_event(20, "order_submitted", strategy_id=intent.strategy_id, symbol=intent.symbol, qty=qty, price=premium)


def _safe_on_tick(strategy: Any, snapshot: MarketSnapshot, ctx: StrategyContext) -> StrategyDecision:
    return strategy.on_tick(snapshot, ctx)


def _normalize_decision(strategy_id: str, decision: Any) -> StrategyDecision:
    if isinstance(decision, StrategyDecision):
        return decision
    return StrategyDecision(intents=[], debug={"error": f"bad_decision:{strategy_id}"})


def _coerce_float(value: Any) -> Optional[float]:
    if value is None:
        return None
    try:
        return float(value)
    except Exception:
        return None


def _first_float(*values: Any) -> Optional[float]:
    for value in values:
        val = _coerce_float(value)
        if val is not None:
            return val
    return None


def _resolve_lot_size(cache: InstrumentCache, instrument_key: str, underlying: str, expiry: str, *, default: int) -> int:
    try:
        meta = cache.get_meta(instrument_key) if instrument_key else None
    except Exception:
        meta = None
    if meta and getattr(meta, "lot_size", None):
        try:
            return max(int(meta.lot_size), 1)
        except Exception:
            pass
    try:
        exp_meta = cache.get_meta(underlying, expiry)
    except Exception:
        exp_meta = None
    if isinstance(exp_meta, tuple) and len(exp_meta) >= 2 and exp_meta[1]:
        try:
            return max(int(exp_meta[1]), 1)
        except Exception:
            pass
    return max(int(default or 1), 1)


__all__ = ["MultiStrategyExecutor"]
