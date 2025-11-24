from __future__ import annotations

import asyncio
import math
import os
import time
from dataclasses import dataclass
from typing import Any, Optional

from engine.config import EngineConfig, IST, SmokeTestConfig
from engine.data import resolve_weekly_expiry
from engine.logging_utils import get_logger
from engine.oms import FINAL_STATES, OrderState
from engine.risk import OrderBudget
from engine.time_machine import now as engine_now


@dataclass
class SmokeTestResult:
    entry_price: Optional[float]
    exit_price: Optional[float]
    pnl_realized: Optional[float]
    instrument_key: str
    symbol: str
    expiry: str
    strike: float
    side: str
    status: str
    reason: Optional[str] = None


@dataclass(frozen=True)
class SelectedContract:
    instrument_key: str
    symbol: str
    expiry: str
    strike: int
    opt_type: str
    mid_price: float
    notional: float


def _is_dry_run() -> bool:
    return str(os.getenv("DRY_RUN", "false")).lower() in {"1", "true", "yes", "on"}


def _strike_for_side(spot: float, step: int, side: str) -> int:
    if step <= 0:
        step = 50
    if side.upper() == "PE":
        return int(math.floor(spot / step) * step)
    return int(math.ceil(spot / step) * step)


def _coerce_float(value: Any) -> Optional[float]:
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _best_depth_qty(depth: list[dict[str, Any]], side: str) -> float:
    if not depth:
        return 0.0
    entry = depth[0]
    qty = entry.get("qty") if isinstance(entry, dict) else None
    qty_val = _coerce_float(qty)
    if qty_val is not None:
        return qty_val
    if not isinstance(entry, dict):
        return 0.0
    if side.upper() == "SELL":
        return _coerce_float(entry.get("bidQ") or entry.get("bid_quantity")) or 0.0
    return _coerce_float(entry.get("askQ") or entry.get("ask_quantity")) or 0.0


def _record_abort(logger: Any, store: Any, meter: Any, reason: str, **context: Any) -> None:
    logger.log_event(20, "smoke_test_abort", reason=reason, **context)
    if store:
        try:
            store.record_incident("SMOKE_TEST_ABORT", {"reason": reason, **context}, ts=engine_now(IST))
        except Exception:
            pass
    if meter:
        try:
            meter.smoke_test_runs_total.labels(status="abort").inc()
            meter.smoke_test_last_notional_rupees.set(context.get("notional", 0.0) or 0.0)
            meter.smoke_test_last_reason.labels(reason=reason).set(1)
            meter.smoke_test_last_ts.set(time.time())
            meter.smoke_test_running.set(0)
        except Exception:
            pass


async def _wait_for_final_state(app_ctx: Any, client_order_id: str, *, timeout: float = 180.0) -> Optional[Any]:
    oms = getattr(app_ctx, "oms", None)
    bus = getattr(app_ctx, "bus", None)
    queue = None
    if bus:
        try:
            queue = await bus.subscribe("orders/fill", maxsize=20)
        except Exception:
            queue = None
    deadline = time.monotonic() + timeout
    try:
        while time.monotonic() < deadline:
            order = oms.get_order(client_order_id) if oms else None
            if order and order.state in FINAL_STATES:
                return order
            if queue:
                try:
                    event = await asyncio.wait_for(queue.get(), timeout=0.5)
                except asyncio.TimeoutError:
                    continue
                if event and event.get("order_id") == client_order_id:
                    order = oms.get_order(client_order_id) if oms else None
                    if order and order.state in FINAL_STATES:
                        return order
            else:
                await asyncio.sleep(0.5)
        return oms.get_order(client_order_id) if oms else None
    finally:
        if queue and bus:
            try:
                await bus.unsubscribe("orders/fill", queue)
            except Exception:
                pass


def _build_symbol(underlying: str, expiry: str, strike: int, side: str) -> str:
    return f"{underlying.upper()}-{expiry}-{int(strike)}{side.upper()}"


def _active_expiry(app_ctx: Any, cfg: EngineConfig) -> str:
    underlying = cfg.smoke_test.underlying
    getter = getattr(app_ctx, "_subscription_expiry_for", None)
    if callable(getter):
        try:
            return getter(underlying)
        except Exception:
            pass
    if isinstance(getattr(app_ctx, "subscription_expiries", None), dict):
        cached = app_ctx.subscription_expiries.get(underlying.upper())
        if cached:
            return cached
    return resolve_weekly_expiry(underlying, engine_now(IST), cfg.data.holidays, weekly_weekday=getattr(cfg.data, "weekly_expiry_weekday", 1)).isoformat()


def _mid_price(quote: dict[str, Any]) -> Optional[float]:
    bid = _coerce_float(quote.get("bid"))
    ask = _coerce_float(quote.get("ask"))
    if bid is not None and bid > 0 and ask is not None and ask > 0:
        return (bid + ask) / 2.0
    ltp = _coerce_float(quote.get("ltp"))
    if ltp is not None and ltp > 0:
        return ltp
    return None


def select_affordable_contract(app_ctx: Any, cfg: EngineConfig) -> Optional[SelectedContract]:
    cache = getattr(app_ctx, "instrument_cache", None)
    broker = getattr(app_ctx, "broker", None)
    if not cache or not broker:
        return None
    expiry = _active_expiry(app_ctx, cfg)
    side = cfg.smoke_test.side.upper()
    try:
        contracts = cache.list_contracts_for_expiry(cfg.smoke_test.underlying, expiry, opt_type=side)
    except Exception:
        contracts = []
    try:
        quotes = broker.cached_option_quotes()
    except Exception:
        quotes = {}
    lot_size = max(int(cfg.data.lot_step or 1), 1)
    lots = max(int(cfg.smoke_test.lots or 1), 1)
    max_notional = min(float(cfg.risk.notional_premium_cap), float(cfg.smoke_test.max_notional_rupees))
    best: Optional[SelectedContract] = None
    for row in contracts:
        key = str(row.get("instrument_key") or "")
        if not key:
            continue
        quote = quotes.get(key)
        if not quote:
            try:
                quote = broker.cached_option_quote(key)
            except Exception:
                quote = None
        if not quote:
            continue
        mid = _mid_price(quote)
        if mid is None or mid <= 0:
            continue
        strike_val = row.get("strike")
        try:
            strike_int = int(float(strike_val)) if strike_val is not None else 0
        except (TypeError, ValueError):
            strike_int = 0
        notional = mid * lot_size * lots
        if notional > max_notional:
            continue
        symbol = _build_symbol(cfg.smoke_test.underlying, expiry, strike_int, side)
        candidate = SelectedContract(
            instrument_key=key,
            symbol=symbol,
            expiry=expiry,
            strike=strike_int,
            opt_type=side,
            mid_price=mid,
            notional=notional,
        )
        if best is None or candidate.mid_price < best.mid_price:
            best = candidate
    return best


async def _await_quote(broker: Any, instrument_key: str, *, logger: Any, timeout: float = 5.0) -> Optional[dict[str, Any]]:
    started = time.monotonic()
    subscribed = False
    while time.monotonic() - started <= timeout:
        try:
            quote = broker.cached_option_quote(instrument_key)
        except Exception:
            quote = None
        if quote and (quote.get("bid") is not None or quote.get("ask") is not None):
            return quote
        if not subscribed and hasattr(broker, "subscribe_marketdata"):
            try:
                await broker.subscribe_marketdata([instrument_key])
            except Exception:
                pass
            subscribed = True
        await asyncio.sleep(0.25)
    logger.log_event(20, "smoke_test_quote_timeout", instrument_key=instrument_key)
    return None


async def run_smoke_test_once(app_ctx: Any, cfg: SmokeTestConfig) -> Optional[SmokeTestResult]:
    logger = getattr(app_ctx, "logger", get_logger("SmokeTest"))
    if getattr(app_ctx, "_smoke_test_running", False):
        logger.log_event(20, "smoke_test_skip", reason="in_progress")
        return None
    setattr(app_ctx, "_smoke_test_running", True)
    meter = getattr(app_ctx, "metrics", None)
    if meter:
        try:
            meter.smoke_test_running.set(1)
        except Exception:
            pass
    if not cfg.enabled:
        logger.log_event(20, "smoke_test_skip", reason="disabled")
        setattr(app_ctx, "_smoke_test_running", False)
        if meter:
            try:
                meter.smoke_test_running.set(0)
            except Exception:
                pass
        return None
    if _is_dry_run():
        logger.log_event(20, "smoke_test_skip", reason="dry_run")
        setattr(app_ctx, "_smoke_test_running", False)
        if meter:
            try:
                meter.smoke_test_running.set(0)
            except Exception:
                pass
        return None

    try:
        broker = getattr(app_ctx, "broker", None)
        risk = getattr(app_ctx, "risk", None)
        oms = getattr(app_ctx, "oms", None)
        cache = getattr(app_ctx, "instrument_cache", None)
        pnl_calc = getattr(app_ctx, "pnl", None)
        store = getattr(app_ctx, "store", None)
        config = getattr(app_ctx, "cfg", None)

        if not broker or not risk or not oms or not cache or not config:
            _record_abort(logger, store, meter, "missing_ctx")
            return None

        decision = getattr(broker, "session_guard_decision", {}) or {}
        if decision.get("action") not in (None, "continue"):
            _record_abort(logger, store, meter, "session_not_live", decision=decision)
            return None

        try:
            halt_reason = risk.halt_reason()
        except Exception:
            halt_reason = None
        if halt_reason:
            _record_abort(logger, store, meter, "risk_halt", halt_reason=halt_reason)
            return None

        if risk.has_open_positions():
            _record_abort(logger, store, meter, "positions_open", count=risk.open_position_count())
            return None

        lot_size = max(int(config.data.lot_step or 1), 1)
        lots = max(int(cfg.lots or 1), 1)
        max_notional = min(float(config.risk.notional_premium_cap), float(cfg.max_notional_rupees))

        selected = select_affordable_contract(app_ctx, config)
        if not selected:
            _record_abort(logger, store, meter, "no_affordable_contract", max_notional=max_notional, lot_size=lot_size, lots=lots)
            return None

        if selected.notional > max_notional:
            _record_abort(logger, store, meter, "notional_too_large", notional=selected.notional)
            return None

        instrument_key = selected.instrument_key
        symbol = selected.symbol
        expiry = selected.expiry
        strike = selected.strike

        tick_size = float(getattr(config.data, "tick_size", 0.05) or 0.05)
        try:
            contract_meta = cache.get_contract(cfg.smoke_test.underlying, expiry, strike, cfg.smoke_test.side)
        except Exception:
            contract_meta = None
        if contract_meta:
            tick_size = _coerce_float(contract_meta.get("tick_size")) or tick_size
        else:
            try:
                meta = cache.get_meta(instrument_key)
            except Exception:
                meta = None
            if meta:
                tick_size = _coerce_float(getattr(meta, "tick_size", None)) or tick_size

        quote = await _await_quote(broker, instrument_key, logger=logger, timeout=5.0)
        if not quote:
            _record_abort(logger, store, meter, "no_market_data", instrument_key=instrument_key)
            return None

        bid = _coerce_float(quote.get("bid"))
        ask = _coerce_float(quote.get("ask"))
        ltp = _coerce_float(quote.get("ltp"))
        depth = quote.get("depth") or {}
        bids = depth.get("bids") or []
        asks = depth.get("asks") or []
        bid_qty = _best_depth_qty(bids if isinstance(bids, list) else [], "SELL")
        ask_qty = _best_depth_qty(asks if isinstance(asks, list) else [], "BUY")

        if bid is None or ask is None:
            _record_abort(logger, store, meter, "no_liquidity", instrument_key=instrument_key)
            return None

        spread = ask - bid
        spread_ticks = spread / tick_size if tick_size > 0 else spread
        depth_threshold = max(int(getattr(config.oms.submit, "depth_threshold", 0)), 0)
        max_spread_ticks = max(int(getattr(config.oms.submit, "max_spread_ticks", 2)), 0)
        if spread_ticks > max_spread_ticks:
            _record_abort(logger, store, meter, "wide_spread", spread=spread, spread_ticks=spread_ticks)
            return None

        available_qty = ask_qty
        if available_qty < depth_threshold:
            _record_abort(logger, store, meter, "thin_depth", available=available_qty)
            return None

        qty = lot_size * lots
        price_ref = ask if ask is not None else ltp
        if price_ref is None:
            _record_abort(logger, store, meter, "no_price")
            return None
        notional = float(price_ref) * qty
        if notional > max_notional:
            _record_abort(logger, store, meter, "notional_too_large", notional=notional)
            return None

        budget = OrderBudget(symbol=symbol, qty=qty, price=float(price_ref), lot_size=lot_size, side="BUY")
        if not risk.budget_ok_for(budget):
            _record_abort(logger, store, meter, "risk_blocked", symbol=symbol)
            return None

        oms.update_market_depth(symbol, bid=bid or 0.0, ask=ask or 0.0, bid_qty=int(bid_qty), ask_qty=int(ask_qty))

        realized_before = None
        try:
            if pnl_calc:
                realized_before = pnl_calc.totals()[0]
        except Exception:
            realized_before = None

        ts_now = engine_now(IST)
        entry_order = await oms.submit(
            strategy=getattr(config, "strategy_tag", "smoke_test"),
            symbol=symbol,
            side="BUY",
            qty=qty,
            order_type="MARKET",
            limit_price=float(price_ref),
            ts=ts_now,
        )
        entry_order = await _wait_for_final_state(app_ctx, entry_order.client_order_id)
        if not entry_order or entry_order.state != OrderState.FILLED:
            _record_abort(logger, store, meter, "entry_not_filled", state=str(getattr(entry_order, "state", "missing")))
            return None

        entry_price = entry_order.avg_fill_price or price_ref
        if store:
            store.record_incident(
                "SMOKE_TEST_FILLED",
                {"symbol": symbol, "instrument_key": instrument_key, "qty": qty, "price": entry_price},
                ts=ts_now,
            )
        logger.log_event(20, "smoke_test_filled", symbol=symbol, instrument_key=instrument_key, qty=qty, price=entry_price)

        await asyncio.sleep(max(int(cfg.hold_seconds), 10))

        exit_quote = quote
        try:
            fresh = broker.cached_option_quote(instrument_key)
            if fresh:
                exit_quote = fresh
        except Exception:
            pass

        bid_exit = _coerce_float(exit_quote.get("bid")) if exit_quote else None
        ask_exit = _coerce_float(exit_quote.get("ask")) if exit_quote else None
        ltp_exit = _coerce_float(exit_quote.get("ltp")) if exit_quote else None
        depth_exit = exit_quote.get("depth") if exit_quote else {}
        bids_exit = depth_exit.get("bids") if isinstance(depth_exit, dict) else []
        asks_exit = depth_exit.get("asks") if isinstance(depth_exit, dict) else []
        bid_qty_exit = _best_depth_qty(bids_exit if isinstance(bids_exit, list) else [], "SELL")
        ask_qty_exit = _best_depth_qty(asks_exit if isinstance(asks_exit, list) else [], "BUY")
        oms.update_market_depth(symbol, bid=bid_exit or bid or 0.0, ask=ask_exit or ask or 0.0, bid_qty=int(bid_qty_exit), ask_qty=int(ask_qty_exit))

        exit_price_hint = bid_exit if bid_exit is not None else ltp_exit or bid
        exit_order = await oms.submit(
            strategy=getattr(config, "strategy_tag", "smoke_test"),
            symbol=symbol,
            side="SELL",
            qty=qty,
            order_type="MARKET",
            limit_price=float(exit_price_hint) if exit_price_hint is not None else None,
            ts=engine_now(IST),
        )
        exit_order = await _wait_for_final_state(app_ctx, exit_order.client_order_id)
        if not exit_order or exit_order.state != OrderState.FILLED:
            _record_abort(logger, store, meter, "exit_not_filled", state=str(getattr(exit_order, "state", "missing")))
            return None

        exit_price = exit_order.avg_fill_price or exit_price_hint or entry_price

        await asyncio.sleep(0.1)
        flat = True
        if store:
            try:
                with store._lock:  # type: ignore[attr-defined]
                    cur = store._conn.execute(  # type: ignore[attr-defined]
                        "SELECT 1 FROM positions WHERE run_id=? AND symbol=? AND ABS(qty) > 0 AND (closed_at IS NULL OR closed_at='')",
                        (getattr(store, "run_id", ""), symbol),
                    )
                    flat = cur.fetchone() is None
            except Exception:
                flat = True
        if not flat:
            _record_abort(logger, store, meter, "position_not_flat", symbol=symbol)
            return None

        pnl_realized = None
        try:
            if pnl_calc:
                realized_after = pnl_calc.totals()[0]
                if realized_after is not None and realized_before is not None:
                    pnl_realized = realized_after - realized_before
        except Exception:
            pnl_realized = None
        if pnl_realized is None and exit_price is not None and entry_price is not None:
            pnl_realized = (exit_price - entry_price) * qty

        payload = {
            "entry_price": entry_price,
            "exit_price": exit_price,
            "pnl_realized": pnl_realized,
            "hold_seconds": int(cfg.hold_seconds),
            "instrument_key": instrument_key,
            "expiry": expiry,
            "strike": strike,
            "side": cfg.side,
            "symbol": symbol,
            "lots": lots,
            "lot_size": lot_size,
            "entry_notional": selected.notional,
        }
        if store:
            store.record_incident("SMOKE_TEST_OK", payload, ts=engine_now(IST))
        logger.log_event(20, "smoke_test_done", **payload)
        if meter:
            try:
                meter.smoke_test_runs_total.labels(status="ok").inc()
                meter.smoke_test_last_notional_rupees.set(selected.notional)
                meter.smoke_test_last_reason.labels(reason="ok").set(1)
                meter.smoke_test_last_ts.set(time.time())
                meter.smoke_test_running.set(0)
            except Exception:
                pass

        result = SmokeTestResult(
            entry_price=entry_price,
            exit_price=exit_price,
            pnl_realized=pnl_realized,
            instrument_key=instrument_key,
            symbol=symbol,
            expiry=expiry,
            strike=strike,
            side=cfg.side,
            status="completed",
        )
        return result
    except Exception as exc:
        logger.log_event(40, "smoke_test_error", error=str(exc))
        if meter:
            try:
                meter.smoke_test_runs_total.labels(status="error").inc()
                meter.smoke_test_last_reason.labels(reason="error").set(1)
                meter.smoke_test_last_ts.set(time.time())
                meter.smoke_test_running.set(0)
            except Exception:
                pass
        return None
    finally:
        try:
            if meter:
                meter.smoke_test_running.set(0)
        except Exception:
            pass
        setattr(app_ctx, "_smoke_test_running", False)


__all__ = ["run_smoke_test_once", "SmokeTestConfig", "SmokeTestResult"]
