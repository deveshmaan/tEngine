from __future__ import annotations

import argparse
import asyncio
import datetime as dt
import os
import random
import signal
import time
from pathlib import Path
from typing import Callable, List, Optional

from dataclasses import replace

from brokerage.upstox_client import CredentialError, load_upstox_credentials
from engine.charges import UpstoxChargesClient
from engine import smoke_test
from engine.alerts import configure_alerts, notify_incident
from engine.broker import UpstoxBroker
from engine.config import EngineConfig, IST
from engine.config_sanity import ConfigError, sanity_check_config
from engine.data import pick_strike_from_spot, pick_subscription_expiry, record_tick_seen, resolve_weekly_expiry
from engine.events import EventBus
from engine.fees import FeeRow, load_fee_config
from engine.instruments import InstrumentResolver
from engine.logging_utils import configure_logging, get_logger
from engine.metrics import EngineMetrics, bind_global_metrics, set_subscription_expiry, start_http_server_if_available
try:
    from engine.metrics import set_capital_config, set_exit_config_metrics, set_risk_dials, set_scalping_config_metrics, set_strategy_config_metrics
except Exception:  # pragma: no cover
    def set_risk_dials(**kwargs): ...
    def set_strategy_config_metrics(*args, **kwargs): ...
    def set_capital_config(*args, **kwargs): ...
    def set_exit_config_metrics(*args, **kwargs): ...
    def set_scalping_config_metrics(*args, **kwargs): ...
from engine.oms import OMS
from engine.pnl import Execution as PnLExecution, PnLCalculator
from engine.recovery import RecoveryManager, enforce_intraday_clean_start
from engine.replay import ReplayConfig, configure_runtime as configure_replay_runtime, replay
from engine.risk import OrderBudget, RiskManager
from engine.exit import ExitEngine
from engine.time_machine import now as engine_now
from persistence import SQLiteStore
from market.instrument_cache import InstrumentCache
from strategy import AdvancedBuyStrategy, IntradayBuyStrategy, ScalpingBuyStrategy

try:  # pragma: no cover - optional acceleration
    import uvloop

    uvloop.install()
except Exception:  # pragma: no cover
    pass


class EngineApp:
    def __init__(self, config: EngineConfig):
        self.cfg = config
        self.logger = get_logger("EngineApp")
        self.bus = EventBus()
        self.store = SQLiteStore(config.persistence_path, run_id=config.run_id)
        self._lot_size_fallbacks: set[str] = set()
        probe_expiries = os.getenv("UPSTOX_ENABLE_EXPIRY_PROBE", "true").lower() in {"1", "true", "yes"}
        self.instrument_cache = InstrumentCache(
            str(config.persistence_path),
            weekly_expiry_weekday=config.data.weekly_expiry_weekday,
            holidays=config.data.holidays,
            enable_remote_expiry_probe=probe_expiries,
            expiry_ttl_minutes=config.data.expiry_ttl_minutes,
        )
        self.instrument_resolver = InstrumentResolver(self.instrument_cache)
        self.metrics = EngineMetrics()
        bind_global_metrics(self.metrics)
        try:
            set_strategy_config_metrics(
                config.strategy.short_ma,
                config.strategy.long_ma,
                config.strategy.iv_threshold,
                config.strategy.vol_breakout_mult,
                getattr(config.banknifty, "vol_breakout_mult", None),
            )
            set_capital_config(config.capital_base, config.risk.risk_percent_per_trade)
            set_exit_config_metrics(
                getattr(config.exit, "trailing_pct", 0.0),
                getattr(config.exit, "trailing_step", 0.0),
                getattr(config.exit, "time_buffer_minutes", 0),
                getattr(config.exit, "partial_tp_mult", 0.0),
                getattr(config.exit, "at_pct", 0.0),
            )
            set_scalping_config_metrics(
                getattr(config.strategy, "breakout_window", 0),
                getattr(config.strategy, "breakout_margin", 0.0),
                getattr(config.strategy, "volume_mult", 0.0),
                getattr(config.strategy, "pcr_range", (0.0, 0.0))[0] if getattr(config.strategy, "pcr_range", None) else 0.0,
                getattr(config.strategy, "pcr_range", (0.0, 0.0))[1] if getattr(config.strategy, "pcr_range", None) else 0.0,
                getattr(config.strategy, "spread_max_pct", 0.0),
                getattr(config.risk, "scalping_risk_pct", 0.0),
            )
        except Exception:
            pass
        # Ensure critical capital/risk gauges are populated even if an earlier metrics call failed.
        try:
            set_capital_config(config.capital_base, config.risk.risk_percent_per_trade)
        except Exception:
            self.logger.log_event(30, "metrics_capital_config_failed")
        self.risk = RiskManager(config.risk, self.store, capital_base=config.capital_base)
        self.subscription_expiries: dict[str, str] = {}
        self.broker = UpstoxBroker(
            config=config.broker,
            ws_failure_callback=self._on_ws_failure,
            instrument_resolver=self.instrument_resolver,
            instrument_cache=self.instrument_cache,
            metrics=self.metrics,
            auth_halt_callback=self.risk.halt_new_entries,
            risk_manager=self.risk,
            bus=self.bus,
            allowed_ips=config.allowed_ips,
        )
        default_meta = (config.data.tick_size, config.data.lot_step, config.data.price_band_low, config.data.price_band_high)
        self.oms = OMS(
            broker=self.broker,
            store=self.store,
            config=config.oms,
            bus=self.bus,
            metrics=self.metrics,
            default_meta=default_meta,
            square_off_time=config.risk.square_off_by,
        )
        self.broker.bind_reconcile_callback(self.oms.reconcile_from_broker)
        self.broker.bind_square_off_callback(self._execute_square_off)
        self.broker.bind_shutdown_callback(self.trigger_shutdown)
        self.exit_engine = ExitEngine(
            config=config.exit,
            risk=self.risk,
            oms=self.oms,
            store=self.store,
            tick_size=config.data.tick_size,
            metrics=self.metrics,
            iv_exit_threshold=getattr(config.strategy, "iv_exit_percentile", 0.0),
            rest_ltp_provider=self.broker.fetch_ltp,
            max_tick_age_seconds=getattr(config.market_data, "max_tick_age_seconds", 0.0),
        )
        tag = getattr(config, "strategy_tag", "").lower()
        if tag == "advanced-buy":
            strategy_cls = AdvancedBuyStrategy
        elif tag == "scalping-buy":
            strategy_cls = ScalpingBuyStrategy
        else:
            strategy_cls = IntradayBuyStrategy
        self.strategy = strategy_cls(
            config,
            self.risk,
            self.oms,
            self.bus,
            self.exit_engine,
            self.instrument_cache,
            self.metrics,
            subscription_expiry_provider=self._subscription_expiry_for,
        )
        self.fee_config = load_fee_config()
        self.pnl = PnLCalculator(self.store, self.fee_config)
        self.charges_client = self._init_charges_client()
        configure_alerts(config.alerts.throttle_seconds)
        self._stop = asyncio.Event()
        self._market_task: Optional[asyncio.Task] = None
        self._square_task: Optional[asyncio.Task] = None
        self._strategy_task: Optional[asyncio.Task] = None
        self._reconcile_task: Optional[asyncio.Task] = None
        self._heartbeat_task: Optional[asyncio.Task] = None
        self._tasks: List[asyncio.Task] = []

    def _choose_subscription_expiry(self, symbol: str) -> str:
        preference = getattr(self.cfg.data, "subscription_expiry_preference", "current")
        pref = preference if preference in {"current", "next", "monthly"} else "current"
        try:
            expiry = pick_subscription_expiry(symbol, pref)
        except Exception as exc:
            self.logger.log_event(30, "subscription_expiry_fallback", symbol=symbol, preference=pref, error=str(exc))
            fallback = resolve_weekly_expiry(
                symbol,
                engine_now(IST),
                self.cfg.data.holidays,
                weekly_weekday=self.cfg.data.weekly_expiry_weekday,
            )
            expiry = fallback.isoformat()
            pref = "current"
        try:
            if self.broker:
                self.broker.record_subscription_expiry(symbol, expiry, pref)
            else:
                set_subscription_expiry(symbol, expiry, pref)
        except Exception:
            set_subscription_expiry(symbol, expiry, pref)
        return expiry

    def _subscription_expiry_for(self, symbol: str) -> str:
        sym = symbol.upper()
        expiry = self.subscription_expiries.get(sym)
        if expiry:
            return expiry
        expiry = self._choose_subscription_expiry(sym)
        self.subscription_expiries[sym] = expiry
        return expiry

    async def run(self, replay_cfg: Optional[ReplayConfig] = None) -> None:
        configure_logging("INFO")
        self.logger.log_event(20, "engine_starting", run_id=self.cfg.run_id, persistence=str(self.cfg.persistence_path))
        self._install_signal_handlers()
        configure_replay_runtime(bus=self.bus, store=self.store)
        metrics_port = self._resolve_metrics_port()
        metrics_started = start_http_server_if_available(metrics_port)
        self.logger.log_event(20, "metrics_bootstrap", port=metrics_port, started=metrics_started)
        self.metrics.engine_up.set(1)
        try:
            self.metrics.beat()
        except Exception:
            pass
        self._subscription_expiry_for(self.cfg.data.index_symbol)
        await self.broker.start()
        decision = self.broker.session_guard_decision or {"action": "continue", "state": 1, "positions": 0}
        halt_flag = 1 if decision.get("action") != "continue" else 0
        self.logger.log_event(20, "session_guard", state=decision.get("state", 1), halt=halt_flag, positions=decision.get("positions", 0))
        if decision.get("action") == "shutdown":
            await self.trigger_shutdown("session_guard_shutdown")
            await self._cleanup()
            return
        await RecoveryManager(self.store, self.oms).reconcile()
        for pos in self.store.list_open_positions():
            try:
                self.exit_engine.on_fill(
                    symbol=pos["symbol"],
                    side="BUY",
                    qty=int(pos.get("qty") or 0),
                    price=float(pos.get("avg_price") or 0.0),
                    ts=pos.get("opened_at") or engine_now(IST),
                )
            except Exception:
                self.logger.log_event(30, "exit_plan_reseed_failed", symbol=pos.get("symbol"))
        self._square_task = asyncio.create_task(self._square_off_watchdog(), name="square-off")
        tasks: List[asyncio.Task] = [self._square_task]
        if replay_cfg:
            self._market_task = asyncio.create_task(self._run_replay_source(replay_cfg), name="replay-source")
        elif _is_dry_run_env():
            self._market_task = asyncio.create_task(self._live_market_feed(), name="market-feed")
        if self._market_task:
            tasks.append(self._market_task)
        fills_task = asyncio.create_task(self._consume_fills(), name="fills-consumer")
        marks_task = asyncio.create_task(self._consume_market_marks(), name="pnl-marks")
        snapshot_task = asyncio.create_task(self._pnl_snapshot_loop(), name="pnl-snapshot")
        control_task = asyncio.create_task(self._watch_control_intents(), name="control-intents")
        self._heartbeat_task = asyncio.create_task(self._heartbeat_loop(), name="metrics-heartbeat")
        if not replay_cfg:
            self._reconcile_task = asyncio.create_task(self._reconcile_loop(), name="order-reconcile")
            tasks.append(self._reconcile_task)
        tasks.extend([fills_task, marks_task, snapshot_task, control_task, self._heartbeat_task])
        self._tasks = [task for task in tasks if task]

        should_run_smoke = bool(self.cfg.smoke_test.enabled and not _is_dry_run_env() and replay_cfg is None)
        if should_run_smoke:
            smoke_task = asyncio.create_task(smoke_test.run_smoke_test_once(self, self.cfg.smoke_test), name="smoke-test-auto")
            self._tasks.append(smoke_task)
            try:
                await smoke_task
            except Exception as exc:
                self.logger.log_event(30, "smoke_test_error", error=str(exc))
        self._start_strategy()
        await self._stop.wait()
        for task in self._tasks:
            task.cancel()
        for task in self._tasks:
            try:
                await task
            except asyncio.CancelledError:
                pass
        await self._cleanup()

    async def trigger_shutdown(self, reason: str) -> None:
        if not self._stop.is_set():
            self.logger.log_event(20, "shutdown", reason=reason)
            self._stop.set()

    async def _cleanup(self) -> None:
        await self.oms.cancel_all(reason="shutdown")
        await self.broker.stop()
        try:
            self.store.close()
        except Exception:
            pass
        self.metrics.engine_up.set(0)
        self.logger.log_event(20, "engine_stopped")

    async def _heartbeat_loop(self) -> None:
        """Emit heartbeat gauge regularly so Grafana lag panels stay current."""

        while not self._stop.is_set():
            try:
                self.metrics.beat()
            except Exception:
                pass
            await asyncio.sleep(0.5)
        try:
            self.metrics.beat()
        except Exception:
            pass

    def _install_signal_handlers(self) -> None:
        loop = asyncio.get_running_loop()
        for sig in (signal.SIGINT, signal.SIGTERM):
            loop.add_signal_handler(sig, lambda sig=sig: asyncio.create_task(self.trigger_shutdown(f"signal:{sig.name}")))

    async def _square_off_watchdog(self) -> None:
        while not self._stop.is_set():
            now = engine_now(IST)
            deadline = dt.datetime.combine(now.date(), self.cfg.risk.square_off_by, tzinfo=IST)
            if now >= deadline:
                self.risk.trigger_kill("SQUARE_OFF_DEADLINE")
                self.metrics.risk_halts_total.inc()
                await self._execute_square_off("SQUARE_OFF_DEADLINE")
                notify_incident("WARN", "Square-off triggered", "Deadline reached", tags=["square_off"])
                await self.oms.cancel_all(reason="square_off")
                await self.trigger_shutdown("square_off")
                return
            try:
                minutes_left = max(0, int((deadline - now).total_seconds() // 60))
                set_risk_dials(minutes_to_sqoff=minutes_left)
            except Exception:
                pass
            await asyncio.sleep((deadline - now).total_seconds())

    async def _consume_fills(self) -> None:
        queue = await self.bus.subscribe("orders/fill", maxsize=500)
        while not self._stop.is_set():
            try:
                event = await asyncio.wait_for(queue.get(), timeout=1.0)
            except asyncio.TimeoutError:
                continue
            try:
                lot_size = self._lot_size_for_symbol(str(event["symbol"]), fallback=event.get("lot_size"))
                expiry, strike, opt_type = self._instrument_meta(str(event["symbol"]))
                exec_obj = PnLExecution(
                    exec_id=str(event.get("exec_id") or event["order_id"]),
                    order_id=str(event["order_id"]),
                    symbol=str(event["symbol"]),
                    side=str(event["side"]),
                    qty=int(event["qty"]),
                    price=float(event["price"]),
                    ts=_parse_ts(event.get("ts"), IST),
                    expiry=expiry,
                    strike=strike,
                    opt_type=opt_type,
                    lot_size=lot_size,
                )
            except (KeyError, ValueError, TypeError):
                continue
            self.pnl.on_execution(exec_obj)
            self.metrics.fills_total.inc()
            try:
                self.exit_engine.on_fill(
                    symbol=exec_obj.symbol,
                    side=exec_obj.side,
                    qty=exec_obj.qty,
                    price=exec_obj.price,
                    ts=exec_obj.ts,
                )
            except Exception:
                self.logger.log_event(30, "exit_plan_seed_failed", symbol=exec_obj.symbol)
            try:
                self.risk.on_fill(symbol=exec_obj.symbol, side=exec_obj.side, qty=exec_obj.qty, price=exec_obj.price, lot_size=lot_size)
            except Exception:
                pass
            try:
                await self.broker.ensure_position_subscription(exec_obj.symbol)
            except Exception as exc:
                self.logger.log_event(30, "position_subscribe_failed", symbol=exec_obj.symbol, error=str(exc))
            self._schedule_charge_calc(exec_obj)

    async def _consume_market_marks(self) -> None:
        queue = await self.bus.subscribe("market/events", maxsize=500)
        while not self._stop.is_set():
            try:
                event = await asyncio.wait_for(queue.get(), timeout=1.0)
            except asyncio.TimeoutError:
                continue
            if event.get("type") != "tick":
                continue
            payload = event.get("payload") or {}
            symbol = payload.get("symbol") or payload.get("underlying")
            price = payload.get("ltp") or payload.get("price")
            if symbol and price is not None:
                try:
                    self.pnl.mark_to_market({symbol: float(price)})
                except (TypeError, ValueError):
                    continue
                try:
                    self.risk.record_underlying_tick(symbol, float(price), _parse_ts(event.get("ts"), IST))
                except Exception:
                    pass

    async def _pnl_snapshot_loop(self) -> None:
        while not self._stop.is_set():
            await asyncio.sleep(30.0)
            self.pnl.snapshot(engine_now())
            realized, unrealized, fees = self.pnl.totals()
            self.metrics.pnl_realized.set(realized)
            self.metrics.pnl_unrealized.set(unrealized)
            self.metrics.pnl_fees.set(fees)
            self.metrics.pnl_net.set(realized + unrealized - fees)
        self.pnl.snapshot(engine_now())
        realized, unrealized, fees = self.pnl.totals()
        self.metrics.pnl_realized.set(realized)
        self.metrics.pnl_unrealized.set(unrealized)
        self.metrics.pnl_fees.set(fees)
        self.metrics.pnl_net.set(realized + unrealized - fees)

    async def _reconcile_loop(self) -> None:
        interval = max(float(self.cfg.oms.reconciliation_interval), 1.0)
        backoff = interval
        while not self._stop.is_set():
            start = time.perf_counter()
            try:
                views = await self.broker.fetch_open_orders()
                await self.oms.reconcile_from_views(views)
                backoff = interval
            except Exception as exc:
                backoff = min(backoff * 2, 30.0)
                self.logger.log_event(30, "reconcile_error", error=str(exc))
                try:
                    self.metrics.order_reconciliation_errors_total.inc()
                except Exception:
                    pass
            else:
                try:
                    duration_ms = (time.perf_counter() - start) * 1000.0
                    self.metrics.order_reconciliation_duration_ms.observe(duration_ms)
                except Exception:
                    pass
            await asyncio.sleep(backoff)

    async def _watch_control_intents(self) -> None:
        last_ts: Optional[str] = None
        while not self._stop.is_set():
            intents = self.store.control_intents_since(last_ts)
            for intent in intents:
                last_ts = intent["ts"]
                action = intent["action"].upper()
                if action == "KILL":
                    self.risk.trigger_kill("CONTROL_KILL")
                    notify_incident("WARN", "Kill switch intent", "Control intent requested halt")
                    await self.exit_engine.handle_risk_halt("CONTROL_KILL")
                    await self.trigger_shutdown("control_kill")
                elif action == "SQUARE_OFF":
                    await self._execute_square_off("CONTROL_SQUARE_OFF")
                elif action == "START_STRATEGY":
                    self._start_strategy(source="control_intent")
                elif action == "SMOKE_TEST":
                    task = asyncio.create_task(smoke_test.run_smoke_test_once(self, self.cfg.smoke_test), name="smoke-test-intent")
                    self._tasks.append(task)
            await asyncio.sleep(1.0)

    def _start_strategy(self, source: str = "auto") -> Optional[asyncio.Task]:
        if self._stop.is_set():
            return None
        if self._strategy_task and not self._strategy_task.done():
            return self._strategy_task
        task = asyncio.create_task(self._run_strategy_with_init(source), name="strategy-loop")
        self._strategy_task = task
        if task not in self._tasks:
            self._tasks.append(task)
        return task

    async def _run_strategy_with_init(self, source: str = "auto") -> None:
        try:
            if not getattr(self.strategy, "_initialized", False):
                await self.strategy.init(self)
        except Exception as exc:
            try:
                self.logger.log_event(30, "strategy_init_failed", error=str(exc))
            except Exception:
                pass
        try:
            self.logger.log_event(20, "strategy_started", source=source)
        except Exception:
            pass
        await self.strategy.run(self._stop)

    def _instrument_meta(self, symbol: str) -> tuple[Optional[str], Optional[float], Optional[str]]:
        meta = self.instrument_resolver.metadata_for(symbol)
        if meta:
            return meta.expiry_date, meta.strike, meta.option_type
        parts = symbol.split("-")
        if len(parts) < 3:
            return None, None, None
        raw_strike = parts[-1]
        opt_type = None
        if raw_strike.endswith(("CE", "PE")):
            opt_type = raw_strike[-2:]
            raw_strike = raw_strike[:-2]
        try:
            strike = float(raw_strike)
        except ValueError:
            strike = None
        expiry = "-".join(parts[1:-1])
        return expiry or None, strike, opt_type

    def _instrument_key_for_symbol(self, symbol: str) -> Optional[str]:
        parts = symbol.split("-")
        if len(parts) < 3:
            return None
        tail = parts[-1]
        opt_type = "CE"
        if tail.endswith(("CE", "PE")):
            opt_type = tail[-2:]
            tail = tail[:-2]
        expiry = "-".join(parts[1:-1])
        try:
            strike = float(tail)
        except ValueError:
            return None
        underlying = parts[0].upper()
        try:
            return self.instrument_cache.lookup(underlying, expiry, strike, opt_type)
        except Exception:
            return None

    async def _on_ws_failure(self, failures: int) -> None:
        self.risk.halt_new_entries("WS_FAILURE")
        self.metrics.risk_halts_total.inc()
        notify_incident("ERROR", "WS reconnect failures", f"{failures} consecutive failures")

    async def _execute_square_off(self, reason: str) -> None:
        now = engine_now(IST)
        budgets = self.risk.square_off_all(reason)
        if not budgets:
            budgets = []
            for pos in self.store.list_open_positions():
                qty = int(pos.get("qty") or 0)
                if qty <= 0:
                    continue
                lot_size = self._lot_size_for_symbol(pos["symbol"], fallback=pos.get("lot_size"))
                budgets.append(
                    OrderBudget(
                        symbol=pos["symbol"],
                        qty=qty,
                        price=float(pos.get("avg_price") or 0.0),
                        lot_size=lot_size,
                        side="SELL",
                    )
                )
        for budget in budgets:
            await self.oms.submit(
                strategy=self.cfg.strategy_tag,
                symbol=budget.symbol,
                side="SELL",
                qty=abs(budget.qty),
                order_type="MARKET",
                limit_price=budget.price,
                ts=now,
            )
        try:
            self.exit_engine.clear_all_plans()
            if self.metrics:
                self.metrics.exit_events_total.labels(reason="SQUARE_OFF").inc()
        except Exception:
            pass

    async def _run_replay_source(self, cfg: ReplayConfig) -> None:
        try:
            await replay(cfg)
        finally:
            await self.trigger_shutdown("replay_complete")

    async def _live_market_feed(self) -> None:
        """DRY_RUN-only mock tick generator. Live mode relies on Upstox WS via broker."""

        while not self._stop.is_set():
            ts = engine_now(IST)
            spot = self._mock_spot()
            expiry_str = self._subscription_expiry_for(self.cfg.data.index_symbol)
            try:
                expiry_dt = dt.date.fromisoformat(expiry_str)
            except ValueError:
                expiry_dt = resolve_weekly_expiry(
                    self.cfg.data.index_symbol,
                    ts,
                    self.cfg.data.holidays,
                    weekly_weekday=self.cfg.data.weekly_expiry_weekday,
                )
                expiry_str = expiry_dt.isoformat()
                self.subscription_expiries[self.cfg.data.index_symbol.upper()] = expiry_str
                try:
                    if self.broker:
                        self.broker.record_subscription_expiry(self.cfg.data.index_symbol, expiry_str, "current")
                    else:
                        set_subscription_expiry(self.cfg.data.index_symbol, expiry_str, "current")
                except Exception:
                    set_subscription_expiry(self.cfg.data.index_symbol, expiry_str, "current")
            strike = pick_strike_from_spot(
                spot,
                step=self._strike_step_for(self.cfg.data.index_symbol),
            )
            symbol = f"{self.cfg.data.index_symbol}-{expiry_str}-{int(strike)}CE"
            instrument_key = self._instrument_key_for_symbol(symbol)
            payload = {
                "underlying": self.cfg.data.index_symbol,
                "ltp": spot,
                "symbol": symbol,
                "instrument_key": instrument_key or symbol,
                "ts": ts.isoformat(),
                "bid": None,
                "ask": None,
            }
            tick_payload = dict(payload)
            tick_payload.setdefault("ts", ts.isoformat())
            if self.broker:
                self.broker.record_market_tick(instrument_key or symbol, tick_payload)
            record_tick_seen(instrument_key=instrument_key or symbol, underlying=self.cfg.data.index_symbol, ts_seconds=ts.timestamp())
            event = {"ts": ts.isoformat(), "type": "tick", "payload": payload}
            self.store.record_market_event("tick", payload, ts=ts)
            await self.bus.publish("market/events", event)
            self.metrics.beat()
            await asyncio.sleep(1.0)

    def _mock_spot(self) -> float:
        base = getattr(self, "_last_spot", 23000.0)
        drift = random.uniform(-5, 5)
        base = max(1.0, base + drift)
        self._last_spot = base
        return base

    def _strike_step_for(self, symbol: str) -> int:
        try:
            step_map = getattr(self.cfg.data, "strike_steps", {}) or {}
            step = int(step_map.get(symbol.upper(), 50))
        except Exception:
            step = 50
        return max(step, 1)

    def _lot_size_for_symbol(self, symbol: str, fallback: Optional[int] = None) -> int:
        fallback_val = max(int(fallback or getattr(self.cfg.data, "lot_step", 1)), 1)
        try:
            meta = self.instrument_resolver.metadata_for(symbol)
        except Exception:
            meta = None
        if meta and getattr(meta, "lot_size", None):
            try:
                return max(int(meta.lot_size), 1)
            except (TypeError, ValueError):
                pass
        key = str(symbol)
        if key not in self._lot_size_fallbacks:
            self._lot_size_fallbacks.add(key)
            self.logger.log_event(30, "lot_size_fallback", symbol=symbol, fallback=fallback_val)
            notify_incident("WARN", "Lot size fallback", f"symbol={symbol} lot_step={fallback_val}", tags=["lot_size_fallback"])
        return fallback_val

    def _init_charges_client(self) -> Optional[UpstoxChargesClient]:
        try:
            creds = load_upstox_credentials()
        except Exception as exc:
            self.logger.log_event(30, "charges_client_unavailable", error=str(exc))
            return None
        try:
            return UpstoxChargesClient(creds.access_token, sandbox=creds.sandbox)
        except Exception as exc:
            self.logger.log_event(30, "charges_client_failed", error=str(exc))
            return None

    def _schedule_charge_calc(self, exec_obj: PnLExecution) -> None:
        if not self.charges_client:
            return
        task = asyncio.create_task(self._fetch_and_store_charges(exec_obj), name=f"charges-{exec_obj.exec_id}")
        self._tasks.append(task)

    async def _fetch_and_store_charges(self, exec_obj: PnLExecution) -> None:
        instrument_token = await self._resolve_instrument_token(exec_obj)
        if not instrument_token:
            self.logger.log_event(30, "charges_no_instrument", exec_id=exec_obj.exec_id, symbol=exec_obj.symbol)
            return
        try:
            breakdown = await asyncio.to_thread(
                self.charges_client.get_brokerage_breakdown,
                instrument_token,
                int(exec_obj.qty),
                "I",
                str(exec_obj.side).upper(),
                float(exec_obj.price),
            )
        except Exception as exc:
            self.logger.log_event(30, "charges_fetch_failed", exec_id=exec_obj.exec_id, error=str(exc))
            return
        if not breakdown:
            return
        rows = breakdown.rows or []
        if not rows and breakdown.total > 0:
            rows = [FeeRow(category="total", amount=breakdown.total, note="charge_api")]
        if not rows:
            return
        try:
            self.store.replace_costs_for_exec(exec_obj.exec_id, rows, ts=exec_obj.ts)
        except Exception as exc:
            self.logger.log_event(30, "charges_store_failed", exec_id=exec_obj.exec_id, error=str(exc))
            return
        try:
            self.pnl.reconcile_exec_charges(exec_obj.exec_id, breakdown.total)
        except Exception as exc:
            self.logger.log_event(30, "charges_reconcile_failed", exec_id=exec_obj.exec_id, error=str(exc))

    async def _resolve_instrument_token(self, exec_obj: PnLExecution) -> Optional[str]:
        symbol = exec_obj.symbol
        if "|" in symbol:
            return symbol
        underlying = symbol.split("-")[0].upper() if "-" in symbol else symbol.upper()
        expiry = exec_obj.expiry
        strike = exec_obj.strike
        opt_type = exec_obj.opt_type or "CE"
        if expiry and strike is not None:
            try:
                token = self.instrument_cache.lookup(underlying, expiry, strike, opt_type)
                if token:
                    return token
            except Exception:
                pass
        try:
            return await self.instrument_resolver.resolve_symbol(symbol)
        except Exception:
            return None

    def _resolve_metrics_port(self) -> int:
        raw = os.getenv("METRICS_PORT", "9103")
        try:
            return int(raw)
        except ValueError:
            self.logger.log_event(30, "metrics_port_invalid", value=raw)
            return 9103


def _parse_args(argv: Optional[List[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Async trading engine")
    parser.add_argument("--config", type=Path, help="(deprecated) config/app.yml is canonical", default=None)
    parser.add_argument("--replay-from-run", dest="replay_from_run", help="Replay from persisted run id")
    parser.add_argument("--replay-from-file", dest="replay_from_file", type=Path, help="Replay from JSONL feed")
    parser.add_argument("--replay-speed", dest="replay_speed", type=float, default=1.0)
    parser.add_argument("--replay-seed", dest="replay_seed", type=int, default=None)
    return parser.parse_args(argv)


def _is_dry_run_env() -> bool:
    return os.getenv("DRY_RUN", "false").lower() in {"1", "true", "yes", "on"}


def _env_bool(name: str, default: bool = False) -> bool:
    raw = os.getenv(name)
    if raw is None:
        return default
    return raw.strip().lower() in {"1", "true", "yes", "on"}


def _startup_underlyings(cfg: EngineConfig) -> set[str]:
    roots = {"NIFTY", "BANKNIFTY"}
    try:
        roots.add(cfg.data.index_symbol.upper())
    except Exception:
        pass
    return {r for r in roots if r}


async def main(argv: Optional[List[str]] = None) -> None:
    configure_logging("INFO")
    args = _parse_args(argv)
    cfg = EngineConfig.load(args.config)
    try:
        load_upstox_credentials(cfg.secrets)
    except CredentialError as exc:
        get_logger("main").log_event(40, "missing_credentials", message=str(exc))
        raise SystemExit(1)
    try:
        sanity_check_config(cfg)
    except ConfigError as exc:
        get_logger("main").log_event(40, "config_sanity_failed", message=str(exc))
        raise SystemExit(1)
    replay_cfg = None
    if args.replay_from_run or args.replay_from_file:
        if args.replay_from_run and args.replay_from_file:
            raise ValueError("Use either --replay-from-run or --replay-from-file")
        replay_cfg = ReplayConfig(
            run_id=args.replay_from_run,
            input_path=args.replay_from_file,
            speed=args.replay_speed,
            seed=args.replay_seed,
        )
        cfg = _with_replay_run_id(cfg, replay_cfg)
    app = EngineApp(cfg)
    strict_mode = _env_bool("INTRADAY_STRICT_MODE", default=True)
    try:
        await enforce_intraday_clean_start(
            app.store,
            instrument_cache=app.instrument_cache,
            underlyings=_startup_underlyings(cfg),
            strict_mode=strict_mode,
            risk=app.risk,
        )
    except Exception as exc:
        app.logger.log_event(40, "startup_guard_failed", error=str(exc), strict_mode=strict_mode)
        await app.trigger_shutdown("startup_guard")
        return
    await app.run(replay_cfg=replay_cfg)


def _with_replay_run_id(cfg: EngineConfig, replay_cfg: ReplayConfig) -> EngineConfig:
    suffix = replay_cfg.run_id or (replay_cfg.input_path.stem if replay_cfg.input_path else "file")
    suffix = suffix.replace("/", "-")
    stamp = engine_now(IST).strftime("%Y%m%d-%H%M%S")
    new_id = f"{cfg.run_id}-replay-{suffix}-{stamp}"
    return replace(cfg, run_id=new_id)


def _parse_ts(value: Optional[str], tz: dt.tzinfo = IST) -> dt.datetime:
    if not value:
        return engine_now(tz)
    try:
        parsed = dt.datetime.fromisoformat(value)
    except ValueError:
        return engine_now(tz)
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=tz)
    return parsed.astimezone(tz)


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:  # pragma: no cover
        pass
