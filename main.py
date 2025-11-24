from __future__ import annotations

import argparse
import asyncio
import datetime as dt
import os
import random
import signal
from pathlib import Path
from typing import Callable, List, Optional

from dataclasses import replace

from engine import smoke_test
from engine.alerts import configure_alerts, notify_incident
from engine.broker import FULL_D30_LIMIT_PER_USER, FullD30Streamer, UpstoxBroker
from engine.config import EngineConfig, IST
from engine.data import pick_strike_from_spot, pick_subscription_expiry, resolve_weekly_expiry
from engine.events import EventBus
from engine.fees import load_fee_config
from engine.instruments import InstrumentResolver
from engine.logging_utils import configure_logging, get_logger
from engine.metrics import EngineMetrics, bind_global_metrics, set_subscription_expiry, start_http_server_if_available
try:
    from engine.metrics import set_risk_dials
except Exception:  # pragma: no cover
    def set_risk_dials(**kwargs): ...
from engine.oms import OMS, OrderValidationError
from engine.pnl import Execution as PnLExecution, PnLCalculator
from engine.recovery import RecoveryManager
from engine.replay import ReplayConfig, configure_runtime as configure_replay_runtime, replay
from engine.risk import OrderBudget, RiskManager
from engine.time_machine import now as engine_now
from persistence import SQLiteStore
from market.instrument_cache import InstrumentCache

try:  # pragma: no cover - optional acceleration
    import uvloop

    uvloop.install()
except Exception:  # pragma: no cover
    pass


class IntradayBuyStrategy:
    """Toy BUY-side strategy that demonstrates the risk→data→oms pipeline."""

    def __init__(
        self,
        config: EngineConfig,
        risk: RiskManager,
        oms: OMS,
        bus: EventBus,
        subscription_expiry_provider: Optional[Callable[[str], str]] = None,
    ):
        self.cfg = config
        self.risk = risk
        self.oms = oms
        self.bus = bus
        self.logger = get_logger("IntradayStrategy")
        self._subscription_expiry_provider = subscription_expiry_provider

    async def run(self, stop_event: asyncio.Event) -> None:
        queue = await self.bus.subscribe("market/events", maxsize=1000)
        while not stop_event.is_set():
            try:
                event = await asyncio.wait_for(queue.get(), timeout=1.0)
            except asyncio.TimeoutError:
                continue
            await self._handle_event(event)

    async def _handle_event(self, event: dict) -> None:
        evt_type = event.get("type")
        if evt_type not in {"tick", "quote", "bar"}:
            return
        payload = event.get("payload") or {}
        spot = self._extract_price(payload)
        if spot is None:
            return
        ts = self._event_ts(event.get("ts"))
        symbol_hint = payload.get("symbol")
        await self._maybe_trade(spot, ts, symbol_hint)

    async def _maybe_trade(self, spot: float, ts: dt.datetime, symbol_hint: Optional[str]) -> None:
        symbol_root = self.cfg.data.index_symbol
        preference = getattr(self.cfg.data, "subscription_expiry_preference", "current")
        expiry_str: Optional[str] = None
        if self._subscription_expiry_provider:
            try:
                expiry_str = self._subscription_expiry_provider(symbol_root)
            except Exception:
                expiry_str = None
        if not expiry_str:
            try:
                expiry_str = pick_subscription_expiry(symbol_root, preference)
            except Exception:
                fallback_expiry = resolve_weekly_expiry(
                    symbol_root,
                    ts,
                    self.cfg.data.holidays,
                    weekly_weekday=self.cfg.data.weekly_expiry_weekday,
                )
                expiry_str = fallback_expiry.isoformat()
        strike = pick_strike_from_spot(
            spot,
            step=self.cfg.data.lot_step,
        )
        option_type = "CE"
        lot_size = self._resolve_lot_size(expiry_str)
        symbol = symbol_hint or f"{self.cfg.data.index_symbol}-{expiry_str}-{int(strike)}{option_type}"
        budget = OrderBudget(symbol=symbol, qty=lot_size, price=spot, lot_size=lot_size, side="BUY")
        self.risk.on_tick(symbol, spot)
        if not self.risk.budget_ok_for(budget):
            return
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
            self.logger.log_event(30, "order_validation_failed", symbol=symbol, code=exc.code, message=str(exc))
            return
        except Exception as exc:
            self.logger.log_event(40, "order_submit_failed", symbol=symbol, error=str(exc))
            return
        self.logger.log_event(20, "submitted", symbol=symbol, price=budget.price)

    def _event_ts(self, value: Optional[str]) -> dt.datetime:
        return _parse_ts(value, IST)

    def _extract_price(self, payload: dict) -> Optional[float]:
        for key in ("ltp", "price", "close", "last"):
            if key in payload and payload[key] is not None:
                try:
                    return float(payload[key])
                except (TypeError, ValueError):
                    continue
        return None

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

    async def _maybe_start_full_d30_streamer(self) -> None:
        """Optional FULL_D30 metrics streamer using the new protobuf feed."""

        if str(self.cfg.market_data.stream_mode).lower() != "full_d30":
            return
        token = os.environ.get("UPSTOX_ACCESS_TOKEN")
        if not token:
            self.logger.warning("FullD30Streamer skipped: UPSTOX_ACCESS_TOKEN not set")
            return
        cache = self.instrument_cache
        if cache is None:
            self.logger.warning("FullD30Streamer skipped: instrument cache unavailable")
            return
        try:
            underlying_key = cache.resolve_index_key(self.cfg.data.index_symbol)
        except Exception as exc:
            self.logger.warning("FullD30Streamer skipped: unable to resolve underlying key: %s", exc)
            underlying_key = None
        expiry = self._subscription_expiry_for(self.cfg.data.index_symbol)
        spot = None
        try:
            spot = await self.broker.get_spot(self.cfg.data.index_symbol)
        except Exception as exc:
            self.logger.warning("FullD30Streamer spot lookup failed: %s", exc)
        window = max(int(self.cfg.market_data.window_steps or 0), 0)
        opt_mode = str(self.cfg.market_data.option_type or "CE").upper()
        opt_types = ["CE", "PE"] if opt_mode == "BOTH" else [opt_mode if opt_mode in {"CE", "PE"} else "CE"]
        instrument_keys: List[str] = []
        if underlying_key:
            instrument_keys.append(underlying_key)
        if expiry and spot is not None:
            for opt in opt_types:
                rows = cache.nearest_strikes(self.cfg.data.index_symbol, expiry, spot, window=window, opt_type=opt)
                for row in rows:
                    key = row.get("instrument_key")
                    if key:
                        instrument_keys.append(str(key))
        if not instrument_keys:
            self.logger.warning("FullD30Streamer skipped: no instrument keys resolved")
            return
        unique_keys = list(dict.fromkeys(instrument_keys))[:FULL_D30_LIMIT_PER_USER]
        streamer = FullD30Streamer(token, unique_keys)
        self._full_d30_streamer = streamer
        await streamer.start()


class EngineApp:
    def __init__(self, config: EngineConfig):
        self.cfg = config
        self.logger = get_logger("EngineApp")
        self.bus = EventBus()
        self.store = SQLiteStore(config.persistence_path, run_id=config.run_id)
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
        self.risk = RiskManager(config.risk, self.store)
        self.subscription_expiries: dict[str, str] = {}
        self.broker = UpstoxBroker(
            config=config.broker,
            ws_failure_callback=self._on_ws_failure,
            instrument_resolver=self.instrument_resolver,
            instrument_cache=self.instrument_cache,
            metrics=self.metrics,
            auth_halt_callback=self.risk.halt_new_entries,
            risk_manager=self.risk,
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
        self.strategy = IntradayBuyStrategy(
            config, self.risk, self.oms, self.bus, subscription_expiry_provider=self._subscription_expiry_for
        )
        self.fee_config = load_fee_config()
        self.pnl = PnLCalculator(self.store, self.fee_config)
        configure_alerts(config.alerts.throttle_seconds)
        self._stop = asyncio.Event()
        self._market_task: Optional[asyncio.Task] = None
        self._square_task: Optional[asyncio.Task] = None
        self._strategy_task: Optional[asyncio.Task] = None
        self._tasks: List[asyncio.Task] = []
        self._full_d30_streamer: Optional[FullD30Streamer] = None

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
        self._subscription_expiry_for(self.cfg.data.index_symbol)
        await self.broker.start()
        decision = self.broker.session_guard_decision or {"action": "continue", "state": 1, "positions": 0}
        halt_flag = 1 if decision.get("action") != "continue" else 0
        self.logger.log_event(20, "session_guard", state=decision.get("state", 1), halt=halt_flag, positions=decision.get("positions", 0))
        if decision.get("action") == "shutdown":
            await self.trigger_shutdown("session_guard_shutdown")
            await self._cleanup()
            return
        try:
            maybe_streamer = getattr(self, "_maybe_start_full_d30_streamer", None)
            if callable(maybe_streamer):
                await maybe_streamer()
        except Exception as exc:
            self.logger.warning("FullD30Streamer failed to start: %s", exc)
        await RecoveryManager(self.store, self.oms).reconcile()
        self._square_task = asyncio.create_task(self._square_off_watchdog(), name="square-off")
        tasks: List[asyncio.Task] = [self._square_task]
        if replay_cfg:
            self._market_task = asyncio.create_task(self._run_replay_source(replay_cfg), name="replay-source")
        else:
            self._market_task = asyncio.create_task(self._live_market_feed(), name="market-feed")
        if self._market_task:
            tasks.append(self._market_task)
        fills_task = asyncio.create_task(self._consume_fills(), name="fills-consumer")
        marks_task = asyncio.create_task(self._consume_market_marks(), name="pnl-marks")
        snapshot_task = asyncio.create_task(self._pnl_snapshot_loop(), name="pnl-snapshot")
        control_task = asyncio.create_task(self._watch_control_intents(), name="control-intents")
        tasks.extend([fills_task, marks_task, snapshot_task, control_task])
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
        if self._full_d30_streamer:
            try:
                await self._full_d30_streamer.stop()
            except Exception:
                pass
        await self.broker.stop()
        self.metrics.engine_up.set(0)
        self.logger.log_event(20, "engine_stopped")

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
                )
            except (KeyError, ValueError, TypeError):
                continue
            self.pnl.on_execution(exec_obj)
            self.metrics.fills_total.inc()

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
        task = asyncio.create_task(self.strategy.run(self._stop), name="strategy-loop")
        self._strategy_task = task
        if task not in self._tasks:
            self._tasks.append(task)
        try:
            self.logger.log_event(20, "strategy_started", source=source)
        except Exception:
            pass
        return task

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
        for budget in self.risk.square_off_all(reason):
            await self.oms.submit(
                strategy=self.cfg.strategy_tag,
                symbol=budget.symbol,
                side="SELL",
                qty=abs(budget.qty),
                order_type="MARKET",
                limit_price=budget.price,
                ts=now,
            )

    async def _run_replay_source(self, cfg: ReplayConfig) -> None:
        try:
            await replay(cfg)
        finally:
            await self.trigger_shutdown("replay_complete")

    async def _live_market_feed(self) -> None:
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
                step=self.cfg.data.lot_step,
            )
            symbol = f"{self.cfg.data.index_symbol}-{expiry_str}-{int(strike)}CE"
            payload = {"underlying": self.cfg.data.index_symbol, "ltp": spot, "symbol": symbol}
            tick_payload = dict(payload)
            tick_payload.setdefault("ts", ts.isoformat())
            instrument_key = self._instrument_key_for_symbol(symbol)
            if self.broker:
                self.broker.record_market_tick(instrument_key or symbol, tick_payload)
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


async def main(argv: Optional[List[str]] = None) -> None:
    args = _parse_args(argv)
    cfg = EngineConfig.load(args.config)
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
