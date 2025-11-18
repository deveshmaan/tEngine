from __future__ import annotations

import asyncio
import inspect
import logging
import time
from typing import Any, Awaitable, Callable, Dict, List, Optional, Union

from engine.events import Bar1s, EventBus, OrderAck, OrderFill, OrderSignal, RiskReject
from engine.order_router import OrderRouter
from engine.risk import RiskGates
from engine.store import SQLiteStore
from engine_metrics import EngineMetrics

StrategyHandler = Callable[[Bar1s], Union[Awaitable[List[OrderSignal]], List[OrderSignal]]]


class ExecutionEngine:
    """Routes strategy signals through risk gates into the order router."""

    def __init__(self, *, bus: EventBus, risk: RiskGates, router: OrderRouter,
                 store: SQLiteStore, metrics: EngineMetrics, clock):
        self.bus = bus
        self.risk = risk
        self.router = router
        self.store = store
        self.metrics = metrics
        self.clock = clock
        self.logger = logging.getLogger("ExecutionEngine")
        self._strategies: Dict[str, Dict[str, Any]] = {}
        self._tasks: List[asyncio.Task] = []
        self._running = False

    def register_strategy(self, name: str, handler: StrategyHandler, *, enabled: bool = False) -> None:
        self._strategies[name] = {"handler": handler, "enabled": enabled}

    def enable_strategy(self, name: str, enabled: bool = True) -> None:
        if name in self._strategies:
            self._strategies[name]["enabled"] = enabled

    async def start(self) -> None:
        if self._running:
            return
        await self.store.start()
        self._tasks.append(asyncio.create_task(self._consume_bars(), name="exec-bars"))
        self._tasks.append(asyncio.create_task(self._consume_acks(), name="exec-acks"))
        self._tasks.append(asyncio.create_task(self._consume_fills(), name="exec-fills"))
        self._running = True

    async def stop(self) -> None:
        for task in self._tasks:
            task.cancel()
        for task in self._tasks:
            try:
                await task
            except asyncio.CancelledError:
                pass
        self._tasks.clear()
        await self.store.stop()
        self._running = False

    async def _consume_bars(self) -> None:
        queue = await self.bus.subscribe("bars/1", maxsize=200)
        while True:
            bar: Bar1s = await queue.get()
            await self.store.log_bar(bar)
            for name, meta in self._strategies.items():
                if not meta.get("enabled"):
                    continue
                handler = meta["handler"]
                try:
                    signals = handler(bar)
                except Exception as exc:  # pragma: no cover - strategy bug guard
                    self.logger.exception("Strategy %s failed: %s", name, exc)
                    continue
                if inspect.isawaitable(signals):
                    signals = await signals  # type: ignore
                for signal in signals or []:
                    await self._process_signal(name, signal)

    async def _process_signal(self, strategy_name: str, signal: OrderSignal) -> None:
        start = time.perf_counter()
        result = self.risk.evaluate(signal)
        if isinstance(result, RiskReject):
            await self.bus.publish("risk/reject", result)
            await self.store.log_risk_event(result)
            self.metrics.order_rejected(signal.instrument, result.reason)
            self.logger.info("Risk reject %s: %s", strategy_name, result.reason)
            return
        intent = result
        signal_latency = (time.perf_counter() - start) * 1000.0
        self.metrics.record_signal_latency(signal_latency)
        ack = await self.router.submit(intent)
        await self.store.log_order_ack(ack, intent_symbol=intent.instrument, qty=intent.qty, side=intent.side, price=intent.price)

    async def _consume_acks(self) -> None:
        queue = await self.bus.subscribe("orders/ack", maxsize=200)
        while True:
            ack: OrderAck = await queue.get()
            latency_ms = max(0.0, (self.clock.now() - ack.ts).total_seconds() * 1000.0)
            self.metrics.record_ack_state_latency(latency_ms)
            self.logger.debug("Order ack %s", ack)

    async def _consume_fills(self) -> None:
        queue = await self.bus.subscribe("orders/fill", maxsize=200)
        while True:
            fill: OrderFill = await queue.get()
            await self.store.log_fill(fill)
            intent = self.router.get_intent(fill.internal_id)
            if intent:
                self.risk.record_fill(intent, fill.price)
