from __future__ import annotations

import asyncio
import logging
import time
import uuid
from dataclasses import dataclass, field
from typing import Dict, Optional

from brokerage.upstox_client import UpstoxSession
from engine.clock import MarketClock
from engine.events import EventBus, OrderAck, OrderFill, OrderIntent
from engine_metrics import EngineMetrics


@dataclass
class RouterConfig:
    product: str = "I"
    validity: str = "DAY"
    paper_mode: bool = True


@dataclass
class _OrderState:
    intent: OrderIntent
    broker_id: Optional[str] = None
    status: str = "PENDING"


class OrderRouter:
    def __init__(self, *, session: Optional[UpstoxSession], bus: EventBus, clock: MarketClock,
                 metrics: EngineMetrics, config: RouterConfig):
        self.session = session
        self.bus = bus
        self.clock = clock
        self.metrics = metrics
        self.cfg = config
        self.logger = logging.getLogger("OrderRouter")
        self._orders: Dict[str, _OrderState] = {}
        self._lock = asyncio.Lock()

    async def submit(self, intent: OrderIntent) -> OrderAck:
        internal_id = self._internal_id()
        state = _OrderState(intent=intent)
        async with self._lock:
            self._orders[internal_id] = state
        start = time.perf_counter()
        if self.cfg.paper_mode or self.session is None or not self.session.config.sandbox:
            ack = await self._simulate_fill(internal_id, intent)
        else:
            ack = await self._place_live(internal_id, intent)
        latency_ms = (time.perf_counter() - start) * 1000.0
        self.metrics.order_submitted(intent.instrument, latency_ms)
        return ack

    async def cancel(self, internal_id: str, reason: str = "user") -> None:
        async with self._lock:
            state = self._orders.get(internal_id)
        if not state:
            return
        ts = self.clock.now()
        await self.bus.publish("orders/cancel", OrderAck(internal_id=internal_id, broker_id=state.broker_id, ts=ts, status="CANCELED", message=reason))

    async def flatten_all(self) -> None:
        async with self._lock:
            ids = list(self._orders.keys())
        for oid in ids:
            await self.cancel(oid, reason="flatten")

    async def _simulate_fill(self, internal_id: str, intent: OrderIntent) -> OrderAck:
        ts = self.clock.now()
        ack = OrderAck(internal_id=internal_id, broker_id=None, ts=ts, status="SIMULATED")
        await self.bus.publish("orders/ack", ack)
        fill = OrderFill(internal_id=internal_id, broker_id=None, ts=ts, qty=intent.qty, price=intent.price or 0.0)
        await self.bus.publish("orders/fill", fill)
        self.metrics.order_filled(intent.instrument)
        return ack

    async def _place_live(self, internal_id: str, intent: OrderIntent) -> OrderAck:
        # Placeholder for actual Upstox order placement. Currently same as simulate to keep sandbox safe.
        self.logger.warning("Live order routing not yet implemented; defaulting to simulation")
        return await self._simulate_fill(internal_id, intent)

    def _internal_id(self) -> str:
        return f"ORD-{int(time.time() * 1000)}-{uuid.uuid4().hex[:6]}"

    def get_intent(self, internal_id: str) -> Optional[OrderIntent]:
        state = self._orders.get(internal_id)
        return state.intent if state else None
