from __future__ import annotations

import asyncio
import datetime as dt
import hashlib
import time
from dataclasses import dataclass, field
from enum import Enum
from typing import Dict, Iterable, List, Optional, Protocol

from engine.config import IST, OMSConfig
from engine.events import EventBus
from engine.logging_utils import get_logger
from engine.metrics import EngineMetrics
from engine.time_machine import now as engine_now, utc_now
from persistence import SQLiteStore


class OrderState(str, Enum):
    NEW = "NEW"
    SUBMITTED = "SUBMITTED"
    ACKNOWLEDGED = "ACKNOWLEDGED"
    PARTIALLY_FILLED = "PARTIALLY_FILLED"
    FILLED = "FILLED"
    REJECTED = "REJECTED"
    CANCELED = "CANCELED"


FINAL_STATES = {OrderState.FILLED, OrderState.REJECTED, OrderState.CANCELED}


@dataclass
class Order:
    client_order_id: str
    strategy: str
    symbol: str
    side: str
    qty: int
    order_type: str = "MARKET"
    limit_price: Optional[float] = None
    state: OrderState = OrderState.NEW
    broker_order_id: Optional[str] = None
    filled_qty: int = 0
    avg_fill_price: float = 0.0
    created_at: dt.datetime = field(default_factory=utc_now)
    updated_at: dt.datetime = field(default_factory=utc_now)
    idempotency_key: Optional[str] = None

    def mark_state(self, new_state: OrderState, *, ts: Optional[dt.datetime] = None) -> None:
        self.state = new_state
        self.updated_at = ts or utc_now()

    @classmethod
    def from_snapshot(cls, snapshot: Dict[str, Any]) -> "Order":
        state = OrderState(snapshot["state"])
        ts = dt.datetime.fromisoformat(snapshot["last_update"])
        return cls(
            client_order_id=snapshot["client_order_id"],
            strategy=snapshot["strategy"],
            symbol=snapshot["symbol"],
            side=snapshot["side"],
            qty=int(snapshot["qty"]),
            order_type="MARKET",
            limit_price=snapshot.get("price"),
            state=state,
            broker_order_id=snapshot.get("broker_order_id"),
            created_at=ts,
            updated_at=ts,
            idempotency_key=snapshot.get("idempotency_key"),
        )


@dataclass
class BrokerOrderAck:
    broker_order_id: str
    status: str


@dataclass
class BrokerOrderView:
    broker_order_id: str
    client_order_id: Optional[str]
    status: str
    filled_qty: int
    avg_price: float


class BrokerClient(Protocol):
    async def submit_order(self, order: Order) -> BrokerOrderAck: ...

    async def replace_order(self, order: Order, *, price: Optional[float], qty: Optional[int]) -> None: ...

    async def cancel_order(self, order: Order) -> None: ...

    async def fetch_open_orders(self) -> List[BrokerOrderView]: ...


class OMS:
    """Stateful order management with deterministic identifiers and persistence."""

    def __init__(self, *, broker: BrokerClient, store: SQLiteStore, config: OMSConfig, bus: Optional[EventBus] = None, metrics: Optional[EngineMetrics] = None):
        self._broker = broker
        self._store = store
        self._cfg = config
        self._bus = bus
        self._metrics = metrics
        self._orders: Dict[str, Order] = {}
        self._lock = asyncio.Lock()
        self._logger = get_logger("OMS")

    # --------------------------------------------------------------- id helpers
    @staticmethod
    def client_order_id(strategy: str, ts: dt.datetime, symbol: str, side: str, qty: int) -> str:
        payload = f"{strategy}|{symbol}|{side}|{qty}|{int(ts.timestamp() * 1_000_000)}"
        digest = hashlib.sha1(payload.encode("utf-8"), usedforsecurity=False).hexdigest()
        return f"{strategy[:4].upper()}-{digest[:16]}"

    @staticmethod
    def idempotency_key(strategy: str, ts: dt.datetime, symbol: str, side: str, qty: int) -> str:
        payload = f"{strategy}|{symbol}|{side}|{qty}|{int(ts.timestamp() * 1_000_000)}|IDEMP"
        digest = hashlib.sha256(payload.encode("utf-8")).hexdigest()
        return digest[:32]

    def get_order(self, client_order_id: str) -> Optional[Order]:
        return self._orders.get(client_order_id)

    def restore_orders(self, orders: Iterable[Order]) -> None:
        for order in orders:
            self._orders[order.client_order_id] = order
        self._update_order_metrics()

    # ----------------------------------------------------------------- commands
    async def submit(
        self,
        *,
        strategy: str,
        symbol: str,
        side: str,
        qty: int,
        order_type: str = "MARKET",
        limit_price: Optional[float] = None,
        ts: Optional[dt.datetime] = None,
        idempotency_key: Optional[str] = None,
    ) -> Order:
        ts = ts or engine_now(IST)
        client_id = self.client_order_id(strategy, ts, symbol, side, qty)
        idem = idempotency_key or self.idempotency_key(strategy, ts, symbol, side, qty)
        async with self._lock:
            existing = self._orders.get(client_id)
            if existing:
                if existing.state in FINAL_STATES:
                    return existing
                return await self._ensure_submitted(existing)
            stored = self._store.find_order_by_idempotency(idem)
            if stored:
                restored = Order.from_snapshot(stored)
                self._orders[restored.client_order_id] = restored
                return await self._ensure_submitted(restored)
            self._ensure_capacity()
            order = Order(
                client_order_id=client_id,
                strategy=strategy,
                symbol=symbol,
                side=side,
                qty=qty,
                order_type=order_type,
                limit_price=limit_price,
                idempotency_key=idem,
            )
            self._orders[client_id] = order
            self._update_order_metrics()
        start = time.perf_counter()
        result = await self._ensure_submitted(order)
        self._record_latency("submit", time.perf_counter() - start)
        return result

    async def replace(self, client_order_id: str, *, price: Optional[float] = None, qty: Optional[int] = None) -> Order:
        async with self._lock:
            order = self._orders[client_order_id]
        start = time.perf_counter()
        await self._broker.replace_order(order, price=price, qty=qty)
        self._record_latency("replace", time.perf_counter() - start)
        if price is not None:
            order.limit_price = price
        if qty is not None:
            order.qty = qty
        await self._persist_transition(order, order.state, order.state, reason="replace")
        return order

    async def cancel(self, client_order_id: str, *, reason: str = "user") -> Order:
        async with self._lock:
            order = self._orders[client_order_id]
        start = time.perf_counter()
        await self._broker.cancel_order(order)
        self._record_latency("cancel", time.perf_counter() - start)
        await self._persist_transition(order, order.state, OrderState.CANCELED, reason=reason)
        self._update_order_metrics()
        return order

    async def cancel_all(self, reason: str = "user") -> None:
        async with self._lock:
            pending = [oid for oid, order in self._orders.items() if order.state not in FINAL_STATES]
        for oid in pending:
            try:
                await self.cancel(oid, reason=reason)
            except Exception:  # pragma: no cover - best effort
                continue

    async def record_fill(self, client_order_id: str, *, qty: int, price: float, broker_order_id: Optional[str] = None) -> None:
        async with self._lock:
            order = self._orders[client_order_id]
        order.filled_qty += qty
        if order.filled_qty > 0:
            order.avg_fill_price = ((order.avg_fill_price * (order.filled_qty - qty)) + (qty * price)) / order.filled_qty
        if broker_order_id:
            order.broker_order_id = broker_order_id
        new_state = OrderState.FILLED if order.filled_qty >= order.qty else OrderState.PARTIALLY_FILLED
        await self._persist_transition(order, order.state, new_state, reason="fill")
        self._store.record_execution(
            order.client_order_id,
            symbol=order.symbol,
            side=order.side,
            qty=qty,
            price=price,
            venue=getattr(self._broker, "venue", "SIM"),
            idempotency_key=order.idempotency_key,
            ts=order.updated_at,
        )
        if self._bus:
            await self._bus.publish(
                "orders/fill",
                {
                    "order_id": order.client_order_id,
                    "symbol": order.symbol,
                    "side": order.side,
                    "qty": qty,
                    "price": price,
                    "ts": order.updated_at.isoformat(),
                    "exec_id": order.client_order_id,
                },
            )
        self._update_order_metrics()

    async def mark_reject(self, client_order_id: str, message: str) -> None:
        async with self._lock:
            order = self._orders[client_order_id]
        await self._persist_transition(order, order.state, OrderState.REJECTED, reason=message)
        self._update_order_metrics()

    # --------------------------------------------------------------- reconciler
    async def reconcile_from_broker(self) -> None:
        remote = await self._broker.fetch_open_orders()
        remote_map = {view.client_order_id: view for view in remote if view.client_order_id}
        async with self._lock:
            local_orders = list(self._orders.values())
        for order in local_orders:
            view = remote_map.get(order.client_order_id)
            if not view:
                if order.state not in FINAL_STATES:
                    await self._persist_transition(order, order.state, OrderState.CANCELED, reason="broker_missing")
                continue
            order.broker_order_id = view.broker_order_id
            if view.filled_qty >= order.qty:
                await self._persist_transition(order, order.state, OrderState.FILLED, reason="reconcile")
            elif view.filled_qty > 0 and order.state != OrderState.PARTIALLY_FILLED:
                await self._persist_transition(order, order.state, OrderState.PARTIALLY_FILLED, reason="reconcile")

    async def handle_reconnect(self) -> None:
        async with self._lock:
            pending = [order for order in self._orders.values() if order.state not in FINAL_STATES and not order.broker_order_id]
        for order in pending:
            await self._ensure_submitted(order)

    # --------------------------------------------------------------- internals
    async def _ensure_submitted(self, order: Order) -> Order:
        if order.state == OrderState.NEW:
            await self._persist_transition(order, OrderState.NEW, OrderState.SUBMITTED, reason="submit")
        elif order.state not in {OrderState.SUBMITTED, OrderState.ACKNOWLEDGED}:
            await self._persist_transition(order, order.state, OrderState.SUBMITTED, reason="submit")
        if order.broker_order_id:
            return order
        try:
            ack = await self._broker.submit_order(order)
        except Exception:
            await self._persist_transition(order, OrderState.SUBMITTED, OrderState.NEW, reason="retry")
            raise
        order.broker_order_id = ack.broker_order_id
        await self._persist_transition(order, OrderState.SUBMITTED, OrderState.ACKNOWLEDGED, reason=ack.status)
        return order

    async def _persist_transition(self, order: Order, prev: OrderState, new: OrderState, *, reason: Optional[str] = None) -> None:
        if prev == new and reason == "replace":
            order.updated_at = utc_now()
            self._store.record_order_snapshot(order)
            return
        order.mark_state(new)
        self._store.record_transition(order.client_order_id, prev, new, reason=reason, ts=order.updated_at)
        self._store.record_order_snapshot(order)
        self._logger.log_event(
            20,
            "order_state",
            client_order_id=order.client_order_id,
            prev_state=prev.value,
            new_state=new.value,
            reason=reason,
        )

    def _ensure_capacity(self) -> None:
        open_orders = sum(1 for order in self._orders.values() if order.state not in FINAL_STATES)
        if open_orders >= self._cfg.max_inflight_orders:
            raise RuntimeError("Inflight order limit reached")

    def _update_order_metrics(self) -> None:
        if not self._metrics:
            return
        pending = sum(1 for order in self._orders.values() if order.state not in FINAL_STATES)
        self._metrics.order_queue_depth.set(pending)

    def _record_latency(self, kind: str, duration: float) -> None:
        if not self._metrics:
            return
        mapping = {
            "submit": self._metrics.submit_latency_ms,
            "replace": self._metrics.replace_latency_ms,
            "cancel": self._metrics.cancel_latency_ms,
        }
        metric = mapping.get(kind)
        if metric:
            metric.observe(duration * 1000.0)


__all__ = [
    "BrokerClient",
    "BrokerOrderAck",
    "BrokerOrderView",
    "OMS",
    "Order",
    "OrderState",
]
