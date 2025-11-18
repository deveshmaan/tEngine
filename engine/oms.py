from __future__ import annotations

import asyncio
import datetime as dt
import hashlib
from dataclasses import dataclass, field
from enum import Enum
from typing import Dict, Iterable, List, Optional, Protocol

from engine.config import IST, OMSConfig
from engine.logging_utils import get_logger
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
    created_at: dt.datetime = field(default_factory=lambda: dt.datetime.now(dt.timezone.utc))
    updated_at: dt.datetime = field(default_factory=lambda: dt.datetime.now(dt.timezone.utc))

    def mark_state(self, new_state: OrderState, *, ts: Optional[dt.datetime] = None) -> None:
        self.state = new_state
        self.updated_at = ts or dt.datetime.now(dt.timezone.utc)


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

    def __init__(self, *, broker: BrokerClient, store: SQLiteStore, config: OMSConfig):
        self._broker = broker
        self._store = store
        self._cfg = config
        self._orders: Dict[str, Order] = {}
        self._lock = asyncio.Lock()
        self._logger = get_logger("OMS")

    # --------------------------------------------------------------- id helpers
    @staticmethod
    def client_order_id(strategy: str, ts: dt.datetime, symbol: str, side: str, qty: int) -> str:
        payload = f"{strategy}|{symbol}|{side}|{qty}|{int(ts.timestamp() * 1_000_000)}"
        digest = hashlib.sha1(payload.encode("utf-8"), usedforsecurity=False).hexdigest()
        return f"{strategy[:4].upper()}-{digest[:16]}"

    def get_order(self, client_order_id: str) -> Optional[Order]:
        return self._orders.get(client_order_id)

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
    ) -> Order:
        ts = ts or dt.datetime.now(IST)
        client_id = self.client_order_id(strategy, ts, symbol, side, qty)
        async with self._lock:
            existing = self._orders.get(client_id)
            if existing:
                if existing.state in FINAL_STATES:
                    return existing
                return await self._ensure_submitted(existing)
            self._ensure_capacity()
            order = Order(
                client_order_id=client_id,
                strategy=strategy,
                symbol=symbol,
                side=side,
                qty=qty,
                order_type=order_type,
                limit_price=limit_price,
            )
            self._orders[client_id] = order
        return await self._ensure_submitted(order)

    async def replace(self, client_order_id: str, *, price: Optional[float] = None, qty: Optional[int] = None) -> Order:
        async with self._lock:
            order = self._orders[client_order_id]
        await self._broker.replace_order(order, price=price, qty=qty)
        if price is not None:
            order.limit_price = price
        if qty is not None:
            order.qty = qty
        await self._persist_transition(order, order.state, order.state, reason="replace")
        return order

    async def cancel(self, client_order_id: str, *, reason: str = "user") -> Order:
        async with self._lock:
            order = self._orders[client_order_id]
        await self._broker.cancel_order(order)
        await self._persist_transition(order, order.state, OrderState.CANCELED, reason=reason)
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
        self._store.record_execution(client_order_id, qty=qty, price=price)

    async def mark_reject(self, client_order_id: str, message: str) -> None:
        async with self._lock:
            order = self._orders[client_order_id]
        await self._persist_transition(order, order.state, OrderState.REJECTED, reason=message)

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
            order.updated_at = dt.datetime.now(dt.timezone.utc)
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


__all__ = [
    "BrokerClient",
    "BrokerOrderAck",
    "BrokerOrderView",
    "OMS",
    "Order",
    "OrderState",
]
