from __future__ import annotations

import asyncio
import datetime as dt
from dataclasses import dataclass
from typing import Any, Dict, List, Optional


@dataclass(frozen=True)
class Tick:
    instrument: str
    ts: dt.datetime
    ltp: float
    bid: float
    ask: float
    volume: float = 0.0


@dataclass(frozen=True)
class Bar1s:
    instrument: str
    ts: dt.datetime
    open: float
    high: float
    low: float
    close: float
    volume: float
    is_stale: bool = False


@dataclass(frozen=True)
class OrderSignal:
    instrument: str
    side: str
    qty: int
    order_type: str = "MARKET"
    limit_price: Optional[float] = None
    meta: Optional[Dict[str, Any]] = None


@dataclass(frozen=True)
class OrderIntent:
    instrument: str
    side: str
    qty: int
    order_type: str
    price: Optional[float]
    tag: str


@dataclass(frozen=True)
class OrderAck:
    internal_id: str
    broker_id: Optional[str]
    ts: dt.datetime
    status: str
    message: Optional[str] = None


@dataclass(frozen=True)
class OrderFill:
    internal_id: str
    broker_id: Optional[str]
    ts: dt.datetime
    qty: int
    price: float


@dataclass(frozen=True)
class OrderCancel:
    internal_id: str
    broker_id: Optional[str]
    ts: dt.datetime
    reason: str


@dataclass(frozen=True)
class RiskReject:
    signal: OrderSignal
    reason: str
    code: str
    ts: dt.datetime


@dataclass(frozen=True)
class Heartbeat:
    state: str
    ts: dt.datetime
    info: Dict[str, Any]


@dataclass(frozen=True)
class StateChange:
    prev: str
    new: str
    ts: dt.datetime


class EventBus:
    """Lightweight async pub/sub for engine components."""

    def __init__(self):
        self._topics: Dict[str, List[asyncio.Queue]] = {}
        self._lock = asyncio.Lock()

    async def publish(self, topic: str, event: Any) -> None:
        async with self._lock:
            queues = list(self._topics.get(topic, []))
        for queue in queues:
            if queue.full():
                try:
                    queue.get_nowait()
                except asyncio.QueueEmpty:
                    pass
            queue.put_nowait(event)

    async def subscribe(self, topic: str, maxsize: int = 100) -> asyncio.Queue:
        queue: asyncio.Queue = asyncio.Queue(maxsize=maxsize)
        async with self._lock:
            self._topics.setdefault(topic, []).append(queue)
        return queue

    async def unsubscribe(self, topic: str, queue: asyncio.Queue) -> None:
        async with self._lock:
            subscribers = self._topics.get(topic)
            if not subscribers:
                return
            if queue in subscribers:
                subscribers.remove(queue)
