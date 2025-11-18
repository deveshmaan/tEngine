from __future__ import annotations

import asyncio
import random
import time
from collections import deque
from typing import Any, Awaitable, Callable, Deque, Iterable, List, Optional, Protocol, Sequence, Union

import upstox_client
from upstox_client.rest import ApiException

from brokerage.upstox_client import UpstoxConfig, UpstoxSession
from engine.config import BrokerConfig
from engine.logging_utils import get_logger
from engine.oms import BrokerOrderAck, BrokerOrderView, Order
from engine.instruments import InstrumentResolver


class BrokerError(Exception):
    def __init__(self, *, code: str, message: str, status: Optional[int] = None, context: Optional[dict[str, Any]] = None):
        super().__init__(message)
        self.code = code
        self.status = status
        self.context = context or {}


class MarketStreamClient(Protocol):
    async def connect(self) -> None: ...

    async def close(self) -> None: ...

    async def subscribe(self, instruments: Sequence[str]) -> None: ...

    async def heartbeat(self) -> None: ...


class RateLimiter:
    def __init__(self, per_second: int):
        self.per_second = max(1, per_second)
        self._ts: Deque[float] = deque()
        self._lock = asyncio.Lock()

    async def acquire(self) -> None:
        while True:
            async with self._lock:
                now = time.monotonic()
                horizon = now - 1.0
                while self._ts and self._ts[0] < horizon:
                    self._ts.popleft()
                if len(self._ts) < self.per_second:
                    self._ts.append(now)
                    return
                delay = max(self._ts[0] + 1.0 - now, 0.0)
            await asyncio.sleep(delay)


class UpstoxBroker:
    """REST + streaming broker wrapper with watchdogs and rate-limits."""

    def __init__(
        self,
        *,
        config: BrokerConfig,
        session_factory: Optional[Callable[[Optional[str]], UpstoxSession]] = None,
        stream_client: Optional[MarketStreamClient] = None,
        token_refresh_cb: Optional[Callable[[], Union[str, Awaitable[str]]]] = None,
        ws_failure_callback: Optional[Callable[[int], Union[None, Awaitable[None]]]] = None,
        instrument_resolver: Optional[InstrumentResolver] = None,
    ):
        self._cfg = config
        self._session_factory = session_factory or self._default_session_factory
        self._session = self._session_factory(None)
        self._stream = stream_client
        self._token_refresh_cb = token_refresh_cb
        self._rate_limiter = RateLimiter(config.max_order_rate or 15)
        self._subscriptions: set[str] = set()
        self._watchdog: Optional[asyncio.Task] = None
        self._stop = asyncio.Event()
        self._logger = get_logger("UpstoxBroker")
        self._ws_failure_cb = ws_failure_callback
        self._resolver = instrument_resolver

    async def start(self) -> None:
        if self._stream and not self._watchdog:
            await self._stream.connect()
            self._watchdog = asyncio.create_task(self._stream_watchdog(), name="upstox-stream-watchdog")

    async def stop(self) -> None:
        self._stop.set()
        if self._watchdog:
            self._watchdog.cancel()
            try:
                await self._watchdog
            except asyncio.CancelledError:
                pass
            self._watchdog = None
        if self._stream:
            await self._stream.close()

    async def submit_order(self, order: Order) -> BrokerOrderAck:
        instrument_symbol = order.symbol
        if self._resolver:
            instrument_symbol = await self._resolver.resolve_symbol(order.symbol)
        payload = upstox_client.PlaceOrderV3Request(
            instrument_token=instrument_symbol,
            transaction_type=order.side.upper(),
            order_type=order.order_type,
            product="I",
            validity="DAY",
            quantity=int(order.qty),
            disclosed_quantity=0,
            trigger_price=0.0,
            price=order.limit_price or 0.0,
            is_amo=False,
            slice=False,
            tag=order.strategy,
        )
        resp = await self._rest_call("submit_order", lambda session: session.order_api_v3.place_order(payload, algo_name=session.config.algo_name))
        broker_id = str(resp.get("data", {}).get("order_id") or resp.get("order_id"))
        status = resp.get("status") or "submitted"
        return BrokerOrderAck(broker_order_id=broker_id, status=status)

    async def replace_order(self, order: Order, *, price: Optional[float], qty: Optional[int]) -> None:
        body = upstox_client.ModifyOrderV3Request(
            instrument_token=order.symbol,
            order_id=order.broker_order_id,
            transaction_type=order.side.upper(),
            order_type=order.order_type,
            product="I",
            validity="DAY",
            quantity=int(qty or order.qty),
            disclosed_quantity=0,
            trigger_price=0.0,
            price=price if price is not None else order.limit_price or 0.0,
        )
        await self._rest_call("replace_order", lambda session: session.order_api_v3.modify_order(body))

    async def cancel_order(self, order: Order) -> None:
        if not order.broker_order_id:
            return
        await self._rest_call("cancel_order", lambda session: session.order_api_v3.cancel_order(order.broker_order_id))

    async def fetch_open_orders(self) -> List[BrokerOrderView]:
        try:
            resp = await self._rest_call("order_book", lambda session: session.order_api_v3.get_order_book())
        except BrokerError:
            return []
        data = resp.get("data") or []
        views: List[BrokerOrderView] = []
        for entry in data:
            status = str(entry.get("status") or "").lower()
            if status in {"complete", "cancelled"}:
                continue
            views.append(
                BrokerOrderView(
                    broker_order_id=str(entry.get("order_id")),
                    client_order_id=str(entry.get("client_order_id") or "") or None,
                    status=status,
                    filled_qty=int(entry.get("filled_quantity") or 0),
                    avg_price=float(entry.get("average_price") or 0.0),
                )
            )
        return views

    async def subscribe_marketdata(self, instruments: Iterable[str]) -> None:
        if not self._stream:
            return
        self._subscriptions = set(instruments)
        await self._stream.subscribe(list(self._subscriptions))
        if not self._watchdog:
            await self.start()

    # ------------------------------------------------------------------ helpers
    async def _rest_call(self, action: str, fn: Callable[[UpstoxSession], Any]) -> Any:
        last_exc: Optional[Exception] = None
        for attempt in (1, 2):
            await self._rate_limiter.acquire()
            try:
                return await asyncio.wait_for(asyncio.to_thread(fn, self._session), timeout=self._cfg.rest_timeout)
            except asyncio.TimeoutError as exc:
                raise BrokerError(code="timeout", message=f"{action} timed out") from exc
            except ApiException as exc:
                last_exc = exc
                if exc.status == 401 and self._token_refresh_cb and attempt == 1:
                    await self._refresh_token()
                    continue
                raise BrokerError(code="api_error", message=str(exc), status=exc.status) from exc
            except Exception as exc:  # pragma: no cover - defensive
                last_exc = exc
        assert last_exc is not None
        raise BrokerError(code="unknown", message=str(last_exc)) from last_exc

    async def _refresh_token(self) -> None:
        if not self._token_refresh_cb:
            return
        token = self._token_refresh_cb()
        if asyncio.iscoroutine(token):
            token = await token
        self._session = self._session_factory(str(token))

    async def _stream_watchdog(self) -> None:
        backoffs = self._cfg.ws_backoff_seconds or (1, 2, 5, 10)
        failures = 0
        while not self._stop.is_set():
            try:
                await asyncio.wait_for(self._stream.heartbeat(), timeout=self._cfg.ws_heartbeat_interval)  # type: ignore[arg-type]
                await asyncio.sleep(self._cfg.ws_heartbeat_interval)
                failures = 0
            except Exception:
                failures += 1
                idx = min(failures - 1, len(backoffs) - 1)
                delay = backoffs[idx]
                await asyncio.sleep(delay + random.uniform(0, delay))
                if self._ws_failure_cb and failures >= len(backoffs):
                    await self._notify_ws_failure(failures)
                try:
                    await self._stream.close()  # type: ignore[union-attr]
                except Exception:
                    pass
                await self._stream.connect()  # type: ignore[union-attr]
                if self._subscriptions:
                    await self._stream.subscribe(list(self._subscriptions))  # type: ignore[union-attr]

    @staticmethod
    def _default_session_factory(token: Optional[str]) -> UpstoxSession:
        cfg = UpstoxConfig(access_token=token, sandbox=False) if token else None  # type: ignore[arg-type]
        return UpstoxSession(cfg)

    async def _notify_ws_failure(self, failures: int) -> None:
        if not self._ws_failure_cb:
            return
        cb = self._ws_failure_cb
        result = cb(failures)
        if asyncio.iscoroutine(result):
            await result


__all__ = ["BrokerError", "MarketStreamClient", "RateLimiter", "UpstoxBroker"]
