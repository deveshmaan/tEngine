from __future__ import annotations

import asyncio
import random
import time
from typing import Any, Awaitable, Callable, Iterable, List, Optional, Protocol, Sequence, Union

import upstox_client
from upstox_client.rest import ApiException

from brokerage.upstox_client import InvalidDateError, UpstoxConfig, UpstoxSession
from engine.config import BrokerConfig
from engine.data import normalize_date, preopen_expiry_smoke
from engine.logging_utils import get_logger
from engine.oms import BrokerOrderAck, BrokerOrderView, Order
from engine.instruments import InstrumentResolver
from engine.metrics import EngineMetrics, set_session_state
from engine.alerts import notify_incident
from engine.risk import RiskManager


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


class TokenBucket:
    def __init__(self, rate_per_sec: float, burst: int):
        self._rate = max(rate_per_sec, 0.1)
        self._capacity = max(1, burst)
        self._tokens = float(self._capacity)
        self._updated = time.monotonic()
        self._lock = asyncio.Lock()

    async def acquire(self) -> float:
        while True:
            async with self._lock:
                now = time.monotonic()
                elapsed = now - self._updated
                self._updated = now
                self._tokens = min(self._capacity, self._tokens + elapsed * self._rate)
                if self._tokens >= 1.0:
                    self._tokens -= 1.0
                    return self._tokens
                deficit = 1.0 - self._tokens
                wait_for = deficit / self._rate if self._rate > 0 else 0.5
            await asyncio.sleep(max(wait_for, 0.0))


class UpstoxBroker:
    """REST + streaming broker wrapper with watchdogs and rate-limits."""

    _SESSION_STATE_MAP = {
        "PREOPEN": 0,
        "LIVE": 1,
        "AFTER_CUTOFF": 2,
        "POST_CLOSE": 3,
    }

    def __init__(
        self,
        *,
        config: BrokerConfig,
        session_factory: Optional[Callable[[Optional[str]], UpstoxSession]] = None,
        stream_client: Optional[MarketStreamClient] = None,
        token_refresh_cb: Optional[Callable[[], Union[str, Awaitable[str]]]] = None,
        ws_failure_callback: Optional[Callable[[int], Union[None, Awaitable[None]]]] = None,
        instrument_resolver: Optional[InstrumentResolver] = None,
        metrics: Optional[EngineMetrics] = None,
        auth_halt_callback: Optional[Callable[[str], Union[None, Awaitable[None]]]] = None,
        reconcile_callback: Optional[Callable[[], Awaitable[None]]] = None,
        risk_manager: Optional[RiskManager] = None,
    ):
        self._cfg = config
        self._session_factory = session_factory or self._default_session_factory
        self._session = self._session_factory(None)
        self._stream = stream_client
        self._token_refresh_cb = token_refresh_cb
        self._subscriptions: set[str] = set()
        self._watchdog: Optional[asyncio.Task] = None
        self._stop = asyncio.Event()
        self._logger = get_logger("UpstoxBroker")
        self._ws_failure_cb = ws_failure_callback
        self._resolver = instrument_resolver
        self._metrics = metrics
        self._auth_halt_cb = auth_halt_callback
        self._reconcile_cb = reconcile_callback
        self._risk = risk_manager
        self._square_off_cb: Optional[Callable[[str], Awaitable[None]]] = None
        self._shutdown_cb: Optional[Callable[[str], Awaitable[None]]] = None
        self._session_guard_decision: Optional[dict[str, Any]] = None
        self._rate_limits = self._build_rate_limits()
        self._pending_requests = {name: 0 for name in self._rate_limits}
        self._halt_new_orders = False
        self._halt_reason: Optional[str] = None
        self._queue_lock = asyncio.Lock()
        self._last_ws_heartbeat = time.monotonic()
        self._preopen_checked = False
        if self._metrics:
            self._metrics.set_risk_halt_state(0)

    async def start(self) -> None:
        await self._run_preopen_probe()
        if not await self._apply_session_guard():
            return
        if self._stream and not self._watchdog:
            await self._stream.connect()
            self._last_ws_heartbeat = time.monotonic()
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
        if self._halt_new_orders and order.side.upper() == "BUY":
            raise BrokerError(code="auth_halt", message="Broker halted due to authentication state")
        instrument_symbol = order.symbol
        if self._resolver:
            instrument_symbol = await self._resolver.resolve_symbol(order.symbol)
        payload = upstox_client.PlaceOrderV3Request(
            instrument_token=instrument_symbol,
            transaction_type=order.side.upper(),
            order_type="LIMIT" if order.order_type == "IOC_LIMIT" else order.order_type,
            product="I",
            validity="IOC" if order.order_type == "IOC_LIMIT" else "DAY",
            quantity=int(order.qty),
            disclosed_quantity=0,
            trigger_price=0.0,
            price=order.limit_price or 0.0,
            is_amo=False,
            slice=False,
            tag=order.strategy,
        )
        context = {
            "symbol": instrument_symbol,
            "side": order.side,
            "qty": order.qty,
            "client_order_id": order.client_order_id,
        }
        resp = await self._rest_call(
            "place",
            "submit_order",
            lambda session: session.order_api_v3.place_order(payload, algo_name=session.config.algo_name),
            context=context,
        )
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
        context = {
            "broker_order_id": order.broker_order_id,
            "client_order_id": order.client_order_id,
            "qty": qty or order.qty,
        }
        await self._rest_call("modify", "replace_order", lambda session: session.order_api_v3.modify_order(body), context=context)

    async def cancel_order(self, order: Order) -> None:
        if not order.broker_order_id:
            return
        context = {"broker_order_id": order.broker_order_id, "client_order_id": order.client_order_id}
        await self._rest_call("cancel", "cancel_order", lambda session: session.order_api_v3.cancel_order(order.broker_order_id), context=context)

    async def fetch_open_orders(self) -> List[BrokerOrderView]:
        try:
            resp = await self._rest_call("history", "order_book", lambda session: session.order_api_v3.get_order_book())
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
        self._last_ws_heartbeat = time.monotonic()

    # ------------------------------------------------------------------ helpers
    def _build_rate_limits(self) -> dict[str, TokenBucket]:
        limits = self._cfg.rate_limits
        return {
            "place": TokenBucket(limits.place.rate_per_sec, limits.place.burst),
            "modify": TokenBucket(limits.modify.rate_per_sec, limits.modify.burst),
            "cancel": TokenBucket(limits.cancel.rate_per_sec, limits.cancel.burst),
            "history": TokenBucket(limits.history.rate_per_sec, limits.history.burst),
        }

    async def _rest_call(self, endpoint: str, action: str, fn: Callable[[UpstoxSession], Any], *, context: Optional[dict[str, Any]] = None) -> Any:
        retries = 3
        backoff = 0.5
        last_exc: Optional[Exception] = None
        sanitized_context = self._sanitize_context(context)
        for attempt in range(1, retries + 1):
            tokens_left = await self._acquire_token(endpoint)
            if self._metrics:
                self._metrics.set_ratelimit_tokens(endpoint, tokens_left)
            try:
                result = await self._invoke_upstox(action, fn, context=sanitized_context)
                return result
            except InvalidDateError as exc:
                raise
            except asyncio.TimeoutError as exc:
                last_exc = exc
                if attempt < retries:
                    await self._record_retry(endpoint)
                    await asyncio.sleep(backoff + random.uniform(0, backoff))
                    backoff *= 2
                    continue
                break
            except ApiException as exc:
                last_exc = exc
                if exc.status == 401:
                    await self._handle_auth_failure(sanitized_context)
                    raise BrokerError(code="auth", message=f"{action} returned 401", status=401, context=sanitized_context) from exc
                if self._should_retry(exc.status) and attempt < retries:
                    await self._record_retry(endpoint)
                    await asyncio.sleep(backoff + random.uniform(0, backoff))
                    backoff *= 2
                    continue
                raise BrokerError(code="api_error", message=str(exc), status=exc.status) from exc
            except Exception as exc:
                last_exc = exc
                if attempt < retries:
                    await self._record_retry(endpoint)
                    await asyncio.sleep(backoff + random.uniform(0, backoff))
                    backoff *= 2
                    continue
                break
        raise BrokerError(code="unknown", message=str(last_exc)) from last_exc

    async def _invoke_upstox(self, action: str, fn: Callable[[UpstoxSession], Any], *, context: Optional[dict[str, Any]] = None) -> Any:
        try:
            return await asyncio.wait_for(asyncio.to_thread(fn, self._session), timeout=self._cfg.rest_timeout)
        except ApiException as exc:
            if self._is_invalid_date_error(exc):
                self._handle_invalid_date(action, exc, context)
                raise InvalidDateError("UDAPI1088 Invalid date") from exc
            raise

    @staticmethod
    def _is_invalid_date_error(exc: ApiException) -> bool:
        body = getattr(exc, "body", "") or ""
        if isinstance(body, bytes):
            body_text = body.decode("utf-8", errors="ignore")
        else:
            body_text = str(body)
        text = body_text or str(exc)
        return "UDAPI1088" in text or "Invalid date" in text

    def _handle_invalid_date(self, action: str, exc: ApiException, context: Optional[dict[str, Any]]) -> None:
        if self._metrics:
            self._metrics.inc_api_error("UDAPI1088")
            self._metrics.inc_orders_rejected("invalid_date")
        body = self._compact_context(action, context)
        notify_incident("ERROR", "UDAPI1088 Invalid date", body, tags=["invalid_date"])

    async def _acquire_token(self, endpoint: str) -> float:
        bucket = self._rate_limits.get(endpoint)
        if bucket is None:
            return 0.0
        async with self._queue_lock:
            self._pending_requests[endpoint] += 1
            self._update_queue_metric(endpoint)
        try:
            tokens_left = await bucket.acquire()
        finally:
            async with self._queue_lock:
                self._pending_requests[endpoint] = max(0, self._pending_requests[endpoint] - 1)
                self._update_queue_metric(endpoint)
        return tokens_left

    def _update_queue_metric(self, endpoint: str) -> None:
        if not self._metrics:
            return
        depth = self._pending_requests.get(endpoint, 0)
        self._metrics.set_broker_queue_depth(endpoint, depth)

    def _sanitize_context(self, context: Optional[dict[str, Any]]) -> dict[str, Any]:
        if not context:
            return {}
        sanitized: dict[str, Any] = {}
        for key, value in context.items():
            if value is None:
                continue
            sanitized[key] = value
        for key, value in list(sanitized.items()):
            lowered = key.lower()
            if any(token in lowered for token in ("date", "expiry")):
                try:
                    sanitized[key] = normalize_date(value)
                except ValueError:
                    sanitized[key] = str(value)
        return sanitized

    def _compact_context(self, action: str, context: Optional[dict[str, Any]]) -> str:
        parts = [f"action={action}"]
        if context:
            for key, value in context.items():
                parts.append(f"{key}={value}")
        return " ".join(parts)

    async def _record_retry(self, endpoint: str) -> None:
        if self._metrics:
            self._metrics.rest_retries_total.labels(endpoint=endpoint).inc()

    @staticmethod
    def _should_retry(status: Optional[int]) -> bool:
        if status is None:
            return True
        if status in {408, 425, 429}:
            return True
        return 500 <= status < 600

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
                self._last_ws_heartbeat = time.monotonic()
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
                if self._metrics:
                    self._metrics.ws_reconnects_total.inc()
                if self._reconcile_cb:
                    await self._reconcile_cb()

    async def _run_preopen_probe(self) -> None:
        if self._preopen_checked:
            return
        self._logger.info("Pre-open expiry probe starting")
        try:
            await asyncio.to_thread(preopen_expiry_smoke)
        except Exception as exc:  # pragma: no cover - network dependent
            self._logger.exception("Pre-open expiry probe FAILED: %s", exc)
            if self._metrics:
                self._metrics.set_risk_halt_state(1)
            if self._auth_halt_cb:
                result = self._auth_halt_cb("EXPIRY_DISCOVERY_FAILED")
                if asyncio.iscoroutine(result):
                    await result
        else:
            self._logger.info("Pre-open expiry probe OK")
        finally:
            self._preopen_checked = True

    async def _apply_session_guard(self) -> bool:
        if not self._risk:
            self._session_guard_decision = {"action": "continue", "reason": "LIVE", "state": 1, "positions": 0}
            set_session_state(1)
            return True
        now = self._risk.session_guard_now()
        positions_count = self._risk.open_position_count()
        decision = self._risk.evaluate_boot_state(now, positions_count > 0)
        state_value = self._SESSION_STATE_MAP.get(decision.get("reason", "").upper(), 4)
        set_session_state(state_value)
        payload = dict(decision)
        payload["state"] = state_value
        payload["positions"] = positions_count
        self._session_guard_decision = payload
        action = decision.get("action")
        reason = decision.get("reason", "UNKNOWN")
        if action == "continue":
            if self._metrics:
                self._metrics.set_risk_halt_state(0)
            return True
        if self._metrics:
            self._metrics.set_risk_halt_state(1)
        if action == "halt":
            self._risk.halt_new_entries(reason)
            if self._auth_halt_cb:
                result = self._auth_halt_cb(reason)
                if asyncio.iscoroutine(result):
                    await result
            self._logger.warning("SessionGuard: HALT (%s)", reason)
            return False
        if action == "squareoff_then_halt":
            if self._square_off_cb:
                await self._square_off_cb("SESSION_END")
            else:
                self._logger.warning("SessionGuard: square-off callback not bound; skipping exits")
            self._risk.halt_new_entries("POST_CLOSE")
            if self._auth_halt_cb:
                result = self._auth_halt_cb("POST_CLOSE")
                if asyncio.iscoroutine(result):
                    await result
            self._logger.warning("SessionGuard: squared-off and HALTED (post-close)")
            return False
        if action == "shutdown":
            self._logger.warning("SessionGuard: shutting down post-close (per config)")
            if self._shutdown_cb:
                await self._shutdown_cb("POST_CLOSE_SHUTDOWN")
            return False
        return True

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

    async def _handle_auth_failure(self, context: Optional[dict[str, Any]]) -> None:
        if self._metrics:
            self._metrics.http_401_total.inc()
        if self._halt_new_orders:
            return
        body = self._compact_context("401", context)
        await self._halt_and_notify("AUTH_401", body, level="ERROR")

    async def _halt_and_notify(self, reason: str, detail: str, level: str = "ERROR") -> None:
        self._halt_new_orders = True
        self._halt_reason = reason
        if self._metrics:
            self._metrics.set_risk_halt_state(1)
        self._logger.log_event(40, "broker_halt", reason=reason, detail=detail)
        notify_incident(level.upper(), reason, detail)
        cb = self._auth_halt_cb
        if cb:
            result = cb(reason)
            if asyncio.iscoroutine(result):
                await result

    def resume_trading(self) -> None:
        self._halt_new_orders = False
        self._halt_reason = None
        if self._metrics:
            self._metrics.set_risk_halt_state(0)

    def halt_reason(self) -> Optional[str]:
        return self._halt_reason

    def bind_reconcile_callback(self, callback: Callable[[], Awaitable[None]]) -> None:
        self._reconcile_cb = callback

    def bind_square_off_callback(self, callback: Callable[[str], Awaitable[None]]) -> None:
        self._square_off_cb = callback

    def bind_shutdown_callback(self, callback: Callable[[str], Awaitable[None]]) -> None:
        self._shutdown_cb = callback

    @property
    def session_guard_decision(self) -> Optional[dict[str, Any]]:
        return self._session_guard_decision


__all__ = ["BrokerError", "MarketStreamClient", "TokenBucket", "UpstoxBroker"]
