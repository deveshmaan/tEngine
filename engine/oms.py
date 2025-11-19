from __future__ import annotations

import asyncio
import datetime as dt
import hashlib
import time
from dataclasses import dataclass, field, replace
from enum import Enum
from typing import Any, Dict, Iterable, List, Optional, Protocol

from engine.config import IST, OMSConfig
from engine.data import get_app_config
from engine.events import EventBus
from engine.logging_utils import get_logger
from engine.metrics import EngineMetrics
from engine.time_machine import now as engine_now, utc_now
from market.instrument_cache import InstrumentCache
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


class OrderValidationError(Exception):
    def __init__(self, code: str, message: str):
        super().__init__(message)
        self.code = code


def round_to_tick(value: float, tick_size: float) -> float:
    if tick_size <= 0:
        return value
    multiple = round(value / tick_size)
    return round(multiple * tick_size, ndigits=8)


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
        self._default_meta = self._load_default_meta()
        self._submit_style = self._load_submit_style()
        self._market_depth: Dict[str, Dict[str, float]] = {}

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

    def update_market_depth(self, symbol: str, *, bid: float, ask: float, bid_qty: int, ask_qty: int) -> None:
        self._market_depth[symbol] = {
            "bid": float(bid),
            "ask": float(ask),
            "bid_qty": int(bid_qty),
            "ask_qty": int(ask_qty),
            "ts": engine_now(IST),
        }

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
            try:
                self._prepare_order(order)
            except OrderValidationError as exc:
                self._record_validation_failure(exc, order)
                raise
            self._orders[client_id] = order
            self._update_order_metrics()
        start = time.perf_counter()
        result = await self._ensure_submitted(order)
        self._record_latency("submit", time.perf_counter() - start)
        return result

    async def replace(self, client_order_id: str, *, price: Optional[float] = None, qty: Optional[int] = None) -> Order:
        async with self._lock:
            order = self._orders[client_order_id]
        new_price = price if price is not None else order.limit_price
        new_qty = qty if qty is not None else order.qty
        candidate = replace(order, limit_price=new_price, qty=new_qty)
        try:
            self._prepare_order(candidate, skip_style=True)
        except OrderValidationError as exc:
            self._record_validation_failure(exc, candidate)
            raise
        start = time.perf_counter()
        await self._broker.replace_order(order, price=candidate.limit_price, qty=candidate.qty)
        self._record_latency("replace", time.perf_counter() - start)
        order.limit_price = candidate.limit_price
        order.qty = candidate.qty
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
        if self._metrics:
            self._metrics.orders_filled_total.inc()
        self._update_order_metrics()

    async def mark_reject(self, client_order_id: str, message: str) -> None:
        async with self._lock:
            order = self._orders[client_order_id]
        await self._persist_transition(order, order.state, OrderState.REJECTED, reason=message)
        self._record_reject_metric("broker")
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
            if self._metrics:
                self._metrics.orders_submitted_total.inc()
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

    def _prepare_order(self, order: Order, *, skip_style: bool = False) -> None:
        if order.qty <= 0:
            raise OrderValidationError("qty_positive", "Quantity must be positive")
        try:
            underlying, expiry, strike, opt_type = self._parse_contract_symbol(order.symbol)
        except OrderValidationError:
            # Fallback for legacy symbols that do not include expiry/strike information.
            fallback_expiry = engine_now(IST).date().isoformat()
            underlying, expiry, strike, opt_type = order.symbol.upper(), fallback_expiry, 0.0, "CE"
        tick_size, lot_size, band_low, band_high, strict_meta = self._lookup_contract_meta(underlying, expiry, strike, opt_type)
        if not skip_style:
            self._apply_submit_style(order, tick_size)
        lot = max(int(lot_size), 1)
        if strict_meta and order.qty % lot != 0:
            raise OrderValidationError("lot_multiple", f"Qty {order.qty} not aligned to lot size {lot}")
        order_type = order.order_type.upper()
        if order_type != "MARKET":
            if order.limit_price is None:
                raise OrderValidationError("price_missing", "Limit orders require a price")
            rounded = round_to_tick(float(order.limit_price), tick_size or self._default_meta[0])
            order.limit_price = rounded
            if band_low and rounded < band_low:
                raise OrderValidationError("price_band_low", f"Price {rounded} below band {band_low}")
            if band_high and band_high > 0 and rounded > band_high:
                raise OrderValidationError("price_band_high", f"Price {rounded} above band {band_high}")

    def _parse_contract_symbol(self, symbol: str) -> tuple[str, str, float, str]:
        parts = symbol.split("-")
        if len(parts) < 3:
            raise OrderValidationError("symbol_format", f"Unsupported symbol format {symbol}")
        expiry = "-".join(parts[1:-1])
        tail = parts[-1]
        opt_type = tail[-2:].upper() if tail[-2:].upper() in {"CE", "PE"} else "CE"
        strike_part = tail[:-2] if opt_type in {"CE", "PE"} else tail
        try:
            dt.date.fromisoformat(expiry)
        except ValueError as exc:
            raise OrderValidationError("expiry_format", f"Invalid expiry {expiry}") from exc
        try:
            strike = float(strike_part)
        except ValueError as exc:
            raise OrderValidationError("strike_parse", f"Invalid strike in {symbol}") from exc
        return parts[0].upper(), expiry, strike, opt_type

    def _lookup_contract_meta(self, symbol: str, expiry: str, strike: float, opt_type: str) -> tuple[float, int, float, float]:
        tick, lot, band_low, band_high = self._default_meta
        strict = False
        cache = InstrumentCache.runtime_cache()
        contract = None
        if cache:
            try:
                contract = cache.get_contract(symbol, expiry, strike, opt_type)
            except Exception:
                contract = None
            if not contract:
                try:
                    cache.refresh_expiry(symbol, expiry)
                    contract = cache.get_contract(symbol, expiry, strike, opt_type)
                except Exception:
                    contract = None
            try:
                expiry_meta = cache.get_meta(symbol, expiry)
            except Exception:
                expiry_meta = None
            if isinstance(expiry_meta, tuple):
                tick = float(expiry_meta[0] or tick)
                lot = int(expiry_meta[1] or lot)
                band_low = float(expiry_meta[2] or band_low)
                band_high = float(expiry_meta[3] or band_high)
                strict = True
        if contract:
            tick = float(contract.get("tick_size") or tick)
            lot = int(contract.get("lot_size") or lot)
            band_low = float(contract.get("band_low") or band_low)
            band_high = float(contract.get("band_high") or band_high)
            strict = True
        return (
            tick or self._default_meta[0],
            int(lot or self._default_meta[1]),
            band_low or self._default_meta[2],
            band_high or self._default_meta[3],
            strict or bool(contract),
        )

    def _apply_submit_style(self, order: Order, tick_size: float) -> None:
        mode = str(self._submit_style.get("mode", "market")).lower()
        depth = self._market_depth.get(order.symbol)
        tick = tick_size or self._default_meta[0]
        if mode in {"ioc", "auto"} and depth:
            spread = max(0.0, depth["ask"] - depth["bid"])
            threshold = float(self._submit_style.get("max_spread_ticks", 1.0))
            min_depth = int(self._submit_style.get("depth_threshold", 0))
            side = order.side.upper()
            available = depth["ask_qty"] if side == "BUY" else depth["bid_qty"]
            if spread <= threshold and available >= order.qty and available >= max(1, min_depth):
                price = depth["ask"] if side == "BUY" else depth["bid"]
                order.limit_price = price
                order.order_type = "IOC_LIMIT"
                return
        if mode in {"limit", "auto"} and order.order_type.upper() == "MARKET" and order.limit_price is not None:
            order.order_type = "LIMIT"

    def _load_default_meta(self) -> tuple[float, int, float, float]:
        cfg = (get_app_config().get("data") or {})
        tick = float(cfg.get("tick_size", 0.05))
        lot = int(cfg.get("lot_size") or cfg.get("lot_step", 1))
        band_low = float(cfg.get("price_band_low", 0.0))
        band_high = float(cfg.get("price_band_high", 0.0))
        return tick, lot, band_low, band_high

    def _load_submit_style(self) -> Dict[str, float | str | int]:
        submit_cfg = ((get_app_config().get("oms") or {}).get("submit", {}))
        return {
            "mode": str(submit_cfg.get("default", "market")).lower(),
            "max_spread_ticks": float(submit_cfg.get("max_spread_ticks", 1.0)),
            "depth_threshold": int(submit_cfg.get("depth_threshold", 0)),
        }

    def _record_validation_failure(self, error: OrderValidationError, order: Optional[Order] = None) -> None:
        if self._metrics:
            self._metrics.orders_rejected_total.labels(reason="validation").inc()
        self._logger.log_event(
            30,
            "order_validation_failed",
            code=error.code,
            message=str(error),
            symbol=getattr(order, "symbol", None),
        )

    def _record_reject_metric(self, reason: str) -> None:
        if self._metrics:
            self._metrics.orders_rejected_total.labels(reason=reason).inc()

    def _update_order_metrics(self) -> None:
        if not self._metrics:
            return
        pending = sum(1 for order in self._orders.values() if order.state not in FINAL_STATES)
        self._metrics.order_queue_depth.set(pending)

    def _record_latency(self, kind: str, duration: float) -> None:
        if not self._metrics:
            return
        self._metrics.order_latency_ms_bucketed.labels(operation=kind).observe(duration * 1000.0)


__all__ = [
    "BrokerClient",
    "BrokerOrderAck",
    "BrokerOrderView",
    "OMS",
    "Order",
    "OrderValidationError",
    "OrderState",
    "round_to_tick",
]
