from __future__ import annotations

import asyncio
import datetime as dt
import hashlib
import time
from dataclasses import dataclass, field, replace
from enum import Enum
from typing import Any, Dict, Iterable, List, Optional, Protocol

from engine.alerts import notify_incident
from engine.config import IST, OMSConfig
from engine.data import get_app_config
from engine.events import EventBus
from engine.logging_utils import get_logger
from engine.metrics import EngineMetrics

try:
    from engine.metrics import (
        observe_ack_to_fill_ms,
        observe_tick_to_submit_ms,
        record_state_transition,
        set_oms_inflight,
        effective_entry_spread_pct,
    )
except Exception:  # pragma: no cover

    def observe_ack_to_fill_ms(ms: float): ...

    def observe_tick_to_submit_ms(ms: float): ...

    def record_state_transition(old: str, new: str): ...

    def set_oms_inflight(n: int): ...

    class _E:
        def labels(self, *a, **k):
            return self

        def set(self, *a, **k):
            return None

    effective_entry_spread_pct = _E()
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
ALLOWED_TRANSITIONS: dict[OrderState, set[OrderState]] = {
    OrderState.NEW: {OrderState.SUBMITTED, OrderState.CANCELED, OrderState.REJECTED, OrderState.PARTIALLY_FILLED},
    OrderState.SUBMITTED: {OrderState.ACKNOWLEDGED, OrderState.CANCELED, OrderState.REJECTED, OrderState.PARTIALLY_FILLED, OrderState.FILLED},
    OrderState.ACKNOWLEDGED: {OrderState.PARTIALLY_FILLED, OrderState.FILLED, OrderState.CANCELED, OrderState.REJECTED},
    OrderState.PARTIALLY_FILLED: {OrderState.PARTIALLY_FILLED, OrderState.FILLED, OrderState.CANCELED, OrderState.REJECTED},
    OrderState.FILLED: set(),
    OrderState.REJECTED: set(),
    OrderState.CANCELED: set(),
}


class OrderValidationError(Exception):
    def __init__(self, code: str, message: str):
        super().__init__(message)
        self.code = code


def round_to_tick(value: float, tick_size: float) -> float:
    if tick_size <= 0:
        return value
    multiple = round(value / tick_size)
    return round(multiple * tick_size, ndigits=8)


def validate_order(
    symbol: str,
    expiry: str,
    strike: float,
    opt_type: str,
    side: str,
    qty: int,
    price_or_trigger: Optional[float],
    *,
    lot_size: int,
    tick: float,
    band_low: float,
    band_high: float,
) -> tuple[int, Optional[float]]:
    """Validate and normalize quantity/price for an order prior to submission."""

    lot = max(int(lot_size or 1), 1)
    qty = int(qty)
    if qty % lot != 0:
        raise OrderValidationError("lot_multiple", f"Qty {qty} not aligned to lot size {lot}")
    rounded_price: Optional[float] = None
    if price_or_trigger is not None:
        rounded_price = round_to_tick(float(price_or_trigger), tick or 0.0)
        if band_low and rounded_price < band_low:
            raise OrderValidationError("price_band_low", f"Price {rounded_price} below band {band_low}")
        if band_high and band_high > 0 and rounded_price > band_high:
            raise OrderValidationError("price_band_high", f"Price {rounded_price} above band {band_high}")
    return qty, rounded_price


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
    last_md_tick_ts: float = 0.0
    submit_ts: float = 0.0
    ack_ts: float = 0.0
    last_update_ts: float = 0.0

    def mark_state(self, new_state: OrderState, *, ts: Optional[dt.datetime] = None, reason: Optional[str] = None) -> None:
        self.state = new_state
        self.updated_at = ts or utc_now()
        self.last_update_ts = time.time()

    @classmethod
    def from_snapshot(cls, snapshot: Dict[str, Any]) -> "Order":
        state = OrderState(snapshot["state"])
        ts = dt.datetime.fromisoformat(snapshot["last_update"])
        obj = cls(
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
        obj.last_update_ts = ts.timestamp()
        return obj


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
    instrument_key: Optional[str] = None
    side: Optional[str] = None


class BrokerClient(Protocol):
    async def submit_order(self, order: Order) -> BrokerOrderAck: ...

    async def replace_order(self, order: Order, *, price: Optional[float], qty: Optional[int]) -> None: ...

    async def cancel_order(self, order: Order) -> None: ...

    async def fetch_open_orders(self) -> List[BrokerOrderView]: ...


class OMS:
    """Stateful order management with deterministic identifiers and persistence."""

    def __init__(
        self,
        *,
        broker: BrokerClient,
        store: SQLiteStore,
        config: OMSConfig,
        bus: Optional[EventBus] = None,
        metrics: Optional[EngineMetrics] = None,
        default_meta: Optional[tuple[float, int, float, float]] = None,
        square_off_time: Optional[dt.time] = None,
    ):
        self._broker = broker
        self._store = store
        self._cfg = config
        self._bus = bus
        self._metrics = metrics
        self._orders: Dict[str, Order] = {}
        self._lock = asyncio.Lock()
        self._logger = get_logger("OMS")
        self._default_meta = default_meta or self._load_default_meta()
        self._submit_cfg = config.submit
        self._market_depth: Dict[str, Dict[str, float]] = {}
        self._square_off_time = square_off_time
        self._square_off_buffer = dt.timedelta(minutes=5)

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
            stored = self._store.find_order_by_client_id(client_id) or self._store.find_order_by_idempotency(idem)
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
                limit_price=limit_price if str(order_type).upper() != "MARKET" else None,
                idempotency_key=idem,
            )
            try:
                self._prepare_order(order)
            except OrderValidationError as exc:
                self._record_validation_failure(exc, order)
                raise
            order.submit_ts = time.time()
            if not order.last_md_tick_ts:
                order.last_md_tick_ts = order.submit_ts
            self._orders[client_id] = order
            self._update_order_metrics()
        start = time.perf_counter()
        result = await self._ensure_submitted(order)
        self._record_latency("submit", time.perf_counter() - start)
        try:
            if order.last_md_tick_ts > 0:
                delta_ms = max(0.0, (order.submit_ts or time.time()) - order.last_md_tick_ts) * 1000.0
                observe_tick_to_submit_ms(delta_ms)
        except Exception:
            pass
        try:
            if order.side.upper() == "BUY":
                depth = self._market_depth.get(order.symbol, {})
                bid = depth.get("bid")
                ask = depth.get("ask")
                ltp = depth.get("ltp") or depth.get("price") or depth.get("close")
                if bid and ask and ltp and ltp > 0:
                    eff = (float(ask) - float(bid)) / float(ltp) * 100.0
                    effective_entry_spread_pct.labels(order.client_order_id, order.symbol).set(eff)
        except Exception:
            pass
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
        try:
            now_ms = time.time() * 1000.0
            if order.ack_ts:
                observe_ack_to_fill_ms(max(0.0, now_ms - (order.ack_ts * 1000.0)))
        except Exception:
            pass
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
        await self.reconcile_from_views(remote)

    async def reconcile_from_views(self, views: List[BrokerOrderView]) -> None:
        remote_by_broker = {view.broker_order_id: view for view in views if view.broker_order_id}
        remote_by_client = {view.client_order_id: view for view in views if view.client_order_id}
        async with self._lock:
            local_orders = list(self._orders.values())
        for order in local_orders:
            view = remote_by_broker.get(order.broker_order_id) or remote_by_client.get(order.client_order_id)
            if not view:
                if order.state not in FINAL_STATES:
                    await self._persist_transition(order, order.state, OrderState.CANCELED, reason="broker_missing")
                    try:
                        self._store.record_incident(
                            "RECON_MISSING_ORDER",
                            {
                                "client_order_id": order.client_order_id,
                                "broker_order_id": order.broker_order_id,
                                "state": order.state.value,
                            },
                            ts=order.updated_at,
                        )
                    except Exception:
                        pass
                continue
            order.broker_order_id = view.broker_order_id
            delta_fill = max(0, int(view.filled_qty) - int(order.filled_qty))
            if delta_fill > 0:
                price = view.avg_price if view.avg_price > 0 else (order.limit_price or order.avg_fill_price or 0.0)
                await self.record_fill(order.client_order_id, qty=delta_fill, price=float(price), broker_order_id=view.broker_order_id)
            view_status = str(view.status or "").lower()
            target_state = order.state
            if view_status in {"complete", "completed", "filled"} or view.filled_qty >= order.qty:
                target_state = OrderState.FILLED
            elif view_status in {"cancelled", "canceled"}:
                target_state = OrderState.CANCELED
            elif view_status in {"rejected", "rejected_by_exchange"}:
                target_state = OrderState.REJECTED
            elif view.filled_qty > 0:
                target_state = OrderState.PARTIALLY_FILLED
            elif view_status in {"open", "pending"} and order.state == OrderState.NEW:
                target_state = OrderState.SUBMITTED
            if target_state != order.state and order.state not in FINAL_STATES:
                await self._persist_transition(order, order.state, target_state, reason="reconcile")

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
        except BrokerError as exc:
            if getattr(exc, "code", "") == "insufficient_funds":
                await self._persist_transition(order, OrderState.SUBMITTED, OrderState.REJECTED, reason="insufficient_funds")
                self._record_reject_metric("broker")
                raise
            await self._persist_transition(order, OrderState.SUBMITTED, OrderState.NEW, reason="retry")
            raise
        except Exception:
            await self._persist_transition(order, OrderState.SUBMITTED, OrderState.NEW, reason="retry")
            raise
        order.broker_order_id = ack.broker_order_id
        await self._persist_transition(order, OrderState.SUBMITTED, OrderState.ACKNOWLEDGED, reason=ack.status)
        return order

    async def _persist_transition(self, order: Order, prev: OrderState, new: OrderState, *, reason: Optional[str] = None) -> None:
        if prev != new and new not in ALLOWED_TRANSITIONS.get(prev, set()):
            self._logger.log_event(
                40,
                "invalid_order_transition",
                client_order_id=order.client_order_id,
                prev_state=prev.value,
                attempted=new.value,
                reason=reason,
            )
            return
        prev_ts = order.updated_at
        if prev == new and reason == "replace":
            order.updated_at = utc_now()
            self._store.record_order_snapshot(order)
            return
        order.mark_state(new)
        now_ts = time.time()
        if new == OrderState.ACKNOWLEDGED:
            order.ack_ts = now_ts
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
        try:
            record_state_transition(prev.value, new.value)
        except Exception:
            pass
        if prev_ts and prev in {OrderState.ACKNOWLEDGED, OrderState.PARTIALLY_FILLED} and new in {
            OrderState.PARTIALLY_FILLED,
            OrderState.FILLED,
        }:
            pass

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
        tick_size, lot_size, band_low, band_high = self._lookup_contract_meta(underlying, expiry, strike, opt_type)
        if not skip_style:
            self._apply_submit_style(order, tick_size, band_low, band_high)
        order_type = order.order_type.upper()
        if order_type != "MARKET" and order.limit_price is None:
            raise OrderValidationError("price_missing", "Limit orders require a price")
        if order_type == "MARKET":
            order.limit_price = None
        qty, rounded_price = validate_order(
            underlying,
            expiry,
            strike,
            opt_type,
            order.side,
            order.qty,
            order.limit_price if order_type != "MARKET" else None,
            lot_size=lot_size or self._default_meta[1],
            tick=tick_size or self._default_meta[0],
            band_low=band_low or self._default_meta[2],
            band_high=band_high or self._default_meta[3],
        )
        order.qty = qty
        if rounded_price is not None:
            order.limit_price = rounded_price

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
        if contract:
            tick = float(contract.get("tick_size") or tick)
            lot = int(contract.get("lot_size") or lot)
            band_low = float(contract.get("band_low") or band_low)
            band_high = float(contract.get("band_high") or band_high)
        return (
            tick or self._default_meta[0],
            int(lot or self._default_meta[1]),
            band_low or self._default_meta[2],
            band_high or self._default_meta[3],
        )

    def _apply_submit_style(self, order: Order, tick_size: float, band_low: float, band_high: float) -> None:
        if self._close_to_square_off():
            order.order_type = "MARKET"
            order.limit_price = None
            return
        depth = self._market_depth.get(order.symbol)
        if not depth:
            return
        tick = tick_size or self._default_meta[0]
        floor_price = max(band_low or 0.0, tick if tick > 0 else 0.0)
        ceiling_price = band_high if band_high and band_high > 0 else None

        def _clamp(price: float) -> float:
            if ceiling_price and price > ceiling_price:
                price = ceiling_price
            if floor_price and price < floor_price:
                price = floor_price
            return price

        spread = max(0.0, depth["ask"] - depth["bid"])
        spread_ticks = spread / tick if tick > 0 else spread
        cfg = self._submit_cfg
        mode = cfg.default
        min_depth = max(cfg.depth_threshold, 0)
        side = order.side.upper()
        available = depth["ask_qty"] if side == "BUY" else depth["bid_qty"]
        if mode in {"auto", "ioc"} and tick > 0 and spread_ticks <= cfg.max_spread_ticks and available >= max(order.qty, min_depth):
            price = depth["ask"] if side == "BUY" else depth["bid"]
            if price is None or price <= 0:
                return
            order.limit_price = _clamp(price)
            order.order_type = "IOC_LIMIT"
            return
        if mode in {"auto", "limit"}:
            price = depth["ask"] if side == "BUY" else depth["bid"]
            if price is None or price <= 0:
                return
            peg_ticks = max(1, min(cfg.max_spread_ticks or 1, 2))
            adjustment = peg_ticks * tick if tick > 0 else 0.0
            if side == "SELL":
                adjustment *= -1
            pegged = _clamp(price + adjustment)
            order.limit_price = pegged
            order.order_type = "LIMIT"

    def _load_default_meta(self) -> tuple[float, int, float, float]:
        cfg = (get_app_config().get("data") or {})
        tick = float(cfg.get("tick_size", 0.05))
        lot = int(cfg.get("lot_size") or cfg.get("lot_step", 1))
        band_low = float(cfg.get("price_band_low", 0.0))
        band_high = float(cfg.get("price_band_high", 0.0))
        return tick, lot, band_low, band_high

    def _close_to_square_off(self) -> bool:
        if not self._square_off_time:
            return False
        now = engine_now(IST)
        square = now.astimezone(IST).replace(
            hour=self._square_off_time.hour,
            minute=self._square_off_time.minute,
            second=self._square_off_time.second,
            microsecond=0,
        )
        if square <= now:
            return False
        return (square - now) <= self._square_off_buffer

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
        notify_incident("WARN", "Local validation reject", f"code={error.code} symbol={getattr(order, 'symbol', '')}")

    def _record_reject_metric(self, reason: str) -> None:
        if self._metrics:
            self._metrics.orders_rejected_total.labels(reason=reason).inc()

    def _update_order_metrics(self) -> None:
        if not self._metrics:
            return
        pending = sum(1 for order in self._orders.values() if order.state not in FINAL_STATES)
        self._metrics.order_queue_depth.set(pending)
        set_oms_inflight(pending)

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
    "validate_order",
]
