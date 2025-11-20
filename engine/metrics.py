from __future__ import annotations

import logging
import os
import time
from typing import Iterable, Mapping, Optional, Sequence

try:  # pragma: no cover - optional dependency
    from prometheus_client import CollectorRegistry, Counter, Gauge, Histogram, start_http_server

    PROM_AVAILABLE = True
except ImportError:  # pragma: no cover
    CollectorRegistry = None  # type: ignore
    Counter = Gauge = Histogram = None  # type: ignore
    start_http_server = None  # type: ignore
    PROM_AVAILABLE = False


class _NoOpMetric:
    def labels(self, *_, **__):
        return self

    def inc(self, *_: float, **__: float) -> None:
        return

    def observe(self, *_: float, **__: float) -> None:
        return

    def set(self, *_: float, **__: float) -> None:
        return


class EngineMetrics:
    """Prometheus-backed metrics with graceful degradation when the client is missing."""

    def __init__(self, registry: Optional[CollectorRegistry] = None) -> None:  # type: ignore[assignment]
        self._registry = registry
        if not PROM_AVAILABLE:
            self.engine_up = _NoOpMetric()
            self.heartbeat_ts = _NoOpMetric()
            self.event_queue_depth = _NoOpMetric()
            self.order_queue_depth = _NoOpMetric()
            self.ws_lag_ms = _NoOpMetric()
            self.pnl_realized = _NoOpMetric()
            self.pnl_unrealized = _NoOpMetric()
            self.pnl_fees = _NoOpMetric()
            self.pnl_net_rupees = _NoOpMetric()
            self.pnl_net = self.pnl_net_rupees
            self.risk_halt_state = _NoOpMetric()
            self.risk_halts_total = _NoOpMetric()
            self.orders_submitted_total = _NoOpMetric()
            self.orders_filled_total = _NoOpMetric()
            self.orders_rejected_total = _NoOpMetric()
            self.fills_total = self.orders_filled_total
            self.rejects_total = self.orders_rejected_total
            self.order_latency_ms_bucketed = _NoOpMetric()
            self.http_401_total = _NoOpMetric()
            self.rest_retries_total = _NoOpMetric()
            self.ws_reconnects_total = _NoOpMetric()
            self.md_subscription_count = _NoOpMetric()
            self.md_decode_errors_total = _NoOpMetric()
            self.ratelimit_tokens = _NoOpMetric()
            self.broker_queue_depth = _NoOpMetric()
            self.api_errors_total = _NoOpMetric()
            self.session_state = _NoOpMetric()
            self.expiry_discovery_attempt_total = _NoOpMetric()
            self.expiry_discovery_success_total = _NoOpMetric()
            self.expiry_override_used_total = _NoOpMetric()
            self.expiry_source = _NoOpMetric()
            self.md_subscription = _NoOpMetric()
            self.option_ltp = _NoOpMetric()
            self.option_bid = _NoOpMetric()
            self.option_ask = _NoOpMetric()
            self.option_iv = _NoOpMetric()
            self.option_oi = _NoOpMetric()
            self.option_bid_depth_price = _NoOpMetric()
            self.option_bid_depth_qty = _NoOpMetric()
            self.option_ask_depth_price = _NoOpMetric()
            self.option_ask_depth_qty = _NoOpMetric()
            self.option_last_ts_seconds = _NoOpMetric()
            self.ltp_underlying = _NoOpMetric()
            self.underlying_last_ts_seconds = _NoOpMetric()
            self.md_subscription_info = _NoOpMetric()
            self.md_last_tick_ts = _NoOpMetric()
            return
        registry_kwargs = {"registry": self._registry} if self._registry is not None else {}
        self.engine_up = Gauge("engine_up", "Engine up status", **registry_kwargs)
        self.heartbeat_ts = Gauge("heartbeat_ts", "Unix timestamp of last heartbeat", **registry_kwargs)
        self.event_queue_depth = Gauge("event_queue_depth", "Depth of market event queue", **registry_kwargs)
        self.order_queue_depth = Gauge("order_queue_depth", "Depth of order queue", **registry_kwargs)
        self.ws_lag_ms = Gauge("ws_lag_ms", "Latest websocket lag in ms", **registry_kwargs)
        self.pnl_realized = Gauge("pnl_realized", "Realized PnL", **registry_kwargs)
        self.pnl_unrealized = Gauge("pnl_unrealized", "Unrealized PnL", **registry_kwargs)
        self.pnl_fees = Gauge("pnl_fees", "Total fees", **registry_kwargs)
        self.pnl_net_rupees = Gauge("pnl_net_rupees", "Net PnL after fees", **registry_kwargs)
        self.pnl_net = self.pnl_net_rupees
        self.risk_halt_state = Gauge("risk_halt_state", "Risk halt state (0/1)", **registry_kwargs)
        self.risk_halts_total = Counter("risk_halts_total", "Risk halts raised", **registry_kwargs)
        self.orders_submitted_total = Counter("orders_submitted_total", "Orders submitted", **registry_kwargs)
        self.orders_filled_total = Counter("orders_filled_total", "Orders filled", **registry_kwargs)
        self.orders_rejected_total = Counter("orders_rejected_total", "Orders rejected", ["reason"], **registry_kwargs)
        self.fills_total = self.orders_filled_total
        self.rejects_total = self.orders_rejected_total
        self.order_latency_ms_bucketed = Histogram("order_latency_ms_bucketed", "Order latency histogram", ["operation"], **registry_kwargs)
        self.http_401_total = Counter("http_401_total", "HTTP 401 responses", **registry_kwargs)
        self.rest_retries_total = Counter("rest_retries_total", "REST retries", ["endpoint"], **registry_kwargs)
        self.ws_reconnects_total = Counter("ws_reconnects_total", "WS reconnect attempts", **registry_kwargs)
        self.md_subscription_count = Gauge("md_subscription_count", "Active market-data subscriptions", **registry_kwargs)
        self.md_decode_errors_total = Counter(
            "md_decode_errors_total",
            "Market-data decode errors",
            ["error"],
            **registry_kwargs,
        )
        self.ratelimit_tokens = Gauge("ratelimit_tokens", "Available tokens per endpoint", ["endpoint"], **registry_kwargs)
        self.broker_queue_depth = Gauge("broker_queue_depth", "Broker queue depth", ["endpoint"], **registry_kwargs)
        self.api_errors_total = Counter("api_errors_total", "API errors", ["code"], **registry_kwargs)
        self.session_state = Gauge("session_state", "Trading session state", **registry_kwargs)
        self.expiry_discovery_attempt_total = Counter("expiry_discovery_attempt_total", "Expiry discovery attempts", ["source"], **registry_kwargs)
        self.expiry_discovery_success_total = Counter("expiry_discovery_success_total", "Expiry discovery successes", ["source"], **registry_kwargs)
        self.expiry_override_used_total = Counter("expiry_override_used_total", "Expiry override activations", **registry_kwargs)
        self.expiry_source = Gauge("expiry_source", "Expiry discovery source (0=contracts,1=instruments,2=override)", ["symbol"], **registry_kwargs)
        option_labels = ["instrument_key", "symbol", "expiry", "opt_type", "strike"]
        self.md_subscription = Gauge(
            "md_subscription",
            "Market data subscription status (1=subscribed)",
            option_labels,
            **registry_kwargs,
        )
        self.option_ltp = Gauge("option_ltp", "Option last traded price", option_labels, **registry_kwargs)
        self.option_bid = Gauge("option_bid", "Option best bid", option_labels, **registry_kwargs)
        self.option_ask = Gauge("option_ask", "Option best ask", option_labels, **registry_kwargs)
        self.option_iv = Gauge("option_iv", "Option implied volatility", option_labels, **registry_kwargs)
        self.option_oi = Gauge("option_oi", "Option open interest", option_labels, **registry_kwargs)
        depth_labels = option_labels + ["level"]
        self.option_bid_depth_price = Gauge("option_bid_depth_price", "Bid depth price by level", depth_labels, **registry_kwargs)
        self.option_bid_depth_qty = Gauge("option_bid_depth_qty", "Bid depth quantity by level", depth_labels, **registry_kwargs)
        self.option_ask_depth_price = Gauge("option_ask_depth_price", "Ask depth price by level", depth_labels, **registry_kwargs)
        self.option_ask_depth_qty = Gauge("option_ask_depth_qty", "Ask depth quantity by level", depth_labels, **registry_kwargs)
        self.option_last_ts_seconds = Gauge(
            "option_last_ts_seconds",
            "Epoch seconds of last tick",
            ["instrument_key"],
            **registry_kwargs,
        )
        self.ltp_underlying = Gauge("ltp_underlying", "Underlying last price", ["instrument"], **registry_kwargs)
        self.underlying_last_ts_seconds = Gauge(
            "underlying_last_ts_seconds",
            "Epoch seconds of last underlying tick",
            ["instrument"],
            **registry_kwargs,
        )
        self.subscription_expiry = Gauge(
            "subscription_expiry",
            "Chosen subscription expiry (0=current, 1=next, 2=monthly)",
            ["symbol", "expiry"],
            **registry_kwargs,
        )
        self.md_subscription_info = Gauge(
            "md_subscription_info",
            "Active market-data subscriptions for options",
            ["instrument", "symbol", "expiry", "strike", "opt", "tag"],
            **registry_kwargs,
        )
        self.md_last_tick_ts = Gauge("md_last_tick_ts", "Epoch seconds of last processed tick", ["instrument"], **registry_kwargs)

    def beat(self) -> None:
        self.heartbeat_ts.set(time.time())

    def inc_api_error(self, code: str) -> None:
        self.api_errors_total.labels(code=code).inc()

    def inc_orders_rejected(self, reason: str) -> None:
        self.orders_rejected_total.labels(reason=reason).inc()

    def set_ratelimit_tokens(self, endpoint: str, tokens: float) -> None:
        self.ratelimit_tokens.labels(endpoint=endpoint).set(tokens)

    def set_broker_queue_depth(self, endpoint: str, depth: int) -> None:
        self.broker_queue_depth.labels(endpoint=endpoint).set(depth)

    def set_risk_halt_state(self, value: int | float) -> None:
        self.risk_halt_state.set(value)


_GLOBAL_METRICS: Optional[EngineMetrics] = None


def bind_global_metrics(metrics: Optional[EngineMetrics]) -> None:
    global _GLOBAL_METRICS
    _GLOBAL_METRICS = metrics


def _maybe_metrics() -> Optional[EngineMetrics]:
    return _GLOBAL_METRICS


def inc_api_error(code: str) -> None:
    meter = _maybe_metrics()
    if meter:
        meter.inc_api_error(code)


def inc_orders_rejected(reason: str) -> None:
    meter = _maybe_metrics()
    if meter:
        meter.inc_orders_rejected(reason)


def set_risk_halt_state(value: int | float) -> None:
    meter = _maybe_metrics()
    if meter:
        meter.set_risk_halt_state(value)


def inc_expiry_attempt(source: str) -> None:
    meter = _maybe_metrics()
    if meter:
        meter.expiry_discovery_attempt_total.labels(source=source).inc()


def inc_expiry_success(source: str) -> None:
    meter = _maybe_metrics()
    if meter:
        meter.expiry_discovery_success_total.labels(source=source).inc()


def inc_expiry_override_used() -> None:
    meter = _maybe_metrics()
    if meter:
        meter.expiry_override_used_total.inc()


def set_expiry_source(symbol: str, value: int | float) -> None:
    meter = _maybe_metrics()
    if meter:
        meter.expiry_source.labels(symbol=symbol.upper()).set(value)


def set_session_state(value: int | float) -> None:
    meter = _maybe_metrics()
    if meter:
        meter.session_state.set(value)


def set_md_subscription_gauge(count: int) -> None:
    meter = _maybe_metrics()
    if meter:
        meter.md_subscription_count.set(int(count))


def record_md_decode_error(msg: str) -> None:
    meter = _maybe_metrics()
    if meter:
        meter.md_decode_errors_total.labels(error=str(msg)[:120]).inc()


def incr_ws_reconnects() -> None:
    meter = _maybe_metrics()
    if meter:
        meter.ws_reconnects_total.inc()


def set_subscription_expiry(symbol: str, expiry: str, mode: str) -> None:
    meter = _maybe_metrics()
    if not meter:
        return
    normalized_mode = str(mode or "current").strip().lower()
    if normalized_mode not in {"current", "next", "monthly"}:
        normalized_mode = "current"
    value = 0 if normalized_mode == "current" else (1 if normalized_mode == "next" else 2)
    meter.subscription_expiry.labels(symbol=symbol.upper(), expiry=expiry).set(value)


def _option_label_values(
    instrument_key: str,
    symbol: str,
    expiry: str,
    opt_type: str,
    strike: str | int | float,
) -> dict[str, str]:
    return {
        "instrument_key": str(instrument_key or ""),
        "symbol": str(symbol or ""),
        "expiry": str(expiry or ""),
        "opt_type": str(opt_type or ""),
        "strike": str(strike),
    }


def set_md_subscription(instrument_key: str, symbol: str, expiry: str, opt_type: str, strike: str | int | float, active: bool) -> None:
    meter = _maybe_metrics()
    if not meter:
        return
    labels = _option_label_values(instrument_key, symbol, expiry, opt_type, strike)
    meter.md_subscription.labels(**labels).set(1 if active else 0)
    if not active:
        # zero-out stale price gauges for unsubscribed instruments
        meter.option_ltp.labels(**labels).set(0.0)
        meter.option_bid.labels(**labels).set(0.0)
        meter.option_ask.labels(**labels).set(0.0)
        meter.option_iv.labels(**labels).set(0.0)
        meter.option_oi.labels(**labels).set(0.0)
        for level in range(1, 6):
            level_labels = dict(labels)
            level_labels["level"] = str(level)
            meter.option_bid_depth_price.labels(**level_labels).set(0.0)
            meter.option_bid_depth_qty.labels(**level_labels).set(0.0)
            meter.option_ask_depth_price.labels(**level_labels).set(0.0)
            meter.option_ask_depth_qty.labels(**level_labels).set(0.0)


def publish_subscription_info(
    instrument: str,
    *,
    symbol: str,
    expiry: str,
    strike: str,
    opt: str,
    tag: str = "",
) -> None:
    meter = _maybe_metrics()
    if not meter:
        return
    meter.md_subscription_info.labels(
        instrument=instrument,
        symbol=symbol,
        expiry=expiry,
        strike=strike,
        opt=opt,
        tag=tag,
    ).set(1)


def clear_subscription_info(instrument: str, symbol: str, expiry: str, strike: str, opt: str, tag: str = "") -> None:
    meter = _maybe_metrics()
    if not meter:
        return
    try:
        meter.md_subscription_info.remove(instrument, symbol, expiry, strike, opt, tag)
    except Exception:
        meter.md_subscription_info.labels(
            instrument=instrument,
            symbol=symbol,
            expiry=expiry,
            strike=strike,
            opt=opt,
            tag=tag,
        ).set(0)


def set_last_tick_ts(instrument: str, ts_seconds: float) -> None:
    meter = _maybe_metrics()
    if not meter:
        return
    meter.md_last_tick_ts.labels(instrument=instrument).set(ts_seconds)


def update_option_quote(
    instrument_key: str,
    symbol: str,
    expiry: str,
    opt_type: str,
    strike: str | int | float,
    *,
    ltp: Optional[float] = None,
    bid: Optional[float] = None,
    ask: Optional[float] = None,
    iv: Optional[float] = None,
    oi: Optional[float] = None,
    ts_seconds: Optional[float] = None,
) -> None:
    meter = _maybe_metrics()
    if not meter:
        return
    labels = _option_label_values(instrument_key, symbol, expiry, opt_type, strike)
    if ltp is not None:
        meter.option_ltp.labels(**labels).set(float(ltp))
    if bid is not None:
        meter.option_bid.labels(**labels).set(float(bid))
    if ask is not None:
        meter.option_ask.labels(**labels).set(float(ask))
    if iv is not None:
        meter.option_iv.labels(**labels).set(float(iv))
    if oi is not None:
        meter.option_oi.labels(**labels).set(float(oi))
    if ts_seconds is not None:
        meter.option_last_ts_seconds.labels(instrument_key=labels["instrument_key"]).set(float(ts_seconds))


def publish_option_quote(
    key: str,
    symbol: str,
    expiry: str,
    opt: str,
    strike: int | float | str,
    *,
    ltp: Optional[float] = None,
    bid: Optional[float] = None,
    ask: Optional[float] = None,
    iv: Optional[float] = None,
    oi: Optional[float] = None,
    ts: Optional[float] = None,
) -> None:
    """Alias wrapper for option quote metrics with normalized labels."""

    update_option_quote(
        key,
        symbol,
        expiry,
        opt,
        strike,
        ltp=ltp,
        bid=bid,
        ask=ask,
        iv=iv,
        oi=oi,
        ts_seconds=ts,
    )


def _coerce_depth(entries: Optional[Iterable[object]], limit: int) -> list[tuple[Optional[float], Optional[float]]]:
    sanitized: list[tuple[Optional[float], Optional[float]]] = []
    for entry in entries or []:
        price = qty = None
        if isinstance(entry, Mapping):
            price = entry.get("price")
            qty = entry.get("qty")
            if price is None:
                price = entry.get("bidP") or entry.get("askP")
            if qty is None:
                qty = entry.get("bidQ") or entry.get("askQ")
        elif isinstance(entry, (list, tuple)) and len(entry) >= 2:
            price, qty = entry[0], entry[1]
        try:
            price_val = float(price) if price is not None else None
        except (TypeError, ValueError):
            price_val = None
        try:
            qty_val = float(qty) if qty is not None else None
        except (TypeError, ValueError):
            qty_val = None
        if price_val is None and qty_val is None:
            continue
        sanitized.append((price_val, qty_val))
        if len(sanitized) >= limit:
            break
    return sanitized


def publish_option_depth(
    key: str,
    symbol: str,
    expiry: str,
    opt: str,
    strike: int | float | str,
    *,
    bids: Optional[Iterable[object]] = None,
    asks: Optional[Iterable[object]] = None,
    levels: int = 5,
) -> None:
    """Publish bid/ask depth arrays, trimming to configured levels."""

    meter = _maybe_metrics()
    if not meter:
        return
    labels = _option_label_values(key, symbol, expiry, opt, strike)
    limit = max(int(levels or 1), 1)
    bid_depth = _coerce_depth(bids, limit)
    ask_depth = _coerce_depth(asks, limit)
    for idx in range(limit):
        level_labels = dict(labels)
        level_labels["level"] = str(idx + 1)
        bid_price, bid_qty = bid_depth[idx] if idx < len(bid_depth) else (None, None)
        ask_price, ask_qty = ask_depth[idx] if idx < len(ask_depth) else (None, None)
        meter.option_bid_depth_price.labels(**level_labels).set(float(bid_price or 0.0))
        meter.option_bid_depth_qty.labels(**level_labels).set(float(bid_qty or 0.0))
        meter.option_ask_depth_price.labels(**level_labels).set(float(ask_price or 0.0))
        meter.option_ask_depth_qty.labels(**level_labels).set(float(ask_qty or 0.0))


def publish_underlying(symbol: str, ltp: float, ts: float | None = None) -> None:
    meter = _maybe_metrics()
    if not meter:
        return
    sym = str(symbol or "").upper()
    meter.ltp_underlying.labels(instrument=sym).set(float(ltp))
    if ts is not None:
        meter.underlying_last_ts_seconds.labels(instrument=sym).set(float(ts))


def start_http_server_if_available(port: Optional[int] = None) -> bool:
    if not PROM_AVAILABLE or start_http_server is None:
        logging.getLogger("metrics").warning("prometheus_client missing; metrics disabled")
        return False
    addr = os.getenv("METRICS_HOST", "0.0.0.0")
    start_http_server(port or 9103, addr=addr)
    return True


__all__ = [
    "EngineMetrics",
    "bind_global_metrics",
    "inc_expiry_attempt",
    "inc_api_error",
    "inc_expiry_override_used",
    "inc_expiry_success",
    "inc_orders_rejected",
    "incr_ws_reconnects",
    "set_md_subscription_gauge",
    "record_md_decode_error",
    "set_md_subscription",
    "publish_subscription_info",
    "clear_subscription_info",
    "set_last_tick_ts",
    "update_option_quote",
    "publish_option_quote",
    "publish_option_depth",
    "publish_underlying",
    "set_expiry_source",
    "set_session_state",
    "set_subscription_expiry",
    "set_risk_halt_state",
    "start_http_server_if_available",
]
