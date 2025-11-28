from __future__ import annotations

import logging
import os
import time
from typing import Any, Iterable, Mapping, Optional, Sequence

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
            self.exit_events_total = _NoOpMetric()
            self.http_401_total = _NoOpMetric()
            self.rest_retries_total = _NoOpMetric()
            self.order_timeouts_total = _NoOpMetric()
            self.order_timeout_retries_total = _NoOpMetric()
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
            self.market_data_stale = _NoOpMetric()
            self.strategy_last_eval_ts = _NoOpMetric()
            self.strategy_evals_total = _NoOpMetric()
            self.strategy_entry_signals_total = _NoOpMetric()
            self.smoke_test_runs_total = _NoOpMetric()
            self.smoke_test_last_notional_rupees = _NoOpMetric()
            self.smoke_test_last_ts = _NoOpMetric()
            self.smoke_test_last_reason = _NoOpMetric()
            self.smoke_test_running = _NoOpMetric()
            self.config_sanity_ok = _NoOpMetric()
            self.startup_unmanaged_positions_count = _NoOpMetric()
            self.market_data_stale_dropped_total = _NoOpMetric()
            self.market_data_stale_blocked_entries_total = _NoOpMetric()
            self.order_reconciliation_errors_total = _NoOpMetric()
            self.order_reconciliation_duration_ms = _NoOpMetric()
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
        self.exit_events_total = Counter("exit_events_total", "Exit triggers fired", ["reason"], **registry_kwargs)
        self.http_401_total = Counter("http_401_total", "HTTP 401 responses", **registry_kwargs)
        self.rest_retries_total = Counter("rest_retries_total", "REST retries", ["endpoint"], **registry_kwargs)
        self.order_timeouts_total = Counter("order_timeouts_total", "Orders that hit submit timeout and were retried", **registry_kwargs)
        self.order_timeout_retries_total = Counter("order_timeout_retries_total", "Order resubmits after timeout", **registry_kwargs)
        self.ws_reconnects_total = Counter("ws_reconnects_total", "WS reconnect attempts", **registry_kwargs)
        self.md_subscription_count = Gauge("md_subscription_count", "Active market-data subscriptions", **registry_kwargs)
        try:
            self.md_decode_errors_total = Counter(
                "md_decode_errors_total",
                "Market-data decode errors",
                ["error"],
                **registry_kwargs,
            )
        except ValueError:
            try:
                from prometheus_client import REGISTRY as _REG  # type: ignore

                existing = getattr(_REG, "_names_to_collectors", {}).get("md_decode_errors_total")  # type: ignore[attr-defined]
                if existing is not None:
                    self.md_decode_errors_total = existing
                else:
                    raise
            except Exception:
                raise
        self.ratelimit_tokens = Gauge("ratelimit_tokens", "Available tokens per endpoint", ["endpoint"], **registry_kwargs)
        self.broker_queue_depth = Gauge("broker_queue_depth", "Broker queue depth", ["endpoint"], **registry_kwargs)
        self.api_errors_total = Counter("api_errors_total", "API errors", ["code"], **registry_kwargs)
        self.session_state = Gauge("session_state", "Trading session state", **registry_kwargs)
        self.config_sanity_ok = Gauge("config_sanity_ok", "1 when config sanity checks pass", **registry_kwargs)
        self.startup_unmanaged_positions_count = Gauge(
            "startup_unmanaged_positions_count",
            "Unmanaged overnight positions detected at startup",
            **registry_kwargs,
        )
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
        self.market_data_stale = Gauge(
            "market_data_stale",
            "Market data staleness flag (1=stale)",
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
        self.smoke_test_runs_total = Counter("smoke_test_runs_total", "Smoke test runs", ["status"], **registry_kwargs)
        self.smoke_test_last_notional_rupees = Gauge("smoke_test_last_notional_rupees", "Last smoke test notional", **registry_kwargs)
        self.smoke_test_last_ts = Gauge("smoke_test_last_ts", "Epoch seconds of last smoke test", **registry_kwargs)
        self.smoke_test_last_reason = Gauge("smoke_test_last_reason", "Last smoke test reason marker", ["reason"], **registry_kwargs)
        self.smoke_test_running = Gauge("smoke_test_running", "Smoke test running flag", **registry_kwargs)
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
        self.strategy_last_eval_ts = Gauge("strategy_last_eval_ts", "Epoch seconds of last strategy evaluation", **registry_kwargs)
        self.strategy_evals_total = Counter("strategy_evals_total", "Total number of strategy evaluations", **registry_kwargs)
        self.strategy_entry_signals_total = Counter("strategy_entry_signals_total", "Strategy entry signals emitted", **registry_kwargs)
        self.market_data_stale_dropped_total = Counter(
            "market_data_stale_dropped_total",
            "Ticks dropped because they exceeded max_tick_age_seconds",
            ["instrument_key"],
            **registry_kwargs,
        )
        self.market_data_stale_blocked_entries_total = Counter(
            "market_data_stale_blocked_entries_total",
            "BUY entries blocked due to stale market data",
            ["instrument"],
            **registry_kwargs,
        )
        self.order_reconciliation_errors_total = Counter(
            "order_reconciliation_errors_total",
            "Errors during order reconciliation",
            **registry_kwargs,
        )
        self.order_reconciliation_duration_ms = Histogram(
            "order_reconciliation_duration_ms",
            "Order reconciliation duration in ms",
            buckets=(5, 10, 20, 50, 100, 200, 500, 1000, 2000, 5000),
            **registry_kwargs,
        )

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
_LAST_CONFIG_SANITY: Optional[float] = None
_LAST_UNMANAGED_POSITIONS: Optional[float] = None


def bind_global_metrics(metrics: Optional[EngineMetrics]) -> None:
    global _GLOBAL_METRICS
    _GLOBAL_METRICS = metrics
    if metrics is None:
        return
    try:
        if _LAST_CONFIG_SANITY is not None:
            metrics.config_sanity_ok.set(_LAST_CONFIG_SANITY)
        if _LAST_UNMANAGED_POSITIONS is not None:
            metrics.startup_unmanaged_positions_count.set(_LAST_UNMANAGED_POSITIONS)
    except Exception:
        pass


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


def set_market_data_stale(instrument_key: str, value: int | float) -> None:
    meter = _maybe_metrics()
    if meter:
        try:
            meter.market_data_stale.labels(instrument_key=str(instrument_key)).set(value)
        except Exception:
            pass


def inc_market_data_stale_drop(instrument_key: str) -> None:
    meter = _maybe_metrics()
    if meter:
        try:
            meter.market_data_stale_dropped_total.labels(instrument_key=str(instrument_key)).inc()
        except Exception:
            pass


def inc_market_data_stale_blocked_entries(instrument: str) -> None:
    meter = _maybe_metrics()
    if meter:
        try:
            meter.market_data_stale_blocked_entries_total.labels(instrument=str(instrument)).inc()
        except Exception:
            pass


def set_config_sanity_ok(value: int | float) -> None:
    global _LAST_CONFIG_SANITY
    _LAST_CONFIG_SANITY = float(value)
    meter = _maybe_metrics()
    if meter:
        meter.config_sanity_ok.set(value)


def set_startup_unmanaged_positions(count: int | float) -> None:
    global _LAST_UNMANAGED_POSITIONS
    _LAST_UNMANAGED_POSITIONS = float(count)
    meter = _maybe_metrics()
    if meter:
        meter.startup_unmanaged_positions_count.set(count)


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
    "set_market_data_stale",
    "inc_market_data_stale_drop",
    "inc_market_data_stale_blocked_entries",
    "order_reconciliation_errors_total",
    "order_reconciliation_duration_ms",
    "set_config_sanity_ok",
    "set_startup_unmanaged_positions",
    "start_http_server_if_available",
]

# ---------------------------------------------------------------------------#
# Extended metrics (market-data, microstructure, OMS, risk)
# ---------------------------------------------------------------------------#
try:  # pragma: no cover - optional
    from prometheus_client import REGISTRY as _PROM_REGISTRY  # type: ignore
except Exception:  # pragma: no cover
    _PROM_REGISTRY = None  # type: ignore


def _get_collector(name: str, ctor, *args, **kwargs):
    """Return existing collector if registered; otherwise create or fall back to no-op."""

    if not PROM_AVAILABLE or ctor is None:
        return _NoOpMetric()
    if _PROM_REGISTRY is not None:
        try:
            existing = _PROM_REGISTRY._names_to_collectors.get(name)  # type: ignore[attr-defined]
            if existing is not None:
                return existing
        except Exception:
            pass
    try:
        return ctor(name, *args, **kwargs)
    except Exception:
        try:
            if _PROM_REGISTRY is not None:
                existing = _PROM_REGISTRY._names_to_collectors.get(name)  # type: ignore[attr-defined]
                if existing is not None:
                    return existing
        except Exception:
            pass
        return _NoOpMetric()


def _counter(name: str, desc: str, *, labelnames: Iterable[str] | tuple[str, ...] = ()) -> Any:
    labels = tuple(labelnames) if labelnames else ()
    return _get_collector(name, Counter, desc, labelnames=labels)


def _gauge(name: str, desc: str, *, labelnames: Iterable[str] | tuple[str, ...] = ()) -> Any:
    labels = tuple(labelnames) if labelnames else ()
    return _get_collector(name, Gauge, desc, labelnames=labels)


def _histogram(
    name: str,
    desc: str,
    *,
    buckets: Iterable[float] | tuple[float, ...],
    labelnames: Iterable[str] | tuple[str, ...] = (),
) -> Any:
    labels = tuple(labelnames) if labelnames else ()
    return _get_collector(name, Histogram, desc, buckets=tuple(buckets), labelnames=labels)


# --- Market–data health ---
md_decode_errors_total = _counter(
    "md_decode_errors_total",
    "Market-data decode/parse errors (protobuf, schema).",
    labelnames=("error",),
)
md_messages_total = _counter(
    "md_messages_total", "WebSocket market-data messages processed (frames)."
)
md_message_bytes_total = _counter(
    "md_message_bytes_total", "Total bytes of market-data messages processed."
)

# --- Option microstructure ---
option_spread = _gauge(
    "option_spread", "Best-ask minus best-bid (₹) for an option.", labelnames=("instrument_key",)
)
option_spread_bp = _gauge(
    "option_spread_bp", "Spread in basis points of price.", labelnames=("instrument_key",)
)
effective_entry_spread_pct = _gauge(
    "effective_entry_spread_pct",
    "Effective entry spread as % of premium recorded at order submit.",
    labelnames=("client_order_id", "instrument_key"),
)
orderbook_bid_total_10 = _gauge("orderbook_bid_total_10", "Sum qty across top-10 bids.", labelnames=("instrument_key",))
orderbook_ask_total_10 = _gauge("orderbook_ask_total_10", "Sum qty across top-10 asks.", labelnames=("instrument_key",))
orderbook_imbalance_10 = _gauge(
    "orderbook_imbalance_10", "Depth imbalance (bid10-ask10)/(bid10+ask10).", labelnames=("instrument_key",)
)
option_spread_ticks = _gauge("option_spread_ticks", "Spread measured in ticks", labelnames=("instrument_key",))

# --- OMS / execution ---
oms_inflight_orders = _gauge("oms_inflight_orders", "Orders currently in-flight.")
order_state_transitions_total = _counter(
    "order_state_transitions_total", "OMS state transitions.", labelnames=("from", "to")
)
tick_to_submit_ms_bucket = _histogram(
    "tick_to_submit_ms_bucket",
    "Tick-to-submit latency in ms.",
    buckets=(1, 2, 5, 10, 20, 50, 100, 200, 500, 1000, 2000),
)
ack_to_fill_ms_bucket = _histogram(
    "ack_to_fill_ms_bucket",
    "Order ack-to-fill latency in ms.",
    buckets=(1, 2, 5, 10, 20, 50, 100, 200, 500, 1000, 2000),
)

# --- Risk / exposure ---
risk_daily_stop_rupees = _gauge("risk_daily_stop_rupees", "Daily stop from config (₹).")
risk_open_lots = _gauge("risk_open_lots", "Total open lots.")
risk_notional_premium_rupees = _gauge("risk_notional_premium_rupees", "Gross notional premium exposure (₹).")
minutes_to_square_off = _gauge("minutes_to_square_off", "Minutes remaining to mandated square-off.")
risk_percent_per_trade = _gauge("risk_percent_per_trade", "Configured risk per trade (% of capital).")
capital_base_rupees = _gauge("capital_base_rupees", "Configured capital base for sizing (₹).")
strategy_short_ma = _gauge("strategy_short_ma", "Short MA lookback for the strategy.")
strategy_long_ma = _gauge("strategy_long_ma", "Long MA lookback for the strategy.")
strategy_iv_threshold = _gauge("strategy_iv_threshold", "IV threshold gate for entries.")


# --- Convenience publishers (safe no-ops if unused) ---
def record_md_frame(nbytes: int) -> None:
    md_messages_total.inc()
    md_message_bytes_total.inc(max(0, int(nbytes)))


def record_md_decode_error(msg: str) -> None:  # type: ignore[override]
    meter = _maybe_metrics()
    if meter:
        try:
            meter.md_decode_errors_total.labels(error=str(msg)[:120]).inc()
        except Exception:
            try:
                meter.md_decode_errors_total.inc()
            except Exception:
                pass
    try:
        md_decode_errors_total.labels(error=str(msg)[:120]).inc()
    except Exception:
        md_decode_errors_total.inc()


def publish_spread(instrument_key: str, bid: float, ask: float, ltp: float) -> None:
    spread = None
    try:
        if bid is not None and ask is not None:
            spread = float(ask) - float(bid)
    except Exception:
        spread = None
    if spread is None or spread <= 0:
        return
    option_spread.labels(instrument_key).set(spread)
    if ltp and ltp > 0:
        option_spread_bp.labels(instrument_key).set(spread / float(ltp) * 10000.0)
    option_spread_ticks.labels(instrument_key).set(spread)


def publish_depth10(instrument_key: str, bid_qty10: float, ask_qty10: float) -> None:
    bid_q = max(0.0, float(bid_qty10 or 0.0))
    ask_q = max(0.0, float(ask_qty10 or 0.0))
    orderbook_bid_total_10.labels(instrument_key).set(bid_q)
    orderbook_ask_total_10.labels(instrument_key).set(ask_q)
    denom = bid_q + ask_q
    if denom > 0:
        orderbook_imbalance_10.labels(instrument_key).set((bid_q - ask_q) / denom)


def set_oms_inflight(n: int) -> None:
    oms_inflight_orders.set(max(0, int(n)))


def record_state_transition(old: str, new: str) -> None:
    order_state_transitions_total.labels(str(old), str(new)).inc()


def observe_tick_to_submit_ms(ms: float) -> None:
    tick_to_submit_ms_bucket.observe(max(0.0, float(ms)))


def observe_ack_to_fill_ms(ms: float) -> None:
    ack_to_fill_ms_bucket.observe(max(0.0, float(ms)))


def set_risk_dials(
    daily_stop_rupees: Optional[float] = None,
    open_lots: Optional[int] = None,
    notional_rupees: Optional[float] = None,
    minutes_to_sqoff: Optional[int] = None,
) -> None:
    if daily_stop_rupees is not None:
        risk_daily_stop_rupees.set(float(daily_stop_rupees))
    if open_lots is not None:
        risk_open_lots.set(int(open_lots))
    if notional_rupees is not None:
        risk_notional_premium_rupees.set(float(notional_rupees))
    if minutes_to_sqoff is not None:
        minutes_to_square_off.set(int(minutes_to_sqoff))


def set_strategy_config_metrics(short_ma: float, long_ma: float, iv_threshold: float) -> None:
    strategy_short_ma.set(float(short_ma))
    strategy_long_ma.set(float(long_ma))
    strategy_iv_threshold.set(float(iv_threshold))


def set_capital_config(capital_base: float, risk_pct: float) -> None:
    capital_base_rupees.set(float(capital_base))
    risk_percent_per_trade.set(float(risk_pct))


__all__ += [
    "md_decode_errors_total",
    "md_messages_total",
    "md_message_bytes_total",
    "option_spread",
    "option_spread_bp",
    "effective_entry_spread_pct",
    "orderbook_bid_total_10",
    "orderbook_ask_total_10",
    "orderbook_imbalance_10",
    "option_spread_ticks",
    "oms_inflight_orders",
    "order_state_transitions_total",
    "tick_to_submit_ms_bucket",
    "ack_to_fill_ms_bucket",
    "order_timeouts_total",
    "order_timeout_retries_total",
    "risk_daily_stop_rupees",
    "risk_open_lots",
    "risk_notional_premium_rupees",
    "minutes_to_square_off",
    "risk_percent_per_trade",
    "capital_base_rupees",
    "strategy_short_ma",
    "strategy_long_ma",
    "strategy_iv_threshold",
    "strategy_entry_signals_total",
    "record_md_frame",
    "publish_spread",
    "publish_depth10",
    "set_oms_inflight",
    "record_state_transition",
    "observe_tick_to_submit_ms",
    "observe_ack_to_fill_ms",
    "set_risk_dials",
    "set_strategy_config_metrics",
    "set_capital_config",
]
