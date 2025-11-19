from __future__ import annotations

import logging
import os
import time
from typing import Optional

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
            self.ratelimit_tokens = _NoOpMetric()
            self.broker_queue_depth = _NoOpMetric()
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
        self.ratelimit_tokens = Gauge("ratelimit_tokens", "Available tokens per endpoint", ["endpoint"], **registry_kwargs)
        self.broker_queue_depth = Gauge("broker_queue_depth", "Broker queue depth", ["endpoint"], **registry_kwargs)

    def beat(self) -> None:
        self.heartbeat_ts.set(time.time())


def start_http_server_if_available(port: Optional[int] = None) -> bool:
    if not PROM_AVAILABLE or start_http_server is None:
        logging.getLogger("metrics").warning("prometheus_client missing; metrics disabled")
        return False
    addr = os.getenv("METRICS_HOST", "0.0.0.0")
    start_http_server(port or 9103, addr=addr)
    return True


__all__ = ["EngineMetrics", "start_http_server_if_available"]
