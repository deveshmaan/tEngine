from __future__ import annotations

import logging
import os
import time
from typing import Optional

try:  # pragma: no cover - optional dependency
    from prometheus_client import Counter, Gauge, Histogram, start_http_server

    PROM_AVAILABLE = True
except ImportError:  # pragma: no cover
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

    def __init__(self) -> None:
        if not PROM_AVAILABLE:
            self.engine_up = _NoOpMetric()
            self.heartbeat_ts = _NoOpMetric()
            self.event_queue_depth = _NoOpMetric()
            self.order_queue_depth = _NoOpMetric()
            self.ws_lag_ms = _NoOpMetric()
            self.submit_latency_ms = _NoOpMetric()
            self.replace_latency_ms = _NoOpMetric()
            self.cancel_latency_ms = _NoOpMetric()
            self.fills_total = _NoOpMetric()
            self.rejects_total = _NoOpMetric()
            self.pnl_realized = _NoOpMetric()
            self.pnl_unrealized = _NoOpMetric()
            self.pnl_fees = _NoOpMetric()
            self.pnl_net = _NoOpMetric()
            self.risk_halts_total = _NoOpMetric()
            return
        self.engine_up = Gauge("engine_up", "Engine up status")
        self.heartbeat_ts = Gauge("heartbeat_ts", "Unix timestamp of last heartbeat")
        self.event_queue_depth = Gauge("event_queue_depth", "Depth of market event queue")
        self.order_queue_depth = Gauge("order_queue_depth", "Depth of order queue")
        self.ws_lag_ms = Gauge("ws_lag_ms", "Latest websocket lag in ms")
        self.submit_latency_ms = Histogram("submit_latency_ms", "Order submit latency ms")
        self.replace_latency_ms = Histogram("replace_latency_ms", "Order replace latency ms")
        self.cancel_latency_ms = Histogram("cancel_latency_ms", "Order cancel latency ms")
        self.fills_total = Counter("fills_total", "Total fills observed")
        self.rejects_total = Counter("rejects_total", "Total rejects observed")
        self.pnl_realized = Gauge("pnl_realized", "Realized PnL")
        self.pnl_unrealized = Gauge("pnl_unrealized", "Unrealized PnL")
        self.pnl_fees = Gauge("pnl_fees", "Total fees")
        self.pnl_net = Gauge("pnl_net", "Net PnL after fees")
        self.risk_halts_total = Counter("risk_halts_total", "Risk halts raised")

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
