"""
engine_metrics.py

Prometheus metrics helper for the intraday BUY engine. The helper keeps the rest of
the codebase clean by exposing small, intention-revealing methods instead of making
every module deal with prometheus_client primitives.
"""
from __future__ import annotations

import os
import time
from typing import Optional

try:
    from prometheus_client import (
        CollectorRegistry,
        Counter,
        Gauge,
        Histogram,
        Summary,
        start_http_server,
    )

    PROM_AVAILABLE = True
    print("Prometheus client available")
except ImportError:  # pragma: no cover - only triggered when dependency missing
    CollectorRegistry = Counter = Gauge = Histogram = Summary = object  # type: ignore
    start_http_server = lambda *_, **__: None  # type: ignore
    PROM_AVAILABLE = False


class EngineMetrics:
    """
    Thin wrapper around prometheus_client with sensible defaults for this engine.

    When prometheus_client is not installed or metrics are disabled, every method
    becomes a no-op so the trading logic can continue without instrumentation.
    """

    def __init__(self, port: int = 9103, enabled: Optional[bool] = None):
        self.enabled = PROM_AVAILABLE if enabled is None else bool(enabled and PROM_AVAILABLE)
        if not self.enabled:
            return

        self.registry = CollectorRegistry()
        self._start_http_server(port)

        # Core counters
        self.signals_ctr = Counter(
            "buy_signals_total", "Number of BUY signals emitted", ["instrument"], registry=self.registry
        )
        self.orders_submitted_ctr = Counter(
            "orders_submitted_total", "Orders submitted to Upstox", ["instrument"], registry=self.registry
        )
        self.orders_filled_ctr = Counter(
            "orders_filled_total", "Orders filled/traded", ["instrument"], registry=self.registry
        )
        self.orders_rejected_ctr = Counter(
            "orders_rejected_total", "Orders rejected/blocked before exchange", ["instrument", "reason"],
            registry=self.registry
        )

        # Gauges
        self.pnl_gauge = Gauge("pnl_net_rupees", "Realized + unrealized PnL (₹)", registry=self.registry)
        self.positions_gauge = Gauge("open_positions", "How many BUY positions are open", registry=self.registry)
        self.margin_gauge = Gauge("margin_available_rupees", "Available margin (₹)", registry=self.registry)
        self.latency_gauge = Gauge("last_execution_latency_ms", "Latency of last order submission", registry=self.registry)
        self.heartbeat_gauge = Gauge("engine_heartbeat_ts", "Unix timestamp of last engine loop iteration", registry=self.registry)
        self.risk_halt_gauge = Gauge("risk_halt_state", "1 when RiskManager halted trading", registry=self.registry)

        # Market telemetry
        self.ltp_gauge = Gauge("ltp_underlying", "Last traded price per instrument", ["instrument"], registry=self.registry)
        self.iv_gauge = Gauge("option_iv", "Option IV snapshot", ["instrument"], registry=self.registry)
        self.ivz_gauge = Gauge("option_iv_zscore", "Option IV z-score snapshot", ["instrument"], registry=self.registry)
        self.health_gauge = Gauge(
            "instrument_health_state",
            "Market data health per instrument (reason label indicates status)",
            ["instrument", "reason"],
            registry=self.registry
        )

        # Latency histogram for Grafana percentile tiles
        self.latency_hist = Histogram(
            "order_latency_ms_bucketed",
            "Distribution of order submission latency (ms)",
            buckets=(5, 10, 25, 50, 75, 100, 250, 500, 1000, float("inf")),
            registry=self.registry,
        )

        # Signal quality
        self.signal_duration_summary = Summary(
            "signal_batch_duration_ms",
            "Time taken to compute a signal batch (ms)",
            registry=self.registry
        )

    # ---- helpers -----------------------------------------------------------------
    def _start_http_server(self, port: int) -> None:
        addr = os.getenv("METRICS_HOST", "0.0.0.0")
        start_http_server(port, addr, registry=self.registry)

    def _label(self, instrument: Optional[str]) -> str:
        return instrument or "unknown"

    # ---- public API --------------------------------------------------------------
    def record_signal(self, instrument: Optional[str], duration_ms: Optional[float] = None) -> None:
        if not self.enabled:
            return
        self.signals_ctr.labels(instrument=self._label(instrument)).inc()
        if duration_ms is not None:
            self.signal_duration_summary.observe(duration_ms)

    def order_submitted(self, instrument: Optional[str], latency_ms: float) -> None:
        if not self.enabled:
            return
        label = self._label(instrument)
        self.orders_submitted_ctr.labels(instrument=label).inc()
        self.latency_gauge.set(latency_ms)
        self.latency_hist.observe(latency_ms)

    def order_filled(self, instrument: Optional[str]) -> None:
        if not self.enabled:
            return
        self.orders_filled_ctr.labels(instrument=self._label(instrument)).inc()

    def order_rejected(self, instrument: Optional[str], reason: str) -> None:
        if not self.enabled:
            return
        self.orders_rejected_ctr.labels(instrument=self._label(instrument), reason=reason or "unknown").inc()

    def update_market(self, instrument: Optional[str], ltp: Optional[float], iv: Optional[float], iv_z: Optional[float]) -> None:
        if not self.enabled:
            return
        label = self._label(instrument)
        if ltp is not None:
            self.ltp_gauge.labels(instrument=label).set(float(ltp))
        if iv is not None:
            self.iv_gauge.labels(instrument=label).set(float(iv))
        if iv_z is not None:
            self.ivz_gauge.labels(instrument=label).set(float(iv_z))

    def set_health(self, instrument: Optional[str], reason: str, value: float = 1.0) -> None:
        if not self.enabled:
            return
        self.health_gauge.labels(instrument=self._label(instrument), reason=reason or "unknown").set(float(value))

    def set_pnl(self, value: float) -> None:
        if not self.enabled:
            return
        self.pnl_gauge.set(float(value))

    def set_open_positions(self, count: int) -> None:
        if not self.enabled:
            return
        self.positions_gauge.set(int(count))

    def set_margin_available(self, value: float) -> None:
        if not self.enabled:
            return
        self.margin_gauge.set(float(value))

    def heartbeat(self) -> None:
        if not self.enabled:
            return
        self.heartbeat_gauge.set(time.time())

    def set_risk_state(self, halted: bool) -> None:
        if not self.enabled:
            return
        self.risk_halt_gauge.set(1 if halted else 0)


# Convenience no-op singleton for callers that do not want to guard metrics usage.
NULL_METRICS = EngineMetrics(enabled=False)
