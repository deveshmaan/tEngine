"""
engine_metrics.py

Prometheus metrics helper for the intraday BUY engine. The helper keeps the rest of
the codebase clean by exposing small, intention-revealing methods instead of making
every module deal with prometheus_client primitives.
"""
from __future__ import annotations

import os
import time
from typing import Dict, Optional

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
        self.cache_refresh_ctr = Counter(
            "instrument_cache_refresh_total", "Option chain refreshes", ["symbol"], registry=self.registry
        )
        self.cache_lookup_ctr = Counter(
            "instrument_cache_lookup_total", "Cache lookups", ["symbol", "result"], registry=self.registry
        )
        self.ltp_calls_ctr = Counter(
            "ltp_calls_total", "LTP API invocations", ["result"], registry=self.registry
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
        self.trading_symbol_gauge = Gauge(
            "instrument_trading_symbol",
            "Trading symbol for instrument (gauge holds a dummy value of 1)",
            ["instrument", "symbol"],
            registry=self.registry
        )
        self.health_gauge = Gauge(
            "instrument_health_state",
            "Market data health per instrument (reason label indicates status)",
            ["instrument", "reason"],
            registry=self.registry
        )
        self.tick_latency_gauge = Gauge(
            "tick_to_bus_ms_last", "Latest tick-to-bus latency (ms)", registry=self.registry
        )
        self.signal_latency_gauge = Gauge(
            "signal_to_post_ms_last", "Latest signal-to-post latency (ms)", registry=self.registry
        )
        self.post_ack_latency_gauge = Gauge(
            "post_to_ack_ms_last", "Latest order post-to-ack latency (ms)", registry=self.registry
        )
        self.ack_state_latency_gauge = Gauge(
            "ack_to_state_ms_last", "Latest ack-to-state latency (ms)", registry=self.registry
        )
        self.state_indicator_gauge = Gauge(
            "engine_state_indicator", "Engine state marker", ["state"], registry=self.registry
        )
        self.heartbeat_state_gauge = Gauge(
            "engine_heartbeat_state", "Engine heartbeat state code", registry=self.registry
        )

        # Latency histograms
        buckets = (1, 2, 5, 10, 25, 50, 75, 100, 250, 500, 1000, float("inf"))
        self.tick_latency_hist = Histogram(
            "tick_to_bus_ms_bucketed",
            "Tick ingest to bus latency (ms)",
            buckets=buckets,
            registry=self.registry,
        )
        self.signal_latency_hist = Histogram(
            "signal_to_post_ms_bucketed",
            "Signal evaluation latency (ms)",
            buckets=buckets,
            registry=self.registry,
        )
        self.post_ack_latency_hist = Histogram(
            "post_to_ack_ms_bucketed",
            "Order POST to ACK latency (ms)",
            buckets=buckets,
            registry=self.registry,
        )
        self.ack_state_latency_hist = Histogram(
            "ack_to_state_ms_bucketed",
            "ACK propagation latency (ms)",
            buckets=buckets,
            registry=self.registry,
        )

        # Signal quality
        self.signal_duration_summary = Summary(
            "signal_batch_duration_ms",
            "Time taken to compute a signal batch (ms)",
            registry=self.registry
        )
        self._state_value = "STARTING"

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
        self.record_post_ack_latency(latency_ms)

    def order_filled(self, instrument: Optional[str]) -> None:
        if not self.enabled:
            return
        self.orders_filled_ctr.labels(instrument=self._label(instrument)).inc()

    def order_rejected(self, instrument: Optional[str], reason: str) -> None:
        if not self.enabled:
            return
        self.orders_rejected_ctr.labels(instrument=self._label(instrument), reason=reason or "unknown").inc()

    def update_market(self, instrument: Optional[str], trading_symbol: Optional[str],
                      ltp: Optional[float], iv: Optional[float], iv_z: Optional[float]) -> None:
        if not self.enabled:
            return
        label = self._label(instrument)
        if ltp is not None:
            self.ltp_gauge.labels(instrument=label).set(float(ltp))
        if iv is not None:
            self.iv_gauge.labels(instrument=label).set(float(iv))
        if iv_z is not None:
            self.ivz_gauge.labels(instrument=label).set(float(iv_z))
        if trading_symbol:
            self.trading_symbol_gauge.labels(instrument=label, symbol=trading_symbol).set(1.0)

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

    def record_tick_latency(self, latency_ms: float) -> None:
        if not self.enabled or latency_ms is None:
            return
        self.tick_latency_gauge.set(latency_ms)
        self.tick_latency_hist.observe(latency_ms)

    def record_signal_latency(self, latency_ms: float) -> None:
        if not self.enabled or latency_ms is None:
            return
        self.signal_latency_gauge.set(latency_ms)
        self.signal_latency_hist.observe(latency_ms)

    def record_post_ack_latency(self, latency_ms: float) -> None:
        if not self.enabled or latency_ms is None:
            return
        self.post_ack_latency_gauge.set(latency_ms)
        self.post_ack_latency_hist.observe(latency_ms)

    def record_ack_state_latency(self, latency_ms: float) -> None:
        if not self.enabled or latency_ms is None:
            return
        self.ack_state_latency_gauge.set(latency_ms)
        self.ack_state_latency_hist.observe(latency_ms)

    def set_state(self, state: str) -> None:
        if not self.enabled:
            return
        self._state_value = state
        self.state_indicator_gauge.labels(state=state).set(1)
        self.heartbeat_state_gauge.set(hash(state) % 1000)

    def runtime_snapshot(self) -> Dict[str, float | str]:
        if not self.enabled:
            return {}
        heartbeat_ts = getattr(self.heartbeat_gauge, "_value", None)
        hb_val = heartbeat_ts.get() if heartbeat_ts else 0.0
        return {
            "state": self._state_value,
            "heartbeat_ts": hb_val,
            "pnl": self.pnl_gauge._value.get() if hasattr(self.pnl_gauge, "_value") else 0.0,
            "open_positions": self.positions_gauge._value.get() if hasattr(self.positions_gauge, "_value") else 0.0,
        }

    def record_cache_refresh(self, symbol: str) -> None:
        if not self.enabled:
            return
        self.cache_refresh_ctr.labels(symbol=symbol.upper()).inc()

    def record_cache_lookup(self, symbol: str, result: str) -> None:
        if not self.enabled:
            return
        self.cache_lookup_ctr.labels(symbol=symbol.upper(), result=result).inc()

    def record_ltp_call(self, success: bool) -> None:
        if not self.enabled:
            return
        self.ltp_calls_ctr.labels(result="success" if success else "failure").inc()

    def snapshot(self) -> dict:
        if not self.enabled:
            return {}
        return {
            "pnl": self.pnl_gauge._value.get(),  # type: ignore[attr-defined]
            "open_positions": self.positions_gauge._value.get(),  # type: ignore[attr-defined]
        }

    def to_logfmt(self) -> str:
        snap = self.snapshot()
        if not snap:
            return ""
        return " ".join(f"{k}={v}" for k, v in snap.items())


# Convenience no-op singleton for callers that do not want to guard metrics usage.
NULL_METRICS = EngineMetrics(enabled=False)
