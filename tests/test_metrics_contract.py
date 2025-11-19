import socket
import time
import urllib.request

import pytest

from engine import metrics as metrics_module
from engine.metrics import EngineMetrics, start_http_server_if_available


@pytest.mark.skipif(not metrics_module.PROM_AVAILABLE, reason="prometheus_client not installed")
def test_metrics_surface_required_series():
    metrics = EngineMetrics()
    metrics.engine_up.set(1)
    metrics.pnl_realized.set(100.0)
    metrics.pnl_unrealized.set(25.0)
    metrics.pnl_fees.set(5.0)
    metrics.pnl_net_rupees.set(120.0)
    metrics.risk_halt_state.set(1)
    metrics.orders_submitted_total.inc(2)
    metrics.orders_filled_total.inc(1)
    metrics.orders_rejected_total.labels(reason="validation").inc()
    metrics.api_errors_total.labels(code="UDAPI1088").inc()
    metrics.order_latency_ms_bucketed.labels(operation="submit").observe(12.5)
    metrics.http_401_total.inc()
    metrics.ws_reconnects_total.inc()
    metrics.rest_retries_total.labels(endpoint="place").inc()
    metrics.ratelimit_tokens.labels(endpoint="place").set(0)
    metrics.broker_queue_depth.labels(endpoint="place").set(1)

    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        port = sock.getsockname()[1]
    started = start_http_server_if_available(port)
    assert started
    time.sleep(0.1)
    resp = urllib.request.urlopen(f"http://127.0.0.1:{port}/metrics", timeout=2)
    payload = resp.read().decode("utf-8")
    required = [
        "pnl_realized",
        "pnl_unrealized",
        "pnl_fees",
        "pnl_net_rupees",
        "risk_halt_state",
        "broker_queue_depth",
        "ratelimit_tokens",
        "orders_submitted_total",
        "orders_filled_total",
        "orders_rejected_total",
        "api_errors_total",
        "ws_reconnects_total",
        "http_401_total",
        "order_latency_ms_bucketed",
    ]
    for metric_name in required:
        assert metric_name in payload
