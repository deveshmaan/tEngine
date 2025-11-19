import json

import pytest
import engine.alerts as alerts
import engine.metrics as metrics
from engine.alerts import AlertService


def test_metrics_increment_when_prometheus_available(monkeypatch: pytest.MonkeyPatch) -> None:
    class StubMetric:
        def __init__(self, name: str):
            self.name = name
            self.calls: list[tuple[str, float]] = []

        def labels(self, *args, **kwargs):
            return self

        def inc(self, value: float = 1.0) -> None:
            self.calls.append(("inc", value))

        def set(self, value: float) -> None:
            self.calls.append(("set", value))

        def observe(self, value: float) -> None:
            self.calls.append(("observe", value))

    created: dict[str, StubMetric] = {}

    def _factory(name: str, _desc: str, *args, **kwargs) -> StubMetric:
        metric = StubMetric(name)
        created[name] = metric
        return metric

    monkeypatch.setattr(metrics, "PROM_AVAILABLE", True)
    monkeypatch.setattr(metrics, "Gauge", _factory)
    monkeypatch.setattr(metrics, "Counter", _factory)
    monkeypatch.setattr(metrics, "Histogram", _factory)
    meter = metrics.EngineMetrics()
    meter.engine_up.set(1)
    meter.orders_filled_total.inc(2)
    meter.fills_total.inc(1)
    meter.order_latency_ms_bucketed.labels(operation="submit").observe(0.5)
    meter.beat()
    assert ("set", 1) in created["engine_up"].calls
    assert ("inc", 2) in created["orders_filled_total"].calls
    assert ("observe", 0.5) in created["order_latency_ms_bucketed"].calls
    assert created["heartbeat_ts"].calls


def test_metrics_noop_when_prometheus_missing(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(metrics, "PROM_AVAILABLE", False)
    monkeypatch.setattr(metrics, "start_http_server", None)
    meter = metrics.EngineMetrics()
    meter.engine_up.set(1)
    assert metrics.start_http_server_if_available(1235) is False


def test_alert_service_throttling(monkeypatch: pytest.MonkeyPatch) -> None:
    sent = []

    def fake_transport(url, body, headers):
        sent.append((url, json.loads(body), headers))

    monkeypatch.setenv("SLACK_WEBHOOK_URL", "https://example.com/hook")
    service = AlertService(throttle_seconds=5.0, transport=fake_transport)
    assert service.notify("CRIT", "Test", "Body", tags=["x"])
    assert not service.notify("CRIT", "Test", "Body", tags=["x"])
    service._last_sent -= service.throttle_seconds
    assert service.notify("WARN", "Later", "Again")
    assert len(sent) == 2
