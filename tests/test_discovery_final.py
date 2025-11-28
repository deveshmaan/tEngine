import json
from types import SimpleNamespace

import pytest
from prometheus_client import CollectorRegistry
from upstox_client.rest import ApiException

from brokerage.upstox_client import InvalidDateError, UpstoxSession
from engine.metrics import EngineMetrics, bind_global_metrics
from market.instrument_cache import InstrumentCache


@pytest.fixture(autouse=True)
def reset_metrics():
    bind_global_metrics(None)
    yield
    bind_global_metrics(None)


def test_option_contracts_kwargs(monkeypatch):
    calls = []

    def passthrough(fn, **_):
        return fn()

    monkeypatch.setattr("brokerage.upstox_client.with_retry", passthrough)
    session = UpstoxSession.__new__(UpstoxSession)
    session._api_client = SimpleNamespace(configuration=SimpleNamespace(debug=False))
    session.options_api = SimpleNamespace(
        get_option_contracts=lambda instrument_key, **kwargs: SimpleNamespace(
            to_dict=lambda: {"instrument": instrument_key, "kwargs": kwargs}
        )
    )

    def capture_call(instrument_key, **kwargs):
        calls.append(kwargs)
        return SimpleNamespace(to_dict=lambda: {"instrument": instrument_key})

    session.options_api.get_option_contracts = capture_call  # type: ignore[attr-defined]

    session.get_option_contracts("KEY")
    session.get_option_contracts("KEY", "2024-08-15")
    assert calls[0] == {}
    assert calls[1] == {"expiry_date": "2024-08-15"}


def test_invalid_date_metrics(monkeypatch):
    registry = CollectorRegistry()
    metrics = EngineMetrics(registry=registry)
    bind_global_metrics(metrics)

    def passthrough(fn, **_):
        return fn()

    monkeypatch.setattr("brokerage.upstox_client.with_retry", passthrough)

    def raise_invalid(instrument_key, **kwargs):
        exc = ApiException(status=400, reason="Invalid date")
        exc.body = b"{\"errors\":[{\"errorCode\":\"UDAPI1088\",\"message\":\"Invalid date\"}]}"
        raise exc

    incidents = []
    monkeypatch.setattr("engine.alerts.notify_incident", lambda **payload: incidents.append(payload))

    session = UpstoxSession.__new__(UpstoxSession)
    session._api_client = SimpleNamespace(configuration=SimpleNamespace(debug=False))
    session.options_api = SimpleNamespace(get_option_contracts=raise_invalid)

    with pytest.raises(InvalidDateError):
        session.get_option_contracts("KEY", "2024-08-15")

    assert registry.get_sample_value("api_errors_total", {"code": "UDAPI1088"}) == 1.0
    assert registry.get_sample_value("orders_rejected_total", {"reason": "invalid_date"}) == 1.0
    assert incidents and incidents[0]["title"].startswith("UDAPI1088")


def test_fallback_expiries_from_json(monkeypatch, tmp_path):
    cache_db = tmp_path / "cache.sqlite"
    fallback_file = tmp_path / "inst.json"
    fallback_file.write_text(json.dumps([
        {"symbol": "NIFTY", "expiry": "2024-08-15"},
        {"symbol": "NIFTY", "expiry": "2024-08-22"},
    ]), encoding="utf-8")
    monkeypatch.setenv("INSTRUMENTS_JSON_PATH", str(fallback_file))

    class FailingSession:
        def get_option_contracts(self, instrument_key, **kwargs):
            raise InvalidDateError("bad discovery")

    cache = InstrumentCache(db_path=cache_db, session_factory=lambda: FailingSession(), expiry_ttl_minutes=1)
    expiries = cache.list_expiries("NIFTY")
    assert expiries == ["2024-08-15", "2024-08-22"]
