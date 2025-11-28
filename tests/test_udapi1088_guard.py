import asyncio

import pytest
from prometheus_client import CollectorRegistry
from upstox_client.rest import ApiException

from engine.broker import UpstoxBroker
from brokerage.upstox_client import InvalidDateError
from engine.config import BrokerConfig
from engine.metrics import EngineMetrics


class DummySession:
    pass


def _broker_config() -> BrokerConfig:
    return BrokerConfig(
        rest_timeout=1.0,
        ws_heartbeat_interval=3.0,
        ws_backoff_seconds=(1, 2, 5),
        oauth_refresh_margin=60,
        max_order_rate=10,
    )


async def _zero_tokens(*_) -> float:
    return 0.0


@pytest.mark.asyncio
async def test_udapi1088_is_non_retry(monkeypatch):
    registry = CollectorRegistry()
    metrics = EngineMetrics(registry=registry)
    broker = UpstoxBroker(
        config=_broker_config(),
        session_factory=lambda token=None: DummySession(),
        metrics=metrics,
    )
    broker._session = DummySession()
    monkeypatch.setattr(broker, "_acquire_token", _zero_tokens)
    retries_recorded = []
    monkeypatch.setattr(broker, "_record_retry", lambda endpoint: retries_recorded.append(endpoint))
    incidents: list[tuple[str, str]] = []
    monkeypatch.setattr(
        "engine.broker.notify_incident",
        lambda level, title, body, tags=None: incidents.append((title, body)),
    )

    def _raise_api(_session):
        exc = ApiException(status=400, reason="Bad Request")
        exc.body = '{"errors":[{"errorCode":"UDAPI1088","message":"Invalid date"}]}'
        raise exc

    with pytest.raises(InvalidDateError):
        await broker._rest_call("place", "submit_order", _raise_api)

    assert not retries_recorded
    api_err = registry.get_sample_value("api_errors_total", {"code": "UDAPI1088"})
    assert api_err == 1.0
    rejected = registry.get_sample_value("orders_rejected_total", {"reason": "invalid_date"})
    assert rejected == 1.0
    assert incidents and "UDAPI1088" in incidents[0][0]
