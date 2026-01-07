import asyncio
from types import SimpleNamespace

import pytest
from upstox_client.rest import ApiException

from engine.broker import BrokerError, UpstoxBroker
from engine.config import BrokerConfig
from engine.oms import Order


class MetricStub:
    def __init__(self) -> None:
        self.value = 0.0

    def inc(self, amount: float = 1.0) -> None:
        self.value += amount

    def set(self, value: float) -> None:
        self.value = value

    def labels(self, **_):  # type: ignore[override]
        return self


class StubMetrics(SimpleNamespace):
    def __init__(self) -> None:
        super().__init__(
            risk_halt_state=MetricStub(),
            http_401_total=MetricStub(),
            rest_retries_total=MetricStub(),
            ratelimit_tokens=MetricStub(),
            broker_queue_depth=MetricStub(),
            ws_reconnects_total=MetricStub(),
            api_errors_total=MetricStub(),
        )

    def set_ratelimit_tokens(self, endpoint: str, tokens: float) -> None:
        self.ratelimit_tokens.set(tokens)

    def set_broker_queue_depth(self, endpoint: str, depth: float) -> None:
        self.broker_queue_depth.set(depth)

    def set_risk_halt_state(self, value: float) -> None:
        self.risk_halt_state.set(value)

    def inc_api_error(self, code: str) -> None:
        self.api_errors_total.labels(code=code).inc()

    def inc_orders_rejected(self, reason: str) -> None:
        return None


class FakeOrderApi:
    def __init__(self, fail_times: int = 0, fail_status: int = 500):
        self.calls = 0
        self.fail_times = fail_times
        self.fail_status = fail_status

    def place_order(self, payload, algo_id=None):
        self.calls += 1
        if self.calls <= self.fail_times:
            raise ApiException(status=self.fail_status, reason="transient")
        return {"data": {"order_id": "OID-1"}, "status": "ok"}

    def modify_order(self, payload):
        return {"status": "ok"}

    def cancel_order(self, order_id):
        return {"status": "ok"}

    def get_order_book(self):
        return {"data": []}


class FakeSession:
    def __init__(self, api):
        self.order_api_v3 = api
        self.config = SimpleNamespace(algo_id="test")


class FailingSession(FakeSession):
    def __init__(self, status):
        super().__init__(FakeOrderApi())
        self._status = status

    def order_api_v3(self):  # type: ignore[override]
        raise ApiException(status=self._status)


class HeartbeatStream:
    def __init__(self):
        self.subscribed = []
        self.reconnects = 0
        self._first = True

    async def connect(self):
        if not self._first:
            self.reconnects += 1
        self._first = False

    async def close(self):
        return None

    async def subscribe(self, instruments):
        self.subscribed = list(instruments)

    async def heartbeat(self):
        await asyncio.sleep(0)
        raise RuntimeError("idle")


@pytest.mark.asyncio
async def test_retries_on_transient_errors(monkeypatch):
    api = FakeOrderApi(fail_times=2, fail_status=500)
    session = FakeSession(api)
    metrics = StubMetrics()
    broker = UpstoxBroker(
        config=BrokerConfig(rest_timeout=0.05, ws_heartbeat_interval=0.05, ws_backoff_seconds=(0.05,), oauth_refresh_margin=10, max_order_rate=10),
        session_factory=lambda _token=None: session,
        metrics=metrics,
    )
    order = Order(client_order_id="1", strategy="s", symbol="TOKEN", side="BUY", qty=50, limit_price=10.0)
    result = await broker.submit_order(order)
    assert result.broker_order_id
    assert api.calls == 3
    assert metrics.rest_retries_total.value == pytest.approx(2)


@pytest.mark.asyncio
async def test_http_401_triggers_halt(monkeypatch):
    api = FakeOrderApi(fail_times=1, fail_status=401)
    session = FakeSession(api)
    metrics = StubMetrics()
    halted = []

    def on_halt(reason: str):
        halted.append(reason)

    broker = UpstoxBroker(
        config=BrokerConfig(rest_timeout=0.05, ws_heartbeat_interval=0.05, ws_backoff_seconds=(0.05,), oauth_refresh_margin=10, max_order_rate=10),
        session_factory=lambda _token=None: session,
        metrics=metrics,
        auth_halt_callback=on_halt,
    )
    order = Order(client_order_id="1", strategy="s", symbol="TOKEN", side="BUY", qty=50, limit_price=10.0)
    with pytest.raises(BrokerError):
        await broker.submit_order(order)
    assert metrics.http_401_total.value == pytest.approx(1)
    assert halted == ["AUTH_401"]
    with pytest.raises(BrokerError):
        await broker.submit_order(order)
    sell_order = Order(client_order_id="2", strategy="s", symbol="TOKEN", side="SELL", qty=50, limit_price=10.0)
    await broker.submit_order(sell_order)
    broker.resume_trading()
    await broker.submit_order(order)


@pytest.mark.asyncio
async def test_ws_watchdog_reconnects(monkeypatch):
    monkeypatch.setattr("engine.broker.preopen_expiry_smoke", lambda symbols=("NIFTY", "BANKNIFTY"): None)
    stream = HeartbeatStream()
    metrics = StubMetrics()
    broker = UpstoxBroker(
        config=BrokerConfig(rest_timeout=0.1, ws_heartbeat_interval=0.05, ws_backoff_seconds=(0.05, 0.1), oauth_refresh_margin=10, max_order_rate=5),
        session_factory=lambda _token=None: FakeSession(FakeOrderApi()),
        stream_client=stream,
        metrics=metrics,
    )
    replays = []

    async def reconcile():
        replays.append(True)

    broker.bind_reconcile_callback(reconcile)
    await broker.subscribe_marketdata(["TOKEN"])
    await asyncio.sleep(0.2)
    await broker.stop()
    assert stream.reconnects >= 1
    assert metrics.ws_reconnects_total.value >= 1
    assert replays
