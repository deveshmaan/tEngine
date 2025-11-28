import datetime as dt

import pytest

from engine.broker import BrokerError
from engine.config import IST, OMSConfig
from engine.oms import BrokerOrderAck, BrokerOrderView, OMS, OrderState
from persistence import SQLiteStore


class FakeBroker:
    def __init__(self, *, fail_first: int = 0):
        self.fail_first = fail_first
        self.calls: list[str] = []
        self.views: dict[str, BrokerOrderView] = {}

    async def submit_order(self, order):
        self.calls.append(order.client_order_id)
        if self.fail_first > 0:
            self.fail_first -= 1
            raise BrokerError(code="transport", message="down")
        broker_id = f"BRK-{len(self.calls)}"
        self.views[order.client_order_id] = BrokerOrderView(
            broker_order_id=broker_id,
            client_order_id=order.client_order_id,
            status="pending",
            filled_qty=0,
            avg_price=0.0,
        )
        return BrokerOrderAck(broker_order_id=broker_id, status="accepted")

    async def replace_order(self, order, *, price=None, qty=None):
        return None

    async def cancel_order(self, order):
        self.views.pop(order.client_order_id, None)

    async def fetch_open_orders(self):
        return list(self.views.values())


def _cfg() -> OMSConfig:
    return OMSConfig(resubmit_backoff=0.1, reconciliation_interval=0.5, max_inflight_orders=100)


DEFAULT_META = (0.05, 50, 10.0, 500.0)
SQUARE_OFF = dt.time(15, 30)


@pytest.mark.asyncio
async def test_client_order_id_deterministic():
    ts = dt.datetime(2024, 7, 1, 9, 30, tzinfo=IST)
    cid1 = OMS.client_order_id("demo", ts, "NIFTY", "BUY", 50)
    cid2 = OMS.client_order_id("demo", ts, "NIFTY", "BUY", 50)
    assert cid1 == cid2


@pytest.mark.asyncio
async def test_oms_transitions_and_fills(tmp_path):
    broker = FakeBroker()
    store = SQLiteStore(tmp_path / "oms.sqlite", run_id="oms-test")
    oms = OMS(broker=broker, store=store, config=_cfg(), default_meta=DEFAULT_META, square_off_time=SQUARE_OFF)
    order = await oms.submit(strategy="demo", symbol="NIFTY", side="BUY", qty=10, ts=dt.datetime.now(IST))
    assert order.state == OrderState.ACKNOWLEDGED
    await oms.record_fill(order.client_order_id, qty=5, price=100.0)
    assert order.state == OrderState.PARTIALLY_FILLED
    await oms.record_fill(order.client_order_id, qty=5, price=101.0)
    assert order.state == OrderState.FILLED


@pytest.mark.asyncio
async def test_reconnect_retries_with_same_id(tmp_path):
    broker = FakeBroker(fail_first=1)
    store = SQLiteStore(tmp_path / "oms.sqlite", run_id="oms-retry")
    oms = OMS(broker=broker, store=store, config=_cfg(), default_meta=DEFAULT_META, square_off_time=SQUARE_OFF)
    ts = dt.datetime(2024, 7, 1, 9, 30, tzinfo=IST)
    with pytest.raises(BrokerError):
        await oms.submit(strategy="demo", symbol="NIFTY", side="BUY", qty=10, ts=ts)
    assert len(broker.calls) == 1
    await oms.handle_reconnect()
    assert len(broker.calls) == 2
    assert broker.calls[0] == broker.calls[1]


@pytest.mark.asyncio
async def test_reconcile_reflects_broker_state(tmp_path):
    broker = FakeBroker()
    store = SQLiteStore(tmp_path / "oms.sqlite", run_id="oms-reconcile")
    oms = OMS(broker=broker, store=store, config=_cfg(), default_meta=DEFAULT_META, square_off_time=SQUARE_OFF)
    order = await oms.submit(strategy="demo", symbol="NIFTY", side="BUY", qty=10, ts=dt.datetime.now(IST))
    view = broker.views[order.client_order_id]
    broker.views[order.client_order_id] = BrokerOrderView(
        broker_order_id=view.broker_order_id,
        client_order_id=view.client_order_id,
        status="complete",
        filled_qty=10,
        avg_price=101.0,
    )
    await oms.reconcile_from_broker()
    assert order.state == OrderState.FILLED


@pytest.mark.asyncio
async def test_burst_of_orders_has_unique_ids(tmp_path):
    broker = FakeBroker()
    store = SQLiteStore(tmp_path / "oms.sqlite", run_id="oms-burst")
    oms = OMS(broker=broker, store=store, config=_cfg(), default_meta=DEFAULT_META, square_off_time=SQUARE_OFF)
    base_ts = dt.datetime(2024, 7, 1, 9, 30, tzinfo=IST)
    ids = []
    for idx in range(50):
        ts = base_ts + dt.timedelta(milliseconds=idx)
        order = await oms.submit(strategy="demo", symbol=f"NIFTY-{idx}", side="BUY", qty=10, ts=ts)
        ids.append(order.client_order_id)
    assert len(set(ids)) == 50
    await oms.handle_reconnect()
    assert len(broker.calls) == 50
