import datetime as dt

import pytest

from engine.oms import OMS, Order, OrderState, BrokerOrderAck
from persistence.store import SQLiteStore


class _StubBroker:
    def __init__(self):
        self.submits = 0

    async def submit_order(self, order: Order):
        self.submits += 1
        return BrokerOrderAck(broker_order_id="brk1", status="submitted")

    async def replace_order(self, order: Order, *, price=None, qty=None):
        return None

    async def cancel_order(self, order: Order):
        return None

    async def fetch_open_orders(self):
        return []


class _DummyStore(SQLiteStore):
    def __init__(self):
        super().__init__(":memory:", "test")


def _oms():
    return OMS(broker=_StubBroker(), store=_DummyStore(), config=type("C", (), {"max_inflight_orders": 10, "resubmit_backoff": 0.1, "reconciliation_interval": 1.0, "submit": type("S", (), {"default": "market", "max_spread_ticks": 1, "depth_threshold": 0})})())


@pytest.mark.asyncio
async def test_valid_transitions():
    oms = _oms()
    order = await oms.submit(strategy="t", symbol="NIFTY-TEST", side="BUY", qty=1)
    assert order.state in {OrderState.SUBMITTED, OrderState.ACKNOWLEDGED}
    await oms.record_fill(order.client_order_id, qty=1, price=100.0)
    assert order.state == OrderState.FILLED
    assert order.filled_qty == 1
    assert order.avg_fill_price == 100.0


@pytest.mark.asyncio
async def test_invalid_transition_is_ignored():
    oms = _oms()
    order = await oms.submit(strategy="t", symbol="NIFTY-TEST", side="BUY", qty=1)
    prev = order.state
    await oms._persist_transition(order, OrderState.FILLED, OrderState.NEW, reason="invalid")
    assert order.state == prev
