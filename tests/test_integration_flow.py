import datetime as dt

import pytest

from engine.broker import BrokerError
from engine.config import IST, OMSConfig, RiskLimits
from engine.oms import BrokerOrderAck, BrokerOrderView, OMS, OrderState
from engine.risk import OrderBudget, RiskManager
from persistence import SQLiteStore


class FlakyBroker:
    def __init__(self):
        self.fail_first = True
        self.views: dict[str, BrokerOrderView] = {}

    async def submit_order(self, order):
        if self.fail_first:
            self.fail_first = False
            raise BrokerError(code="link", message="disconnect")
        broker_id = f"BRK-{order.client_order_id}"
        self.views[order.client_order_id] = BrokerOrderView(
            broker_order_id=broker_id,
            client_order_id=order.client_order_id,
            status="open",
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


def _oms_cfg() -> OMSConfig:
    return OMSConfig(resubmit_backoff=0.1, reconciliation_interval=0.5, max_inflight_orders=20)


def _risk_limits() -> RiskLimits:
    return RiskLimits(
        daily_pnl_stop=500,
        per_symbol_loss_stop=250,
        max_open_lots=5,
        notional_premium_cap=200000,
        max_order_rate=50,
        no_new_entries_after=dt.time(15, 10),
        square_off_by=dt.time(15, 22),
    )


@pytest.mark.asyncio
async def test_integration_partial_reject_and_reconnect(tmp_path):
    broker = FlakyBroker()
    store = SQLiteStore(tmp_path / "integration.sqlite", run_id="int")
    oms = OMS(broker=broker, store=store, config=_oms_cfg())
    clock_now = dt.datetime(2024, 7, 1, 9, 45, tzinfo=IST)
    risk = RiskManager(_risk_limits(), store, clock=lambda: clock_now)
    ts = clock_now
    order_id = OMS.client_order_id("demo", ts, "NIFTY", "BUY", 50)
    budget = OrderBudget(symbol="NIFTY", qty=50, price=200.0, lot_size=50, side="BUY")
    assert risk.budget_ok_for(budget)
    with pytest.raises(BrokerError):
        await oms.submit(strategy="demo", symbol="NIFTY", side="BUY", qty=50, ts=ts, limit_price=200.0)
    assert oms.get_order(order_id) is not None
    await oms.handle_reconnect()
    order = oms.get_order(order_id)
    assert order is not None
    assert order.state.name in {"SUBMITTED", "ACKNOWLEDGED"}
    await oms.record_fill(order_id, qty=25, price=200.0)
    risk.on_fill(symbol="NIFTY", side="BUY", qty=25, price=200.0, lot_size=50)
    await oms.mark_reject(order_id, "manual_fail")
    assert order.state == OrderState.REJECTED
