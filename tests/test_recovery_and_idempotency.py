import datetime as dt
from pathlib import Path

import pytest

from engine.config import IST, OMSConfig, RiskLimits
from engine.oms import BrokerOrderAck, BrokerOrderView, OMS
from engine.recovery import RecoveryManager
from engine.risk import OrderBudget, RiskManager
from persistence import SQLiteStore


class ReplayBroker:
    def __init__(self):
        self.submits = 0
        self.views: dict[str, BrokerOrderView] = {}

    async def submit_order(self, order):
        self.submits += 1
        view = BrokerOrderView(
            broker_order_id=f"BRK-{order.client_order_id}",
            client_order_id=order.client_order_id,
            status="open",
            filled_qty=0,
            avg_price=0.0,
        )
        self.views[order.client_order_id] = view
        return BrokerOrderAck(broker_order_id=view.broker_order_id, status="accepted")

    async def replace_order(self, order, *, price=None, qty=None):
        return None

    async def cancel_order(self, order):
        self.views.pop(order.client_order_id, None)

    async def fetch_open_orders(self):
        return list(self.views.values())


def _cfg() -> OMSConfig:
    return OMSConfig(resubmit_backoff=0.1, reconciliation_interval=0.5, max_inflight_orders=10)


def _risk_limits() -> RiskLimits:
    return RiskLimits(
        daily_pnl_stop=5000.0,
        per_symbol_loss_stop=2500.0,
        max_open_lots=10,
        notional_premium_cap=1_000_000.0,
        max_order_rate=40,
        no_new_entries_after=dt.time(15, 0),
        square_off_by=dt.time(15, 30),
    )


@pytest.mark.asyncio
async def test_idempotency_avoids_duplicate_submits(tmp_path: Path) -> None:
    store = SQLiteStore(tmp_path / "idem.sqlite", run_id="idem")
    broker = ReplayBroker()
    oms = OMS(broker=broker, store=store, config=_cfg())
    ts = dt.datetime(2024, 7, 1, 9, 30, tzinfo=IST)
    order = await oms.submit(strategy="demo", symbol="NIFTY-TEST", side="BUY", qty=10, ts=ts)
    assert broker.submits == 1
    # simulate restart by creating new OMS with same store
    broker2 = ReplayBroker()
    broker2.views = broker.views
    oms2 = OMS(broker=broker2, store=store, config=_cfg())
    await oms2.submit(strategy="demo", symbol="NIFTY-TEST", side="BUY", qty=10, ts=ts)
    assert broker2.submits == 0
    assert order.client_order_id in broker2.views


@pytest.mark.asyncio
async def test_recovery_rehydrates_orders(tmp_path: Path) -> None:
    store = SQLiteStore(tmp_path / "recovery.sqlite", run_id="recovery")
    broker = ReplayBroker()
    oms = OMS(broker=broker, store=store, config=_cfg())
    ts = dt.datetime(2024, 7, 1, 10, 0, tzinfo=IST)
    order = await oms.submit(strategy="demo", symbol="NIFTY-REC", side="BUY", qty=10, ts=ts)
    broker2 = ReplayBroker()
    broker2.views = broker.views
    oms2 = OMS(broker=broker2, store=store, config=_cfg())
    manager = RecoveryManager(store, oms2)
    await manager.reconcile()
    assert oms2.get_order(order.client_order_id) is not None
    assert len(broker2.views) == 1


def test_kill_switch_blocks_entries_but_allows_square_off(tmp_path: Path) -> None:
    store = SQLiteStore(tmp_path / "risk.sqlite", run_id="risk")
    now = dt.datetime(2024, 7, 1, 9, 31, tzinfo=IST)
    risk = RiskManager(_risk_limits(), store, clock=lambda: now)
    budget = OrderBudget(symbol="NIFTY-KILL", qty=50, price=100.0, lot_size=50, side="BUY")
    assert risk.budget_ok_for(budget)
    risk.trigger_kill("MANUAL_OVERRIDE")
    assert not risk.budget_ok_for(budget)
    risk.on_fill(symbol="NIFTY-KILL", side="BUY", qty=50, price=100.0, lot_size=50)
    risk.on_tick("NIFTY-KILL", 104.0)
    exits = risk.square_off_all("MANUAL_OVERRIDE")
    assert exits
    assert exits[0].side == "SELL"
    assert exits[0].symbol == "NIFTY-KILL"
    events = store.list_risk_events()
    assert any(evt["code"] == "KILL" for evt in events)
