import datetime as dt

from engine.pnl import PnLCalculator, Execution
from persistence.store import SQLiteStore
from engine.fees import load_fee_config


class _DummyStore(SQLiteStore):
    def __init__(self):
        super().__init__(":memory:", "test")


def test_pnl_full_roundtrip_profit():
    store = _DummyStore()
    fees = load_fee_config()
    pnl = PnLCalculator(store, fees)
    buy = Execution(
        exec_id="1",
        order_id="o1",
        symbol="NIFTY-TEST",
        side="BUY",
        qty=1,
        price=100.0,
        ts=dt.datetime.now(),
    )
    sell = Execution(
        exec_id="2",
        order_id="o2",
        symbol="NIFTY-TEST",
        side="SELL",
        qty=1,
        price=110.0,
        ts=dt.datetime.now(),
    )
    pnl.on_execution(buy)
    pnl.on_execution(sell)
    pnl.snapshot(dt.datetime.now())
    realized, _, _ = pnl.totals()
    assert realized > 0


def test_pnl_partial_fills_accumulate():
    store = _DummyStore()
    fees = load_fee_config()
    pnl = PnLCalculator(store, fees)
    buy1 = Execution("1", "o1", "NIFTY-TEST", "BUY", 1, 100.0, dt.datetime.now())
    buy2 = Execution("2", "o1", "NIFTY-TEST", "BUY", 1, 102.0, dt.datetime.now())
    sell = Execution("3", "o2", "NIFTY-TEST", "SELL", 2, 105.0, dt.datetime.now())
    pnl.on_execution(buy1)
    pnl.on_execution(buy2)
    pnl.on_execution(sell)
    pnl.snapshot(dt.datetime.now())
    realized, _, _ = pnl.totals()
    assert realized > 0
