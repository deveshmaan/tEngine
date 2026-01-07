import datetime as dt

import pytest

from engine.fees import FeeConfig
from engine.pnl import Execution, PnLCalculator
from persistence import SQLiteStore


def test_pnl_prefers_stored_charges(tmp_path):
    store = SQLiteStore(tmp_path / "pnl.sqlite", run_id="test-run")
    exec_id = "exec-1"
    store.record_cost(exec_id, category="brokerage", amount=15.0, currency="INR", note="charge_api", ts=dt.datetime(2024, 7, 1))

    pnl = PnLCalculator(store, FeeConfig(brokerage_per_order=20.0))
    exec_obj = Execution(
        exec_id=exec_id,
        order_id=exec_id,
        symbol="TEST",
        side="BUY",
        qty=1,
        price=100.0,
        ts=dt.datetime(2024, 7, 1, 10, 0),
    )
    pnl.on_execution(exec_obj)
    _, _, fees = pnl.totals()
    assert fees == pytest.approx(15.0)
