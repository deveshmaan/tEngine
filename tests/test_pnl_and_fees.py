import datetime as dt
from pathlib import Path

import pytest

from engine.config import IST
from engine.fees import FeeConfig, fees_for_execution
from engine.pnl import Execution, PnLCalculator
from persistence import SQLiteStore


def _exec(exec_id: str, side: str, qty: int, price: float, ts: dt.datetime) -> Execution:
    return Execution(exec_id=exec_id, order_id=f"order-{exec_id}", symbol="OPT", side=side, qty=qty, price=price, ts=ts)


def test_partial_fills_snapshot_and_fees(tmp_path: Path) -> None:
    store = SQLiteStore(tmp_path / "pnl.sqlite", run_id="run-fee")
    cfg = FeeConfig(
        brokerage_per_order=1.0,
        exchange_txn_rate_per_turnover=0.001,
        gst_rate_on_services=0.1,
        other_fees={"infra": 0.5},
    )
    calc = PnLCalculator(store, cfg)
    ts = dt.datetime(2024, 1, 1, 9, 30, tzinfo=IST)
    execs = [
        _exec("e1", "BUY", 100, 100.0, ts),
        _exec("e2", "BUY", 50, 110.0, ts + dt.timedelta(minutes=1)),
        _exec("e3", "SELL", 80, 120.0, ts + dt.timedelta(minutes=2)),
        _exec("e4", "SELL", 40, 130.0, ts + dt.timedelta(minutes=3)),
    ]
    for ex in execs:
        calc.on_execution(ex)
    calc.mark_to_market({"OPT": 118.0})
    calc.snapshot(ts + dt.timedelta(minutes=5))
    snaps = store.list_pnl_snapshots()
    assert snaps
    latest = snaps[-1]
    assert latest["realized"] == pytest.approx(2600.0)
    assert latest["unrealized"] == pytest.approx(240.0)
    total_fees = 39.73
    assert latest["fees"] == pytest.approx(total_fees)
    assert latest["net"] == pytest.approx(latest["realized"] + latest["unrealized"] - total_fees)
    ledger = store.list_cost_ledger()
    assert pytest.approx(sum(entry["amount"] for entry in ledger)) == total_fees
    expected_categories = {"brokerage", "exchange_txn", "gst", "infra"}
    for ex in execs:
        per_exec = [row for row in ledger if row["exec_id"] == ex.exec_id]
        assert {row["category"] for row in per_exec} == expected_categories


def test_fee_config_changes_output():
    ts = dt.datetime(2024, 1, 1, tzinfo=dt.timezone.utc)
    execution = _exec("fee", "BUY", 10, 100.0, ts)
    cfg_one = FeeConfig(brokerage_per_order=1.0)
    cfg_two = FeeConfig(brokerage_per_order=3.0)
    total_one = sum(row.amount for row in fees_for_execution(execution, cfg_one))
    total_two = sum(row.amount for row in fees_for_execution(execution, cfg_two))
    assert total_two > total_one
