import datetime as dt

from engine.config import IST, RiskLimits
from engine.risk import OrderBudget, RiskManager
from persistence import SQLiteStore


def _limits() -> RiskLimits:
    return RiskLimits(
        daily_pnl_stop=1000,
        per_symbol_loss_stop=600,
        max_open_lots=5,
        notional_premium_cap=50000,
        max_order_rate=10,
        no_new_entries_after=dt.time(15, 10),
        square_off_by=dt.time(15, 22),
    )


def test_daily_pnl_stop_triggers_kill(tmp_path):
    store = SQLiteStore(tmp_path / "risk.sqlite", run_id="test-run")
    now = dt.datetime(2024, 7, 1, 10, 0, tzinfo=IST)
    risk = RiskManager(_limits(), store, clock=lambda: now)
    budget = OrderBudget(symbol="NIFTY-EXP-20000", qty=50, price=200.0, lot_size=50, side="BUY")
    assert risk.budget_ok_for(budget)
    risk.on_fill(symbol=budget.symbol, side="BUY", qty=50, price=200.0, lot_size=50)
    risk.on_tick(budget.symbol, 198.0)
    assert not risk.should_halt()
    risk.on_tick(budget.symbol, 0.0)
    assert risk.should_halt()
    events = store.list_risk_events()
    assert any(evt["code"] == "KILL" for evt in events)


def test_entry_cutoff_blocks_new_orders(tmp_path):
    store = SQLiteStore(tmp_path / "risk.db", run_id="test-run")
    after_cutoff = dt.datetime(2024, 7, 1, 16, 0, tzinfo=IST)
    risk = RiskManager(_limits(), store, clock=lambda: after_cutoff)
    budget = OrderBudget(symbol="NIFTY-EXP-20000", qty=50, price=200.0, lot_size=50, side="BUY")
    assert not risk.budget_ok_for(budget)
    assert store.list_risk_events()[-1]["code"] == "ENTRY_WINDOW"
