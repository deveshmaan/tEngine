import datetime as dt

import pytest

from engine.config import IST, RiskLimits
from engine.risk import OrderBudget, RiskManager
from persistence import SQLiteStore


def _risk(tmp_path, **overrides) -> RiskManager:
    base = dict(
        daily_pnl_stop=-10000,
        per_symbol_loss_stop=-5000,
        max_open_lots=10,
        notional_premium_cap=1_000_000,
        max_order_rate=100,
        no_new_entries_after=dt.time(15, 20),
        square_off_by=dt.time(15, 25),
    )
    base.update(overrides)
    cfg = RiskLimits.from_dict(base)
    store = SQLiteStore(tmp_path / "risk.sqlite", run_id="t")
    return RiskManager(cfg, store, capital_base=100000, clock=lambda: dt.datetime(2024, 1, 1, 10, 0, tzinfo=IST))


def test_trade_count_blocks_after_limit(tmp_path):
    risk = _risk(tmp_path, max_trades_per_day=1)
    # open position
    risk.on_fill(symbol="SYM", side="BUY", qty=1, price=100, lot_size=1)
    # close position -> counts as one trade
    risk.on_fill(symbol="SYM", side="SELL", qty=1, price=101, lot_size=1)
    budget = OrderBudget(symbol="SYM", qty=1, price=100, lot_size=1, side="BUY")
    assert not risk.budget_ok_for(budget)
    assert risk.halt_reason() in {None, "MAX_TRADES_REACHED"}


def test_slippage_halt(tmp_path):
    risk = _risk(tmp_path, max_slippage_pct_per_trade=0.01, max_slippage_trades=1)
    risk.record_expected_price("SYM", 100.0)
    risk.on_fill(symbol="SYM", side="BUY", qty=1, price=103.0, lot_size=1)  # 3% slippage
    budget = OrderBudget(symbol="SYM", qty=1, price=100, lot_size=1, side="BUY")
    assert not risk.budget_ok_for(budget)
    assert risk.halt_reason() in {None, "EXCESSIVE_SLIPPAGE"}


def test_entry_spread_block(tmp_path):
    risk = _risk(tmp_path, max_entry_spread_pct=0.05)
    risk.record_expected_price("SYM", 100.0, spread_pct=0.06)
    budget = OrderBudget(symbol="SYM", qty=1, price=100, lot_size=1, side="BUY")
    assert not risk.budget_ok_for(budget)
