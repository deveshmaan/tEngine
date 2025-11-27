import datetime as dt

import pytest

from engine.config import RiskLimits
from engine.risk import OrderBudget, RiskManager
from persistence.store import SQLiteStore


class _DummyStore(SQLiteStore):
    def __init__(self):
        super().__init__(":memory:", "test")


def _risk(cfg: RiskLimits) -> RiskManager:
    return RiskManager(cfg, _DummyStore(), clock=lambda: dt.datetime(2024, 1, 1, 9, 20))


def test_risk_daily_stop_blocks_new_entries():
    cfg = RiskLimits(
        daily_pnl_stop=-100,
        per_symbol_loss_stop=-100,
        max_open_lots=1,
        notional_premium_cap=10000,
        max_order_rate=15,
        no_new_entries_after=dt.time(15, 10),
        square_off_by=dt.time(15, 20),
    )
    r = _risk(cfg)
    r._positions["NIFTY-TEST"] = r._positions.get("NIFTY-TEST", r._make_state())
    r._positions["NIFTY-TEST"].realized_pnl = -200
    budget = OrderBudget(symbol="NIFTY-TEST", qty=1, price=100, lot_size=1, side="BUY")
    assert r.budget_ok_for(budget) is False


def test_risk_allows_sell_to_flatten():
    cfg = RiskLimits(
        daily_pnl_stop=-100,
        per_symbol_loss_stop=-100,
        max_open_lots=1,
        notional_premium_cap=10000,
        max_order_rate=15,
        no_new_entries_after=dt.time(15, 10),
        square_off_by=dt.time(15, 20),
    )
    r = _risk(cfg)
    budget = OrderBudget(symbol="NIFTY-TEST", qty=1, price=100, lot_size=1, side="SELL")
    assert r.budget_ok_for(budget) is True


def test_risk_notional_cap():
    cfg = RiskLimits(
        daily_pnl_stop=-1000,
        per_symbol_loss_stop=-1000,
        max_open_lots=1,
        notional_premium_cap=100,
        max_order_rate=15,
        no_new_entries_after=dt.time(15, 10),
        square_off_by=dt.time(15, 20),
    )
    r = _risk(cfg)
    budget = OrderBudget(symbol="NIFTY-TEST", qty=10, price=20, lot_size=1, side="BUY")
    assert r.budget_ok_for(budget) is False


def test_risk_max_open_lots():
    cfg = RiskLimits(
        daily_pnl_stop=-1000,
        per_symbol_loss_stop=-1000,
        max_open_lots=0,
        notional_premium_cap=10000,
        max_order_rate=15,
        no_new_entries_after=dt.time(15, 10),
        square_off_by=dt.time(15, 20),
    )
    r = _risk(cfg)
    budget = OrderBudget(symbol="NIFTY-TEST", qty=1, price=10, lot_size=1, side="BUY")
    assert r.budget_ok_for(budget) is False
