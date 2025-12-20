from __future__ import annotations

import datetime as dt

import pytest

from engine.backtest.costs import apply_slippage, apply_spread
from engine.backtest.execution_config import ExecutionConfig
from engine.backtesting import BacktestingEngine
from engine.config import EngineConfig, IST
from engine.fees import FeeConfig
from engine.pnl import Execution, PnLCalculator
from persistence import SQLiteStore
from strategy.base import BaseStrategy


def test_apply_spread_worsens_in_trade_direction() -> None:
    assert apply_spread(100.0, "BUY", 10.0) == pytest.approx(100.1)
    assert apply_spread(100.0, "SELL", 10.0) == pytest.approx(99.9)


def test_apply_slippage_bps_and_ticks() -> None:
    assert apply_slippage(100.0, "BUY", "bps", 20.0, 0, 0.05) == pytest.approx(100.2)
    assert apply_slippage(100.0, "SELL", "bps", 20.0, 0, 0.05) == pytest.approx(99.8)
    assert apply_slippage(100.0, "BUY", "ticks", 0.0, 2, 0.05) == pytest.approx(100.1)
    assert apply_slippage(100.0, "SELL", "ticks", 0.0, 2, 0.05) == pytest.approx(99.9)


def test_apply_slippage_invalid_model_raises() -> None:
    with pytest.raises(ValueError):
        apply_slippage(100.0, "BUY", "wat", 0.0, 0, 0.05)


def test_brokerage_charged_once_per_order(tmp_path) -> None:
    store = SQLiteStore(tmp_path / "fees.sqlite", run_id="run-fee")
    cfg = FeeConfig(brokerage_per_order=2.0)
    pnl = PnLCalculator(store, cfg)
    ts = dt.datetime(2024, 1, 1, 9, 30, tzinfo=IST)

    e1 = Execution(exec_id="e1", order_id="o1", symbol="OPT", side="BUY", qty=1, price=100.0, ts=ts)
    e2 = Execution(exec_id="e2", order_id="o1", symbol="OPT", side="BUY", qty=1, price=101.0, ts=ts + dt.timedelta(seconds=1))

    pnl.on_execution(e1)
    pnl.on_execution(e2)

    ledger = store.list_cost_ledger()
    brokerage_rows = [row for row in ledger if row["category"] == "brokerage"]
    assert len(brokerage_rows) == 1
    assert sum(float(row["amount"]) for row in brokerage_rows) == pytest.approx(2.0)


class _OneBuyStrategy(BaseStrategy):
    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self._sent = False

    async def on_tick(self, event: dict) -> None:
        payload = event.get("payload") or {}
        symbol = str(payload.get("symbol") or "").upper()
        if symbol != self.cfg.data.index_symbol.upper():
            return
        if self._sent:
            return
        self._sent = True
        ts = self._event_ts(event.get("ts"))
        qty = int(self.cfg.data.lot_step)
        await self.oms.submit(strategy="dummy", symbol=symbol, side="BUY", qty=qty, order_type="MARKET", ts=ts)

    async def on_fill(self, fill: dict) -> None:
        return


@pytest.mark.asyncio
async def test_backtest_applies_slippage_and_persists_raw_vs_effective(monkeypatch: pytest.MonkeyPatch) -> None:
    import engine.backtesting as backtesting_mod

    monkeypatch.setattr(backtesting_mod, "get_strategy_class", lambda name: _OneBuyStrategy)

    from dataclasses import replace

    cfg = EngineConfig.load()
    cfg = replace(
        cfg,
        exit=replace(
            cfg.exit,
            stop_pct=0.0,
            target1_pct=0.0,
            partial_fraction=0.0,
            trailing_stop_pct=0.0,
            time_stop_minutes=0,
            max_holding_minutes=0,
            partial_target_multiplier=0.0,
            trailing_pct=0.0,
            trailing_step=0.0,
            time_buffer_minutes=0,
            partial_tp_mult=0.0,
            at_pct=0.0,
            scalping_profit_target_pct=0.0,
            scalping_stop_loss_pct=0.0,
            scalping_time_limit_minutes=0,
        ),
    )
    engine = BacktestingEngine(
        config=cfg,
        execution_config=ExecutionConfig(fill_model="same_tick", latency_ms=0, allow_partial_fills=False),
        slippage_model="ticks",
        slippage_ticks=2,
        spread_bps=0.0,
    )
    try:
        data = [
            {"ts": dt.datetime(2024, 1, 2, 9, 15, tzinfo=IST), "open": 100.0, "high": 100.0, "low": 100.0, "close": 100.0, "volume": 1000},
        ]
        res = await engine.run_backtest_async("DummyStrategy", dt.date(2024, 1, 2), dt.date(2024, 1, 2), data=data, interval="1minute")
        executions = res.get("executions") or []
        buy_exec = next(row for row in executions if str(row.get("side")).upper() == "BUY")
        assert float(buy_exec["raw_price"]) == pytest.approx(100.0)
        assert float(buy_exec["effective_price"]) == pytest.approx(100.1)
        assert float(buy_exec["price"]) == pytest.approx(100.1)
    finally:
        engine.close()
