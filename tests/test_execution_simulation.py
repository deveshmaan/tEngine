from __future__ import annotations

import datetime as dt

import pytest

from engine.backtest.execution_config import ExecutionConfig
from engine.backtesting import BacktestingEngine
from engine.config import EngineConfig
from strategy.base import BaseStrategy


IST = dt.timezone(dt.timedelta(hours=5, minutes=30))


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


def _disable_exit_automation(cfg: EngineConfig) -> EngineConfig:
    from dataclasses import replace

    return replace(
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


@pytest.mark.asyncio
async def test_fill_model_next_tick_delays_fill_ts(monkeypatch: pytest.MonkeyPatch) -> None:
    import engine.backtesting as backtesting_mod

    monkeypatch.setattr(backtesting_mod, "get_strategy_class", lambda name: _OneBuyStrategy)

    cfg = _disable_exit_automation(EngineConfig.load())
    engine = BacktestingEngine(config=cfg, execution_config=ExecutionConfig(fill_model="next_tick", latency_ms=0, allow_partial_fills=False))
    try:
        data = [
            {"ts": dt.datetime(2024, 1, 2, 9, 15, tzinfo=IST), "open": 100, "high": 100, "low": 100, "close": 100, "volume": 1000},
            {"ts": dt.datetime(2024, 1, 2, 9, 16, tzinfo=IST), "open": 101, "high": 101, "low": 101, "close": 101, "volume": 1000},
        ]
        res = await engine.run_backtest_async("DummyStrategy", dt.date(2024, 1, 2), dt.date(2024, 1, 2), data=data, interval="1minute")
        executions = res.get("executions") or []
        buy_exec = next(row for row in executions if str(row.get("side")).upper() == "BUY")
        ts = dt.datetime.fromisoformat(str(buy_exec["ts"]))
        assert ts.astimezone(IST).replace(microsecond=0).time() == dt.time(9, 16, 0)
    finally:
        engine.close()


@pytest.mark.asyncio
async def test_latency_ms_delays_fill_ts(monkeypatch: pytest.MonkeyPatch) -> None:
    import engine.backtesting as backtesting_mod

    monkeypatch.setattr(backtesting_mod, "get_strategy_class", lambda name: _OneBuyStrategy)

    cfg = _disable_exit_automation(EngineConfig.load())
    engine = BacktestingEngine(config=cfg, execution_config=ExecutionConfig(fill_model="same_tick", latency_ms=16_000, allow_partial_fills=False))
    try:
        data = [
            {"ts": dt.datetime(2024, 1, 2, 9, 15, tzinfo=IST), "open": 100, "high": 100, "low": 100, "close": 100, "volume": 1000},
        ]
        res = await engine.run_backtest_async("DummyStrategy", dt.date(2024, 1, 2), dt.date(2024, 1, 2), data=data, interval="1minute")
        executions = res.get("executions") or []
        buy_exec = next(row for row in executions if str(row.get("side")).upper() == "BUY")
        ts = dt.datetime.fromisoformat(str(buy_exec["ts"]))
        assert ts.astimezone(IST).replace(microsecond=0).time() == dt.time(9, 15, 30)
    finally:
        engine.close()


@pytest.mark.asyncio
async def test_partial_fills_respect_volume(monkeypatch: pytest.MonkeyPatch) -> None:
    import engine.backtesting as backtesting_mod

    monkeypatch.setattr(backtesting_mod, "get_strategy_class", lambda name: _OneBuyStrategy)

    cfg = _disable_exit_automation(EngineConfig.load())
    from dataclasses import replace

    # lot_size = 5, order_qty = 10 (2 lots) for clean partial fills.
    cfg = replace(cfg, data=replace(cfg.data, lot_step=5))

    class _FixedQtyStrategy(_OneBuyStrategy):
        async def on_tick(self, event: dict) -> None:
            payload = event.get("payload") or {}
            symbol = str(payload.get("symbol") or "").upper()
            if symbol != self.cfg.data.index_symbol.upper():
                return
            if self._sent:
                return
            self._sent = True
            ts = self._event_ts(event.get("ts"))
            await self.oms.submit(strategy="dummy", symbol=symbol, side="BUY", qty=10, order_type="MARKET", ts=ts)

    monkeypatch.setattr(backtesting_mod, "get_strategy_class", lambda name: _FixedQtyStrategy)

    data = [
        {"ts": dt.datetime(2024, 1, 2, 9, 15, tzinfo=IST), "open": 100, "high": 100, "low": 100, "close": 100, "volume": 5},
        {"ts": dt.datetime(2024, 1, 2, 9, 16, tzinfo=IST), "open": 100, "high": 100, "low": 100, "close": 100, "volume": 20},
    ]

    engine_partials = BacktestingEngine(config=cfg, execution_config=ExecutionConfig(fill_model="same_tick", latency_ms=0, allow_partial_fills=True))
    try:
        res = await engine_partials.run_backtest_async("DummyStrategy", dt.date(2024, 1, 2), dt.date(2024, 1, 2), data=data, interval="1minute")
        executions = [row for row in (res.get("executions") or []) if str(row.get("side")).upper() == "BUY"]
        assert [int(row["qty"]) for row in executions] == [5, 5]
    finally:
        engine_partials.close()

    engine_full = BacktestingEngine(config=cfg, execution_config=ExecutionConfig(fill_model="same_tick", latency_ms=0, allow_partial_fills=False))
    try:
        res = await engine_full.run_backtest_async("DummyStrategy", dt.date(2024, 1, 2), dt.date(2024, 1, 2), data=data, interval="1minute")
        executions = [row for row in (res.get("executions") or []) if str(row.get("side")).upper() == "BUY"]
        assert [int(row["qty"]) for row in executions] == [10]
        ts = dt.datetime.fromisoformat(str(executions[0]["ts"]))
        assert ts.astimezone(IST).replace(microsecond=0).time() == dt.time(9, 16, 0)
    finally:
        engine_full.close()
