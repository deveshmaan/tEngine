import datetime as dt

import pytest

from engine.backtesting import BacktestingEngine
from engine.config import EngineConfig
from engine.fees import FeeConfig
from engine.pnl import PnLCalculator
from strategy.base import BaseStrategy


class DummyStrategy(BaseStrategy):
    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self._ticks = 0

    async def on_tick(self, event: dict) -> None:
        payload = event.get("payload") or {}
        symbol = str(payload.get("symbol") or "").upper()
        if symbol != self.cfg.data.index_symbol.upper():
            return
        self._ticks += 1
        ts = self._event_ts(event.get("ts"))
        qty = int(self.cfg.data.lot_step)
        if self._ticks == 1:
            await self.oms.submit(strategy="dummy", symbol=symbol, side="BUY", qty=qty, order_type="MARKET", ts=ts)
        # BacktestingEngine emits multiple ticks per candle; sell on the next candle.
        elif self._ticks == 5:
            await self.oms.submit(strategy="dummy", symbol=symbol, side="SELL", qty=qty, order_type="MARKET", ts=ts)

    async def on_fill(self, fill: dict) -> None:
        return


@pytest.mark.asyncio
async def test_backtesting_engine_executes_and_computes_pnl(monkeypatch: pytest.MonkeyPatch) -> None:
    import engine.backtesting as backtesting_mod

    monkeypatch.setattr(backtesting_mod, "get_strategy_class", lambda name: DummyStrategy)

    # Disable ExitEngine automation so the dummy strategy controls exits deterministically.
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

    engine = BacktestingEngine(config=cfg)
    engine.pnl = PnLCalculator(engine.store, FeeConfig())  # disable fees for deterministic assertion
    try:
        ist = dt.timezone(dt.timedelta(hours=5, minutes=30))
        data = [
            {"ts": dt.datetime(2024, 1, 2, 9, 15, tzinfo=ist), "open": 100, "high": 100, "low": 100, "close": 100, "volume": 0},
            {"ts": dt.datetime(2024, 1, 2, 9, 16, tzinfo=ist), "open": 110, "high": 110, "low": 110, "close": 110, "volume": 0},
        ]
        res = await engine.run_backtest_async("DummyStrategy", dt.date(2024, 1, 2), dt.date(2024, 1, 2), data=data)

        assert res["trades"] == 1
        assert res["errors"] == []
        assert res["net_pnl"] == pytest.approx(10.0 * int(engine.cfg.data.lot_step))
        assert len(res["trade_log"]) == 1
        assert res["trade_log"][0]["net_pnl"] == pytest.approx(10.0 * int(engine.cfg.data.lot_step))
    finally:
        engine.close()
