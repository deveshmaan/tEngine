import datetime as dt
from typing import Optional

import pytest

from engine.config import ExitConfig, IST
from engine.exit import ExitEngine
from persistence import SQLiteStore


class DummyRisk:
    def __init__(self) -> None:
        self._halt_reason = None

    def square_off_all(self, reason: str):
        self._halt_reason = reason
        return []

    def halt_reason(self) -> Optional[str]:
        return self._halt_reason

    def should_halt(self) -> bool:
        return False


class DummyOMS:
    def __init__(self) -> None:
        self.orders: list[dict] = []

    async def submit(self, *, strategy: str, symbol: str, side: str, qty: int, order_type: str, limit_price: float, ts: dt.datetime):
        self.orders.append(
            {
                "strategy": strategy,
                "symbol": symbol,
                "side": side,
                "qty": qty,
                "limit_price": limit_price,
                "ts": ts,
            }
        )
        return self.orders[-1]


def _engine(tmp_path, cfg: ExitConfig) -> tuple[ExitEngine, SQLiteStore, DummyOMS]:
    store = SQLiteStore(tmp_path / "exit.sqlite", run_id="test-run")
    oms = DummyOMS()
    risk = DummyRisk()
    engine = ExitEngine(config=cfg, risk=risk, oms=oms, store=store, tick_size=1.0, metrics=None)
    return engine, store, oms


def _seed_position(store: SQLiteStore, symbol: str, qty: int, price: float, ts: dt.datetime) -> None:
    store.upsert_position(symbol=symbol, expiry=None, strike=None, opt_type=None, qty=qty, avg_price=price, lot_size=1, opened_at=ts, closed_at=None)


@pytest.mark.asyncio
async def test_exit_engine_triggers_stop(tmp_path):
    cfg = ExitConfig(stop_pct=0.25, target1_pct=0.5, partial_fraction=0.5, trail_lock_pct=0.4, trail_giveback_pct=0.5, min_trail_ticks=1, max_holding_minutes=60)
    engine, store, oms = _engine(tmp_path, cfg)
    ts = dt.datetime(2024, 7, 1, 9, 30, tzinfo=IST)
    symbol = "TEST-SYM"
    _seed_position(store, symbol, qty=10, price=100.0, ts=ts)
    engine.on_fill(symbol=symbol, side="BUY", qty=10, price=100.0, ts=ts)

    await engine.on_tick(symbol, 70.0, ts + dt.timedelta(minutes=1))

    assert len(oms.orders) == 1
    assert oms.orders[0]["side"] == "SELL"
    plan = store.load_exit_plan(symbol)
    assert plan and plan.get("pending_exit_reason") == "STOP"


@pytest.mark.asyncio
async def test_exit_engine_target_then_trail(tmp_path):
    cfg = ExitConfig(stop_pct=0.25, target1_pct=0.5, partial_fraction=0.5, trail_lock_pct=0.4, trail_giveback_pct=0.5, min_trail_ticks=1, max_holding_minutes=60)
    engine, store, oms = _engine(tmp_path, cfg)
    ts = dt.datetime(2024, 7, 1, 9, 30, tzinfo=IST)
    symbol = "TEST-TGT"
    _seed_position(store, symbol, qty=10, price=100.0, ts=ts)
    engine.on_fill(symbol=symbol, side="BUY", qty=10, price=100.0, ts=ts)

    await engine.on_tick(symbol, 155.0, ts + dt.timedelta(minutes=5))
    assert len(oms.orders) == 1
    first_plan = store.load_exit_plan(symbol)
    assert first_plan and first_plan.get("trailing_active")
    assert first_plan.get("partial_filled_qty") == 5
    assert first_plan.get("trailing_stop") and first_plan["trailing_stop"] > 120.0

    _seed_position(store, symbol, qty=5, price=100.0, ts=ts)
    engine.on_fill(symbol=symbol, side="SELL", qty=5, price=155.0, ts=ts + dt.timedelta(minutes=5))

    await engine.on_tick(symbol, 170.0, ts + dt.timedelta(minutes=6))
    updated_plan = store.load_exit_plan(symbol)
    assert updated_plan and pytest.approx(updated_plan["trailing_stop"], rel=1e-3) == 135.0

    await engine.on_tick(symbol, 134.0, ts + dt.timedelta(minutes=7))
    assert len(oms.orders) == 2
    final_plan = store.load_exit_plan(symbol)
    assert final_plan and final_plan.get("pending_exit_reason") == "TRAIL"


@pytest.mark.asyncio
async def test_exit_engine_time_stop(tmp_path):
    cfg = ExitConfig(stop_pct=0.0, target1_pct=0.0, partial_fraction=0.5, trail_lock_pct=0.4, trail_giveback_pct=0.5, min_trail_ticks=1, max_holding_minutes=1)
    engine, store, oms = _engine(tmp_path, cfg)
    ts = dt.datetime(2024, 7, 1, 9, 30, tzinfo=IST)
    symbol = "TEST-TIME"
    _seed_position(store, symbol, qty=2, price=50.0, ts=ts)
    engine.on_fill(symbol=symbol, side="BUY", qty=2, price=50.0, ts=ts)

    await engine.on_tick(symbol, 52.0, ts + dt.timedelta(minutes=2))
    assert len(oms.orders) == 1
    plan = store.load_exit_plan(symbol)
    assert plan and plan.get("pending_exit_reason") == "TIME"


@pytest.mark.asyncio
async def test_exit_engine_oi_reversal_triggers(tmp_path):
    cfg = ExitConfig(stop_pct=0.0, target1_pct=0.0, partial_fraction=0.5, trail_lock_pct=0.4, trail_giveback_pct=0.5, min_trail_ticks=1, max_holding_minutes=60, trailing_stop_pct=0.0)
    engine, store, oms = _engine(tmp_path, cfg)
    ts = dt.datetime(2024, 7, 1, 9, 30, tzinfo=IST)
    symbol = "TEST-OI"
    _seed_position(store, symbol, qty=10, price=100.0, ts=ts)
    engine.on_fill(symbol=symbol, side="BUY", qty=10, price=100.0, ts=ts)

    # two ticks: price falling, OI rising -> should trigger OI_REVERSAL
    await engine.on_tick(symbol, 105.0, ts + dt.timedelta(minutes=1))
    await engine.on_tick(symbol, 99.0, ts + dt.timedelta(minutes=2), oi=120.0)

    assert len(oms.orders) == 1
    plan = store.load_exit_plan(symbol)
    assert plan and plan.get("pending_exit_reason") == "OI_REVERSAL"
