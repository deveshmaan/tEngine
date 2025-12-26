from __future__ import annotations

import datetime as dt
import gzip
import json
import sqlite3
from dataclasses import replace
from pathlib import Path

import pandas as pd
import pytest

from engine.backtest.execution_config import ExecutionConfig
from engine.backtest.strategy_spec import LegSpec, StrategySpec
from engine.backtesting import BacktestingEngine
from engine.config import EngineConfig, IST


UNDERLYING_SYMBOL = "NIFTY"
UNDERLYING_KEY = "NSE_INDEX|Nifty 50"


def _disable_exit_automation(cfg: EngineConfig) -> EngineConfig:
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


def _expiry_ms_for(expiry: str) -> int:
    d = dt.date.fromisoformat(expiry)
    expiry_dt = dt.datetime.combine(d, dt.time(23, 59, 59), tzinfo=IST)
    return int(expiry_dt.astimezone(dt.timezone.utc).timestamp() * 1000)


def _write_instrument_master(
    path: Path,
    *,
    underlying_key: str,
    expiry: str,
    instrument_keys: dict[tuple[str, int], str],
) -> None:
    payload: list[dict[str, object]] = []
    for (opt_type, strike), instrument_key in instrument_keys.items():
        payload.append(
            {
                "instrument_key": instrument_key,
                "instrument_type": str(opt_type).upper(),
                "underlying_key": str(underlying_key),
                "strike_price": float(strike),
                "expiry": _expiry_ms_for(expiry),
            }
        )
    with gzip.open(path, "wt", encoding="utf-8") as handle:
        json.dump(payload, handle)


class FakeUpstoxSession:
    def __init__(self, candles_by_key: dict[str, pd.DataFrame]) -> None:
        self._candles_by_key = dict(candles_by_key)
        self.calls: list[tuple[str, dt.date, dt.date, str]] = []

    def get_historical_data(
        self,
        instrument_key: str,
        start_date: dt.date,
        end_date: dt.date,
        interval: str,
        *,
        as_dataframe: bool = False,
        tz: dt.tzinfo | None = None,
        **_: object,
    ) -> pd.DataFrame:
        self.calls.append((str(instrument_key), start_date, end_date, str(interval)))
        df = self._candles_by_key.get(str(instrument_key))
        if df is None:
            return pd.DataFrame(columns=["ts", "open", "high", "low", "close", "volume", "oi"])
        return df.copy()


def _underlying_rows(day: dt.date, *, times: list[dt.time], price: float = 20000.0) -> list[dict[str, object]]:
    rows: list[dict[str, object]] = []
    for t in times:
        ts = dt.datetime.combine(day, t, tzinfo=IST)
        rows.append({"ts": ts, "open": price, "high": price, "low": price, "close": price, "volume": 1_000.0})
    return rows


def _option_df(day: dt.date, *, times: list[dt.time], price: float, volumes: list[int], oi: float = 1_000.0) -> pd.DataFrame:
    if len(times) != len(volumes):
        raise ValueError("times and volumes must match")
    rows: list[dict[str, object]] = []
    for t, vol in zip(times, volumes):
        ts = dt.datetime.combine(day, t, tzinfo=IST)
        rows.append({"ts": ts, "open": price, "high": price, "low": price, "close": price, "volume": float(vol), "oi": float(oi)})
    return pd.DataFrame(rows)


def _spec(
    *,
    day: dt.date,
    fill_model: str,
    allow_partial_fills: bool,
    latency_ms: int,
    qty_lots: int,
) -> StrategySpec:
    return StrategySpec(
        name="TestSpec",
        underlying_instrument_key=UNDERLYING_KEY,
        start_date=day,
        end_date=day,
        candle_interval="1minute",
        entry_time="09:15",
        exit_time="15:20",
        fill_model=fill_model,
        allow_partial_fills=allow_partial_fills,
        latency_ms=latency_ms,
        slippage_model="none",
        slippage_bps=0.0,
        slippage_ticks=0,
        spread_bps=0.0,
        brokerage_profile="india_options_default",
        starting_capital=100000.0,
        legs=(
            LegSpec(
                side="BUY",
                opt_type="CE",
                qty_lots=int(qty_lots),
                expiry_mode="WEEKLY_CURRENT",
                strike_mode="ATM",
            ),
        ),
    )


async def _run_multi_leg(
    *,
    tmp_path: Path,
    cfg: EngineConfig,
    spec: StrategySpec,
    session: FakeUpstoxSession,
    instrument_master_path: Path,
    history_cache_path: Path,
    store_path: Path,
    underlying_rows: list[dict[str, object]],
) -> dict[str, object]:
    exec_cfg = ExecutionConfig(fill_model=str(spec.fill_model), latency_ms=int(spec.latency_ms), allow_partial_fills=bool(spec.allow_partial_fills))
    engine = BacktestingEngine(
        config=cfg,
        session=session,
        history_cache_path=history_cache_path,
        instrument_master_path=instrument_master_path,
        store_path=store_path,
        execution_config=exec_cfg,
        slippage_model=str(spec.slippage_model),
        slippage_bps=float(spec.slippage_bps),
        slippage_ticks=int(spec.slippage_ticks),
        spread_bps=float(spec.spread_bps),
        starting_capital=float(spec.starting_capital),
    )
    try:
        engine.strategy_spec = spec
        engine.strategy_spec_json = spec.to_json()
        return await engine.run_backtest_async(
            "MultiLegSpecStrategy",
            spec.start_date,
            spec.end_date,
            data=underlying_rows,
            interval=str(spec.candle_interval),
            underlying_symbol=UNDERLYING_SYMBOL,
        )
    finally:
        engine.close()


@pytest.mark.asyncio
async def test_fill_model_next_tick_changes_entry(tmp_path: Path) -> None:
    cfg = _disable_exit_automation(EngineConfig.load())
    cfg = replace(cfg, data=replace(cfg.data, lot_step=5))

    day = dt.date(2024, 1, 1)  # Monday -> weekly expiry resolves to Tuesday (2024-01-02)
    expiry = "2024-01-02"
    times = [dt.time(9, 15), dt.time(9, 16)]

    instrument_master_path = tmp_path / "nse_master.json.gz"
    history_cache_path = tmp_path / "history.sqlite"
    store_path = tmp_path / "engine_state.sqlite"

    instrument_keys = {
        ("CE", 20000): "NSE_FO|TEST_CE_20000",
        ("CE", 20050): "NSE_FO|TEST_CE_20050",
        ("PE", 20000): "NSE_FO|TEST_PE_20000",
        ("PE", 19950): "NSE_FO|TEST_PE_19950",
    }
    _write_instrument_master(instrument_master_path, underlying_key=UNDERLYING_KEY, expiry=expiry, instrument_keys=instrument_keys)

    option_candles = {
        instrument_keys[("CE", 20000)]: _option_df(day, times=times, price=100.0, volumes=[1_000, 1_000]),
        instrument_keys[("CE", 20050)]: _option_df(day, times=times, price=90.0, volumes=[1_000, 1_000]),
        instrument_keys[("PE", 20000)]: _option_df(day, times=times, price=110.0, volumes=[1_000, 1_000]),
        instrument_keys[("PE", 19950)]: _option_df(day, times=times, price=95.0, volumes=[1_000, 1_000]),
    }

    underlying = _underlying_rows(day, times=times, price=20000.0)
    expected_symbol = f"{UNDERLYING_SYMBOL}-{expiry}-20000CE"

    spec_same = _spec(day=day, fill_model="same_tick", allow_partial_fills=False, latency_ms=0, qty_lots=1)
    res_same = await _run_multi_leg(
        tmp_path=tmp_path,
        cfg=cfg,
        spec=spec_same,
        session=FakeUpstoxSession(option_candles),
        instrument_master_path=instrument_master_path,
        history_cache_path=history_cache_path,
        store_path=store_path,
        underlying_rows=underlying,
    )

    spec_next = _spec(day=day, fill_model="next_tick", allow_partial_fills=False, latency_ms=0, qty_lots=1)
    res_next = await _run_multi_leg(
        tmp_path=tmp_path,
        cfg=cfg,
        spec=spec_next,
        session=FakeUpstoxSession(option_candles),
        instrument_master_path=instrument_master_path,
        history_cache_path=tmp_path / "history_next.sqlite",
        store_path=tmp_path / "engine_state_next.sqlite",
        underlying_rows=underlying,
    )

    same_exec = next(r for r in (res_same.get("executions") or []) if str(r.get("symbol")) == expected_symbol and str(r.get("side")).upper() == "BUY")
    next_exec = next(r for r in (res_next.get("executions") or []) if str(r.get("symbol")) == expected_symbol and str(r.get("side")).upper() == "BUY")

    same_ts = dt.datetime.fromisoformat(str(same_exec["ts"])).astimezone(IST)
    next_ts = dt.datetime.fromisoformat(str(next_exec["ts"])).astimezone(IST)
    assert next_ts > same_ts


@pytest.mark.asyncio
async def test_partial_fills_toggle_changes_fills(tmp_path: Path) -> None:
    cfg = _disable_exit_automation(EngineConfig.load())
    cfg = replace(cfg, data=replace(cfg.data, lot_step=5))
    lot_size = int(cfg.data.lot_step)

    day = dt.date(2024, 1, 1)
    expiry = "2024-01-02"
    times = [dt.time(9, 15), dt.time(9, 16), dt.time(9, 17)]

    instrument_master_path = tmp_path / "nse_master.json.gz"
    instrument_keys = {
        ("CE", 20000): "NSE_FO|TEST_CE_20000",
        ("CE", 20050): "NSE_FO|TEST_CE_20050",
        ("PE", 20000): "NSE_FO|TEST_PE_20000",
        ("PE", 19950): "NSE_FO|TEST_PE_19950",
    }
    _write_instrument_master(instrument_master_path, underlying_key=UNDERLYING_KEY, expiry=expiry, instrument_keys=instrument_keys)

    order_qty = lot_size * 3
    option_candles = {
        instrument_keys[("CE", 20000)]: _option_df(day, times=times, price=100.0, volumes=[lot_size, lot_size, 100]),
        instrument_keys[("CE", 20050)]: _option_df(day, times=times, price=90.0, volumes=[1_000, 1_000, 1_000]),
        instrument_keys[("PE", 20000)]: _option_df(day, times=times, price=110.0, volumes=[1_000, 1_000, 1_000]),
        instrument_keys[("PE", 19950)]: _option_df(day, times=times, price=95.0, volumes=[1_000, 1_000, 1_000]),
    }

    underlying = _underlying_rows(day, times=times, price=20000.0)
    expected_symbol = f"{UNDERLYING_SYMBOL}-{expiry}-20000CE"

    spec_partials = _spec(day=day, fill_model="same_tick", allow_partial_fills=True, latency_ms=0, qty_lots=3)
    res_partials = await _run_multi_leg(
        tmp_path=tmp_path,
        cfg=cfg,
        spec=spec_partials,
        session=FakeUpstoxSession(option_candles),
        instrument_master_path=instrument_master_path,
        history_cache_path=tmp_path / "history_partials.sqlite",
        store_path=tmp_path / "engine_state_partials.sqlite",
        underlying_rows=underlying,
    )
    buys_partials = [r for r in (res_partials.get("executions") or []) if str(r.get("symbol")) == expected_symbol and str(r.get("side")).upper() == "BUY"]
    assert [int(r.get("qty") or 0) for r in buys_partials] == [lot_size, lot_size, lot_size]

    spec_full = _spec(day=day, fill_model="same_tick", allow_partial_fills=False, latency_ms=0, qty_lots=3)
    res_full = await _run_multi_leg(
        tmp_path=tmp_path,
        cfg=cfg,
        spec=spec_full,
        session=FakeUpstoxSession(option_candles),
        instrument_master_path=instrument_master_path,
        history_cache_path=tmp_path / "history_full.sqlite",
        store_path=tmp_path / "engine_state_full.sqlite",
        underlying_rows=underlying,
    )
    buys_full = [r for r in (res_full.get("executions") or []) if str(r.get("symbol")) == expected_symbol and str(r.get("side")).upper() == "BUY"]
    assert [int(r.get("qty") or 0) for r in buys_full] == [order_qty]


@pytest.mark.asyncio
async def test_fees_reduce_net_pnl(tmp_path: Path) -> None:
    cfg = _disable_exit_automation(EngineConfig.load())
    cfg = replace(cfg, data=replace(cfg.data, lot_step=5))
    lot_size = int(cfg.data.lot_step)

    day = dt.date(2024, 1, 1)
    expiry = "2024-01-02"
    times = [dt.time(9, 15), dt.time(9, 16)]

    instrument_master_path = tmp_path / "nse_master.json.gz"
    instrument_keys = {
        ("CE", 20000): "NSE_FO|TEST_CE_20000",
        ("CE", 20050): "NSE_FO|TEST_CE_20050",
        ("PE", 20000): "NSE_FO|TEST_PE_20000",
        ("PE", 19950): "NSE_FO|TEST_PE_19950",
    }
    _write_instrument_master(instrument_master_path, underlying_key=UNDERLYING_KEY, expiry=expiry, instrument_keys=instrument_keys)

    qty = lot_size
    option_candles = {
        instrument_keys[("CE", 20000)]: _option_df(day, times=times, price=100.0, volumes=[1_000, 1_000]),
        instrument_keys[("CE", 20050)]: _option_df(day, times=times, price=90.0, volumes=[1_000, 1_000]),
        instrument_keys[("PE", 20000)]: _option_df(day, times=times, price=110.0, volumes=[1_000, 1_000]),
        instrument_keys[("PE", 19950)]: _option_df(day, times=times, price=95.0, volumes=[1_000, 1_000]),
    }

    underlying = _underlying_rows(day, times=times, price=20000.0)

    spec_obj = _spec(day=day, fill_model="same_tick", allow_partial_fills=False, latency_ms=0, qty_lots=1)
    res = await _run_multi_leg(
        tmp_path=tmp_path,
        cfg=cfg,
        spec=spec_obj,
        session=FakeUpstoxSession(option_candles),
        instrument_master_path=instrument_master_path,
        history_cache_path=tmp_path / "history_fees.sqlite",
        store_path=tmp_path / "engine_state_fees.sqlite",
        underlying_rows=underlying,
    )

    assert int(res.get("trades") or 0) >= 1
    assert float(res.get("total_fees") or 0.0) > 0.0
    assert float(res.get("net_pnl") or 0.0) < float(res.get("gross_pnl") or 0.0)


@pytest.mark.asyncio
async def test_option_data_not_synthetic(tmp_path: Path) -> None:
    cfg = _disable_exit_automation(EngineConfig.load())
    cfg = replace(cfg, data=replace(cfg.data, lot_step=5))

    day = dt.date(2024, 1, 1)
    expiry = "2024-01-02"
    times = [dt.time(9, 15), dt.time(9, 16)]

    instrument_master_path = tmp_path / "nse_master.json.gz"
    history_cache_path = tmp_path / "history.sqlite"
    store_path = tmp_path / "engine_state.sqlite"

    instrument_keys = {
        ("CE", 20000): "NSE_FO|TEST_CE_20000",
        ("CE", 20050): "NSE_FO|TEST_CE_20050",
        ("PE", 20000): "NSE_FO|TEST_PE_20000",
        ("PE", 19950): "NSE_FO|TEST_PE_19950",
    }
    _write_instrument_master(instrument_master_path, underlying_key=UNDERLYING_KEY, expiry=expiry, instrument_keys=instrument_keys)

    volumes = [123, 456]
    option_candles = {
        instrument_keys[("CE", 20000)]: _option_df(day, times=times, price=100.0, volumes=volumes),
        instrument_keys[("CE", 20050)]: _option_df(day, times=times, price=90.0, volumes=volumes),
        instrument_keys[("PE", 20000)]: _option_df(day, times=times, price=110.0, volumes=volumes),
        instrument_keys[("PE", 19950)]: _option_df(day, times=times, price=95.0, volumes=volumes),
    }
    session = FakeUpstoxSession(option_candles)
    underlying = _underlying_rows(day, times=times, price=20000.0)

    spec_obj = _spec(day=day, fill_model="same_tick", allow_partial_fills=False, latency_ms=0, qty_lots=1)
    await _run_multi_leg(
        tmp_path=tmp_path,
        cfg=cfg,
        spec=spec_obj,
        session=session,
        instrument_master_path=instrument_master_path,
        history_cache_path=history_cache_path,
        store_path=store_path,
        underlying_rows=underlying,
    )

    opt_key = instrument_keys[("CE", 20000)]
    assert any(call[0] == opt_key for call in session.calls)

    with sqlite3.connect(history_cache_path) as conn:
        rows = conn.execute(
            "SELECT volume, source FROM candles WHERE instrument_key=? AND interval=? ORDER BY ts ASC",
            (opt_key, "1minute"),
        ).fetchall()
    assert rows
    stored_vols = [float(r[0]) for r in rows]
    assert stored_vols == [float(v) for v in volumes]
    assert all(str(r[1]) == "upstox" for r in rows)
