from __future__ import annotations

import datetime as dt
import gzip
import json
from dataclasses import replace
from pathlib import Path

import pandas as pd
import pytest

from engine.backtest.strategy_spec import (
    BacktestConfig,
    BacktestRunSpec,
    ExecutionModelSpec,
    ExpirySelectorSpec,
    LegRiskRuleSpec,
    OptionLegSpec,
    RiskRuleSpec,
    StrikeSelectorSpec,
)
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


def _option_rows(day: dt.date, *, times: list[dt.time], ohlc: tuple[float, float, float, float], volume: float = 1_000.0) -> pd.DataFrame:
    o, h, l, c = ohlc
    rows: list[dict[str, object]] = []
    for t in times:
        ts = dt.datetime.combine(day, t, tzinfo=IST)
        rows.append({"ts": ts, "open": float(o), "high": float(h), "low": float(l), "close": float(c), "volume": float(volume), "oi": 1_000.0})
    return pd.DataFrame(rows)


@pytest.mark.asyncio
async def test_options_runner_short_straddle_entries_exits_and_grouping(tmp_path: Path) -> None:
    cfg = _disable_exit_automation(EngineConfig.load())
    cfg = replace(cfg, data=replace(cfg.data, lot_step=5))

    day = dt.date(2024, 1, 1)
    expiry_date = dt.date(2024, 1, 2)
    expiry = expiry_date.isoformat()

    instrument_master_path = tmp_path / "instrument_master.json.gz"
    history_cache_path = tmp_path / "history.sqlite"
    store_path = tmp_path / "engine_state.sqlite"

    instrument_keys = {
        ("CE", 20000): "NSE_FO|TEST_CE_20000",
        ("PE", 20000): "NSE_FO|TEST_PE_20000",
    }
    _write_instrument_master(instrument_master_path, underlying_key=UNDERLYING_KEY, expiry=expiry, instrument_keys=instrument_keys)

    option_candles = {
        instrument_keys[("CE", 20000)]: _option_rows(day, times=[dt.time(9, 15), dt.time(9, 17)], ohlc=(100.0, 100.0, 100.0, 100.0)),
        instrument_keys[("PE", 20000)]: _option_rows(day, times=[dt.time(9, 15), dt.time(9, 17)], ohlc=(110.0, 110.0, 110.0, 110.0)),
    }

    run_spec = BacktestRunSpec(
        name="ShortStraddle",
        config=BacktestConfig(
            underlying_instrument_key=UNDERLYING_KEY,
            start_date=day,
            end_date=day,
            interval="1minute",
            entry_time="09:15",
            exit_time="09:17",
            timezone="Asia/Kolkata",
            starting_capital=100000.0,
            brokerage_profile="india_options_default",
        ),
        legs=(
            OptionLegSpec(
                leg_id="leg_ce",
                side="SELL",
                option_type="CE",
                qty=1,
                expiry_selector=ExpirySelectorSpec(mode="WEEKLY_CURRENT", expiry_date=expiry_date),
                strike_selector=StrikeSelectorSpec(mode="ATM"),
            ),
            OptionLegSpec(
                leg_id="leg_pe",
                side="SELL",
                option_type="PE",
                qty=1,
                expiry_selector=ExpirySelectorSpec(mode="WEEKLY_CURRENT", expiry_date=expiry_date),
                strike_selector=StrikeSelectorSpec(mode="ATM"),
            ),
        ),
        execution_model=ExecutionModelSpec(fill_model="same_tick", latency_ms=0, allow_partial_fills=False, spread_bps=0.0),
    )

    engine = BacktestingEngine(
        config=cfg,
        session=FakeUpstoxSession(option_candles),
        history_cache_path=history_cache_path,
        instrument_master_path=instrument_master_path,
        store_path=store_path,
        execution_config=run_spec.to_execution_config(),
        starting_capital=float(run_spec.config.starting_capital),
    )
    try:
        engine.strategy_spec = run_spec
        engine.strategy_spec_json = run_spec.to_json()
        res = await engine.run_backtest_async(
            "OptionsBacktestRunner",
            day,
            day,
            data=_underlying_rows(day, times=[dt.time(9, 15), dt.time(9, 17)], price=20000.0),
            interval=str(run_spec.config.interval),
            underlying_symbol=UNDERLYING_SYMBOL,
        )
    finally:
        engine.close()

    orders = res.get("orders") or []
    assert sum(1 for o in orders if str(o.get("side")).upper() == "SELL") == 2
    assert sum(1 for o in orders if str(o.get("side")).upper() == "BUY") == 2

    trades = res.get("trade_log") or []
    assert len(trades) == 2
    assert {t.get("leg_id") for t in trades} == {"leg_ce", "leg_pe"}
    assert all(str(t.get("strategy_trade_id") or "").startswith(day.isoformat()) for t in trades)

    strat_trades = res.get("strategy_trade_log") or []
    assert len(strat_trades) == 1
    assert strat_trades[0]["strategy_trade_id"].startswith(day.isoformat())
    assert int(strat_trades[0]["legs"]) == 2
    assert set(strat_trades[0]["leg_ids"]) == {"leg_ce", "leg_pe"}


@pytest.mark.asyncio
async def test_options_runner_reentry_cap_respected(tmp_path: Path) -> None:
    cfg = _disable_exit_automation(EngineConfig.load())
    cfg = replace(cfg, data=replace(cfg.data, lot_step=5))

    day = dt.date(2024, 1, 1)
    expiry_date = dt.date(2024, 1, 2)
    expiry = expiry_date.isoformat()

    instrument_master_path = tmp_path / "instrument_master.json.gz"
    history_cache_path = tmp_path / "history.sqlite"
    store_path = tmp_path / "engine_state.sqlite"

    instrument_keys = {
        ("CE", 20000): "NSE_FO|TEST_CE_20000",
    }
    _write_instrument_master(instrument_master_path, underlying_key=UNDERLYING_KEY, expiry=expiry, instrument_keys=instrument_keys)

    # Two stoploss events: option premium spikes to 130 within the minute (via high tick).
    option_candles = {
        instrument_keys[("CE", 20000)]: pd.concat(
            [
                _option_rows(day, times=[dt.time(9, 15)], ohlc=(100.0, 130.0, 100.0, 100.0)),
                _option_rows(day, times=[dt.time(9, 16)], ohlc=(100.0, 130.0, 100.0, 100.0)),
                _option_rows(day, times=[dt.time(9, 17)], ohlc=(100.0, 100.0, 100.0, 100.0)),
            ],
            ignore_index=True,
        )
    }

    run_spec = BacktestRunSpec(
        name="ReentryCap",
        config=BacktestConfig(
            underlying_instrument_key=UNDERLYING_KEY,
            start_date=day,
            end_date=day,
            interval="1minute",
            entry_time="09:15",
            exit_time="09:17",
            timezone="Asia/Kolkata",
            starting_capital=100000.0,
            brokerage_profile="india_options_default",
        ),
        legs=(
            OptionLegSpec(
                leg_id="leg_1",
                side="SELL",
                option_type="CE",
                qty=1,
                expiry_selector=ExpirySelectorSpec(mode="WEEKLY_CURRENT", expiry_date=expiry_date),
                strike_selector=StrikeSelectorSpec(mode="ATM"),
            ),
        ),
        risk=RiskRuleSpec(
            per_leg=(
                LegRiskRuleSpec(
                    leg_id="leg_1",
                    stoploss_type="PREMIUM_PCT",
                    stoploss_value=0.2,
                    reentry_enabled=True,
                    max_reentries=1,
                    cool_down_minutes=0,
                    reentry_condition="after_stoploss",
                ),
            )
        ),
        execution_model=ExecutionModelSpec(fill_model="same_tick", latency_ms=0, allow_partial_fills=False, spread_bps=0.0),
    )

    engine = BacktestingEngine(
        config=cfg,
        session=FakeUpstoxSession(option_candles),
        history_cache_path=history_cache_path,
        instrument_master_path=instrument_master_path,
        store_path=store_path,
        execution_config=run_spec.to_execution_config(),
        starting_capital=float(run_spec.config.starting_capital),
    )
    try:
        engine.strategy_spec = run_spec
        engine.strategy_spec_json = run_spec.to_json()
        res = await engine.run_backtest_async(
            "OptionsBacktestRunner",
            day,
            day,
            data=_underlying_rows(day, times=[dt.time(9, 15), dt.time(9, 16), dt.time(9, 17)], price=20000.0),
            interval=str(run_spec.config.interval),
            underlying_symbol=UNDERLYING_SYMBOL,
        )
    finally:
        engine.close()

    orders = res.get("orders") or []
    entry_orders = [o for o in orders if str(o.get("side")).upper() == "SELL"]
    assert len(entry_orders) == 2  # initial + 1 re-entry

    # Ensure we didn't submit more than one re-entry.
    reentry_orders = [o for o in entry_orders if "REENTRY" in str(o.get("strategy") or "")]
    assert len(reentry_orders) == 1

    trades = res.get("trade_log") or []
    strategy_trade_ids = {str(t.get("strategy_trade_id") or "") for t in trades if str(t.get("strategy_trade_id") or "")}
    assert len(strategy_trade_ids) == 2

