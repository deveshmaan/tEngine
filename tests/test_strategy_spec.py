import datetime as dt

import pytest

from engine.backtest import templates
from engine.backtest.strategy_spec import LegSpec, ProfitLockSpec, StrategySpec


def test_strategy_spec_json_roundtrip():
    spec = templates.ShortStraddle(
        underlying_instrument_key="NSE_INDEX|Nifty 50",
        start_date=dt.date(2024, 1, 2),
        end_date=dt.date(2024, 1, 5),
        candle_interval="5minute",
        qty_lots=2,
    )
    raw = spec.to_json()
    restored = StrategySpec.from_json(raw)
    assert restored == spec


def test_leg_spec_requires_offset_for_atm_offset_mode():
    with pytest.raises(ValueError, match="strike_offset_points is required"):
        LegSpec(
            side="SELL",
            opt_type="CE",
            qty_lots=1,
            expiry_mode="WEEKLY_CURRENT",
            strike_mode="ATM_OFFSET",
            strike_offset_points=None,
        )


def test_profit_lock_normalizes_percent_values():
    lock = ProfitLockSpec(trigger_pct=25, lock_to_pct=10)
    assert lock.trigger_pct == pytest.approx(0.25)
    assert lock.lock_to_pct == pytest.approx(0.10)


def test_strategy_spec_validates_slippage_fields():
    with pytest.raises(ValueError, match="slippage_bps must be > 0"):
        StrategySpec(
            name="BadSlippage",
            underlying_instrument_key="NSE_INDEX|Nifty 50",
            start_date=dt.date(2024, 1, 2),
            end_date=dt.date(2024, 1, 2),
            candle_interval="5minute",
            entry_time="09:30",
            exit_time="15:20",
            fill_model="next_tick",
            allow_partial_fills=False,
            latency_ms=0,
            slippage_model="bps",
            slippage_bps=0.0,
            slippage_ticks=0,
            spread_bps=0.0,
            brokerage_profile="india_options_default",
            starting_capital=100000.0,
            legs=(),
        )

