import datetime as dt

import pytest

from engine.backtest import templates
from engine.backtest.strategy_spec import BacktestRunSpec, LegSpec, ProfitLockSpec, StrategySpec


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


def test_strategy_spec_risk_fields_json_roundtrip():
    spec = StrategySpec(
        name="Risky",
        underlying_instrument_key="NSE_INDEX|Nifty 50",
        start_date=dt.date(2024, 1, 2),
        end_date=dt.date(2024, 1, 2),
        candle_interval="5minute",
        entry_time="09:30",
        exit_time="15:20",
        fill_model="next_tick",
        allow_partial_fills=False,
        latency_ms=0,
        slippage_model="none",
        slippage_bps=0.0,
        slippage_ticks=0,
        spread_bps=0.0,
        brokerage_profile="india_options_default",
        starting_capital=100000.0,
        strategy_risk={
            "max_daily_loss_mtm": 1000.0,
            "max_daily_profit_mtm": 2000.0,
            "force_exit_time": "15:15",
            "disable_entries_after_time": "14:30",
            "max_concurrent_positions": 2,
            "max_trades_per_day": 5,
        },
        legs=(
            LegSpec(
                side="SELL",
                opt_type="CE",
                qty_lots=1,
                expiry_mode="WEEKLY_CURRENT",
                strike_mode="ATM",
                stoploss_type="PREMIUM_PCT",
                stoploss_value=0.25,
                profit_target_type="POINTS",
                profit_target_value=10.0,
                trailing_enabled=True,
                trailing_type="POINTS",
                trailing_trigger=5.0,
                trailing_step=2.0,
                reentry_enabled=True,
                max_reentries=2,
                cool_down_minutes=15,
                reentry_condition="after_stoploss",
            ),
        ),
    )
    restored = StrategySpec.from_json(spec.to_json())
    assert restored == spec


def test_backtest_run_spec_json_roundtrip_and_converts_to_strategy_spec():
    spec = templates.ShortStraddle(
        underlying_instrument_key="NSE_INDEX|Nifty 50",
        start_date=dt.date(2024, 1, 2),
        end_date=dt.date(2024, 1, 2),
        candle_interval="5minute",
        qty_lots=1,
    )
    run_spec = BacktestRunSpec.from_strategy_spec(spec, leg_ids=["leg_a", "leg_b"], timezone="Asia/Kolkata", tags=["smoke"])
    restored = BacktestRunSpec.from_json(run_spec.to_json())
    assert restored == run_spec
    assert restored.to_strategy_spec() == spec
