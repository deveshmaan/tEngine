from __future__ import annotations

import datetime as dt

import pytest

from engine.backtest.strike_resolver import resolve_atm_strike, resolve_strike_offset, resolve_target_premium


def test_resolve_atm_strike_rounds_to_grid():
    assert resolve_atm_strike(spot_price=22654, strike_step=50) == 22650


def test_resolve_strike_offset_direction():
    assert resolve_strike_offset(atm=20000, opt_type="CE", offset_points=200) == 20200
    assert resolve_strike_offset(atm=20000, opt_type="PE", offset_points=200) == 19800
    assert resolve_strike_offset(atm=20000, opt_type="CE", offset_points=-200) == 19800
    assert resolve_strike_offset(atm=20000, opt_type="PE", offset_points=-200) == 20200


def test_resolve_target_premium_picks_closest_and_calls_once_per_strike():
    entry_ts = dt.datetime(2024, 1, 2, 9, 30)
    expiry = dt.date(2024, 1, 4)
    strike_grid = [19800, 19900, 20000, 20100, 20200, 20000]  # includes dup
    premiums = {19800: 150.0, 19900: 120.0, 20000: 100.0, 20100: 80.0, 20200: 60.0}
    calls: dict[int, int] = {}

    def price_fn(ts: dt.datetime, exp: dt.date, opt_type: str, strike: int):
        assert ts == entry_ts
        assert exp == expiry
        assert opt_type == "CE"
        calls[strike] = calls.get(strike, 0) + 1
        return premiums.get(strike)

    strike = resolve_target_premium(
        entry_ts=entry_ts,
        expiry_date=expiry,
        opt_type="CE",
        target_premium=100.0,
        strike_grid=strike_grid,
        get_option_price_fn=price_fn,
    )
    assert strike == 20000
    assert calls == {19800: 1, 19900: 1, 20000: 1, 20100: 1, 20200: 1}


def test_resolve_target_premium_skips_missing_prices():
    entry_ts = dt.datetime(2024, 1, 2, 9, 30)
    expiry = dt.date(2024, 1, 4)
    strike_grid = [20000, 20100]

    def price_fn(_ts: dt.datetime, _exp: dt.date, _opt_type: str, strike: int):
        return None if strike == 20100 else 95.0

    assert (
        resolve_target_premium(
            entry_ts=entry_ts,
            expiry_date=expiry,
            opt_type="CE",
            target_premium=100.0,
            strike_grid=strike_grid,
            get_option_price_fn=price_fn,
        )
        == 20000
    )


def test_resolve_target_premium_raises_when_all_missing():
    entry_ts = dt.datetime(2024, 1, 2, 9, 30)
    expiry = dt.date(2024, 1, 4)

    def price_fn(*_args, **_kwargs):
        return None

    with pytest.raises(ValueError, match="No valid premiums"):
        resolve_target_premium(
            entry_ts=entry_ts,
            expiry_date=expiry,
            opt_type="CE",
            target_premium=100.0,
            strike_grid=[20000, 20100],
            get_option_price_fn=price_fn,
        )

