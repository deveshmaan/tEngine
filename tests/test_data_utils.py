import datetime as dt

from engine.config import IST
from engine.data import pick_strike_from_spot, resolve_weekly_expiry


def test_resolve_weekly_expiry_skips_holidays():
    monday = dt.datetime(2024, 7, 1, 9, 30, tzinfo=IST)
    holidays = {dt.date(2024, 7, 4)}
    expiry = resolve_weekly_expiry("NIFTY", monday, holidays)
    assert expiry == dt.date(2024, 7, 3)


def test_pick_strike_from_spot_and_bands():
    strike = pick_strike_from_spot(spot=22654, lot_step=50, tick_size=0.05, mode="ATM", price_band=(1.0, 50000.0))
    assert strike == 22650.0
    otm = pick_strike_from_spot(spot=22654, lot_step=50, tick_size=0.05, mode="OTM")
    assert otm == 22700.0
