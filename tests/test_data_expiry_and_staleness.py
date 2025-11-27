import datetime as dt

from engine.data import is_market_data_stale, record_tick_seen, resolve_weekly_expiry


def test_weekly_expiry_uses_config_weekday():
    # Tuesday (1) should roll to same week if before expiry
    now = dt.datetime(2024, 1, 1, 9, 30)  # Monday
    expiry = resolve_weekly_expiry("NIFTY", now, holidays=[], weekly_weekday=1)
    assert expiry.weekday() == 1


def test_staleness_detection():
    record_tick_seen(instrument_key="ABC", underlying="NIFTY", ts_seconds=10.0)
    assert is_market_data_stale("ABC", threshold=0.0) is False
    assert is_market_data_stale("ABC", threshold=1.0, now=12.1) is True
