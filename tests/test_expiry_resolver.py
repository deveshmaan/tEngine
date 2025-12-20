from __future__ import annotations

import datetime as dt

import pytest

from engine.backtest import expiry_resolver


def test_weekly_expiry_back_adjusts_holiday():
    trade_date = dt.date(2024, 7, 1)  # Monday
    holidays = {dt.date(2024, 7, 4)}  # Thursday holiday
    expiry = expiry_resolver.resolve_expiry(
        trade_date,
        "WEEKLY_CURRENT",
        weekly_expiry_weekday=3,  # Thursday
        holiday_calendar=holidays,
    )
    assert expiry == dt.date(2024, 7, 3)  # Wednesday


def test_monthly_expiry_rolls_forward_after_month_end():
    trade_date = dt.date(2024, 7, 26)  # after last Thursday of July 2024
    expiry = expiry_resolver.resolve_expiry(
        trade_date,
        "MONTHLY",
        weekly_expiry_weekday=3,  # Thursday (monthly assumed last Thursday)
        holiday_calendar=set(),
    )
    assert expiry == dt.date(2024, 8, 29)


def test_defaults_weekday_from_config(monkeypatch: pytest.MonkeyPatch, tmp_path):
    cfg_path = tmp_path / "app.yml"
    cfg_path.write_text(
        "\n".join(
            [
                "data:",
                "  weekly_expiry_weekday: 3",
                "  holidays:",
                '    - "2024-07-04"',
            ]
        )
        + "\n",
        encoding="utf-8",
    )
    monkeypatch.setenv("APP_CONFIG_PATH", str(cfg_path))

    import engine.data as data_mod

    data_mod._reset_app_config_cache()
    expiry_resolver._config_defaults.cache_clear()

    expiry = expiry_resolver.resolve_expiry(dt.date(2024, 7, 1), "WEEKLY_CURRENT", weekly_expiry_weekday=None, holiday_calendar=None)
    assert expiry == dt.date(2024, 7, 3)


def test_explain_expiry_includes_mode_and_expiry_date():
    text = expiry_resolver.explain_expiry(dt.date(2024, 7, 1), "WEEKLY_CURRENT", weekly_expiry_weekday=3, holiday_calendar={"2024-07-04"})
    assert "WEEKLY_CURRENT" in text
    assert "expiry=2024-07-03" in text

