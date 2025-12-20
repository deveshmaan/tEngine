from __future__ import annotations

import datetime as dt
from functools import lru_cache
from typing import Iterable, Optional, Set, Tuple

EXPIRY_MODE_WEEKLY_CURRENT = "WEEKLY_CURRENT"
EXPIRY_MODE_WEEKLY_NEXT = "WEEKLY_NEXT"
EXPIRY_MODE_MONTHLY = "MONTHLY"

ALLOWED_EXPIRY_MODES = {
    EXPIRY_MODE_WEEKLY_CURRENT,
    EXPIRY_MODE_WEEKLY_NEXT,
    EXPIRY_MODE_MONTHLY,
}


def _coerce_date(value: object, field_name: str) -> dt.date:
    if isinstance(value, dt.datetime):
        return value.date()
    if isinstance(value, dt.date):
        return value
    text = str(value).strip()
    try:
        return dt.date.fromisoformat(text)
    except ValueError as exc:
        raise ValueError(f"{field_name} must be a date (YYYY-MM-DD); got {value!r}") from exc


def _normalize_weekday(value: object) -> int:
    try:
        weekday = int(value)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"weekly_expiry_weekday must be int 0-6; got {value!r}") from exc
    if weekday < 0 or weekday > 6:
        raise ValueError(f"weekly_expiry_weekday must be in 0..6; got {value!r}")
    return weekday


def _normalize_holidays(calendar: Optional[Iterable[object]]) -> Set[dt.date]:
    out: Set[dt.date] = set()
    if not calendar:
        return out
    for raw in calendar:
        if raw is None:
            continue
        try:
            out.add(_coerce_date(raw, "holiday_calendar"))
        except Exception:
            continue
    return out


def _is_trading_day(day: dt.date, holidays: Set[dt.date]) -> bool:
    return day.weekday() < 5 and day not in holidays


def _back_adjust(day: dt.date, holidays: Set[dt.date]) -> Tuple[dt.date, int]:
    """
    Back-adjust expiry to the previous trading day if it falls on a weekend/holiday.

    Returns (adjusted_day, steps_back).
    """

    cursor = day
    steps = 0
    while not _is_trading_day(cursor, holidays):
        cursor -= dt.timedelta(days=1)
        steps += 1
        if steps > 14:
            raise ValueError("holiday_calendar too restrictive; could not find trading day within 14 days")
    return cursor, steps


def _next_weekday_on_or_after(day: dt.date, target_weekday: int) -> dt.date:
    delta = (target_weekday - day.weekday()) % 7
    return day + dt.timedelta(days=delta)


def _last_weekday_of_month(year: int, month: int, target_weekday: int) -> dt.date:
    if month < 1 or month > 12:
        raise ValueError(f"month must be in 1..12; got {month}")
    if month == 12:
        first_next = dt.date(year + 1, 1, 1)
    else:
        first_next = dt.date(year, month + 1, 1)
    last_day = first_next - dt.timedelta(days=1)
    delta = (last_day.weekday() - target_weekday) % 7
    return last_day - dt.timedelta(days=delta)


@lru_cache(maxsize=8)
def _config_defaults() -> tuple[int, Set[dt.date]]:
    weekday_default = 1
    holidays: Set[dt.date] = set()
    try:
        from engine.data import get_app_config

        cfg = get_app_config()
        data_cfg = (cfg.get("data") or {}) if isinstance(cfg, dict) else {}
        if "weekly_expiry_weekday" in data_cfg:
            try:
                weekday_default = _normalize_weekday(data_cfg.get("weekly_expiry_weekday"))
            except Exception:
                weekday_default = 1
        holidays = _normalize_holidays(data_cfg.get("holidays", []))
    except Exception:
        weekday_default = 1
        holidays = set()
    return weekday_default, holidays


def resolve_expiry(
    trade_date: dt.date | dt.datetime | str,
    expiry_mode: str,
    weekly_expiry_weekday: Optional[int] = None,
    holiday_calendar: Optional[Iterable[object]] = None,
) -> dt.date:
    """
    Resolve an expiry date (weekly/monthly) for a trade date.

    Holiday handling follows engine conventions: if the computed expiry falls on a
    holiday/weekend, it is back-adjusted to the previous trading day.
    """

    td = _coerce_date(trade_date, "trade_date")
    mode = str(expiry_mode or "").strip().upper()
    if mode not in ALLOWED_EXPIRY_MODES:
        raise ValueError(f"expiry_mode must be one of {sorted(ALLOWED_EXPIRY_MODES)}; got {expiry_mode!r}")

    cfg_weekday, cfg_holidays = _config_defaults()
    weekday = cfg_weekday if weekly_expiry_weekday is None else _normalize_weekday(weekly_expiry_weekday)
    holidays = cfg_holidays if holiday_calendar is None else _normalize_holidays(holiday_calendar)

    if mode in {EXPIRY_MODE_WEEKLY_CURRENT, EXPIRY_MODE_WEEKLY_NEXT}:
        base = _next_weekday_on_or_after(td, weekday)
        if mode == EXPIRY_MODE_WEEKLY_NEXT:
            base = base + dt.timedelta(days=7)

        while True:
            adjusted, _steps = _back_adjust(base, holidays)
            if adjusted >= td:
                return adjusted
            base = base + dt.timedelta(days=7)

    # Monthly: use last `weekday` of the month, else roll to next month.
    year, month = td.year, td.month
    while True:
        base = _last_weekday_of_month(year, month, weekday)
        adjusted, _steps = _back_adjust(base, holidays)
        if adjusted >= td:
            return adjusted
        if month == 12:
            year += 1
            month = 1
        else:
            month += 1


def explain_expiry(
    trade_date: dt.date | dt.datetime | str,
    expiry_mode: str,
    weekly_expiry_weekday: Optional[int] = None,
    holiday_calendar: Optional[Iterable[object]] = None,
) -> str:
    """
    Human-readable explanation for UI/debug logs.
    """

    td = _coerce_date(trade_date, "trade_date")
    mode = str(expiry_mode or "").strip().upper()
    cfg_weekday, cfg_holidays = _config_defaults()
    weekday_source = "config" if weekly_expiry_weekday is None else "param"
    weekday = cfg_weekday if weekly_expiry_weekday is None else _normalize_weekday(weekly_expiry_weekday)
    holidays = cfg_holidays if holiday_calendar is None else _normalize_holidays(holiday_calendar)
    holiday_source = "config" if holiday_calendar is None else "param"

    if mode not in ALLOWED_EXPIRY_MODES:
        return f"expiry_mode={expiry_mode!r} invalid (allowed={sorted(ALLOWED_EXPIRY_MODES)})"

    base_note = ""
    if mode in {EXPIRY_MODE_WEEKLY_CURRENT, EXPIRY_MODE_WEEKLY_NEXT}:
        base = _next_weekday_on_or_after(td, weekday)
        if mode == EXPIRY_MODE_WEEKLY_NEXT:
            base = base + dt.timedelta(days=7)
        adjusted, steps = _back_adjust(base, holidays)
        rolls = 0
        while adjusted < td:
            rolls += 1
            base = base + dt.timedelta(days=7)
            adjusted, steps = _back_adjust(base, holidays)
        if steps:
            base_note = f" back_adjusted -{steps}d"
        if rolls:
            base_note += f" rolled +{7*rolls}d"
        return (
            f"{mode} trade_date={td.isoformat()} target_weekday={weekday}({weekday_source}) "
            f"base={base.isoformat()} expiry={adjusted.isoformat()}{base_note} holidays={len(holidays)}({holiday_source})"
        )

    year, month = td.year, td.month
    base = _last_weekday_of_month(year, month, weekday)
    adjusted, steps = _back_adjust(base, holidays)
    month_rolls = 0
    while adjusted < td:
        month_rolls += 1
        if month == 12:
            year += 1
            month = 1
        else:
            month += 1
        base = _last_weekday_of_month(year, month, weekday)
        adjusted, steps = _back_adjust(base, holidays)
    if steps:
        base_note = f" back_adjusted -{steps}d"
    if month_rolls:
        base_note += f" rolled +{month_rolls}mo"
    return (
        f"{mode} trade_date={td.isoformat()} target_weekday={weekday}({weekday_source}) "
        f"base={base.isoformat()} expiry={adjusted.isoformat()}{base_note} holidays={len(holidays)}({holiday_source})"
    )


__all__ = [
    "ALLOWED_EXPIRY_MODES",
    "EXPIRY_MODE_MONTHLY",
    "EXPIRY_MODE_WEEKLY_CURRENT",
    "EXPIRY_MODE_WEEKLY_NEXT",
    "explain_expiry",
    "resolve_expiry",
]

