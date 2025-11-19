from __future__ import annotations

import datetime as dt
import os
from pathlib import Path
from typing import Iterable, Optional, Sequence, Set

import yaml

from engine.config import IST
from engine.time_machine import now as engine_now
from market.instrument_cache import InstrumentCache

_APP_CONFIG_CACHE: Optional[dict] = None
_APP_CONFIG_PATH: Optional[Path] = None


def _load_app_config() -> dict:
    global _APP_CONFIG_CACHE, _APP_CONFIG_PATH
    path = Path(os.getenv("APP_CONFIG_PATH", "config/app.yml"))
    if _APP_CONFIG_CACHE is not None and _APP_CONFIG_PATH == path:
        return _APP_CONFIG_CACHE
    if not path.exists():
        _APP_CONFIG_CACHE = {}
        _APP_CONFIG_PATH = path
        return _APP_CONFIG_CACHE
    with path.open("r", encoding="utf-8") as handle:
        data = yaml.safe_load(handle) or {}
    _APP_CONFIG_PATH = path
    _APP_CONFIG_CACHE = data
    return data


def _reset_app_config_cache() -> None:  # pragma: no cover - used in tests
    global _APP_CONFIG_CACHE, _APP_CONFIG_PATH
    _APP_CONFIG_CACHE = None
    _APP_CONFIG_PATH = None


def get_app_config() -> dict:
    """Expose the parsed application configuration for consumers that need overrides."""

    return dict(_load_app_config())


def _holiday_calendar() -> Set[dt.date]:
    data_cfg = (_load_app_config().get("data") or {})
    holidays: Set[dt.date] = set()
    for raw in data_cfg.get("holidays", []):
        try:
            holidays.add(dt.date.fromisoformat(str(raw)))
        except ValueError:
            continue
    return holidays


def _legacy_weekly(symbol: str, now: dt.datetime, holidays: Optional[Iterable[dt.date]] = None) -> dt.date:
    weekday = now.weekday()  # Monday=0
    if weekday <= 3:
        expiry = now.date() + dt.timedelta(days=3 - weekday)
    else:
        expiry = now.date() + dt.timedelta(days=(7 - weekday) + 3)
    holiday_set: Set[dt.date] = set(holidays or [])
    while expiry.weekday() >= 5 or expiry in holiday_set:
        expiry -= dt.timedelta(days=1)
    return expiry


def _active_cache() -> Optional[InstrumentCache]:
    return InstrumentCache.runtime_cache()


def _filter_future(expiries: Sequence[str], today: dt.date) -> list[str]:
    filtered: list[str] = []
    for raw in expiries:
        try:
            exp_dt = dt.date.fromisoformat(raw)
        except ValueError:
            continue
        if exp_dt >= today:
            filtered.append(raw)
    return filtered


def _back_adjust(expiry: dt.date, holidays: Set[dt.date]) -> dt.date:
    while expiry in holidays:
        expiry -= dt.timedelta(days=1)
    return expiry


def _holiday_adjusted_choice(symbol: str, candidates: Sequence[str], holidays: Set[dt.date], cache: InstrumentCache) -> str:
    if not holidays:
        return candidates[0]
    for raw in candidates:
        try:
            exp_dt = dt.date.fromisoformat(raw)
        except ValueError:
            continue
        if exp_dt not in holidays:
            return raw
    # fall back to the previous available trading day if every candidate is a holiday
    chosen = candidates[0]
    all_expiries = cache.list_expiries(symbol)
    ordered = sorted(dt.date.fromisoformat(exp) for exp in all_expiries if exp)
    chosen_dt = dt.date.fromisoformat(chosen)
    try:
        idx = ordered.index(chosen_dt)
    except ValueError:
        idx = len(ordered)
    while idx > 0:
        idx -= 1
        candidate = ordered[idx]
        if candidate in holidays:
            continue
        return candidate.isoformat()
    return chosen


def resolve_next_expiry(symbol: str, now_ist: dt.datetime, kind: str = "weekly") -> str:
    """Discover next expiry via the instrument cache, applying holiday back-adjustment."""

    now = now_ist.astimezone(IST)
    cache = _active_cache()
    kind_lower = kind.lower()
    if cache is None:
        fallback = _legacy_weekly(symbol, now, holidays=_holiday_calendar())
        return fallback.isoformat()
    today = now.date()
    try:
        weekly = _filter_future(cache.list_expiries(symbol, kind="weekly"), today)
        monthly = _filter_future(cache.list_expiries(symbol, kind="monthly"), today)
    except Exception:
        fallback = _legacy_weekly(symbol, now, holidays=_holiday_calendar())
        return fallback.isoformat()
    candidates: list[str]
    if kind_lower == "monthly":
        candidates = monthly
    else:
        candidates = weekly or monthly
    if not candidates:
        fallback = _legacy_weekly(symbol, now, holidays=_holiday_calendar())
        return fallback.isoformat()
    holidays = _holiday_calendar()
    return _holiday_adjusted_choice(symbol, tuple(candidates), holidays, cache)


def resolve_weekly_expiry(index_symbol: str, now_ist: Optional[dt.datetime] = None, holidays: Optional[Iterable[dt.date]] = None) -> dt.date:
    """Return the weekly expiry leveraging cached discovery when available."""

    now = (now_ist or engine_now(IST)).astimezone(IST)
    try:
        expiry_str = resolve_next_expiry(index_symbol, now, kind="weekly")
        expiry = dt.date.fromisoformat(expiry_str)
    except Exception:
        expiry = _legacy_weekly(index_symbol, now, holidays)
    else:
        if holidays:
            expiry = _back_adjust(expiry, set(holidays))
    return expiry


def pick_strike_from_spot(
    spot: float,
    step: Optional[int] = None,
    mode: str = "ATM",
    delta_target: Optional[float] = None,
    **legacy: object,
) -> int:
    """Return the strike determined by rounding to the nearest configured step."""
    if step is None:
        legacy_step = legacy.pop("lot_step", None)
        if legacy_step is None:
            raise TypeError("pick_strike_from_spot requires 'step' or legacy 'lot_step'")
        step = int(legacy_step)
    if step <= 0:
        raise ValueError("step must be positive")
    base = int(round(spot / step) * step)
    mode_upper = mode.upper()
    if mode_upper == "ITM":
        base -= step
    elif mode_upper == "OTM":
        base += step
    if delta_target is not None:
        if delta_target > 0.55:
            base -= step
        elif delta_target < 0.30:
            base += step
    return base


def align_to_tick(value: float, tick_size: float) -> float:
    if tick_size <= 0:
        return value
    multiple = round(value / tick_size)
    return round(multiple * tick_size, ndigits=8)


def enforce_price_band(price: float, lower: Optional[float], upper: Optional[float]) -> None:
    if lower is not None and price < lower:
        raise ValueError(f"Price {price} below band {lower}")
    if upper is not None and price > upper:
        raise ValueError(f"Price {price} above band {upper}")


__all__ = [
    "align_to_tick",
    "enforce_price_band",
    "get_app_config",
    "pick_strike_from_spot",
    "resolve_next_expiry",
    "resolve_weekly_expiry",
]
