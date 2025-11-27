from __future__ import annotations

import datetime as dt
import logging
import os
import re
import time
from pathlib import Path
from typing import Iterable, List, Literal, Optional, Sequence, Set, TYPE_CHECKING

import yaml

from engine.config import IST
from engine.time_machine import now as engine_now

if TYPE_CHECKING:  # pragma: no cover - typing only
    from market.instrument_cache import InstrumentCache

_APP_CONFIG_CACHE: Optional[dict] = None
_APP_CONFIG_PATH: Optional[Path] = None
_LAST_TICK_TS: dict[str, float] = {}
LOG = logging.getLogger("engine.data")

# Market data modes:
#   - Live: Upstox WS feed decoded in engine.broker publishes tick events to the bus.
#   - Replay: engine.replay replays persisted ticks/events into the same bus.
#   - DRY_RUN: main._live_market_feed generates synthetic ticks for local testing only.


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


def record_tick_seen(*, instrument_key: Optional[str] = None, underlying: Optional[str] = None, ts_seconds: Optional[float] = None) -> None:
    """Record the latest tick timestamp for staleness checks (instrument + underlying)."""

    global _LAST_TICK_TS
    if ts_seconds is None:
        ts_seconds = engine_now(IST).timestamp()
    for key in (instrument_key, underlying):
        if key:
            _LAST_TICK_TS[str(key).upper()] = float(ts_seconds)


def last_tick_age_seconds(identifier: str, *, now: Optional[float] = None) -> Optional[float]:
    ts = _LAST_TICK_TS.get(str(identifier).upper(), None)
    if ts is None:
        return None
    ref = now if now is not None else engine_now(IST).timestamp()
    return max(0.0, float(ref) - float(ts))


def is_market_data_stale(identifier: str, *, threshold: float, now: Optional[float] = None) -> bool:
    """Return True if last tick for identifier is older than threshold seconds."""

    if threshold <= 0:
        return False
    age = last_tick_age_seconds(identifier, now=now)
    if age is None:
        return True
    return age > threshold


def _legacy_weekly(symbol: str, now: dt.datetime, holidays: Optional[Iterable[dt.date]] = None, target_weekday: int = 3) -> dt.date:
    weekday = now.weekday()  # Monday=0
    delta = (target_weekday - weekday) % 7
    if delta == 0 and now.time() >= dt.time(15, 30):
        delta = 7
    expiry = now.date() + dt.timedelta(days=delta)
    holiday_set: Set[dt.date] = set(holidays or [])
    while expiry.weekday() >= 5 or expiry in holiday_set:
        expiry -= dt.timedelta(days=1)
    return expiry


def _active_cache() -> Optional["InstrumentCache"]:
    from market.instrument_cache import InstrumentCache

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


def _holiday_adjusted_choice(symbol: str, candidates: Sequence[str], holidays: Set[dt.date], cache: "InstrumentCache") -> str:
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


def resolve_next_expiry(symbol: str, now_ist: dt.datetime, kind: str = "weekly", weekly_weekday: int = 3) -> str:
    """Discover next expiry via the instrument cache, applying holiday back-adjustment."""

    now = now_ist.astimezone(IST)
    cache = _active_cache()
    kind_lower = kind.lower()
    if cache is None:
        fallback = _legacy_weekly(symbol, now, holidays=_holiday_calendar(), target_weekday=weekly_weekday)
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
        fallback = _legacy_weekly(symbol, now, holidays=_holiday_calendar(), target_weekday=weekly_weekday)
        return fallback.isoformat()
    holidays = _holiday_calendar()
    return _holiday_adjusted_choice(symbol, tuple(candidates), holidays, cache)


def resolve_weekly_expiry(
    index_symbol: str,
    now_ist: Optional[dt.datetime] = None,
    holidays: Optional[Iterable[dt.date]] = None,
    weekly_weekday: int = 3,
) -> dt.date:
    """Return the weekly expiry leveraging cached discovery when available."""

    now = (now_ist or engine_now(IST)).astimezone(IST)
    try:
        expiry_str = resolve_next_expiry(index_symbol, now, kind="weekly", weekly_weekday=weekly_weekday)
        expiry = dt.date.fromisoformat(expiry_str)
    except Exception:
        expiry = _legacy_weekly(index_symbol, now, holidays, target_weekday=weekly_weekday)
    else:
        if holidays:
            expiry = _back_adjust(expiry, set(holidays))
    return expiry


def normalize_date(value: object) -> str:
    """Return strict YYYY-MM-DD string for the provided value."""

    if isinstance(value, dt.datetime):
        return value.date().strftime("%Y-%m-%d")
    if isinstance(value, dt.date):
        return value.strftime("%Y-%m-%d")
    text = str(value).strip()
    if not text:
        raise ValueError("Date must be in YYYY-MM-DD format")
    if re.search(r"[T\s/:+]", text):
        raise ValueError(f"Date must be in YYYY-MM-DD format: {text}")
    try:
        parsed = dt.datetime.strptime(text, "%Y-%m-%d")
    except ValueError as exc:
        raise ValueError(f"Invalid date format: {text}") from exc
    return parsed.strftime("%Y-%m-%d")


def list_expiries(symbol: Literal["NIFTY", "BANKNIFTY"]) -> List[str]:
    cache = _active_cache()
    if cache is None:
        raise RuntimeError("Instrument cache not initialised")
    expiries = cache.list_expiries(symbol)
    normalized = []
    for exp in expiries:
        try:
            normalized.append(normalize_date(exp))
        except ValueError:
            continue
    return sorted(set(normalized))


def pick_subscription_expiry(symbol: str, preference: str) -> str:
    """
    Returns a single expiry 'YYYY-MM-DD' based on preference.
    preference ∈ {'current','next','monthly'}
    - 'current': earliest weekly from list_expiries(symbol)
    - 'next': the next weekly after the earliest (if present), else earliest
    - 'monthly': pick the nearest monthly for symbol (first expiry with day>=24, else farthest)
    """

    pref = str(preference or "current").strip().lower()
    if pref not in {"current", "next", "monthly"}:
        pref = "current"
    expiries = list_expiries(symbol)  # already normalized and sorted ascending
    if not expiries:
        raise RuntimeError(f"No expiries for {symbol}")
    if pref == "monthly":
        for e in expiries:
            try:
                parsed = dt.datetime.strptime(e, "%Y-%m-%d")
            except ValueError:
                continue
            if parsed.day >= 24:
                return e
        return expiries[-1]
    if pref == "next":
        return expiries[1] if len(expiries) >= 2 else expiries[0]
    return expiries[0]


def resolve_expiries_with_fallback(symbol: str) -> List[str]:
    cache = _active_cache()
    if cache is None:
        return []
    try:
        expiries = cache.list_expiries(symbol)
    except Exception:
        return []
    return list(expiries or [])


def assert_valid_expiry(symbol: str, expiry: str) -> str:
    expiry_norm = normalize_date(expiry)
    expiries = list_expiries(symbol)
    if expiry_norm in expiries:
        return expiry_norm
    today = engine_now(IST).date()
    future: list[dt.date] = []
    for exp in expiries:
        try:
            parsed = dt.date.fromisoformat(exp)
        except ValueError:
            continue
        if parsed >= today:
            future.append(parsed)
    if future:
        fallback = min(future).strftime("%Y-%m-%d")
    elif expiries:
        fallback = expiries[0]
    else:
        raise ValueError(f"No expiries available for {symbol}")
    LOG.warning("Expiry %s not valid for %s; falling back to %s", expiry_norm, symbol, fallback)
    return fallback


class PreopenExpiryCheckFailed(RuntimeError):
    """Raised when the pre-open expiry probe fails."""


def preopen_expiry_smoke(symbols: Iterable[str] = ("NIFTY", "BANKNIFTY")) -> None:
    cache = _active_cache()
    if cache is None:
        raise PreopenExpiryCheckFailed("Instrument cache unavailable for pre-open check")
    session = cache.upstox_session()
    for symbol in symbols:
        expiries = resolve_expiries_with_fallback(symbol)
        LOG.info("preopen_expiry_smoke: %s expiries=%d sample=%s", symbol, len(expiries), expiries[:5])
        if not expiries:
            raise PreopenExpiryCheckFailed(f"no expiries for {symbol}")
        target = expiries[0]
        try:
            session.get_option_contracts(cache.resolve_index_key(symbol), target)
        except Exception as exc:  # pragma: no cover - network dependent
            raise PreopenExpiryCheckFailed(f"{symbol} expiry {target} probe failed: {exc}") from exc


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
    "assert_valid_expiry",
    "enforce_price_band",
    "get_app_config",
    "record_tick_seen",
    "last_tick_age_seconds",
    "is_market_data_stale",
    "list_expiries",
    "normalize_date",
    "pick_subscription_expiry",
    "preopen_expiry_smoke",
    "pick_strike_from_spot",
    "resolve_expiries_with_fallback",
    "resolve_next_expiry",
    "resolve_weekly_expiry",
    "PreopenExpiryCheckFailed",
]
