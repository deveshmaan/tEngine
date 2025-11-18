from __future__ import annotations

import datetime as dt
from typing import Iterable, Optional, Sequence, Set

from engine.config import IST
from engine.time_machine import now as engine_now


def resolve_weekly_expiry(index_symbol: str, now_ist: Optional[dt.datetime] = None, holidays: Optional[Iterable[dt.date]] = None) -> dt.date:
    """Return the weekly expiry (Thursday) for the provided timestamp, falling back to the previous business day on holidays."""

    now = (now_ist or engine_now(IST)).astimezone(IST)
    weekday = now.weekday()  # Monday=0
    if weekday <= 3:
        expiry = now.date() + dt.timedelta(days=3 - weekday)
    else:
        expiry = now.date() + dt.timedelta(days=(7 - weekday) + 3)
    holiday_set: Set[dt.date] = set(holidays or [])
    while expiry.weekday() >= 5 or expiry in holiday_set:
        expiry -= dt.timedelta(days=1)
    return expiry


def pick_strike_from_spot(
    spot: float,
    lot_step: int,
    tick_size: float,
    *,
    delta_target: Optional[float] = None,
    mode: str = "ATM",
    price_band: Optional[Sequence[float]] = None,
) -> float:
    """Return an exchange-compliant strike for the provided spot."""

    if lot_step <= 0:
        raise ValueError("lot_step must be positive")
    if tick_size <= 0:
        raise ValueError("tick_size must be positive")
    base = round(spot / lot_step) * lot_step
    mode_upper = mode.upper()
    if mode_upper == "ITM":
        base -= lot_step
    elif mode_upper == "OTM":
        base += lot_step
    if delta_target is not None:
        if delta_target > 0.55:
            base -= lot_step
        elif delta_target < 0.30:
            base += lot_step
    strike = align_to_tick(float(base), float(tick_size))
    if price_band:
        lower = price_band[0] if len(price_band) > 0 else None
        upper = price_band[1] if len(price_band) > 1 else None
        enforce_price_band(strike, lower, upper)
    return strike


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


__all__ = ["align_to_tick", "enforce_price_band", "pick_strike_from_spot", "resolve_weekly_expiry"]
