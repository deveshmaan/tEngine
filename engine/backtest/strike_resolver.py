from __future__ import annotations

import datetime as dt
from typing import Callable, Iterable, Optional, Tuple

from engine.data import pick_strike_from_spot

ALLOWED_OPT_TYPES = {"CE", "PE"}


def resolve_atm_strike(spot_price: float, strike_step: int) -> int:
    """
    Return the nearest ATM strike aligned to `strike_step`.

    This intentionally reuses the rounding behavior from `engine.data.pick_strike_from_spot`
    to avoid duplicating strike rounding logic across live/backtest.
    """

    try:
        step = int(strike_step)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"strike_step must be an int; got {strike_step!r}") from exc
    strike = pick_strike_from_spot(float(spot_price), step=step, mode="ATM")
    return int(strike)


def resolve_strike_offset(atm: int, opt_type: str, offset_points: int) -> int:
    """
    Resolve a strike by applying a signed offset from ATM.

    Convention:
    - For CE: strike = ATM + offset_points
    - For PE: strike = ATM - offset_points

    This makes a positive `offset_points` mean "further OTM" for both CE and PE.
    (For PE, OTM strikes are lower than ATM.)
    """

    try:
        atm_i = int(atm)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"atm must be an int; got {atm!r}") from exc
    try:
        off = int(offset_points)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"offset_points must be an int; got {offset_points!r}") from exc
    t = str(opt_type or "").strip().upper()
    if t not in ALLOWED_OPT_TYPES:
        raise ValueError(f"opt_type must be one of {sorted(ALLOWED_OPT_TYPES)}; got {opt_type!r}")
    return int(atm_i + off) if t == "CE" else int(atm_i - off)


def _coerce_datetime(value: object, field_name: str) -> dt.datetime:
    if isinstance(value, dt.datetime):
        return value
    text = str(value).strip()
    try:
        return dt.datetime.fromisoformat(text)
    except ValueError as exc:
        raise ValueError(f"{field_name} must be an ISO datetime; got {value!r}") from exc


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


def resolve_target_premium(
    entry_ts: dt.datetime | str,
    expiry_date: dt.date | dt.datetime | str,
    opt_type: str,
    target_premium: float,
    strike_grid: Iterable[int],
    get_option_price_fn: Callable[[dt.datetime, dt.date, str, int], Optional[float]],
) -> int:
    """
    Resolve a strike by scanning `strike_grid` and choosing the one whose premium at
    `entry_ts` is closest to `target_premium`.

    Notes:
    - Upstox doesn't provide historical option-chain snapshots. Callers should supply
      `get_option_price_fn()` that prices a strike using the candle at `entry_ts` (open/close)
      according to the chosen fill model.
    - Deterministic + cache-friendly: this function is pure, processes strikes in sorted
      order, and calls the price function at most once per unique strike.
    """

    ts = _coerce_datetime(entry_ts, "entry_ts")
    exp = _coerce_date(expiry_date, "expiry_date")
    t = str(opt_type or "").strip().upper()
    if t not in ALLOWED_OPT_TYPES:
        raise ValueError(f"opt_type must be one of {sorted(ALLOWED_OPT_TYPES)}; got {opt_type!r}")
    try:
        tgt = float(target_premium)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"target_premium must be a number; got {target_premium!r}") from exc
    if tgt <= 0:
        raise ValueError("target_premium must be > 0")

    strikes_in = list(strike_grid or [])
    if not strikes_in:
        raise ValueError("strike_grid must be non-empty")
    strikes: list[int] = sorted({int(s) for s in strikes_in})

    best: Optional[Tuple[float, int, float]] = None  # (abs_diff, strike, price)
    price_cache: dict[int, Optional[float]] = {}
    for strike in strikes:
        if strike in price_cache:
            price = price_cache[strike]
        else:
            try:
                price = get_option_price_fn(ts, exp, t, int(strike))
            except Exception:
                price = None
            price_cache[strike] = price
        if price is None:
            continue
        try:
            price_f = float(price)
        except (TypeError, ValueError):
            continue
        if price_f <= 0:
            continue
        diff = abs(price_f - tgt)
        candidate = (diff, int(strike), price_f)
        if best is None or candidate[:2] < best[:2]:
            best = candidate

    if best is None:
        raise ValueError("No valid premiums returned for strike_grid")
    return int(best[1])


__all__ = [
    "resolve_atm_strike",
    "resolve_strike_offset",
    "resolve_target_premium",
]

