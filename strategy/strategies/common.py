from __future__ import annotations

import math
from dataclasses import replace
from typing import Any, Optional, Tuple

from engine.data import pick_strike_from_spot
from strategy.contracts import Direction, TradeIntent
from strategy.market_snapshot import MarketSnapshot, OptionQuote


def direction_to_opt(direction: Direction) -> str:
    return "CE" if direction == "CALL" else "PE"


def build_symbol(underlying: str, expiry: str, strike: int, opt_type: str) -> str:
    return f"{underlying.upper()}-{expiry}-{int(strike)}{opt_type.upper()}"


def pick_best_option(
    snapshot: MarketSnapshot,
    *,
    direction: Direction,
    expiry: Optional[str],
    spot: float,
    strike_step: int,
    delta_target: float = 0.4,
    delta_min: float = 0.25,
    delta_max: float = 0.55,
) -> Tuple[Optional[OptionQuote], str]:
    opt_type = direction_to_opt(direction)
    expiry_text = str(expiry or snapshot.meta.get("expiry") or "").strip() or None
    candidates = snapshot.option_candidates(opt_type=opt_type, expiry=expiry_text)
    if candidates:
        # Prefer candidates with delta inside the window; fall back to nearest strike.
        with_delta = [c for c in candidates if c.delta is not None]
        if with_delta:
            def _score_delta(opt: OptionQuote) -> tuple[float, float]:
                abs_delta = abs(float(opt.delta or 0.0))
                return (abs(abs_delta - float(delta_target)), opt.spread_pct() or 0.0)

            in_band = [c for c in with_delta if delta_min <= abs(float(c.delta or 0.0)) <= delta_max]
            pool = in_band or with_delta
            best = min(pool, key=_score_delta)
            return best, build_symbol(snapshot.underlying, best.expiry, best.strike, opt_type)

        # Delta unavailable: choose the nearest strike available.
        nearest = min(candidates, key=lambda c: abs(float(c.strike) - float(spot)))
        return nearest, build_symbol(snapshot.underlying, nearest.expiry, nearest.strike, opt_type)

    # No subscribed option quotes available: fall back to a strike estimate.
    step = max(int(strike_step or 1), 1)
    strike = pick_strike_from_spot(float(spot), step=step, delta_target=float(delta_target))
    expiry_fallback = str(expiry or snapshot.meta.get("expiry") or "").strip()
    return None, build_symbol(snapshot.underlying, expiry_fallback, strike, opt_type)


def with_intent_defaults(intent: TradeIntent, *, strategy_id: str, underlying: str) -> TradeIntent:
    updated = intent
    if intent.strategy_id != strategy_id or intent.underlying != underlying:
        updated = replace(intent, strategy_id=strategy_id, underlying=underlying)
    return updated


def ema(prev: Optional[float], value: float, alpha: float) -> float:
    if prev is None:
        return float(value)
    return float(prev) + float(alpha) * (float(value) - float(prev))


def clamp(value: float, lo: float, hi: float) -> float:
    return max(float(lo), min(float(value), float(hi)))


def safe_div(num: float, den: float, default: float = 0.0) -> float:
    try:
        if den == 0:
            return default
        return float(num) / float(den)
    except Exception:
        return default


def pct_change(curr: float, prev: float) -> float:
    return safe_div(curr - prev, prev, default=0.0)


def score_breakout(distance: float, range_width: float) -> float:
    width = max(float(range_width), 1e-9)
    return clamp(abs(float(distance)) / width, 0.0, 5.0)


__all__ = [
    "build_symbol",
    "clamp",
    "direction_to_opt",
    "ema",
    "pct_change",
    "pick_best_option",
    "safe_div",
    "score_breakout",
    "with_intent_defaults",
]

