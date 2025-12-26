from __future__ import annotations

import datetime as dt
from typing import Any, Dict, Optional

from engine.backtest.strategy_spec import (
    FILL_MODE_NEXT_TICK,
    LegSpec,
    StrategySpec,
)

from .registry import TemplateDefinition, get_template, list_templates


def _replace_legs(spec: StrategySpec, legs: tuple[LegSpec, ...]) -> StrategySpec:
    return StrategySpec(
        name=spec.name,
        underlying_instrument_key=spec.underlying_instrument_key,
        start_date=spec.start_date,
        end_date=spec.end_date,
        candle_interval=spec.candle_interval,
        entry_time=spec.entry_time,
        exit_time=spec.exit_time,
        fill_model=spec.fill_model,
        allow_partial_fills=spec.allow_partial_fills,
        latency_ms=spec.latency_ms,
        slippage_model=spec.slippage_model,
        slippage_bps=spec.slippage_bps,
        slippage_ticks=spec.slippage_ticks,
        spread_bps=spec.spread_bps,
        brokerage_profile=spec.brokerage_profile,
        starting_capital=spec.starting_capital,
        legs=legs,
    )


def _default_period() -> tuple[dt.date, dt.date]:
    today = dt.date.today()
    end = today - dt.timedelta(days=1)
    start = end - dt.timedelta(days=7)
    return start, end


def build_template(name: str, **kwargs: Any) -> StrategySpec:
    """
    Build a `StrategySpec` from a registered template and optional overrides.

    Common overrides used by Streamlit / tests:
    - underlying_instrument_key, start_date, end_date, candle_interval, qty_lots
    - fill_model, allow_partial_fills, latency_ms
    - slippage_model, slippage_bps, slippage_ticks, spread_bps
    - brokerage_profile, starting_capital
    """

    tmpl = get_template(str(name))

    start_date = kwargs.get("start_date")
    end_date = kwargs.get("end_date")
    if start_date is None or end_date is None:
        default_start, default_end = _default_period()
        start_date = default_start if start_date is None else start_date
        end_date = default_end if end_date is None else end_date

    underlying_instrument_key = str(
        kwargs.get("underlying_instrument_key") or tmpl.default_underlying_instrument_key
    )

    candle_interval = str(kwargs.get("candle_interval") or tmpl.candle_interval)
    entry_time = kwargs.get("entry_time") or tmpl.entry_time
    exit_time = kwargs.get("exit_time") or tmpl.exit_time

    fill_model = str(kwargs.get("fill_model") or FILL_MODE_NEXT_TICK)
    allow_partial_fills = bool(kwargs.get("allow_partial_fills", False))
    latency_ms = int(kwargs.get("latency_ms") or 0)

    slippage_model = str(kwargs.get("slippage_model") or "none")
    slippage_bps = float(kwargs.get("slippage_bps") or 0.0)
    slippage_ticks = int(kwargs.get("slippage_ticks") or 0)
    spread_bps = float(kwargs.get("spread_bps") or 0.0)

    brokerage_profile = str(kwargs.get("brokerage_profile") or "india_options_default")
    starting_capital = float(kwargs.get("starting_capital") or 100000.0)

    qty_lots_override = kwargs.get("qty_lots")
    legs: list[LegSpec] = []
    for leg_payload in tmpl.legs:
        payload: Dict[str, Any] = dict(leg_payload)
        if qty_lots_override is not None:
            payload["qty_lots"] = int(qty_lots_override)
        legs.append(LegSpec.from_dict(payload))

    return StrategySpec(
        name=str(kwargs.get("spec_name") or tmpl.template_id),
        underlying_instrument_key=underlying_instrument_key,
        start_date=start_date,
        end_date=end_date,
        candle_interval=candle_interval,
        entry_time=entry_time,
        exit_time=exit_time,
        fill_model=fill_model,
        allow_partial_fills=allow_partial_fills,
        latency_ms=latency_ms,
        slippage_model=slippage_model,
        slippage_bps=slippage_bps,
        slippage_ticks=slippage_ticks,
        spread_bps=spread_bps,
        brokerage_profile=brokerage_profile,
        starting_capital=starting_capital,
        legs=tuple(legs),
    )


def ShortStraddle(
    *,
    underlying_instrument_key: str = "NSE_INDEX|Nifty 50",
    start_date: Optional[dt.date] = None,
    end_date: Optional[dt.date] = None,
    candle_interval: str = "5minute",
    qty_lots: int = 1,
    **kwargs: Any,
) -> StrategySpec:
    return build_template(
        "ShortStraddle",
        underlying_instrument_key=underlying_instrument_key,
        start_date=start_date,
        end_date=end_date,
        candle_interval=candle_interval,
        qty_lots=qty_lots,
        **kwargs,
    )


def ShortStrangle(
    *,
    underlying_instrument_key: str = "NSE_INDEX|Nifty 50",
    start_date: Optional[dt.date] = None,
    end_date: Optional[dt.date] = None,
    candle_interval: str = "5minute",
    qty_lots: int = 1,
    offset_points: int = 200,
    **kwargs: Any,
) -> StrategySpec:
    spec = build_template(
        "ShortStrangle",
        underlying_instrument_key=underlying_instrument_key,
        start_date=start_date,
        end_date=end_date,
        candle_interval=candle_interval,
        qty_lots=qty_lots,
        **kwargs,
    )
    off = abs(int(offset_points))
    legs = tuple(
        LegSpec(**{**leg.to_dict(), "strike_offset_points": off})
        if leg.strike_mode == "ATM_OFFSET"
        else leg
        for leg in spec.legs
    )
    return _replace_legs(spec, legs)


def IronCondor(
    *,
    underlying_instrument_key: str = "NSE_INDEX|Nifty 50",
    start_date: Optional[dt.date] = None,
    end_date: Optional[dt.date] = None,
    candle_interval: str = "5minute",
    qty_lots: int = 1,
    short_offset_points: int = 200,
    wing_width_points: int = 200,
    **kwargs: Any,
) -> StrategySpec:
    spec = build_template(
        "IronCondor",
        underlying_instrument_key=underlying_instrument_key,
        start_date=start_date,
        end_date=end_date,
        candle_interval=candle_interval,
        qty_lots=qty_lots,
        **kwargs,
    )
    short_offset = abs(int(short_offset_points))
    wing_width = abs(int(wing_width_points))
    updated: list[LegSpec] = []
    for leg in spec.legs:
        if leg.strike_mode != "ATM_OFFSET":
            updated.append(leg)
            continue
        desired = short_offset + wing_width if leg.side == "BUY" else short_offset
        updated.append(LegSpec(**{**leg.to_dict(), "strike_offset_points": desired}))
    return _replace_legs(spec, tuple(updated))


__all__ = [
    "TemplateDefinition",
    "IronCondor",
    "ShortStraddle",
    "ShortStrangle",
    "build_template",
    "get_template",
    "list_templates",
]
