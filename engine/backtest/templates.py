from __future__ import annotations

import datetime as dt
from typing import Callable, Dict, Optional

from engine.backtest.strategy_spec import (
    FILL_MODE_NEXT_TICK,
    LegSpec,
    StrategySpec,
)


def _default_period() -> tuple[dt.date, dt.date]:
    today = dt.date.today()
    end = today - dt.timedelta(days=1)
    start = end - dt.timedelta(days=7)
    return start, end


def _base_spec(
    *,
    name: str,
    underlying_instrument_key: str = "NSE_INDEX|Nifty 50",
    start_date: Optional[dt.date] = None,
    end_date: Optional[dt.date] = None,
    candle_interval: str = "5minute",
    entry_time: dt.time | str = "09:30",
    exit_time: dt.time | str = "15:20",
    fill_model: str = FILL_MODE_NEXT_TICK,
    allow_partial_fills: bool = False,
    latency_ms: int = 0,
    slippage_model: str = "none",
    slippage_bps: float = 0.0,
    slippage_ticks: int = 0,
    spread_bps: float = 0.0,
    brokerage_profile: str = "india_options_default",
    starting_capital: float = 100000.0,
) -> Dict[str, object]:
    if start_date is None or end_date is None:
        start_default, end_default = _default_period()
        start_date = start_default if start_date is None else start_date
        end_date = end_default if end_date is None else end_date
    return {
        "name": name,
        "underlying_instrument_key": underlying_instrument_key,
        "start_date": start_date,
        "end_date": end_date,
        "candle_interval": candle_interval,
        "entry_time": entry_time,
        "exit_time": exit_time,
        "fill_model": fill_model,
        "allow_partial_fills": allow_partial_fills,
        "latency_ms": latency_ms,
        "slippage_model": slippage_model,
        "slippage_bps": slippage_bps,
        "slippage_ticks": slippage_ticks,
        "spread_bps": spread_bps,
        "brokerage_profile": brokerage_profile,
        "starting_capital": starting_capital,
    }


def ShortStraddle(
    *,
    underlying_instrument_key: str = "NSE_INDEX|Nifty 50",
    start_date: Optional[dt.date] = None,
    end_date: Optional[dt.date] = None,
    candle_interval: str = "5minute",
    qty_lots: int = 1,
) -> StrategySpec:
    base = _base_spec(
        name="ShortStraddle",
        underlying_instrument_key=underlying_instrument_key,
        start_date=start_date,
        end_date=end_date,
        candle_interval=candle_interval,
    )
    legs = (
        LegSpec(side="SELL", opt_type="CE", qty_lots=qty_lots, expiry_mode="WEEKLY_CURRENT", strike_mode="ATM"),
        LegSpec(side="SELL", opt_type="PE", qty_lots=qty_lots, expiry_mode="WEEKLY_CURRENT", strike_mode="ATM"),
    )
    return StrategySpec(**base, legs=legs)


def ShortStrangle(
    *,
    underlying_instrument_key: str = "NSE_INDEX|Nifty 50",
    start_date: Optional[dt.date] = None,
    end_date: Optional[dt.date] = None,
    candle_interval: str = "5minute",
    qty_lots: int = 1,
    offset_points: int = 200,
) -> StrategySpec:
    base = _base_spec(
        name="ShortStrangle",
        underlying_instrument_key=underlying_instrument_key,
        start_date=start_date,
        end_date=end_date,
        candle_interval=candle_interval,
    )
    legs = (
        LegSpec(side="SELL", opt_type="CE", qty_lots=qty_lots, expiry_mode="WEEKLY_CURRENT", strike_mode="ATM_OFFSET", strike_offset_points=abs(int(offset_points))),
        LegSpec(side="SELL", opt_type="PE", qty_lots=qty_lots, expiry_mode="WEEKLY_CURRENT", strike_mode="ATM_OFFSET", strike_offset_points=abs(int(offset_points))),
    )
    return StrategySpec(**base, legs=legs)


def IronCondor(
    *,
    underlying_instrument_key: str = "NSE_INDEX|Nifty 50",
    start_date: Optional[dt.date] = None,
    end_date: Optional[dt.date] = None,
    candle_interval: str = "5minute",
    qty_lots: int = 1,
    short_offset_points: int = 200,
    wing_width_points: int = 200,
) -> StrategySpec:
    base = _base_spec(
        name="IronCondor",
        underlying_instrument_key=underlying_instrument_key,
        start_date=start_date,
        end_date=end_date,
        candle_interval=candle_interval,
    )
    short_offset = abs(int(short_offset_points))
    wing_width = abs(int(wing_width_points))
    legs = (
        LegSpec(side="SELL", opt_type="CE", qty_lots=qty_lots, expiry_mode="WEEKLY_CURRENT", strike_mode="ATM_OFFSET", strike_offset_points=short_offset),
        LegSpec(side="BUY", opt_type="CE", qty_lots=qty_lots, expiry_mode="WEEKLY_CURRENT", strike_mode="ATM_OFFSET", strike_offset_points=short_offset + wing_width),
        LegSpec(side="SELL", opt_type="PE", qty_lots=qty_lots, expiry_mode="WEEKLY_CURRENT", strike_mode="ATM_OFFSET", strike_offset_points=short_offset),
        LegSpec(side="BUY", opt_type="PE", qty_lots=qty_lots, expiry_mode="WEEKLY_CURRENT", strike_mode="ATM_OFFSET", strike_offset_points=short_offset + wing_width),
    )
    return StrategySpec(**base, legs=legs)


def BullCallSpread(
    *,
    underlying_instrument_key: str = "NSE_INDEX|Nifty 50",
    start_date: Optional[dt.date] = None,
    end_date: Optional[dt.date] = None,
    candle_interval: str = "5minute",
    qty_lots: int = 1,
    width_points: int = 200,
) -> StrategySpec:
    base = _base_spec(
        name="BullCallSpread",
        underlying_instrument_key=underlying_instrument_key,
        start_date=start_date,
        end_date=end_date,
        candle_interval=candle_interval,
    )
    width = abs(int(width_points))
    legs = (
        LegSpec(side="BUY", opt_type="CE", qty_lots=qty_lots, expiry_mode="WEEKLY_CURRENT", strike_mode="ATM"),
        LegSpec(side="SELL", opt_type="CE", qty_lots=qty_lots, expiry_mode="WEEKLY_CURRENT", strike_mode="ATM_OFFSET", strike_offset_points=width),
    )
    return StrategySpec(**base, legs=legs)


def BearPutSpread(
    *,
    underlying_instrument_key: str = "NSE_INDEX|Nifty 50",
    start_date: Optional[dt.date] = None,
    end_date: Optional[dt.date] = None,
    candle_interval: str = "5minute",
    qty_lots: int = 1,
    width_points: int = 200,
) -> StrategySpec:
    base = _base_spec(
        name="BearPutSpread",
        underlying_instrument_key=underlying_instrument_key,
        start_date=start_date,
        end_date=end_date,
        candle_interval=candle_interval,
    )
    width = abs(int(width_points))
    legs = (
        LegSpec(side="BUY", opt_type="PE", qty_lots=qty_lots, expiry_mode="WEEKLY_CURRENT", strike_mode="ATM"),
        LegSpec(side="SELL", opt_type="PE", qty_lots=qty_lots, expiry_mode="WEEKLY_CURRENT", strike_mode="ATM_OFFSET", strike_offset_points=width),
    )
    return StrategySpec(**base, legs=legs)


TEMPLATE_BUILDERS: Dict[str, Callable[..., StrategySpec]] = {
    "ShortStraddle": ShortStraddle,
    "ShortStrangle": ShortStrangle,
    "IronCondor": IronCondor,
    "BullCallSpread": BullCallSpread,
    "BearPutSpread": BearPutSpread,
}


def list_templates() -> list[str]:
    return sorted(TEMPLATE_BUILDERS.keys())


def build_template(name: str, **kwargs) -> StrategySpec:
    key = str(name or "").strip()
    if key not in TEMPLATE_BUILDERS:
        raise KeyError(f"Unknown template: {name!r}. Available: {list_templates()}")
    return TEMPLATE_BUILDERS[key](**kwargs)


__all__ = [
    "BearPutSpread",
    "BullCallSpread",
    "IronCondor",
    "ShortStraddle",
    "ShortStrangle",
    "TEMPLATE_BUILDERS",
    "build_template",
    "list_templates",
]
