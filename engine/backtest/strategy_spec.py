from __future__ import annotations

import datetime as dt
import json
import re
from dataclasses import dataclass, field
from typing import Any, Dict, Optional

_HHMM_RE = re.compile(r"^(?P<h>\d{1,2}):(?P<m>\d{2})$")

FILL_MODE_NEXT_TICK = "next_tick"
FILL_MODE_SAME_TICK = "same_tick"
_FILL_MODEL_ALIASES = {
    "same_tick_close": FILL_MODE_SAME_TICK,
}
ALLOWED_FILL_MODELS = {FILL_MODE_NEXT_TICK, FILL_MODE_SAME_TICK}

ALLOWED_SLIPPAGE_MODELS = {"none", "bps", "ticks"}
ALLOWED_EXPIRY_MODES = {"WEEKLY_CURRENT", "WEEKLY_NEXT", "MONTHLY"}
ALLOWED_STRIKE_MODES = {"ATM", "ATM_OFFSET", "TARGET_PREMIUM"}
ALLOWED_SIDES = {"BUY", "SELL"}
ALLOWED_OPT_TYPES = {"CE", "PE"}

# Mirrors current Streamlit backtest interval dropdown.
ALLOWED_CANDLE_INTERVALS = {"1minute", "5minute", "15minute", "30minute", "1day"}


def _coerce_date(value: object, field_name: str) -> dt.date:
    if isinstance(value, dt.date) and not isinstance(value, dt.datetime):
        return value
    text = str(value).strip()
    try:
        return dt.date.fromisoformat(text)
    except Exception as exc:
        raise ValueError(f"{field_name} must be a date (YYYY-MM-DD); got {value!r}") from exc


def _parse_hhmm(value: object, field_name: str) -> dt.time:
    if isinstance(value, dt.time):
        if value.tzinfo is not None:
            raise ValueError(f"{field_name} must be a naive time (HH:MM); got tz-aware {value!r}")
        if value.second != 0 or value.microsecond != 0:
            raise ValueError(f"{field_name} must be HH:MM (no seconds); got {value!r}")
        return value
    text = str(value).strip()
    match = _HHMM_RE.fullmatch(text)
    if not match:
        raise ValueError(f"{field_name} must be HH:MM; got {value!r}")
    hour = int(match.group("h"))
    minute = int(match.group("m"))
    if not (0 <= hour <= 23 and 0 <= minute <= 59):
        raise ValueError(f"{field_name} must be HH:MM; got {value!r}")
    return dt.time(hour=hour, minute=minute)


def _format_hhmm(value: dt.time) -> str:
    return f"{value.hour:02d}:{value.minute:02d}"


def _normalize_pct(value: Optional[object], field_name: str) -> Optional[float]:
    if value is None:
        return None
    try:
        num = float(value)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"{field_name} must be a number; got {value!r}") from exc
    if num <= 0:
        raise ValueError(f"{field_name} must be > 0; got {value!r}")
    # Accept both fractions (0.25) and percentages (25).
    if num > 1.0:
        num = num / 100.0
    if num <= 0 or num > 1.0:
        raise ValueError(f"{field_name} must be in (0, 1] (or 0-100); got {value!r}")
    return float(num)


@dataclass(frozen=True)
class ProfitLockSpec:
    trigger_pct: float
    lock_to_pct: float

    def __post_init__(self) -> None:
        trigger = _normalize_pct(self.trigger_pct, "profit_lock.trigger_pct")
        lock_to = _normalize_pct(self.lock_to_pct, "profit_lock.lock_to_pct")
        assert trigger is not None and lock_to is not None
        if lock_to > trigger:
            raise ValueError("profit_lock.lock_to_pct must be <= profit_lock.trigger_pct")
        object.__setattr__(self, "trigger_pct", float(trigger))
        object.__setattr__(self, "lock_to_pct", float(lock_to))

    def to_dict(self) -> Dict[str, Any]:
        return {
            "trigger_pct": float(self.trigger_pct),
            "lock_to_pct": float(self.lock_to_pct),
        }

    @classmethod
    def from_dict(cls, payload: Dict[str, Any]) -> "ProfitLockSpec":
        return cls(
            trigger_pct=payload.get("trigger_pct"),
            lock_to_pct=payload.get("lock_to_pct"),
        )


@dataclass(frozen=True)
class LegSpec:
    side: str
    opt_type: str
    qty_lots: int
    expiry_mode: str
    strike_mode: str
    strike_offset_points: Optional[int] = None
    target_premium: Optional[float] = None
    stoploss_pct: Optional[float] = None
    profit_lock: Optional[ProfitLockSpec] = None
    reentry_enabled: bool = False
    max_reentries: int = 0

    def __post_init__(self) -> None:
        side = str(self.side or "").strip().upper()
        if side not in ALLOWED_SIDES:
            raise ValueError(f"LegSpec.side must be one of {sorted(ALLOWED_SIDES)}; got {self.side!r}")
        object.__setattr__(self, "side", side)

        opt_type = str(self.opt_type or "").strip().upper()
        if opt_type not in ALLOWED_OPT_TYPES:
            raise ValueError(f"LegSpec.opt_type must be one of {sorted(ALLOWED_OPT_TYPES)}; got {self.opt_type!r}")
        object.__setattr__(self, "opt_type", opt_type)

        expiry_mode = str(self.expiry_mode or "").strip().upper()
        if expiry_mode not in ALLOWED_EXPIRY_MODES:
            raise ValueError(f"LegSpec.expiry_mode must be one of {sorted(ALLOWED_EXPIRY_MODES)}; got {self.expiry_mode!r}")
        object.__setattr__(self, "expiry_mode", expiry_mode)

        strike_mode = str(self.strike_mode or "").strip().upper()
        if strike_mode not in ALLOWED_STRIKE_MODES:
            raise ValueError(f"LegSpec.strike_mode must be one of {sorted(ALLOWED_STRIKE_MODES)}; got {self.strike_mode!r}")
        object.__setattr__(self, "strike_mode", strike_mode)

        try:
            qty_lots = int(self.qty_lots)
        except (TypeError, ValueError) as exc:
            raise ValueError(f"LegSpec.qty_lots must be an int; got {self.qty_lots!r}") from exc
        if qty_lots <= 0:
            raise ValueError(f"LegSpec.qty_lots must be > 0; got {self.qty_lots!r}")
        object.__setattr__(self, "qty_lots", qty_lots)

        strike_offset_points = self.strike_offset_points
        if strike_offset_points is not None:
            try:
                strike_offset_points = int(strike_offset_points)
            except (TypeError, ValueError) as exc:
                raise ValueError("LegSpec.strike_offset_points must be an int (points)") from exc

        target_premium = self.target_premium
        if target_premium is not None:
            try:
                target_premium = float(target_premium)
            except (TypeError, ValueError) as exc:
                raise ValueError("LegSpec.target_premium must be a float") from exc
            if target_premium <= 0:
                raise ValueError("LegSpec.target_premium must be > 0")

        if strike_mode == "ATM":
            if strike_offset_points not in (None, 0):
                raise ValueError("LegSpec.strike_offset_points must be omitted for strike_mode=ATM")
            if target_premium is not None:
                raise ValueError("LegSpec.target_premium must be omitted for strike_mode=ATM")
            strike_offset_points = None
        elif strike_mode == "ATM_OFFSET":
            if strike_offset_points is None:
                raise ValueError("LegSpec.strike_offset_points is required for strike_mode=ATM_OFFSET")
            if target_premium is not None:
                raise ValueError("LegSpec.target_premium must be omitted for strike_mode=ATM_OFFSET")
        elif strike_mode == "TARGET_PREMIUM":
            if target_premium is None:
                raise ValueError("LegSpec.target_premium is required for strike_mode=TARGET_PREMIUM")
            if strike_offset_points not in (None, 0):
                raise ValueError("LegSpec.strike_offset_points must be omitted for strike_mode=TARGET_PREMIUM")
            strike_offset_points = None
        else:  # pragma: no cover (guarded above)
            raise ValueError(f"Unsupported strike_mode: {strike_mode}")

        object.__setattr__(self, "strike_offset_points", strike_offset_points)
        object.__setattr__(self, "target_premium", target_premium)

        stoploss_pct = _normalize_pct(self.stoploss_pct, "stoploss_pct") if self.stoploss_pct is not None else None
        object.__setattr__(self, "stoploss_pct", stoploss_pct)

        profit_lock = self.profit_lock
        if profit_lock is not None and not isinstance(profit_lock, ProfitLockSpec):
            if isinstance(profit_lock, dict):
                profit_lock = ProfitLockSpec.from_dict(profit_lock)
            else:
                raise ValueError("LegSpec.profit_lock must be ProfitLockSpec or dict")
        object.__setattr__(self, "profit_lock", profit_lock)

        max_reentries = int(self.max_reentries or 0)
        reentry_enabled = bool(self.reentry_enabled)
        if not reentry_enabled and max_reentries != 0:
            raise ValueError("LegSpec.max_reentries must be 0 when reentry_enabled is false")
        if reentry_enabled and max_reentries <= 0:
            raise ValueError("LegSpec.max_reentries must be > 0 when reentry_enabled is true")
        object.__setattr__(self, "reentry_enabled", reentry_enabled)
        object.__setattr__(self, "max_reentries", max_reentries)

    def to_dict(self) -> Dict[str, Any]:
        out: Dict[str, Any] = {
            "side": self.side,
            "opt_type": self.opt_type,
            "qty_lots": int(self.qty_lots),
            "expiry_mode": self.expiry_mode,
            "strike_mode": self.strike_mode,
            "reentry_enabled": bool(self.reentry_enabled),
            "max_reentries": int(self.max_reentries),
        }
        if self.strike_offset_points is not None:
            out["strike_offset_points"] = int(self.strike_offset_points)
        if self.target_premium is not None:
            out["target_premium"] = float(self.target_premium)
        if self.stoploss_pct is not None:
            out["stoploss_pct"] = float(self.stoploss_pct)
        if self.profit_lock is not None:
            out["profit_lock"] = self.profit_lock.to_dict()
        return out

    @classmethod
    def from_dict(cls, payload: Dict[str, Any]) -> "LegSpec":
        return cls(
            side=payload.get("side"),
            opt_type=payload.get("opt_type"),
            qty_lots=payload.get("qty_lots"),
            expiry_mode=payload.get("expiry_mode"),
            strike_mode=payload.get("strike_mode"),
            strike_offset_points=payload.get("strike_offset_points"),
            target_premium=payload.get("target_premium"),
            stoploss_pct=payload.get("stoploss_pct"),
            profit_lock=payload.get("profit_lock"),
            reentry_enabled=payload.get("reentry_enabled", False),
            max_reentries=payload.get("max_reentries", 0),
        )


@dataclass(frozen=True)
class StrategySpec:
    name: str
    underlying_instrument_key: str
    start_date: dt.date
    end_date: dt.date
    candle_interval: str
    entry_time: dt.time
    exit_time: dt.time
    fill_model: str
    allow_partial_fills: bool
    latency_ms: int
    slippage_model: str
    slippage_bps: float = 0.0
    slippage_ticks: int = 0
    spread_bps: float = 0.0
    brokerage_profile: str = "india_options_default"
    starting_capital: float = 100000.0
    legs: tuple[LegSpec, ...] = field(default_factory=tuple)

    def __post_init__(self) -> None:
        name = str(self.name or "").strip()
        if not name:
            raise ValueError("StrategySpec.name must be non-empty")
        object.__setattr__(self, "name", name)

        underlying = str(self.underlying_instrument_key or "").strip()
        if not underlying:
            raise ValueError("StrategySpec.underlying_instrument_key must be non-empty")
        object.__setattr__(self, "underlying_instrument_key", underlying)

        if not isinstance(self.start_date, dt.date) or isinstance(self.start_date, dt.datetime):
            raise ValueError("StrategySpec.start_date must be a date")
        if not isinstance(self.end_date, dt.date) or isinstance(self.end_date, dt.datetime):
            raise ValueError("StrategySpec.end_date must be a date")
        if self.end_date < self.start_date:
            raise ValueError("StrategySpec.end_date must be >= start_date")

        interval = str(self.candle_interval or "").strip().lower()
        if interval not in ALLOWED_CANDLE_INTERVALS:
            raise ValueError(f"StrategySpec.candle_interval must be one of {sorted(ALLOWED_CANDLE_INTERVALS)}; got {self.candle_interval!r}")
        object.__setattr__(self, "candle_interval", interval)

        entry_time = _parse_hhmm(self.entry_time, "entry_time")
        exit_time = _parse_hhmm(self.exit_time, "exit_time")
        if entry_time >= exit_time:
            raise ValueError("StrategySpec.exit_time must be after entry_time")
        object.__setattr__(self, "entry_time", entry_time)
        object.__setattr__(self, "exit_time", exit_time)

        fill_model = str(self.fill_model or "").strip().lower()
        fill_model = _FILL_MODEL_ALIASES.get(fill_model, fill_model)
        if fill_model not in ALLOWED_FILL_MODELS:
            raise ValueError(f"StrategySpec.fill_model must be one of {sorted(ALLOWED_FILL_MODELS)}; got {self.fill_model!r}")
        object.__setattr__(self, "fill_model", fill_model)

        allow_partials = bool(self.allow_partial_fills)
        object.__setattr__(self, "allow_partial_fills", allow_partials)

        try:
            latency_ms = int(self.latency_ms or 0)
        except (TypeError, ValueError) as exc:
            raise ValueError("StrategySpec.latency_ms must be an int") from exc
        if latency_ms < 0:
            raise ValueError("StrategySpec.latency_ms must be >= 0")
        object.__setattr__(self, "latency_ms", latency_ms)

        slippage_model = str(self.slippage_model or "").strip().lower()
        if slippage_model not in ALLOWED_SLIPPAGE_MODELS:
            raise ValueError(f"StrategySpec.slippage_model must be one of {sorted(ALLOWED_SLIPPAGE_MODELS)}; got {self.slippage_model!r}")
        object.__setattr__(self, "slippage_model", slippage_model)

        try:
            slippage_bps = float(self.slippage_bps or 0.0)
        except (TypeError, ValueError) as exc:
            raise ValueError("StrategySpec.slippage_bps must be a number") from exc
        try:
            slippage_ticks = int(self.slippage_ticks or 0)
        except (TypeError, ValueError) as exc:
            raise ValueError("StrategySpec.slippage_ticks must be an int") from exc

        if slippage_model == "none":
            if slippage_bps != 0.0 or slippage_ticks != 0:
                raise ValueError("slippage_bps/slippage_ticks must be 0 when slippage_model=none")
        elif slippage_model == "bps":
            if slippage_bps <= 0:
                raise ValueError("slippage_bps must be > 0 when slippage_model=bps")
            if slippage_ticks != 0:
                raise ValueError("slippage_ticks must be 0 when slippage_model=bps")
        elif slippage_model == "ticks":
            if slippage_ticks <= 0:
                raise ValueError("slippage_ticks must be > 0 when slippage_model=ticks")
            if slippage_bps != 0.0:
                raise ValueError("slippage_bps must be 0 when slippage_model=ticks")
        object.__setattr__(self, "slippage_bps", float(slippage_bps))
        object.__setattr__(self, "slippage_ticks", int(slippage_ticks))

        try:
            spread_bps = float(self.spread_bps or 0.0)
        except (TypeError, ValueError) as exc:
            raise ValueError("StrategySpec.spread_bps must be a number") from exc
        if spread_bps < 0:
            raise ValueError("StrategySpec.spread_bps must be >= 0")
        object.__setattr__(self, "spread_bps", float(spread_bps))

        profile = str(self.brokerage_profile or "").strip()
        if not profile:
            raise ValueError("StrategySpec.brokerage_profile must be non-empty")
        object.__setattr__(self, "brokerage_profile", profile)

        try:
            starting_capital = float(self.starting_capital)
        except (TypeError, ValueError) as exc:
            raise ValueError("StrategySpec.starting_capital must be a number") from exc
        if starting_capital <= 0:
            raise ValueError("StrategySpec.starting_capital must be > 0")
        object.__setattr__(self, "starting_capital", float(starting_capital))

        legs = self.legs
        if legs is None:
            legs = tuple()
        if not isinstance(legs, tuple):
            legs = tuple(legs)
        for leg in legs:
            if not isinstance(leg, LegSpec):
                raise ValueError("StrategySpec.legs must contain LegSpec items")
        object.__setattr__(self, "legs", legs)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "name": self.name,
            "underlying_instrument_key": self.underlying_instrument_key,
            "start_date": self.start_date.isoformat(),
            "end_date": self.end_date.isoformat(),
            "candle_interval": self.candle_interval,
            "entry_time": _format_hhmm(self.entry_time),
            "exit_time": _format_hhmm(self.exit_time),
            "fill_model": self.fill_model,
            "allow_partial_fills": bool(self.allow_partial_fills),
            "latency_ms": int(self.latency_ms),
            "slippage_model": self.slippage_model,
            "slippage_bps": float(self.slippage_bps),
            "slippage_ticks": int(self.slippage_ticks),
            "spread_bps": float(self.spread_bps),
            "brokerage_profile": self.brokerage_profile,
            "starting_capital": float(self.starting_capital),
            "legs": [leg.to_dict() for leg in self.legs],
        }

    def to_json(self) -> str:
        return json.dumps(self.to_dict(), separators=(",", ":"), sort_keys=True)

    @classmethod
    def from_dict(cls, payload: Dict[str, Any]) -> "StrategySpec":
        legs_raw = payload.get("legs") or []
        legs = tuple(LegSpec.from_dict(item) if isinstance(item, dict) else item for item in legs_raw)
        return cls(
            name=payload.get("name"),
            underlying_instrument_key=payload.get("underlying_instrument_key"),
            start_date=_coerce_date(payload.get("start_date"), "start_date"),
            end_date=_coerce_date(payload.get("end_date"), "end_date"),
            candle_interval=payload.get("candle_interval"),
            entry_time=_parse_hhmm(payload.get("entry_time"), "entry_time"),
            exit_time=_parse_hhmm(payload.get("exit_time"), "exit_time"),
            fill_model=payload.get("fill_model", FILL_MODE_NEXT_TICK),
            allow_partial_fills=bool(payload.get("allow_partial_fills", False)),
            latency_ms=payload.get("latency_ms", 0),
            slippage_model=payload.get("slippage_model", "none"),
            slippage_bps=payload.get("slippage_bps", 0.0),
            slippage_ticks=payload.get("slippage_ticks", 0),
            spread_bps=payload.get("spread_bps", 0.0),
            brokerage_profile=payload.get("brokerage_profile", "india_options_default"),
            starting_capital=payload.get("starting_capital", 100000.0),
            legs=legs,
        )

    @classmethod
    def from_json(cls, raw: str) -> "StrategySpec":
        try:
            payload = json.loads(raw)
        except json.JSONDecodeError as exc:
            raise ValueError(f"Invalid StrategySpec JSON: {exc}") from exc
        if not isinstance(payload, dict):
            raise ValueError("StrategySpec JSON must be an object")
        return cls.from_dict(payload)


__all__ = [
    "ALLOWED_CANDLE_INTERVALS",
    "ALLOWED_EXPIRY_MODES",
    "ALLOWED_FILL_MODELS",
    "ALLOWED_OPT_TYPES",
    "ALLOWED_SIDES",
    "ALLOWED_SLIPPAGE_MODELS",
    "ALLOWED_STRIKE_MODES",
    "FILL_MODE_NEXT_TICK",
    "FILL_MODE_SAME_TICK",
    "LegSpec",
    "ProfitLockSpec",
    "StrategySpec",
]

