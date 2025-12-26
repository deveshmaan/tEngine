from __future__ import annotations

import datetime as dt
import json
import re
import uuid
from dataclasses import dataclass, field
from typing import Any, Dict, Iterable, Optional

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
ALLOWED_LEG_ROLES = {"main", "hedge"}

STOPLOSS_TYPE_PREMIUM_PCT = "PREMIUM_PCT"
STOPLOSS_TYPE_POINTS = "POINTS"
STOPLOSS_TYPE_MTM = "MTM"
_STOPLOSS_TYPE_ALIASES = {
    "PCT": STOPLOSS_TYPE_PREMIUM_PCT,
    "PERCENT": STOPLOSS_TYPE_PREMIUM_PCT,
    "PREMIUM%": STOPLOSS_TYPE_PREMIUM_PCT,
    "PREMIUM_PCT": STOPLOSS_TYPE_PREMIUM_PCT,
    "%": STOPLOSS_TYPE_PREMIUM_PCT,
    "POINT": STOPLOSS_TYPE_POINTS,
    "POINTS": STOPLOSS_TYPE_POINTS,
    "ABS": STOPLOSS_TYPE_POINTS,
    "ABS_POINTS": STOPLOSS_TYPE_POINTS,
    "MTM": STOPLOSS_TYPE_MTM,
}
ALLOWED_STOPLOSS_TYPES = {STOPLOSS_TYPE_PREMIUM_PCT, STOPLOSS_TYPE_POINTS, STOPLOSS_TYPE_MTM}
ALLOWED_PROFIT_TARGET_TYPES = set(ALLOWED_STOPLOSS_TYPES)
ALLOWED_TRAILING_TYPES = set(ALLOWED_STOPLOSS_TYPES)

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


def _parse_optional_hhmm(value: object, field_name: str) -> Optional[dt.time]:
    if value is None:
        return None
    if isinstance(value, dt.time):
        return _parse_hhmm(value, field_name)
    text = str(value).strip()
    if not text:
        return None
    return _parse_hhmm(text, field_name)


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
class StrategyRiskSpec:
    max_daily_loss_mtm: Optional[float] = None
    max_daily_profit_mtm: Optional[float] = None
    force_exit_time: Optional[dt.time] = None
    disable_entries_after_time: Optional[dt.time] = None
    max_concurrent_positions: Optional[int] = None
    max_trades_per_day: Optional[int] = None

    def __post_init__(self) -> None:
        if self.max_daily_loss_mtm is not None:
            try:
                loss = float(self.max_daily_loss_mtm)
            except (TypeError, ValueError) as exc:
                raise ValueError("strategy_risk.max_daily_loss_mtm must be a number") from exc
            if loss <= 0:
                raise ValueError("strategy_risk.max_daily_loss_mtm must be > 0")
            object.__setattr__(self, "max_daily_loss_mtm", float(loss))

        if self.max_daily_profit_mtm is not None:
            try:
                profit = float(self.max_daily_profit_mtm)
            except (TypeError, ValueError) as exc:
                raise ValueError("strategy_risk.max_daily_profit_mtm must be a number") from exc
            if profit <= 0:
                raise ValueError("strategy_risk.max_daily_profit_mtm must be > 0")
            object.__setattr__(self, "max_daily_profit_mtm", float(profit))

        force_exit_time = _parse_optional_hhmm(self.force_exit_time, "strategy_risk.force_exit_time")
        disable_entries_after_time = _parse_optional_hhmm(
            self.disable_entries_after_time, "strategy_risk.disable_entries_after_time"
        )
        object.__setattr__(self, "force_exit_time", force_exit_time)
        object.__setattr__(self, "disable_entries_after_time", disable_entries_after_time)

        if self.max_concurrent_positions is not None:
            try:
                max_pos = int(self.max_concurrent_positions)
            except (TypeError, ValueError) as exc:
                raise ValueError("strategy_risk.max_concurrent_positions must be an int") from exc
            if max_pos <= 0:
                raise ValueError("strategy_risk.max_concurrent_positions must be > 0")
            object.__setattr__(self, "max_concurrent_positions", int(max_pos))

        if self.max_trades_per_day is not None:
            try:
                max_trades = int(self.max_trades_per_day)
            except (TypeError, ValueError) as exc:
                raise ValueError("strategy_risk.max_trades_per_day must be an int") from exc
            if max_trades <= 0:
                raise ValueError("strategy_risk.max_trades_per_day must be > 0")
            object.__setattr__(self, "max_trades_per_day", int(max_trades))

    def to_dict(self) -> Dict[str, Any]:
        out: Dict[str, Any] = {}
        if self.max_daily_loss_mtm is not None:
            out["max_daily_loss_mtm"] = float(self.max_daily_loss_mtm)
        if self.max_daily_profit_mtm is not None:
            out["max_daily_profit_mtm"] = float(self.max_daily_profit_mtm)
        if self.force_exit_time is not None:
            out["force_exit_time"] = _format_hhmm(self.force_exit_time)
        if self.disable_entries_after_time is not None:
            out["disable_entries_after_time"] = _format_hhmm(self.disable_entries_after_time)
        if self.max_concurrent_positions is not None:
            out["max_concurrent_positions"] = int(self.max_concurrent_positions)
        if self.max_trades_per_day is not None:
            out["max_trades_per_day"] = int(self.max_trades_per_day)
        return out

    @classmethod
    def from_dict(cls, payload: Dict[str, Any]) -> "StrategyRiskSpec":
        return cls(
            max_daily_loss_mtm=payload.get("max_daily_loss_mtm"),
            max_daily_profit_mtm=payload.get("max_daily_profit_mtm"),
            force_exit_time=payload.get("force_exit_time"),
            disable_entries_after_time=payload.get("disable_entries_after_time"),
            max_concurrent_positions=payload.get("max_concurrent_positions"),
            max_trades_per_day=payload.get("max_trades_per_day"),
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
    stoploss_type: Optional[str] = None
    stoploss_value: Optional[float] = None
    profit_target_type: Optional[str] = None
    profit_target_value: Optional[float] = None
    trailing_enabled: bool = False
    trailing_type: Optional[str] = None
    trailing_trigger: Optional[float] = None
    trailing_step: Optional[float] = None
    profit_lock: Optional[ProfitLockSpec] = None
    reentry_enabled: bool = False
    max_reentries: int = 0
    cool_down_minutes: int = 0
    reentry_condition: Optional[str] = None

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

        stoploss_type = str(self.stoploss_type or "").strip().upper()
        stoploss_type = _STOPLOSS_TYPE_ALIASES.get(stoploss_type, stoploss_type)
        stoploss_value: Optional[float] = None
        stoploss_pct: Optional[float] = _normalize_pct(self.stoploss_pct, "stoploss_pct") if self.stoploss_pct is not None else None

        if stoploss_type:
            if stoploss_type not in ALLOWED_STOPLOSS_TYPES:
                raise ValueError(f"LegSpec.stoploss_type must be one of {sorted(ALLOWED_STOPLOSS_TYPES)}; got {self.stoploss_type!r}")
            raw_val = self.stoploss_value
            if raw_val is None:
                raise ValueError("LegSpec.stoploss_value is required when stoploss_type is set")
            if stoploss_type == STOPLOSS_TYPE_PREMIUM_PCT:
                stoploss_value = _normalize_pct(raw_val, "stoploss_value")
                stoploss_pct = stoploss_value
            else:
                try:
                    stoploss_value = float(raw_val)
                except (TypeError, ValueError) as exc:
                    raise ValueError("stoploss_value must be a number") from exc
                if stoploss_value <= 0:
                    raise ValueError("stoploss_value must be > 0")
                stoploss_pct = None
        else:
            stoploss_type = None
            if stoploss_pct is not None:
                stoploss_type = STOPLOSS_TYPE_PREMIUM_PCT
                stoploss_value = float(stoploss_pct)

        object.__setattr__(self, "stoploss_type", stoploss_type)
        object.__setattr__(self, "stoploss_value", stoploss_value)
        object.__setattr__(self, "stoploss_pct", stoploss_pct)

        profit_target_type = str(self.profit_target_type or "").strip().upper()
        profit_target_type = _STOPLOSS_TYPE_ALIASES.get(profit_target_type, profit_target_type)
        profit_target_value: Optional[float] = None
        if profit_target_type:
            if profit_target_type not in ALLOWED_PROFIT_TARGET_TYPES:
                raise ValueError(
                    f"LegSpec.profit_target_type must be one of {sorted(ALLOWED_PROFIT_TARGET_TYPES)}; got {self.profit_target_type!r}"
                )
            if self.profit_target_value is None:
                raise ValueError("LegSpec.profit_target_value is required when profit_target_type is set")
            if profit_target_type == STOPLOSS_TYPE_PREMIUM_PCT:
                profit_target_value = _normalize_pct(self.profit_target_value, "profit_target_value")
            else:
                try:
                    profit_target_value = float(self.profit_target_value)
                except (TypeError, ValueError) as exc:
                    raise ValueError("profit_target_value must be a number") from exc
                if profit_target_value <= 0:
                    raise ValueError("profit_target_value must be > 0")
        else:
            profit_target_type = None
        object.__setattr__(self, "profit_target_type", profit_target_type)
        object.__setattr__(self, "profit_target_value", profit_target_value)

        trailing_enabled = bool(self.trailing_enabled)
        trailing_type = str(self.trailing_type or "").strip().upper()
        trailing_type = _STOPLOSS_TYPE_ALIASES.get(trailing_type, trailing_type)
        trailing_trigger: Optional[float] = None
        trailing_step: Optional[float] = None
        if trailing_enabled:
            if not trailing_type:
                trailing_type = STOPLOSS_TYPE_POINTS
            if trailing_type not in ALLOWED_TRAILING_TYPES:
                raise ValueError(
                    f"LegSpec.trailing_type must be one of {sorted(ALLOWED_TRAILING_TYPES)}; got {self.trailing_type!r}"
                )
            if self.trailing_trigger is None or self.trailing_step is None:
                raise ValueError("LegSpec.trailing_trigger and trailing_step are required when trailing_enabled is true")
            if trailing_type == STOPLOSS_TYPE_PREMIUM_PCT:
                trailing_trigger = _normalize_pct(self.trailing_trigger, "trailing_trigger")
                trailing_step = _normalize_pct(self.trailing_step, "trailing_step")
            else:
                try:
                    trailing_trigger = float(self.trailing_trigger)
                    trailing_step = float(self.trailing_step)
                except (TypeError, ValueError) as exc:
                    raise ValueError("trailing_trigger/trailing_step must be numbers") from exc
                if trailing_trigger <= 0 or trailing_step <= 0:
                    raise ValueError("trailing_trigger and trailing_step must be > 0")
        else:
            trailing_type = None
        object.__setattr__(self, "trailing_enabled", trailing_enabled)
        object.__setattr__(self, "trailing_type", trailing_type)
        object.__setattr__(self, "trailing_trigger", trailing_trigger)
        object.__setattr__(self, "trailing_step", trailing_step)

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

        try:
            cool_down = int(self.cool_down_minutes or 0)
        except (TypeError, ValueError) as exc:
            raise ValueError("LegSpec.cool_down_minutes must be an int") from exc
        if cool_down < 0:
            raise ValueError("LegSpec.cool_down_minutes must be >= 0")
        object.__setattr__(self, "cool_down_minutes", int(cool_down))

        condition = str(self.reentry_condition or "").strip()
        if reentry_enabled and not condition:
            condition = "premium_returns_to_entry_zone"
        if not reentry_enabled:
            condition = ""
        object.__setattr__(self, "reentry_condition", condition or None)

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
        if self.stoploss_type is not None:
            out["stoploss_type"] = str(self.stoploss_type)
        if self.stoploss_value is not None:
            out["stoploss_value"] = float(self.stoploss_value)
        if self.profit_target_type is not None:
            out["profit_target_type"] = str(self.profit_target_type)
        if self.profit_target_value is not None:
            out["profit_target_value"] = float(self.profit_target_value)
        if self.trailing_enabled:
            out["trailing_enabled"] = True
            if self.trailing_type is not None:
                out["trailing_type"] = str(self.trailing_type)
            if self.trailing_trigger is not None:
                out["trailing_trigger"] = float(self.trailing_trigger)
            if self.trailing_step is not None:
                out["trailing_step"] = float(self.trailing_step)
        if self.profit_lock is not None:
            out["profit_lock"] = self.profit_lock.to_dict()
        if self.cool_down_minutes:
            out["cool_down_minutes"] = int(self.cool_down_minutes)
        if self.reentry_condition:
            out["reentry_condition"] = str(self.reentry_condition)
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
            stoploss_type=payload.get("stoploss_type"),
            stoploss_value=payload.get("stoploss_value"),
            profit_target_type=payload.get("profit_target_type"),
            profit_target_value=payload.get("profit_target_value"),
            trailing_enabled=payload.get("trailing_enabled", False),
            trailing_type=payload.get("trailing_type"),
            trailing_trigger=payload.get("trailing_trigger"),
            trailing_step=payload.get("trailing_step"),
            profit_lock=payload.get("profit_lock"),
            reentry_enabled=payload.get("reentry_enabled", False),
            max_reentries=payload.get("max_reentries", 0),
            cool_down_minutes=payload.get("cool_down_minutes", 0),
            reentry_condition=payload.get("reentry_condition"),
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
    strategy_risk: Optional[StrategyRiskSpec] = None

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

        risk = self.strategy_risk
        if risk is not None and not isinstance(risk, StrategyRiskSpec):
            if isinstance(risk, dict):
                risk = StrategyRiskSpec.from_dict(risk)
            else:
                raise ValueError("StrategySpec.strategy_risk must be StrategyRiskSpec or dict")
        object.__setattr__(self, "strategy_risk", risk)

    def to_dict(self) -> Dict[str, Any]:
        out = {
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
        if self.strategy_risk is not None:
            out["strategy_risk"] = self.strategy_risk.to_dict()
        return out

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
            strategy_risk=payload.get("strategy_risk"),
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


# ---------------------------------------------------------------------------
# BacktestRunSpec v2 (builder / persistence friendly)


def _short_uuid() -> str:
    return uuid.uuid4().hex[:8]


@dataclass(frozen=True)
class BacktestConfig:
    underlying_instrument_key: str
    start_date: dt.date
    end_date: dt.date
    interval: str
    entry_time: dt.time
    exit_time: dt.time
    timezone: str = "Asia/Kolkata"
    starting_capital: float = 100000.0
    brokerage_profile: str = "india_options_default"

    def __post_init__(self) -> None:
        underlying = str(self.underlying_instrument_key or "").strip()
        if not underlying:
            raise ValueError("BacktestConfig.underlying_instrument_key must be non-empty")
        object.__setattr__(self, "underlying_instrument_key", underlying)

        if not isinstance(self.start_date, dt.date) or isinstance(self.start_date, dt.datetime):
            raise ValueError("BacktestConfig.start_date must be a date")
        if not isinstance(self.end_date, dt.date) or isinstance(self.end_date, dt.datetime):
            raise ValueError("BacktestConfig.end_date must be a date")
        if self.end_date < self.start_date:
            raise ValueError("BacktestConfig.end_date must be >= start_date")

        interval = str(self.interval or "").strip().lower()
        if interval not in ALLOWED_CANDLE_INTERVALS:
            raise ValueError(f"BacktestConfig.interval must be one of {sorted(ALLOWED_CANDLE_INTERVALS)}; got {self.interval!r}")
        object.__setattr__(self, "interval", interval)

        entry_time = _parse_hhmm(self.entry_time, "BacktestConfig.entry_time")
        exit_time = _parse_hhmm(self.exit_time, "BacktestConfig.exit_time")
        if entry_time >= exit_time:
            raise ValueError("BacktestConfig.exit_time must be after entry_time")
        object.__setattr__(self, "entry_time", entry_time)
        object.__setattr__(self, "exit_time", exit_time)

        timezone = str(self.timezone or "").strip()
        if not timezone:
            timezone = "Asia/Kolkata"
        object.__setattr__(self, "timezone", timezone)

        try:
            starting_capital = float(self.starting_capital)
        except (TypeError, ValueError) as exc:
            raise ValueError("BacktestConfig.starting_capital must be a number") from exc
        if starting_capital <= 0:
            raise ValueError("BacktestConfig.starting_capital must be > 0")
        object.__setattr__(self, "starting_capital", float(starting_capital))

        profile = str(self.brokerage_profile or "").strip()
        if not profile:
            profile = "india_options_default"
        object.__setattr__(self, "brokerage_profile", profile)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "underlying_instrument_key": self.underlying_instrument_key,
            "start_date": self.start_date.isoformat(),
            "end_date": self.end_date.isoformat(),
            "interval": self.interval,
            "entry_time": _format_hhmm(self.entry_time),
            "exit_time": _format_hhmm(self.exit_time),
            "timezone": self.timezone,
            "starting_capital": float(self.starting_capital),
            "brokerage_profile": self.brokerage_profile,
        }

    @classmethod
    def from_dict(cls, payload: Dict[str, Any]) -> "BacktestConfig":
        return cls(
            underlying_instrument_key=payload.get("underlying_instrument_key"),
            start_date=_coerce_date(payload.get("start_date"), "start_date"),
            end_date=_coerce_date(payload.get("end_date"), "end_date"),
            interval=payload.get("interval") or payload.get("candle_interval"),
            entry_time=_parse_hhmm(payload.get("entry_time"), "entry_time"),
            exit_time=_parse_hhmm(payload.get("exit_time"), "exit_time"),
            timezone=payload.get("timezone") or "Asia/Kolkata",
            starting_capital=payload.get("starting_capital", 100000.0),
            brokerage_profile=payload.get("brokerage_profile", "india_options_default"),
        )


@dataclass(frozen=True)
class ExpirySelectorSpec:
    mode: str
    expiry_date: Optional[dt.date] = None

    def __post_init__(self) -> None:
        mode = str(self.mode or "").strip().upper()
        if mode not in ALLOWED_EXPIRY_MODES:
            raise ValueError(f"ExpirySelectorSpec.mode must be one of {sorted(ALLOWED_EXPIRY_MODES)}; got {self.mode!r}")
        object.__setattr__(self, "mode", mode)

        expiry_date = self.expiry_date
        if expiry_date is not None and not isinstance(expiry_date, dt.date):
            expiry_date = _coerce_date(expiry_date, "expiry_date")
        object.__setattr__(self, "expiry_date", expiry_date)

    def to_dict(self) -> Dict[str, Any]:
        out: Dict[str, Any] = {"mode": self.mode}
        if self.expiry_date is not None:
            out["expiry_date"] = self.expiry_date.isoformat()
        return out

    @classmethod
    def from_dict(cls, payload: Dict[str, Any]) -> "ExpirySelectorSpec":
        expiry_date = payload.get("expiry_date")
        return cls(
            mode=payload.get("mode") or payload.get("expiry_mode"),
            expiry_date=_coerce_date(expiry_date, "expiry_date") if expiry_date else None,
        )


@dataclass(frozen=True)
class StrikeSelectorSpec:
    mode: str
    offset_points: Optional[int] = None
    target_premium: Optional[float] = None

    def __post_init__(self) -> None:
        mode = str(self.mode or "").strip().upper()
        if mode not in ALLOWED_STRIKE_MODES:
            raise ValueError(f"StrikeSelectorSpec.mode must be one of {sorted(ALLOWED_STRIKE_MODES)}; got {self.mode!r}")
        object.__setattr__(self, "mode", mode)

        offset_points = self.offset_points
        if offset_points is not None:
            try:
                offset_points = int(offset_points)
            except (TypeError, ValueError) as exc:
                raise ValueError("StrikeSelectorSpec.offset_points must be an int") from exc

        target_premium = self.target_premium
        if target_premium is not None:
            try:
                target_premium = float(target_premium)
            except (TypeError, ValueError) as exc:
                raise ValueError("StrikeSelectorSpec.target_premium must be a number") from exc
            if target_premium <= 0:
                raise ValueError("StrikeSelectorSpec.target_premium must be > 0")

        if mode == "ATM":
            offset_points = None
            target_premium = None
        elif mode == "ATM_OFFSET":
            if offset_points is None:
                raise ValueError("StrikeSelectorSpec.offset_points is required for mode=ATM_OFFSET")
            target_premium = None
        elif mode == "TARGET_PREMIUM":
            if target_premium is None:
                raise ValueError("StrikeSelectorSpec.target_premium is required for mode=TARGET_PREMIUM")
            offset_points = None
        object.__setattr__(self, "offset_points", offset_points)
        object.__setattr__(self, "target_premium", target_premium)

    def to_dict(self) -> Dict[str, Any]:
        out: Dict[str, Any] = {"mode": self.mode}
        if self.offset_points is not None:
            out["offset_points"] = int(self.offset_points)
        if self.target_premium is not None:
            out["target_premium"] = float(self.target_premium)
        return out

    @classmethod
    def from_dict(cls, payload: Dict[str, Any]) -> "StrikeSelectorSpec":
        return cls(
            mode=payload.get("mode") or payload.get("strike_mode"),
            offset_points=payload.get("offset_points") or payload.get("strike_offset_points"),
            target_premium=payload.get("target_premium"),
        )


@dataclass(frozen=True)
class OptionLegSpec:
    leg_id: str
    side: str
    option_type: str
    qty: int
    expiry_selector: ExpirySelectorSpec
    strike_selector: StrikeSelectorSpec
    leg_role: str = "main"

    def __post_init__(self) -> None:
        leg_id = str(self.leg_id or "").strip()
        if not leg_id:
            raise ValueError("OptionLegSpec.leg_id must be non-empty")
        object.__setattr__(self, "leg_id", leg_id)

        role = str(self.leg_role or "main").strip().lower()
        if role not in ALLOWED_LEG_ROLES:
            raise ValueError(f"OptionLegSpec.leg_role must be one of {sorted(ALLOWED_LEG_ROLES)}; got {self.leg_role!r}")
        object.__setattr__(self, "leg_role", role)

        side = str(self.side or "").strip().upper()
        if side not in ALLOWED_SIDES:
            raise ValueError(f"OptionLegSpec.side must be one of {sorted(ALLOWED_SIDES)}; got {self.side!r}")
        object.__setattr__(self, "side", side)

        opt_type = str(self.option_type or "").strip().upper()
        if opt_type not in ALLOWED_OPT_TYPES:
            raise ValueError(f"OptionLegSpec.option_type must be one of {sorted(ALLOWED_OPT_TYPES)}; got {self.option_type!r}")
        object.__setattr__(self, "option_type", opt_type)

        try:
            qty = int(self.qty)
        except (TypeError, ValueError) as exc:
            raise ValueError("OptionLegSpec.qty must be an int") from exc
        if qty <= 0:
            raise ValueError("OptionLegSpec.qty must be > 0")
        object.__setattr__(self, "qty", qty)

        expiry_selector = self.expiry_selector
        if isinstance(expiry_selector, dict):
            expiry_selector = ExpirySelectorSpec.from_dict(expiry_selector)
        if not isinstance(expiry_selector, ExpirySelectorSpec):
            raise ValueError("OptionLegSpec.expiry_selector must be ExpirySelectorSpec or dict")
        object.__setattr__(self, "expiry_selector", expiry_selector)

        strike_selector = self.strike_selector
        if isinstance(strike_selector, dict):
            strike_selector = StrikeSelectorSpec.from_dict(strike_selector)
        if not isinstance(strike_selector, StrikeSelectorSpec):
            raise ValueError("OptionLegSpec.strike_selector must be StrikeSelectorSpec or dict")
        object.__setattr__(self, "strike_selector", strike_selector)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "leg_id": self.leg_id,
            "leg_role": self.leg_role,
            "side": self.side,
            "option_type": self.option_type,
            "qty": int(self.qty),
            "expiry_selector": self.expiry_selector.to_dict(),
            "strike_selector": self.strike_selector.to_dict(),
        }

    @classmethod
    def from_dict(cls, payload: Dict[str, Any]) -> "OptionLegSpec":
        return cls(
            leg_id=payload.get("leg_id") or _short_uuid(),
            leg_role=payload.get("leg_role") or payload.get("role") or "main",
            side=payload.get("side"),
            option_type=payload.get("option_type") or payload.get("opt_type"),
            qty=payload.get("qty") or payload.get("qty_lots"),
            expiry_selector=payload.get("expiry_selector") or payload.get("expiry") or {"mode": payload.get("expiry_mode")},
            strike_selector=payload.get("strike_selector") or payload.get("strike") or {"mode": payload.get("strike_mode")},
        )


@dataclass(frozen=True)
class LegRiskRuleSpec:
    leg_id: str
    stoploss_type: Optional[str] = None
    stoploss_value: Optional[float] = None
    profit_target_type: Optional[str] = None
    profit_target_value: Optional[float] = None
    trailing_enabled: bool = False
    trailing_type: Optional[str] = None
    trailing_trigger: Optional[float] = None
    trailing_step: Optional[float] = None
    profit_lock: Optional[ProfitLockSpec] = None
    reentry_enabled: bool = False
    max_reentries: int = 0
    cool_down_minutes: int = 0
    reentry_condition: Optional[str] = None

    def __post_init__(self) -> None:
        leg_id = str(self.leg_id or "").strip()
        if not leg_id:
            raise ValueError("LegRiskRuleSpec.leg_id must be non-empty")
        object.__setattr__(self, "leg_id", leg_id)

        def _norm_type(raw: object, field_name: str) -> Optional[str]:
            text = str(raw or "").strip().upper()
            if not text or text == "(NONE)":
                return None
            return _STOPLOSS_TYPE_ALIASES.get(text, text)

        stoploss_type = _norm_type(self.stoploss_type, "stoploss_type")
        stoploss_value = self.stoploss_value
        if stoploss_type is not None:
            if stoploss_type not in ALLOWED_STOPLOSS_TYPES:
                raise ValueError(f"LegRiskRuleSpec.stoploss_type must be one of {sorted(ALLOWED_STOPLOSS_TYPES)}; got {self.stoploss_type!r}")
            if stoploss_value is None:
                raise ValueError("LegRiskRuleSpec.stoploss_value is required when stoploss_type is set")
            if stoploss_type == STOPLOSS_TYPE_PREMIUM_PCT:
                stoploss_value = _normalize_pct(stoploss_value, "stoploss_value")
            else:
                try:
                    stoploss_value = float(stoploss_value)
                except (TypeError, ValueError) as exc:
                    raise ValueError("stoploss_value must be a number") from exc
                if stoploss_value <= 0:
                    raise ValueError("stoploss_value must be > 0")
        object.__setattr__(self, "stoploss_type", stoploss_type)
        object.__setattr__(self, "stoploss_value", stoploss_value if stoploss_type is not None else None)

        profit_target_type = _norm_type(self.profit_target_type, "profit_target_type")
        profit_target_value = self.profit_target_value
        if profit_target_type is not None:
            if profit_target_type not in ALLOWED_PROFIT_TARGET_TYPES:
                raise ValueError(
                    f"LegRiskRuleSpec.profit_target_type must be one of {sorted(ALLOWED_PROFIT_TARGET_TYPES)}; got {self.profit_target_type!r}"
                )
            if profit_target_value is None:
                raise ValueError("LegRiskRuleSpec.profit_target_value is required when profit_target_type is set")
            if profit_target_type == STOPLOSS_TYPE_PREMIUM_PCT:
                profit_target_value = _normalize_pct(profit_target_value, "profit_target_value")
            else:
                try:
                    profit_target_value = float(profit_target_value)
                except (TypeError, ValueError) as exc:
                    raise ValueError("profit_target_value must be a number") from exc
                if profit_target_value <= 0:
                    raise ValueError("profit_target_value must be > 0")
        object.__setattr__(self, "profit_target_type", profit_target_type)
        object.__setattr__(self, "profit_target_value", profit_target_value if profit_target_type is not None else None)

        trailing_enabled = bool(self.trailing_enabled)
        object.__setattr__(self, "trailing_enabled", trailing_enabled)
        trailing_type = _norm_type(self.trailing_type, "trailing_type")
        trailing_trigger = self.trailing_trigger
        trailing_step = self.trailing_step
        if trailing_enabled:
            if trailing_type is None:
                trailing_type = STOPLOSS_TYPE_POINTS
            if trailing_type not in ALLOWED_TRAILING_TYPES:
                raise ValueError(f"LegRiskRuleSpec.trailing_type must be one of {sorted(ALLOWED_TRAILING_TYPES)}; got {self.trailing_type!r}")
            if trailing_trigger is None or trailing_step is None:
                raise ValueError("LegRiskRuleSpec.trailing_trigger and trailing_step are required when trailing_enabled is true")
            if trailing_type == STOPLOSS_TYPE_PREMIUM_PCT:
                trailing_trigger = _normalize_pct(trailing_trigger, "trailing_trigger")
                trailing_step = _normalize_pct(trailing_step, "trailing_step")
            else:
                try:
                    trailing_trigger = float(trailing_trigger)
                    trailing_step = float(trailing_step)
                except (TypeError, ValueError) as exc:
                    raise ValueError("trailing_trigger/trailing_step must be numbers") from exc
                if trailing_trigger <= 0 or trailing_step <= 0:
                    raise ValueError("trailing_trigger/trailing_step must be > 0")
        else:
            trailing_type = None
            trailing_trigger = None
            trailing_step = None
        object.__setattr__(self, "trailing_type", trailing_type)
        object.__setattr__(self, "trailing_trigger", trailing_trigger)
        object.__setattr__(self, "trailing_step", trailing_step)

        profit_lock = self.profit_lock
        if profit_lock is not None and not isinstance(profit_lock, ProfitLockSpec):
            if isinstance(profit_lock, dict):
                profit_lock = ProfitLockSpec.from_dict(profit_lock)
            else:
                raise ValueError("LegRiskRuleSpec.profit_lock must be ProfitLockSpec or dict")
        object.__setattr__(self, "profit_lock", profit_lock)

        reentry_enabled = bool(self.reentry_enabled)
        object.__setattr__(self, "reentry_enabled", reentry_enabled)

        try:
            max_reentries = int(self.max_reentries or 0)
        except (TypeError, ValueError) as exc:
            raise ValueError("LegRiskRuleSpec.max_reentries must be an int") from exc
        if max_reentries < 0:
            raise ValueError("LegRiskRuleSpec.max_reentries must be >= 0")
        if reentry_enabled and max_reentries <= 0:
            raise ValueError("LegRiskRuleSpec.max_reentries must be > 0 when reentry_enabled is true")
        if not reentry_enabled:
            max_reentries = 0
        object.__setattr__(self, "max_reentries", max_reentries)

        try:
            cool_down = int(self.cool_down_minutes or 0)
        except (TypeError, ValueError) as exc:
            raise ValueError("LegRiskRuleSpec.cool_down_minutes must be an int") from exc
        if cool_down < 0:
            raise ValueError("LegRiskRuleSpec.cool_down_minutes must be >= 0")
        if not reentry_enabled:
            cool_down = 0
        object.__setattr__(self, "cool_down_minutes", cool_down)

        condition = str(self.reentry_condition or "").strip() or None
        if reentry_enabled and not condition:
            condition = "premium_returns_to_entry_zone"
        if not reentry_enabled:
            condition = None
        object.__setattr__(self, "reentry_condition", condition)

    def to_dict(self) -> Dict[str, Any]:
        out: Dict[str, Any] = {"leg_id": self.leg_id}
        if self.stoploss_type is not None:
            out["stoploss_type"] = self.stoploss_type
            out["stoploss_value"] = float(self.stoploss_value or 0.0)
        if self.profit_target_type is not None:
            out["profit_target_type"] = self.profit_target_type
            out["profit_target_value"] = float(self.profit_target_value or 0.0)
        if self.trailing_enabled:
            out["trailing_enabled"] = True
            if self.trailing_type is not None:
                out["trailing_type"] = self.trailing_type
            if self.trailing_trigger is not None:
                out["trailing_trigger"] = float(self.trailing_trigger)
            if self.trailing_step is not None:
                out["trailing_step"] = float(self.trailing_step)
        if self.profit_lock is not None:
            out["profit_lock"] = self.profit_lock.to_dict()
        if self.reentry_enabled:
            out["reentry_enabled"] = True
            out["max_reentries"] = int(self.max_reentries)
            if self.cool_down_minutes:
                out["cool_down_minutes"] = int(self.cool_down_minutes)
            if self.reentry_condition:
                out["reentry_condition"] = str(self.reentry_condition)
        return out

    @classmethod
    def from_dict(cls, payload: Dict[str, Any]) -> "LegRiskRuleSpec":
        return cls(
            leg_id=payload.get("leg_id"),
            stoploss_type=payload.get("stoploss_type"),
            stoploss_value=payload.get("stoploss_value"),
            profit_target_type=payload.get("profit_target_type"),
            profit_target_value=payload.get("profit_target_value"),
            trailing_enabled=payload.get("trailing_enabled", False),
            trailing_type=payload.get("trailing_type"),
            trailing_trigger=payload.get("trailing_trigger"),
            trailing_step=payload.get("trailing_step"),
            profit_lock=payload.get("profit_lock"),
            reentry_enabled=payload.get("reentry_enabled", False),
            max_reentries=payload.get("max_reentries", 0),
            cool_down_minutes=payload.get("cool_down_minutes", 0),
            reentry_condition=payload.get("reentry_condition"),
        )


@dataclass(frozen=True)
class RiskRuleSpec:
    per_leg: tuple[LegRiskRuleSpec, ...] = field(default_factory=tuple)
    strategy: Optional[StrategyRiskSpec] = None

    def __post_init__(self) -> None:
        per_leg = self.per_leg
        if per_leg is None:
            per_leg = tuple()
        if not isinstance(per_leg, tuple):
            per_leg = tuple(per_leg)
        for item in per_leg:
            if not isinstance(item, LegRiskRuleSpec):
                raise ValueError("RiskRuleSpec.per_leg must contain LegRiskRuleSpec items")
        object.__setattr__(self, "per_leg", per_leg)

        strategy = self.strategy
        if strategy is not None and not isinstance(strategy, StrategyRiskSpec):
            if isinstance(strategy, dict):
                strategy = StrategyRiskSpec.from_dict(strategy)
            else:
                raise ValueError("RiskRuleSpec.strategy must be StrategyRiskSpec or dict")
        object.__setattr__(self, "strategy", strategy)

    def to_dict(self) -> Dict[str, Any]:
        out: Dict[str, Any] = {}
        if self.per_leg:
            out["per_leg"] = [item.to_dict() for item in self.per_leg]
        if self.strategy is not None:
            out["strategy"] = self.strategy.to_dict()
        return out

    @classmethod
    def from_dict(cls, payload: Dict[str, Any]) -> "RiskRuleSpec":
        per_leg_raw = payload.get("per_leg") or payload.get("legs") or []
        per_leg = tuple(
            LegRiskRuleSpec.from_dict(item) if isinstance(item, dict) else item for item in (per_leg_raw or [])
        )
        return cls(per_leg=per_leg, strategy=payload.get("strategy"))


@dataclass(frozen=True)
class ExecutionModelSpec:
    fill_model: str = FILL_MODE_NEXT_TICK
    latency_ms: int = 0
    allow_partial_fills: bool = False
    fill_price_rule: str = "auto"
    spread_bps: float = 0.0
    slippage_model: str = "none"
    slippage_bps: float = 0.0
    slippage_ticks: int = 0
    participation_rate: float = 1.0

    def __post_init__(self) -> None:
        fill_model = str(self.fill_model or "").strip().lower()
        fill_model = _FILL_MODEL_ALIASES.get(fill_model, fill_model)
        if fill_model not in ALLOWED_FILL_MODELS:
            raise ValueError(f"ExecutionModelSpec.fill_model must be one of {sorted(ALLOWED_FILL_MODELS)}; got {self.fill_model!r}")
        object.__setattr__(self, "fill_model", fill_model)

        try:
            latency_ms = int(self.latency_ms or 0)
        except (TypeError, ValueError) as exc:
            raise ValueError("ExecutionModelSpec.latency_ms must be an int") from exc
        if latency_ms < 0:
            raise ValueError("ExecutionModelSpec.latency_ms must be >= 0")
        object.__setattr__(self, "latency_ms", latency_ms)

        object.__setattr__(self, "allow_partial_fills", bool(self.allow_partial_fills))

        fill_price_rule = str(self.fill_price_rule or "auto").strip().lower()
        if not fill_price_rule:
            fill_price_rule = "auto"
        object.__setattr__(self, "fill_price_rule", fill_price_rule)

        try:
            spread_bps = float(self.spread_bps or 0.0)
        except (TypeError, ValueError) as exc:
            raise ValueError("ExecutionModelSpec.spread_bps must be a number") from exc
        if spread_bps < 0:
            raise ValueError("ExecutionModelSpec.spread_bps must be >= 0")
        object.__setattr__(self, "spread_bps", float(spread_bps))

        slippage_model = str(self.slippage_model or "").strip().lower()
        if slippage_model not in ALLOWED_SLIPPAGE_MODELS:
            raise ValueError(
                f"ExecutionModelSpec.slippage_model must be one of {sorted(ALLOWED_SLIPPAGE_MODELS)}; got {self.slippage_model!r}"
            )
        object.__setattr__(self, "slippage_model", slippage_model)

        try:
            slippage_bps = float(self.slippage_bps or 0.0)
        except (TypeError, ValueError) as exc:
            raise ValueError("ExecutionModelSpec.slippage_bps must be a number") from exc
        try:
            slippage_ticks = int(self.slippage_ticks or 0)
        except (TypeError, ValueError) as exc:
            raise ValueError("ExecutionModelSpec.slippage_ticks must be an int") from exc

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
            participation_rate = float(self.participation_rate if self.participation_rate is not None else 1.0)
        except (TypeError, ValueError) as exc:
            raise ValueError("ExecutionModelSpec.participation_rate must be a number") from exc
        if participation_rate <= 0 or participation_rate > 1.0:
            raise ValueError("ExecutionModelSpec.participation_rate must be in (0, 1]")
        object.__setattr__(self, "participation_rate", float(participation_rate))

    def to_dict(self) -> Dict[str, Any]:
        return {
            "fill_model": self.fill_model,
            "latency_ms": int(self.latency_ms),
            "allow_partial_fills": bool(self.allow_partial_fills),
            "fill_price_rule": self.fill_price_rule,
            "spread_bps": float(self.spread_bps),
            "slippage_model": self.slippage_model,
            "slippage_bps": float(self.slippage_bps),
            "slippage_ticks": int(self.slippage_ticks),
            "participation_rate": float(self.participation_rate),
        }

    @classmethod
    def from_dict(cls, payload: Dict[str, Any]) -> "ExecutionModelSpec":
        return cls(
            fill_model=payload.get("fill_model", FILL_MODE_NEXT_TICK),
            latency_ms=payload.get("latency_ms", 0),
            allow_partial_fills=payload.get("allow_partial_fills", False),
            fill_price_rule=payload.get("fill_price_rule", "auto"),
            spread_bps=payload.get("spread_bps", 0.0),
            slippage_model=payload.get("slippage_model", "none"),
            slippage_bps=payload.get("slippage_bps", 0.0),
            slippage_ticks=payload.get("slippage_ticks", 0),
            participation_rate=payload.get("participation_rate", 1.0),
        )


@dataclass(frozen=True)
class BacktestRunSpec:
    config: BacktestConfig
    legs: tuple[OptionLegSpec, ...] = field(default_factory=tuple)
    risk: RiskRuleSpec = field(default_factory=RiskRuleSpec)
    execution_model: ExecutionModelSpec = field(default_factory=ExecutionModelSpec)
    name: str = "BacktestRunSpec"
    tags: tuple[str, ...] = field(default_factory=tuple)
    spec_version: int = 2

    def __post_init__(self) -> None:
        config = self.config
        if isinstance(config, dict):
            config = BacktestConfig.from_dict(config)
        if not isinstance(config, BacktestConfig):
            raise ValueError("BacktestRunSpec.config must be BacktestConfig or dict")
        object.__setattr__(self, "config", config)

        legs = self.legs
        if legs is None:
            legs = tuple()
        if not isinstance(legs, tuple):
            legs = tuple(legs)
        norm_legs: list[OptionLegSpec] = []
        seen: set[str] = set()
        for item in legs:
            if isinstance(item, dict):
                item = OptionLegSpec.from_dict(item)
            if not isinstance(item, OptionLegSpec):
                raise ValueError("BacktestRunSpec.legs must contain OptionLegSpec items")
            if item.leg_id in seen:
                raise ValueError(f"Duplicate leg_id in BacktestRunSpec.legs: {item.leg_id}")
            seen.add(item.leg_id)
            norm_legs.append(item)
        object.__setattr__(self, "legs", tuple(norm_legs))

        risk = self.risk
        if isinstance(risk, dict):
            risk = RiskRuleSpec.from_dict(risk)
        if not isinstance(risk, RiskRuleSpec):
            raise ValueError("BacktestRunSpec.risk must be RiskRuleSpec or dict")
        object.__setattr__(self, "risk", risk)

        execution_model = self.execution_model
        if isinstance(execution_model, dict):
            execution_model = ExecutionModelSpec.from_dict(execution_model)
        if not isinstance(execution_model, ExecutionModelSpec):
            raise ValueError("BacktestRunSpec.execution_model must be ExecutionModelSpec or dict")
        object.__setattr__(self, "execution_model", execution_model)

        name = str(self.name or "").strip()
        if not name:
            name = "BacktestRunSpec"
        object.__setattr__(self, "name", name)

        tags = self.tags
        if tags is None:
            tags = tuple()
        if not isinstance(tags, tuple):
            tags = tuple(str(t).strip() for t in tags if str(t).strip())
        else:
            tags = tuple(str(t).strip() for t in tags if str(t).strip())
        object.__setattr__(self, "tags", tags)

        try:
            spec_version = int(self.spec_version)
        except (TypeError, ValueError) as exc:
            raise ValueError("BacktestRunSpec.spec_version must be an int") from exc
        if spec_version <= 0:
            raise ValueError("BacktestRunSpec.spec_version must be > 0")
        object.__setattr__(self, "spec_version", spec_version)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "type": "BacktestRunSpec",
            "spec_version": int(self.spec_version),
            "name": self.name,
            "tags": list(self.tags),
            "config": self.config.to_dict(),
            "legs": [leg.to_dict() for leg in self.legs],
            "risk": self.risk.to_dict(),
            "execution_model": self.execution_model.to_dict(),
        }

    def to_json(self) -> str:
        return json.dumps(self.to_dict(), separators=(",", ":"), sort_keys=True)

    @classmethod
    def from_dict(cls, payload: Dict[str, Any]) -> "BacktestRunSpec":
        config = payload.get("config") or {}
        legs_raw = payload.get("legs") or []
        risk = payload.get("risk") or {}
        execution = payload.get("execution_model") or payload.get("execution") or {}
        metadata = payload.get("metadata")
        meta_name: Optional[str] = None
        meta_tags: object = ()
        if isinstance(metadata, dict):
            meta_name = metadata.get("name")
            meta_tags = metadata.get("tags") or ()
        return cls(
            config=config,
            legs=tuple(legs_raw) if isinstance(legs_raw, (list, tuple)) else tuple(),
            risk=risk,
            execution_model=execution,
            name=payload.get("name") or meta_name or "BacktestRunSpec",
            tags=tuple(payload.get("tags") or meta_tags or ()),
            spec_version=payload.get("spec_version", 2),
        )

    @classmethod
    def from_json(cls, raw: str) -> "BacktestRunSpec":
        try:
            payload = json.loads(raw)
        except json.JSONDecodeError as exc:
            raise ValueError(f"Invalid BacktestRunSpec JSON: {exc}") from exc
        if not isinstance(payload, dict):
            raise ValueError("BacktestRunSpec JSON must be an object")
        return cls.from_dict(payload)

    def to_strategy_spec(self) -> StrategySpec:
        """
        Convert to the legacy `StrategySpec` consumed by `MultiLegSpecStrategy`.

        Note: `leg_id` is not preserved in `StrategySpec` (it is kept in BacktestRunSpec).
        """

        per_leg = {r.leg_id: r for r in (self.risk.per_leg or ())}

        legs: list[LegSpec] = []
        for leg in self.legs:
            risk = per_leg.get(leg.leg_id)
            legs.append(
                LegSpec(
                    side=leg.side,
                    opt_type=leg.option_type,
                    qty_lots=int(leg.qty),
                    expiry_mode=leg.expiry_selector.mode,
                    strike_mode=leg.strike_selector.mode,
                    strike_offset_points=leg.strike_selector.offset_points,
                    target_premium=leg.strike_selector.target_premium,
                    stoploss_type=(risk.stoploss_type if risk else None),
                    stoploss_value=(risk.stoploss_value if risk else None),
                    profit_target_type=(risk.profit_target_type if risk else None),
                    profit_target_value=(risk.profit_target_value if risk else None),
                    trailing_enabled=bool(risk.trailing_enabled) if risk else False,
                    trailing_type=(risk.trailing_type if risk else None),
                    trailing_trigger=(risk.trailing_trigger if risk else None),
                    trailing_step=(risk.trailing_step if risk else None),
                    profit_lock=(risk.profit_lock if risk else None),
                    reentry_enabled=bool(risk.reentry_enabled) if risk else False,
                    max_reentries=int(risk.max_reentries) if risk else 0,
                    cool_down_minutes=int(risk.cool_down_minutes) if risk else 0,
                    reentry_condition=(risk.reentry_condition if risk else None),
                )
            )

        return StrategySpec(
            name=str(self.name),
            underlying_instrument_key=self.config.underlying_instrument_key,
            start_date=self.config.start_date,
            end_date=self.config.end_date,
            candle_interval=str(self.config.interval),
            entry_time=self.config.entry_time,
            exit_time=self.config.exit_time,
            fill_model=str(self.execution_model.fill_model),
            allow_partial_fills=bool(self.execution_model.allow_partial_fills),
            latency_ms=int(self.execution_model.latency_ms),
            slippage_model=str(self.execution_model.slippage_model),
            slippage_bps=float(self.execution_model.slippage_bps),
            slippage_ticks=int(self.execution_model.slippage_ticks),
            spread_bps=float(self.execution_model.spread_bps),
            brokerage_profile=str(self.config.brokerage_profile),
            starting_capital=float(self.config.starting_capital),
            legs=tuple(legs),
            strategy_risk=self.risk.strategy,
        )

    def to_execution_config(self) -> "ExecutionConfig":
        from engine.backtest.execution_config import ExecutionConfig  # local import to avoid cycles

        return ExecutionConfig(
            fill_model=str(self.execution_model.fill_model),
            latency_ms=int(self.execution_model.latency_ms),
            allow_partial_fills=bool(self.execution_model.allow_partial_fills),
        )

    @classmethod
    def from_strategy_spec(
        cls,
        spec: StrategySpec,
        *,
        leg_ids: Optional[Iterable[str]] = None,
        timezone: str = "Asia/Kolkata",
        tags: Optional[Iterable[str]] = None,
    ) -> "BacktestRunSpec":
        """
        Best-effort conversion from the legacy `StrategySpec`.

        If `leg_ids` is omitted, new random ids are generated (stable only for this conversion).
        """

        if not isinstance(spec, StrategySpec):
            raise TypeError("spec must be a StrategySpec")

        ids: list[str] = []
        if leg_ids is not None:
            ids = [str(x).strip() for x in leg_ids if str(x).strip()]
        while len(ids) < len(spec.legs):
            ids.append(_short_uuid())

        option_legs: list[OptionLegSpec] = []
        risk_legs: list[LegRiskRuleSpec] = []
        for leg_id, leg in zip(ids, spec.legs):
            option_legs.append(
                OptionLegSpec(
                    leg_id=leg_id,
                    side=leg.side,
                    option_type=leg.opt_type,
                    qty=int(leg.qty_lots),
                    expiry_selector=ExpirySelectorSpec(mode=leg.expiry_mode),
                    strike_selector=StrikeSelectorSpec(
                        mode=leg.strike_mode,
                        offset_points=leg.strike_offset_points,
                        target_premium=leg.target_premium,
                    ),
                )
            )

            risk_legs.append(
                LegRiskRuleSpec(
                    leg_id=leg_id,
                    stoploss_type=leg.stoploss_type,
                    stoploss_value=leg.stoploss_value if leg.stoploss_type else None,
                    profit_target_type=leg.profit_target_type,
                    profit_target_value=leg.profit_target_value if leg.profit_target_type else None,
                    trailing_enabled=bool(leg.trailing_enabled),
                    trailing_type=leg.trailing_type,
                    trailing_trigger=leg.trailing_trigger,
                    trailing_step=leg.trailing_step,
                    profit_lock=leg.profit_lock,
                    reentry_enabled=bool(leg.reentry_enabled),
                    max_reentries=int(leg.max_reentries or 0),
                    cool_down_minutes=int(getattr(leg, "cool_down_minutes", 0) or 0),
                    reentry_condition=getattr(leg, "reentry_condition", None),
                )
            )

        risk_spec = RiskRuleSpec(per_leg=tuple(risk_legs), strategy=spec.strategy_risk)

        return cls(
            config=BacktestConfig(
                underlying_instrument_key=spec.underlying_instrument_key,
                start_date=spec.start_date,
                end_date=spec.end_date,
                interval=spec.candle_interval,
                entry_time=spec.entry_time,
                exit_time=spec.exit_time,
                timezone=timezone,
                starting_capital=spec.starting_capital,
                brokerage_profile=spec.brokerage_profile,
            ),
            legs=tuple(option_legs),
            risk=risk_spec,
            execution_model=ExecutionModelSpec(
                fill_model=spec.fill_model,
                latency_ms=int(spec.latency_ms),
                allow_partial_fills=bool(spec.allow_partial_fills),
                spread_bps=float(spec.spread_bps),
                slippage_model=str(spec.slippage_model),
                slippage_bps=float(spec.slippage_bps),
                slippage_ticks=int(spec.slippage_ticks),
            ),
            name=str(spec.name),
            tags=tuple(tags or ()),
            spec_version=2,
        )


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
    "BacktestConfig",
    "BacktestRunSpec",
    "ExecutionModelSpec",
    "ExpirySelectorSpec",
    "LegSpec",
    "LegRiskRuleSpec",
    "OptionLegSpec",
    "ProfitLockSpec",
    "RiskRuleSpec",
    "StrikeSelectorSpec",
    "StrategyRiskSpec",
    "StrategySpec",
]
