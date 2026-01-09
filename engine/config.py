from __future__ import annotations

import datetime as dt
import logging
import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, Iterable, Mapping, Optional

import yaml

from engine.time_machine import now as engine_now

IST = dt.timezone(dt.timedelta(hours=5, minutes=30), name="Asia/Kolkata")

_WEEKDAY_ALIASES = {
    "mon": 0,
    "monday": 0,
    "tue": 1,
    "tuesday": 1,
    "wed": 2,
    "wednesday": 2,
    "thu": 3,
    "thursday": 3,
    "fri": 4,
    "friday": 4,
}

_DEPRECATION_WARNED = False
_LOGGER = logging.getLogger("engine.config")
_SUBSCRIPTION_PREFS = {"current", "next", "monthly"}


def _canonical_config() -> Path:
    return Path(os.getenv("APP_CONFIG_PATH", "config/app.yml"))


def _read_config_payload(*, strict: bool) -> Dict[str, Any]:
    cfg_path = _canonical_config()
    if not cfg_path.exists():
        if strict:
            raise FileNotFoundError(f"Canonical config {cfg_path} not found")
        _LOGGER.warning("Config file %s missing; returning defaults", cfg_path)
        return {}
    _warn_engine_config()
    with cfg_path.open("r", encoding="utf-8") as handle:
        data = yaml.safe_load(handle) or {}
    return data


def _parse_time_str(value: str) -> dt.time:
    value = value.strip()
    for fmt in ("%H:%M:%S", "%H:%M"):
        try:
            return dt.datetime.strptime(value, fmt).time()
        except ValueError:
            continue
    raise ValueError(f"Invalid HH:MM[:SS] time string: {value}")


def _parse_holidays(raw: Iterable[str]) -> tuple[dt.date, ...]:
    holidays: list[dt.date] = []
    for entry in raw:
        try:
            holidays.append(dt.date.fromisoformat(str(entry)))
        except ValueError:
            continue
    return tuple(sorted(set(holidays)))


def _parse_weekday(value: Any, default: int = 1) -> int:
    if value is None:
        return default
    if isinstance(value, int):
        day = value
    else:
        text = str(value).strip().lower()
        if not text:
            return default
        if text.isdigit():
            day = int(text)
        else:
            day = _WEEKDAY_ALIASES.get(text, default)
    if day < 0 or day > 6:
        return default
    return day


def _strike_steps(payload: Mapping[str, Any]) -> dict[str, int]:
    steps: dict[str, int] = {}
    for symbol, raw in (payload or {}).items():
        try:
            steps[str(symbol).upper()] = int(raw)
        except (TypeError, ValueError):
            continue
    return steps


def _parse_subscription_preference(value: Any) -> str:
    text = str(value or "current").strip().lower()
    if text not in _SUBSCRIPTION_PREFS:
        return "current"
    return text


def _parse_option_type(value: Any) -> str:
    text = str(value or "CE").strip().upper()
    if text not in {"CE", "PE", "BOTH"}:
        return "CE"
    return text


def _read_env(name: str) -> Optional[str]:
    value = os.getenv(name)
    if value is None:
        return None
    text = value.strip()
    return text or None


def _parse_allowed_ips(raw: Any) -> tuple[str, ...]:
    env_value = _read_env("ALLOWED_IPS")
    source = env_value if env_value is not None else raw
    if source is None:
        return tuple()
    ips: list[str] = []
    if isinstance(source, str):
        parts = source.replace(";", ",").split(",")
        ips = [part.strip() for part in parts if part.strip()]
    elif isinstance(source, Iterable):
        ips = [str(entry).strip() for entry in source if str(entry).strip()]
    return tuple(dict.fromkeys(ips))


@dataclass(frozen=True)
class RiskLimits:
    daily_pnl_stop: float
    per_symbol_loss_stop: float
    max_open_lots: int
    notional_premium_cap: float
    max_order_rate: int
    no_new_entries_after: dt.time
    square_off_by: dt.time
    risk_percent_per_trade: float = 1.0
    post_close_behavior: str = "halt_if_flat"
    scalping_risk_pct: float = 0.5
    scalping_trades_per_hour: int = 30
    max_slippage_pct_per_trade: float = 0.0
    max_avg_slippage_pct_per_day: float = 0.0
    max_slippage_trades: int = 0
    max_entry_spread_pct: float = 0.0
    max_trades_per_day: int = 0
    max_consecutive_losses: int = 0
    max_intraday_index_move_pct_window: float = 0.0
    extreme_move_window_seconds: int = 0

    @staticmethod
    def from_dict(payload: Mapping[str, Any]) -> "RiskLimits":
        behavior = str(payload.get("post_close_behavior", "halt_if_flat")).strip().lower() or "halt_if_flat"
        if behavior not in {"halt_if_flat", "shutdown"}:
            behavior = "halt_if_flat"
        return RiskLimits(
            daily_pnl_stop=float(payload["daily_pnl_stop"]),
            per_symbol_loss_stop=float(payload["per_symbol_loss_stop"]),
            max_open_lots=int(payload["max_open_lots"]),
            notional_premium_cap=float(payload["notional_premium_cap"]),
            max_order_rate=int(payload.get("max_order_rate", 15)),
            no_new_entries_after=_parse_time_str(str(payload["no_new_entries_after"])),
            square_off_by=_parse_time_str(str(payload["square_off_by"])),
            risk_percent_per_trade=float(payload.get("risk_percent_per_trade", 1.0)),
            post_close_behavior=behavior,
            scalping_risk_pct=float(payload.get("scalping_risk_pct", 0.5)),
            scalping_trades_per_hour=int(payload.get("scalping_trades_per_hour", 30)),
            max_slippage_pct_per_trade=float(payload.get("max_slippage_pct_per_trade", 0.0)),
            max_avg_slippage_pct_per_day=float(payload.get("max_avg_slippage_pct_per_day", 0.0)),
            max_slippage_trades=int(payload.get("max_slippage_trades", 0)),
            max_entry_spread_pct=float(payload.get("max_entry_spread_pct", 0.0)),
            max_trades_per_day=int(payload.get("max_trades_per_day", 0)),
            max_consecutive_losses=int(payload.get("max_consecutive_losses", 0)),
            max_intraday_index_move_pct_window=float(payload.get("max_intraday_index_move_pct_window", 0.0)),
            extreme_move_window_seconds=int(payload.get("extreme_move_window_seconds", 0)),
        )


@dataclass(frozen=True)
class DataConfig:
    index_symbol: str
    lot_step: int
    tick_size: float
    price_band_low: float
    price_band_high: float
    strike_steps: dict[str, int]
    holidays: tuple[dt.date, ...]
    expiry_ttl_minutes: int
    weekly_expiry_weekday: int
    allow_expiry_override: bool = False
    expiry_override_path: Optional[str] = None
    expiry_override_max_age_minutes: int = 90
    subscription_expiry_preference: str = "current"

    @staticmethod
    def from_dict(payload: Mapping[str, Any]) -> "DataConfig":
        raw_path = payload.get("expiry_override_path")
        path_value = str(raw_path).strip() if raw_path else ""
        return DataConfig(
            index_symbol=str(payload.get("index_symbol", "NIFTY")).upper(),
            lot_step=int(payload.get("lot_step", 50)),
            tick_size=float(payload.get("tick_size", 0.05)),
            price_band_low=float(payload.get("price_band_low", 0.05)),
            price_band_high=float(payload.get("price_band_high", 5000.0)),
            strike_steps=_strike_steps(payload.get("strike_steps", {})),
            holidays=_parse_holidays(payload.get("holidays", [])),
            expiry_ttl_minutes=int(payload.get("expiry_ttl_minutes", 5)),
            weekly_expiry_weekday=_parse_weekday(payload.get("weekly_expiry_weekday"), default=1),
            allow_expiry_override=bool(payload.get("allow_expiry_override", False)),
            expiry_override_path=path_value or None,
            expiry_override_max_age_minutes=int(payload.get("expiry_override_max_age_minutes", 90)),
            subscription_expiry_preference=_parse_subscription_preference(payload.get("subscription_expiry_preference")),
        )

    def get(self, key: str, default: Any = None) -> Any:
        return getattr(self, key, default)


@dataclass(frozen=True)
class RateLimitConfig:
    rate_per_sec: float
    burst: int


@dataclass(frozen=True)
class BrokerRateLimits:
    place: RateLimitConfig
    modify: RateLimitConfig
    cancel: RateLimitConfig
    history: RateLimitConfig


def _build_rate_limit(payload: Mapping[str, Any], fallback_rate: float, fallback_burst: int) -> RateLimitConfig:
    rate = float(payload.get("rate_per_sec", fallback_rate))
    burst = int(payload.get("burst", fallback_burst))
    return RateLimitConfig(rate_per_sec=max(rate, 0.1), burst=max(burst, 1))


def _rate_limits(payload: Mapping[str, Any], default_rate: float) -> BrokerRateLimits:
    return BrokerRateLimits(
        place=_build_rate_limit(payload.get("place", {}), default_rate, int(default_rate * 2)),
        modify=_build_rate_limit(payload.get("modify", {}), default_rate, int(default_rate * 2)),
        cancel=_build_rate_limit(payload.get("cancel", {}), default_rate, int(default_rate * 2)),
        history=_build_rate_limit(payload.get("history", {}), max(default_rate / 2, 1.0), int(default_rate)),
    )


@dataclass(frozen=True)
class BrokerConfig:
    rest_timeout: float
    ws_heartbeat_interval: float
    ws_backoff_seconds: tuple[float, ...]
    oauth_refresh_margin: int
    rate_limits: BrokerRateLimits = field(default_factory=lambda: _rate_limits({}, 15.0))
    max_order_rate: int = 15

    @staticmethod
    def from_dict(payload: Mapping[str, Any]) -> "BrokerConfig":
        base_rate = float(payload.get("max_order_rate", 15))
        return BrokerConfig(
            rest_timeout=float(payload.get("rest_timeout", 3.0)),
            ws_heartbeat_interval=float(payload.get("ws_heartbeat_interval", 3.0)),
            ws_backoff_seconds=tuple(float(v) for v in payload.get("ws_backoff_seconds", (1, 2, 5, 10))),
            oauth_refresh_margin=int(payload.get("oauth_refresh_margin", 60)),
            rate_limits=_rate_limits(payload.get("rate_limits", {}), max(base_rate, 1.0)),
            max_order_rate=int(base_rate),
        )


@dataclass(frozen=True)
class OMSSubmitConfig:
    default: str
    max_spread_ticks: int
    depth_threshold: int


@dataclass(frozen=True)
class OMSConfig:
    resubmit_backoff: float
    reconciliation_interval: float
    max_inflight_orders: int
    submit: OMSSubmitConfig = field(default_factory=lambda: OMSSubmitConfig(default="market", max_spread_ticks=1, depth_threshold=0))
    order_timeout_seconds: float = 0.0

    @staticmethod
    def from_dict(payload: Mapping[str, Any]) -> "OMSConfig":
        submit_raw = payload.get("submit", {})
        submit = OMSSubmitConfig(
            default=str(submit_raw.get("default", "market")).lower(),
            max_spread_ticks=int(submit_raw.get("max_spread_ticks", 1)),
            depth_threshold=int(submit_raw.get("depth_threshold", 0)),
        )
        return OMSConfig(
            resubmit_backoff=float(payload.get("resubmit_backoff", 0.5)),
            reconciliation_interval=float(payload.get("reconciliation_interval", 2.0)),
            max_inflight_orders=int(payload.get("max_inflight_orders", 8)),
            submit=submit,
            order_timeout_seconds=float(payload.get("order_timeout_seconds", 0.0)),
        )


@dataclass(frozen=True)
class AlertConfig:
    throttle_seconds: float = 30.0

    @staticmethod
    def from_dict(payload: Mapping[str, Any]) -> "AlertConfig":
        return AlertConfig(throttle_seconds=float(payload.get("throttle_seconds", 30.0)))


@dataclass(frozen=True)
class TelemetryConfig:
    metrics_port_env: str = "METRICS_PORT"

    @staticmethod
    def from_dict(payload: Mapping[str, Any]) -> "TelemetryConfig":
        return TelemetryConfig(metrics_port_env=str(payload.get("metrics_port_env", "METRICS_PORT")))


@dataclass(frozen=True)
class ReplayConfig:
    default_speed: float = 1.0

    @staticmethod
    def from_dict(payload: Mapping[str, Any]) -> "ReplayConfig":
        return ReplayConfig(default_speed=float(payload.get("default_speed", 1.0)))


@dataclass(frozen=True)
class MarketDataConfig:
    window_steps: int = 2
    option_type: str = "CE"
    stream_mode: str = "full_d30"
    depth_levels: int = 5
    max_tick_age_seconds: float = 2.0

    @staticmethod
    def from_dict(payload: Mapping[str, Any]) -> "MarketDataConfig":
        try:
            window_val = int(payload.get("window_steps", 2))
        except (TypeError, ValueError):
            window_val = 2
        try:
            depth_val = int(payload.get("depth_levels", 5))
        except (TypeError, ValueError):
            depth_val = 5
        try:
            max_age = float(payload.get("max_tick_age_seconds", 2.0))
        except (TypeError, ValueError):
            max_age = 2.0
        raw_mode = str(payload.get("stream_mode", "full_d30")).strip().lower()
        if not raw_mode:
            raw_mode = "full_d30"
        return MarketDataConfig(
            window_steps=max(window_val, 0),
            option_type=_parse_option_type(payload.get("option_type", "CE")),
            stream_mode=raw_mode,
            depth_levels=max(depth_val, 1),
            max_tick_age_seconds=max(0.0, max_age),
        )

    def get(self, key: str, default: Any = None) -> Any:
        return getattr(self, key, default)


@dataclass(frozen=True)
class SecretsConfig:
    upstox_access_token: Optional[str] = None
    upstox_api_key: Optional[str] = None
    upstox_api_secret: Optional[str] = None

    @staticmethod
    def from_env() -> "SecretsConfig":
        return SecretsConfig(
            upstox_access_token=_read_env("UPSTOX_ACCESS_TOKEN"),
            upstox_api_key=_read_env("UPSTOX_API_KEY"),
            upstox_api_secret=_read_env("UPSTOX_API_SECRET"),
        )


@dataclass(frozen=True)
class StrategyConfig:
    short_ma: int = 5
    long_ma: int = 20
    iv_threshold: float = 0.0
    vol_breakout_mult: float = 1.5
    imi_period: int = 14
    pcr_extreme_high: float = 1.3
    pcr_extreme_low: float = 0.7
    iv_percentile_threshold: float = 0.6
    iv_exit_percentile: float = 0.9
    oi_volume_min_threshold: float = 0.0
    event_file_path: Optional[str] = None
    gamma_threshold: float = 0.0
    min_minutes_to_expiry: int = 0
    event_halt_minutes: int = 0
    enable_call_entries: bool = True
    enable_put_entries: bool = False
    # Scalping-specific
    breakout_window: int = 5
    breakout_margin: float = 0.0
    volume_mult: float = 1.5
    pcr_range: tuple[float, float] = (0.7, 1.3)
    spread_max_pct: float = 0.05
    opening_range_minutes: int = 15
    volume_surge_ratio: float = 1.5
    vwap_confirmation: bool = True
    orb_atr_period: int = 14
    min_or_atr_mult: float = 0.55
    max_or_atr_mult: float = 2.5
    buffer_atr_mult: float = 0.35
    breakout_buffer_pts: float = 5.0
    orb_max_trades_per_session: int = 1
    use_atr_filter: bool = True
    # Liquidity pool breaker strategy
    top_n_zones: int = 3
    chain_refresh_seconds: int = 300
    chain_min_cooldown_seconds: int = 30
    liquidity_weight: float = 1.0
    min_oi: float = 0.0
    min_vol: float = 0.0
    oi_drop_pct_trigger: float = 0.15
    absorption_oi_rise_pct: float = 0.1
    min_vol_delta: float = 100.0
    absorption_confirm_seconds: int = 45
    reclaim_buffer_points: float = 2.0
    stop_buffer_points: float = 3.0
    risk_pct: float = 0.0
    max_hold_minutes: int = 0
    no_new_entries_after_time: Optional[dt.time] = None

    @staticmethod
    def from_dict(payload: Mapping[str, Any]) -> "StrategyConfig":
        try:
            short_val = int(payload.get("short_ma", 5))
        except (TypeError, ValueError):
            short_val = 5
        try:
            long_val = int(payload.get("long_ma", 20))
        except (TypeError, ValueError):
            long_val = 20
        try:
            iv_val = float(payload.get("iv_threshold", 0.0))
        except (TypeError, ValueError):
            iv_val = 0.0
        try:
            vol_mult_val = float(payload.get("vol_breakout_mult", 1.5))
        except (TypeError, ValueError):
            vol_mult_val = 1.5
        try:
            imi_val = int(payload.get("imi_period", 14))
        except (TypeError, ValueError):
            imi_val = 14
        try:
            pcr_hi = float(payload.get("pcr_extreme_high", 1.3))
        except (TypeError, ValueError):
            pcr_hi = 1.3
        try:
            pcr_lo = float(payload.get("pcr_extreme_low", 0.7))
        except (TypeError, ValueError):
            pcr_lo = 0.7
        try:
            iv_pct = float(payload.get("iv_percentile_threshold", 0.6))
        except (TypeError, ValueError):
            iv_pct = 0.6
        try:
            iv_exit_pct = float(payload.get("iv_exit_percentile", 0.9))
        except (TypeError, ValueError):
            iv_exit_pct = 0.9
        try:
            oi_vol_min = float(payload.get("oi_volume_min_threshold", 0.0))
        except (TypeError, ValueError):
            oi_vol_min = 0.0
        try:
            gamma_thr = float(payload.get("gamma_threshold", 0.0))
        except (TypeError, ValueError):
            gamma_thr = 0.0
        try:
            min_expiry_minutes = int(payload.get("min_minutes_to_expiry", 0))
        except (TypeError, ValueError):
            min_expiry_minutes = 0
        try:
            event_halt = int(payload.get("event_halt_minutes", 0))
        except (TypeError, ValueError):
            event_halt = 0
        try:
            breakout_window = int(payload.get("breakout_window", 5))
        except (TypeError, ValueError):
            breakout_window = 5
        try:
            breakout_margin = float(payload.get("breakout_margin", 0.0))
        except (TypeError, ValueError):
            breakout_margin = 0.0
        try:
            volume_mult = float(payload.get("volume_mult", 1.5))
        except (TypeError, ValueError):
            volume_mult = 1.5
        try:
            pcr_low = float(payload.get("pcr_range", (0.7, 1.3))[0] if isinstance(payload.get("pcr_range"), (list, tuple)) else payload.get("pcr_range_low", 0.7))
        except Exception:
            pcr_low = 0.7
        try:
            pcr_high = float(payload.get("pcr_range", (0.7, 1.3))[1] if isinstance(payload.get("pcr_range"), (list, tuple)) else payload.get("pcr_range_high", 1.3))
        except Exception:
            pcr_high = 1.3
        try:
            spread_max_pct = float(payload.get("spread_max_pct", 0.05))
        except (TypeError, ValueError):
            spread_max_pct = 0.05
        try:
            opening_range_minutes = int(payload.get("opening_range_minutes", 15))
        except (TypeError, ValueError):
            opening_range_minutes = 15
        try:
            volume_surge_ratio = float(payload.get("volume_surge_ratio", 1.5))
        except (TypeError, ValueError):
            volume_surge_ratio = 1.5
        vwap_confirmation = bool(payload.get("vwap_confirmation", True))
        try:
            orb_atr_period = int(payload.get("orb_atr_period", 14))
        except (TypeError, ValueError):
            orb_atr_period = 14
        try:
            min_or_atr_mult = float(payload.get("min_or_atr_mult", 0.55))
        except (TypeError, ValueError):
            min_or_atr_mult = 0.55
        try:
            max_or_atr_mult = float(payload.get("max_or_atr_mult", 2.5))
        except (TypeError, ValueError):
            max_or_atr_mult = 2.5
        try:
            buffer_atr_mult = float(payload.get("buffer_atr_mult", 0.35))
        except (TypeError, ValueError):
            buffer_atr_mult = 0.35
        try:
            breakout_buffer_pts = float(payload.get("breakout_buffer_pts", 5.0))
        except (TypeError, ValueError):
            breakout_buffer_pts = 5.0
        try:
            orb_max_trades = int(payload.get("orb_max_trades_per_session", 1))
        except (TypeError, ValueError):
            orb_max_trades = 1
        try:
            top_n_zones = int(payload.get("top_n_zones", 3))
        except (TypeError, ValueError):
            top_n_zones = 3
        try:
            chain_refresh_seconds = int(payload.get("chain_refresh_seconds", 300))
        except (TypeError, ValueError):
            chain_refresh_seconds = 300
        try:
            chain_min_cooldown_seconds = int(payload.get("chain_min_cooldown_seconds", 30))
        except (TypeError, ValueError):
            chain_min_cooldown_seconds = 30
        try:
            liquidity_weight = float(payload.get("liquidity_weight", 1.0))
        except (TypeError, ValueError):
            liquidity_weight = 1.0
        try:
            min_oi = float(payload.get("min_oi", 0.0))
        except (TypeError, ValueError):
            min_oi = 0.0
        try:
            min_vol = float(payload.get("min_vol", 0.0))
        except (TypeError, ValueError):
            min_vol = 0.0
        try:
            oi_drop_pct_trigger = float(payload.get("oi_drop_pct_trigger", 0.15))
        except (TypeError, ValueError):
            oi_drop_pct_trigger = 0.15
        try:
            absorption_oi_rise_pct = float(payload.get("absorption_oi_rise_pct", 0.1))
        except (TypeError, ValueError):
            absorption_oi_rise_pct = 0.1
        try:
            min_vol_delta = float(payload.get("min_vol_delta", 100.0))
        except (TypeError, ValueError):
            min_vol_delta = 100.0
        try:
            absorption_confirm_seconds = int(payload.get("absorption_confirm_seconds", 45))
        except (TypeError, ValueError):
            absorption_confirm_seconds = 45
        try:
            reclaim_buffer_points = float(payload.get("reclaim_buffer_points", 2.0))
        except (TypeError, ValueError):
            reclaim_buffer_points = 2.0
        try:
            stop_buffer_points = float(payload.get("stop_buffer_points", 3.0))
        except (TypeError, ValueError):
            stop_buffer_points = 3.0
        try:
            risk_pct = float(payload.get("risk_pct", 0.0))
        except (TypeError, ValueError):
            risk_pct = 0.0
        try:
            max_hold_minutes = int(payload.get("max_hold_minutes", 0))
        except (TypeError, ValueError):
            max_hold_minutes = 0
        no_new_entries_after_time: Optional[dt.time] = None
        raw_no_new_entries = payload.get("no_new_entries_after_time")
        if raw_no_new_entries:
            try:
                no_new_entries_after_time = _parse_time_str(str(raw_no_new_entries))
            except (TypeError, ValueError):
                no_new_entries_after_time = None
        enable_call = bool(payload.get("enable_call_entries", True))
        enable_put = bool(payload.get("enable_put_entries", False))
        use_atr_filter = bool(payload.get("use_atr_filter", True))
        event_path = str(payload.get("event_file_path") or "").strip() or None
        short = max(short_val, 1)
        long = max(long_val, short + 1)
        return StrategyConfig(
            short_ma=short,
            long_ma=long,
            iv_threshold=max(iv_val, 0.0),
            vol_breakout_mult=max(vol_mult_val, 0.0),
            imi_period=max(imi_val, 1),
            pcr_extreme_high=max(pcr_hi, 0.0),
            pcr_extreme_low=max(pcr_lo, 0.0),
            iv_percentile_threshold=max(iv_pct, 0.0),
            iv_exit_percentile=max(iv_exit_pct, 0.0),
            oi_volume_min_threshold=max(oi_vol_min, 0.0),
            event_file_path=event_path,
            gamma_threshold=max(gamma_thr, 0.0),
            min_minutes_to_expiry=max(min_expiry_minutes, 0),
            event_halt_minutes=max(event_halt, 0),
            breakout_window=max(breakout_window, 1),
            breakout_margin=max(breakout_margin, 0.0),
            volume_mult=max(volume_mult, 0.0),
            pcr_range=(max(pcr_low, 0.0), max(pcr_high, 0.0)),
            spread_max_pct=max(spread_max_pct, 0.0),
            opening_range_minutes=max(opening_range_minutes, 1),
            volume_surge_ratio=max(volume_surge_ratio, 0.0),
            vwap_confirmation=bool(vwap_confirmation),
            orb_atr_period=max(orb_atr_period, 1),
            min_or_atr_mult=max(min_or_atr_mult, 0.0),
            max_or_atr_mult=max(max_or_atr_mult, 0.0),
            buffer_atr_mult=max(buffer_atr_mult, 0.0),
            breakout_buffer_pts=max(breakout_buffer_pts, 0.0),
            orb_max_trades_per_session=max(orb_max_trades, 1),
            enable_call_entries=enable_call,
            enable_put_entries=enable_put,
            use_atr_filter=use_atr_filter,
            top_n_zones=max(top_n_zones, 1),
            chain_refresh_seconds=max(chain_refresh_seconds, 1),
            chain_min_cooldown_seconds=max(chain_min_cooldown_seconds, 1),
            liquidity_weight=max(liquidity_weight, 0.0),
            min_oi=max(min_oi, 0.0),
            min_vol=max(min_vol, 0.0),
            oi_drop_pct_trigger=max(oi_drop_pct_trigger, 0.0),
            absorption_oi_rise_pct=max(absorption_oi_rise_pct, 0.0),
            min_vol_delta=max(min_vol_delta, 0.0),
            absorption_confirm_seconds=max(absorption_confirm_seconds, 1),
            reclaim_buffer_points=max(reclaim_buffer_points, 0.0),
            stop_buffer_points=max(stop_buffer_points, 0.0),
            risk_pct=max(risk_pct, 0.0),
            max_hold_minutes=max(max_hold_minutes, 0),
            no_new_entries_after_time=no_new_entries_after_time,
        )


@dataclass(frozen=True)
class BankNiftyConfig:
    vol_breakout_mult: float = 1.2

    @staticmethod
    def from_dict(payload: Mapping[str, Any]) -> "BankNiftyConfig":
        try:
            mult_val = float(payload.get("vol_breakout_mult", 1.2))
        except (TypeError, ValueError):
            mult_val = 1.2
        return BankNiftyConfig(vol_breakout_mult=max(mult_val, 0.0))


@dataclass(frozen=True)
class SmokeTestConfig:
    enabled: bool = False
    underlying: str = "NIFTY"
    side: str = "CE"
    hold_seconds: int = 60
    lots: int = 1
    max_notional_rupees: float = 4000.0

    @staticmethod
    def from_dict(payload: Mapping[str, Any]) -> "SmokeTestConfig":
        try:
            lots_val = int(payload.get("lots", 1))
        except (TypeError, ValueError):
            lots_val = 1
        try:
            hold_val = int(payload.get("hold_seconds", 60))
        except (TypeError, ValueError):
            hold_val = 60
        side_val = str(payload.get("side", "CE")).strip().upper() or "CE"
        if side_val not in {"CE", "PE"}:
            side_val = "CE"
        return SmokeTestConfig(
            enabled=bool(payload.get("enabled", False)),
            underlying=str(payload.get("underlying", "NIFTY")).upper(),
            side=side_val,
            hold_seconds=max(hold_val, 1),
            lots=max(lots_val, 1),
            max_notional_rupees=float(payload.get("max_notional_rupees", 4000.0)),
        )


@dataclass(frozen=True)
class ExitConfig:
    stop_pct: float = 0.25
    target1_pct: float = 0.5
    partial_fraction: float = 0.5
    trail_lock_pct: float = 0.4
    trail_giveback_pct: float = 0.5
    min_trail_ticks: int = 3
    max_holding_minutes: int = 20
    trailing_stop_pct: float = 0.0
    time_stop_minutes: int = 0
    partial_target_multiplier: float = 0.0
    trailing_pct: float = 0.0
    trailing_step: float = 0.0
    time_buffer_minutes: int = 0
    partial_tp_mult: float = 0.0
    at_pct: float = 0.0
    scalping_profit_target_pct: float = 0.0
    scalping_stop_loss_pct: float = 0.0
    scalping_time_limit_minutes: int = 0

    @staticmethod
    def from_dict(payload: Mapping[str, Any]) -> "ExitConfig":
        def _coerce_float(key: str, default: float) -> float:
            try:
                return float(payload.get(key, default))
            except (TypeError, ValueError):
                return default

        def _coerce_int(key: str, default: int) -> int:
            try:
                return int(payload.get(key, default))
            except (TypeError, ValueError):
                return default

        return ExitConfig(
            stop_pct=_coerce_float("stop_pct", 0.25),
            target1_pct=_coerce_float("target1_pct", 0.5),
            partial_fraction=_coerce_float("partial_fraction", 0.5),
            trail_lock_pct=_coerce_float("trail_lock_pct", 0.4),
            trail_giveback_pct=_coerce_float("trail_giveback_pct", 0.5),
            min_trail_ticks=_coerce_int("min_trail_ticks", 3),
            max_holding_minutes=_coerce_int("max_holding_minutes", 20),
            trailing_stop_pct=_coerce_float("trailing_stop_pct", 0.0),
            time_stop_minutes=_coerce_int("time_stop_minutes", 0),
            partial_target_multiplier=_coerce_float("partial_target_multiplier", 0.0),
            trailing_pct=_coerce_float("trailing_pct", 0.0),
            trailing_step=_coerce_float("trailing_step", 0.0),
            time_buffer_minutes=_coerce_int("time_buffer_minutes", 0),
            partial_tp_mult=_coerce_float("partial_tp_mult", 0.0),
            at_pct=_coerce_float("at_pct", 0.0),
            scalping_profit_target_pct=_coerce_float("scalping_profit_target_pct", 0.0),
            scalping_stop_loss_pct=_coerce_float("scalping_stop_loss_pct", 0.0),
            scalping_time_limit_minutes=_coerce_int("scalping_time_limit_minutes", 0),
        )


@dataclass(frozen=True)
class EngineConfig:
    run_id: str
    persistence_path: Path
    strategy_tag: str
    strategy: StrategyConfig
    risk: RiskLimits
    data: DataConfig
    market_data: MarketDataConfig
    broker: BrokerConfig
    oms: OMSConfig
    alerts: AlertConfig
    telemetry: TelemetryConfig
    replay: ReplayConfig
    smoke_test: SmokeTestConfig
    exit: ExitConfig
    banknifty: BankNiftyConfig = field(default_factory=BankNiftyConfig)
    capital_base: float = 0.0
    allowed_ips: tuple[str, ...] = field(default_factory=tuple)
    secrets: SecretsConfig = field(default_factory=SecretsConfig)

    @staticmethod
    def load(path: Optional[str | Path] = None) -> "EngineConfig":
        cfg_path = _canonical_config()
        if path and Path(path) != cfg_path:
            _LOGGER.warning("Ignoring explicit config path %s; using canonical %s", path, cfg_path)
        raw = _read_config_payload(strict=True)
        run_id = str(raw.get("run_id") or f"run-{engine_now(IST).strftime('%Y%m%d')}")
        persistence_path = Path(raw.get("persistence_path", "engine_state.sqlite"))
        strategy_tag = str(raw.get("strategy_tag", "intraday-buy"))
        strategy_cfg = StrategyConfig.from_dict(raw.get("strategy", {}))
        risk = RiskLimits.from_dict(raw.get("risk", {}))
        data = DataConfig.from_dict(raw.get("data", {}))
        market_data = MarketDataConfig.from_dict(raw.get("market_data", {}))
        broker = BrokerConfig.from_dict(raw.get("broker", {}))
        oms = OMSConfig.from_dict(raw.get("oms", {}))
        alerts = AlertConfig.from_dict(raw.get("alerts", {}))
        telemetry = TelemetryConfig.from_dict(raw.get("telemetry", {}))
        replay = ReplayConfig.from_dict(raw.get("replay", {}))
        smoke_test = SmokeTestConfig.from_dict(raw.get("smoke_test", {}))
        exit_cfg = ExitConfig.from_dict(raw.get("exit", {}))
        banknifty = BankNiftyConfig.from_dict(raw.get("banknifty", {}))
        capital_base = float(raw.get("capital_base", 0.0))
        allowed_ips = _parse_allowed_ips(raw.get("allowed_ips"))
        secrets = SecretsConfig.from_env()
        return EngineConfig(
            run_id=run_id,
            persistence_path=persistence_path,
            strategy_tag=strategy_tag,
            strategy=strategy_cfg,
            risk=risk,
            data=data,
            market_data=market_data,
            broker=broker,
            oms=oms,
            alerts=alerts,
            telemetry=telemetry,
            replay=replay,
            smoke_test=smoke_test,
            exit=exit_cfg,
            banknifty=banknifty,
            capital_base=capital_base,
            allowed_ips=allowed_ips,
            secrets=secrets,
        )


class _ConfigHandle:
    def __init__(self) -> None:
        self._config: Optional[EngineConfig] = None

    def _ensure(self) -> EngineConfig:
        if self._config is None:
            self._config = EngineConfig.load()
        return self._config

    def reload(self) -> EngineConfig:
        self._config = EngineConfig.load()
        return self._config

    def value(self) -> EngineConfig:
        return self._ensure()

    def __getattr__(self, item: str):  # pragma: no cover - simple proxy
        return getattr(self._ensure(), item)


CONFIG = _ConfigHandle()


def _warn_engine_config() -> None:
    global _DEPRECATION_WARNED
    if _DEPRECATION_WARNED:
        return
    legacy = Path("engine_config.yaml")
    if legacy.exists():
        _LOGGER.warning("DEPRECATED config file 'engine_config.yaml' detected. Ignoring; using config/app.yml only.")
    _DEPRECATION_WARNED = True


def read_config() -> Dict[str, Any]:
    """Return the raw config/app.yml payload for UI consumers."""

    return dict(_read_config_payload(strict=False))


__all__ = [
    "AlertConfig",
    "BrokerConfig",
    "BankNiftyConfig",
    "CONFIG",
    "DataConfig",
    "EngineConfig",
    "IST",
    "MarketDataConfig",
    "OMSConfig",
    "OMSSubmitConfig",
    "ExitConfig",
    "read_config",
    "ReplayConfig",
    "RiskLimits",
    "SecretsConfig",
    "StrategyConfig",
    "TelemetryConfig",
    "SmokeTestConfig",
]
