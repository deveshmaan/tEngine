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
        short = max(short_val, 1)
        long = max(long_val, short + 1)
        return StrategyConfig(short_ma=short, long_ma=long, iv_threshold=max(iv_val, 0.0), vol_breakout_mult=max(vol_mult_val, 0.0))


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
