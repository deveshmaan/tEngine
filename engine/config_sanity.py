from __future__ import annotations

import ipaddress
import logging
from typing import Iterable

from engine.config import EngineConfig
from engine.logging_utils import get_logger
from engine.metrics import set_config_sanity_ok

LOG = get_logger("ConfigSanity")


class ConfigError(RuntimeError):
    """Raised when config validation fails."""


def _within(value: float, low: float, high: float, *, inclusive_high: bool = False) -> bool:
    if inclusive_high:
        return low < value <= high
    return low < value < high


def _positive(value: float) -> bool:
    return value > 0


def _validate_rate_limits(values: Iterable[float]) -> bool:
    return all(_positive(v) and v < 50 for v in values)


def sanity_check_config(cfg: EngineConfig) -> None:
    """
    Validate critical numeric parameters before the engine boots.

    Raises:
        ConfigError: when any invariant is violated.
    """

    errors: list[str] = []

    def _err(code: str, message: str) -> None:
        errors.append(message)
        try:
            LOG.log_event(40, code, message=message)
        except Exception:
            logging.getLogger("ConfigSanity").error("%s: %s", code, message)

    if cfg.risk.daily_pnl_stop >= 0:
        _err("risk_daily_pnl_stop", "risk.daily_pnl_stop must be negative (loss stop).")
    if not _positive(cfg.risk.notional_premium_cap):
        _err("risk_notional_premium_cap", "risk.notional_premium_cap must be > 0.")
    if cfg.risk.max_open_lots < 0:
        _err("risk_max_open_lots", "risk.max_open_lots must be >= 0.")
    risk_pct = getattr(cfg.risk, "risk_percent_per_trade", 0.0)
    if not _within(float(risk_pct), 0.0, 100.0, inclusive_high=True):
        _err("risk_percent_per_trade", "risk.risk_percent_per_trade must be > 0 and <= 100.")

    if getattr(cfg, "capital_base", 0.0) <= 0:
        _err("capital_base", "capital_base must be > 0.")

    short_ma = getattr(cfg.strategy, "short_ma", 0)
    long_ma = getattr(cfg.strategy, "long_ma", 0)
    if short_ma <= 0 or long_ma <= short_ma:
        _err("strategy_ma", "strategy.short_ma must be > 0 and < strategy.long_ma.")
    iv_threshold = getattr(cfg.strategy, "iv_threshold", 0.0)
    if iv_threshold < 0:
        _err("strategy_iv_threshold", "strategy.iv_threshold must be >= 0.")
    vol_mult = getattr(cfg.strategy, "vol_breakout_mult", 0.0)
    if vol_mult <= 0:
        _err("strategy_vol_breakout_mult", "strategy.vol_breakout_mult must be > 0.")
    bn_vol_mult = getattr(cfg.banknifty, "vol_breakout_mult", 0.0)
    if bn_vol_mult <= 0:
        _err("banknifty_vol_breakout_mult", "banknifty.vol_breakout_mult must be > 0.")

    for ip in getattr(cfg, "allowed_ips", ()):
        try:
            ipaddress.ip_address(ip)
        except ValueError:
            _err("allowed_ips", f"Invalid allowed IP entry: {ip}")

    if cfg.exit.stop_pct < 0 or cfg.exit.stop_pct >= 1:
        _err("exit_stop_pct", "exit.stop_pct must be >= 0 and < 1.")
    if cfg.exit.target1_pct < 0 or cfg.exit.target1_pct >= 3.0:
        _err("exit_target1_pct", "exit.target1_pct must be >= 0 and < 3.0.")
    if not _within(float(cfg.exit.max_holding_minutes), 0.0, 120.0, inclusive_high=True):
        _err("exit_max_holding_minutes", "exit.max_holding_minutes must be > 0 and <= 120 minutes.")
    if cfg.exit.trailing_stop_pct < 0 or cfg.exit.trailing_stop_pct >= 1:
        _err("exit_trailing_stop_pct", "exit.trailing_stop_pct must be >= 0 and < 1.")
    if cfg.exit.time_stop_minutes < 0:
        _err("exit_time_stop_minutes", "exit.time_stop_minutes must be >= 0.")
    if cfg.exit.partial_target_multiplier < 0:
        _err("exit_partial_target_multiplier", "exit.partial_target_multiplier must be >= 0.")
    if getattr(cfg.exit, "trailing_pct", 0.0) < 0 or getattr(cfg.exit, "trailing_pct", 0.0) >= 1:
        _err("exit_trailing_pct", "exit.trailing_pct must be >= 0 and < 1.")
    if getattr(cfg.exit, "trailing_step", 0.0) < 0 or getattr(cfg.exit, "trailing_step", 0.0) >= 1:
        _err("exit_trailing_step", "exit.trailing_step must be >= 0 and < 1.")
    if getattr(cfg.exit, "time_buffer_minutes", 0) < 0:
        _err("exit_time_buffer_minutes", "exit.time_buffer_minutes must be >= 0.")
    if getattr(cfg.exit, "partial_tp_mult", 0.0) < 0:
        _err("exit_partial_tp_mult", "exit.partial_tp_mult must be >= 0.")

    rate_limits = cfg.broker.rate_limits
    rates = (
        rate_limits.place.rate_per_sec,
        rate_limits.modify.rate_per_sec,
        rate_limits.cancel.rate_per_sec,
    )
    if not _validate_rate_limits(rates):
        _err("broker_rate_limits", "broker rate limits (place/modify/cancel) must be > 0 and < 50 per second.")

    if errors:
        set_config_sanity_ok(0)
        raise ConfigError("; ".join(errors))
    set_config_sanity_ok(1)


__all__ = ["ConfigError", "sanity_check_config"]
