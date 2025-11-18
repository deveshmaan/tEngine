from __future__ import annotations

import datetime as dt
import os
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Iterable, Optional, Set

import yaml

IST = dt.timezone(dt.timedelta(hours=5, minutes=30), name="Asia/Kolkata")


def _parse_time_str(value: str) -> dt.time:
    value = value.strip()
    try:
        return dt.datetime.strptime(value, "%H:%M").time()
    except ValueError as exc:  # pragma: no cover - config error
        raise ValueError(f"Invalid HH:MM time string: {value}") from exc


def _parse_holidays(raw: Iterable[str]) -> Set[dt.date]:
    holidays: Set[dt.date] = set()
    for entry in raw:
        try:
            holidays.add(dt.date.fromisoformat(entry))
        except ValueError:
            continue
    return holidays


@dataclass(frozen=True)
class RiskLimits:
    daily_pnl_stop: float
    per_symbol_loss_stop: float
    max_open_lots: int
    notional_premium_cap: float
    max_order_rate: int
    no_new_entries_after: dt.time
    square_off_by: dt.time

    @staticmethod
    def from_dict(payload: Dict[str, Any]) -> "RiskLimits":
        return RiskLimits(
            daily_pnl_stop=float(payload["daily_pnl_stop"]),
            per_symbol_loss_stop=float(payload["per_symbol_loss_stop"]),
            max_open_lots=int(payload["max_open_lots"]),
            notional_premium_cap=float(payload["notional_premium_cap"]),
            max_order_rate=int(payload["max_order_rate"]),
            no_new_entries_after=_parse_time_str(payload["no_new_entries_after"]),
            square_off_by=_parse_time_str(payload["square_off_by"]),
        )


@dataclass(frozen=True)
class DataConfig:
    index_symbol: str
    lot_step: int
    tick_size: float
    price_band_low: float
    price_band_high: float
    holidays: Set[dt.date]

    @staticmethod
    def from_dict(payload: Dict[str, Any]) -> "DataConfig":
        return DataConfig(
            index_symbol=str(payload.get("index_symbol", "NIFTY")).upper(),
            lot_step=int(payload["lot_step"]),
            tick_size=float(payload["tick_size"]),
            price_band_low=float(payload.get("price_band_low", 0.05)),
            price_band_high=float(payload.get("price_band_high", 5000.0)),
            holidays=_parse_holidays(payload.get("holidays", [])),
        )


@dataclass(frozen=True)
class BrokerConfig:
    rest_timeout: float
    ws_heartbeat_interval: float
    ws_backoff_seconds: tuple[float, ...]
    oauth_refresh_margin: int
    max_order_rate: int

    @staticmethod
    def from_dict(payload: Dict[str, Any]) -> "BrokerConfig":
        return BrokerConfig(
            rest_timeout=float(payload.get("rest_timeout", 3.0)),
            ws_heartbeat_interval=float(payload.get("ws_heartbeat_interval", 3.0)),
            ws_backoff_seconds=tuple(float(v) for v in payload.get("ws_backoff_seconds", (1, 2, 5, 10))),
            oauth_refresh_margin=int(payload.get("oauth_refresh_margin", 60)),
            max_order_rate=int(payload.get("max_order_rate", 15)),
        )


@dataclass(frozen=True)
class OMSConfig:
    resubmit_backoff: float
    reconciliation_interval: float
    max_inflight_orders: int

    @staticmethod
    def from_dict(payload: Dict[str, Any]) -> "OMSConfig":
        return OMSConfig(
            resubmit_backoff=float(payload.get("resubmit_backoff", 0.5)),
            reconciliation_interval=float(payload.get("reconciliation_interval", 2.0)),
            max_inflight_orders=int(payload.get("max_inflight_orders", 8)),
        )


@dataclass(frozen=True)
class EngineConfig:
    run_id: str
    persistence_path: Path
    risk: RiskLimits
    data: DataConfig
    broker: BrokerConfig
    oms: OMSConfig
    strategy_tag: str

    @staticmethod
    def load(path: Optional[str | Path] = None) -> "EngineConfig":
        cfg_path = Path(path or os.getenv("ENGINE_CONFIG", "engine_config.yaml"))
        if not cfg_path.exists():  # pragma: no cover - guard for deployments
            raise FileNotFoundError(f"Engine configuration {cfg_path} not found")
        with cfg_path.open("r", encoding="utf-8") as handle:
            raw = yaml.safe_load(handle) or {}
        run_id = str(raw.get("run_id") or f"run-{dt.datetime.now(IST).strftime('%Y%m%d')}")
        persistence_path = Path(raw.get("persistence_path", "engine_state.sqlite"))
        risk = RiskLimits.from_dict(raw["risk"])
        data = DataConfig.from_dict(raw["data"])
        broker = BrokerConfig.from_dict(raw.get("broker", {}))
        oms = OMSConfig.from_dict(raw.get("oms", {}))
        strategy_tag = str(raw.get("strategy_tag", "intraday-buy"))
        return EngineConfig(
            run_id=run_id,
            persistence_path=persistence_path,
            risk=risk,
            data=data,
            broker=broker,
            oms=oms,
            strategy_tag=strategy_tag,
        )


__all__ = [
    "DataConfig",
    "EngineConfig",
    "IST",
    "OMSConfig",
    "RiskLimits",
    "BrokerConfig",
]
