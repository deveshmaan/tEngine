from __future__ import annotations

import datetime as dt
import json
from pathlib import Path

import pytest

from brokerage.upstox_client import UpstoxSession
from engine import data as data_mod
from engine.config import CONFIG
from market.instrument_cache import InstrumentCache


class _DummyOptionsApi:
    def __init__(self) -> None:
        self.calls: list[dict[str, str]] = []

    def get_option_contracts(self, **kwargs):
        self.calls.append(dict(kwargs))
        return {"data": []}


class _DiscoveryOnlySession:
    def __init__(self) -> None:
        self.calls: list[dict[str, str | None]] = []

    def get_option_contracts(self, instrument_key: str, expiry_date: str | None = None):
        self.calls.append({"instrument_key": instrument_key, "expiry_date": expiry_date})
        return {"data": []}


class _OverrideSession:
    def __init__(self, valid: bool) -> None:
        self.calls: list[tuple[str, str | None]] = []
        self._valid = valid

    def get_option_contracts(self, instrument_key: str, expiry_date: str | None = None):
        self.calls.append((instrument_key, expiry_date))
        if expiry_date:
            if self._valid:
                return {"data": [{"expiry": expiry_date}]}
            return {"data": []}
        return {"data": []}


def _write_override_config(path: Path, override_path: Path) -> None:
    payload = f"""
run_id: "test"
persistence_path: "engine_state.sqlite"
strategy_tag: "test"

data:
  index_symbol: "NIFTY"
  lot_step: 50
  tick_size: 0.05
  price_band_low: 1.0
  price_band_high: 9999.0
  expiry_ttl_minutes: 5
  weekly_expiry_weekday: 3
  allow_expiry_override: true
  expiry_override_path: "{override_path}"
  expiry_override_max_age_minutes: 120

risk:
  daily_pnl_stop: -1
  per_symbol_loss_stop: -1
  max_open_lots: 1
  notional_premium_cap: 1
  max_order_rate: 1
  no_new_entries_after: "15:00"
  square_off_by: "15:30"

broker:
  max_order_rate: 1
  rest_timeout: 1.0
  ws_heartbeat_interval: 1.0
  ws_backoff_seconds: [1, 2]
  oauth_refresh_margin: 60

oms:
  resubmit_backoff: 0.5
  reconciliation_interval: 1.0
  max_inflight_orders: 1
  submit:
    default: "market"
    max_spread_ticks: 1
    depth_threshold: 0

alerts:
  throttle_seconds: 1

telemetry:
  metrics_port_env: "METRICS_PORT"

replay:
  default_speed: 1.0
"""
    path.write_text(payload, encoding="utf-8")


def _write_override_file(path: Path, expiries: list[str]) -> None:
    payload = {
        "generated_at": dt.datetime.now(dt.timezone.utc).isoformat(),
        "underlyings": {"NIFTY": expiries},
    }
    path.write_text(json.dumps(payload), encoding="utf-8")


def _reset_cache(cache: InstrumentCache) -> None:
    cache.close()
    InstrumentCache._runtime = None  # type: ignore[attr-defined]


def test_upstox_discovery_omits_expiry_kwarg(monkeypatch):
    session = object.__new__(UpstoxSession)
    options = _DummyOptionsApi()
    session.options_api = options
    result = UpstoxSession.get_option_contracts(session, "NSE_INDEX|Nifty 50")
    assert result == {"data": []}
    assert options.calls[0] == {"instrument_key": "NSE_INDEX|Nifty 50"}
    options.calls.clear()
    UpstoxSession.get_option_contracts(session, "NSE_INDEX|Nifty 50", "2024-07-25")
    assert options.calls[0] == {"instrument_key": "NSE_INDEX|Nifty 50", "expiry_date": "2024-07-25"}


def test_cache_fallback_uses_instruments_dataset(tmp_path, monkeypatch):
    session = _DiscoveryOnlySession()
    monkeypatch.setattr(
        InstrumentCache,
        "_load_instruments_json",
        lambda self: [
            {"instrument_type": "OPTIDX", "underlying_key": "NSE_INDEX|Nifty 50", "expiry": "2024-07-11"},
            {"instrument_type": "OPTIDX", "underlying_key": "NSE_INDEX|Nifty 50", "expiry": "2024-07-25"},
        ],
        raising=False,
    )
    cache = InstrumentCache(str(tmp_path / "cache.sqlite"), session_factory=lambda: session)
    try:
        expiries = cache.list_expiries("NIFTY")
    finally:
        _reset_cache(cache)
    assert session.calls and session.calls[0]["expiry_date"] is None
    assert expiries == ["2024-07-11", "2024-07-25"]


def test_cache_uses_local_override_when_validated(tmp_path, monkeypatch):
    override_path = tmp_path / "override.json"
    _write_override_file(override_path, ["2024-07-25", "2024-07-18"])
    cfg_path = tmp_path / "app.yml"
    _write_override_config(cfg_path, override_path)
    monkeypatch.setenv("APP_CONFIG_PATH", str(cfg_path))
    data_mod._reset_app_config_cache()
    CONFIG.reload()
    session = _OverrideSession(valid=True)
    monkeypatch.setattr(InstrumentCache, "_load_instruments_json", lambda self: [], raising=False)
    cache = InstrumentCache(str(tmp_path / "override.sqlite"), session_factory=lambda: session)
    try:
        expiries = cache.list_expiries("NIFTY")
    finally:
        _reset_cache(cache)
        monkeypatch.delenv("APP_CONFIG_PATH", raising=False)
        CONFIG.reload()
        data_mod._reset_app_config_cache()
    assert expiries == ["2024-07-18", "2024-07-25"]
    assert len(session.calls) == 2
    assert session.calls[1][1] == "2024-07-18"


def test_override_rejected_when_validation_fails(tmp_path, monkeypatch):
    override_path = tmp_path / "override.json"
    _write_override_file(override_path, ["2024-07-25"])
    cfg_path = tmp_path / "app.yml"
    _write_override_config(cfg_path, override_path)
    monkeypatch.setenv("APP_CONFIG_PATH", str(cfg_path))
    data_mod._reset_app_config_cache()
    CONFIG.reload()
    session = _OverrideSession(valid=False)
    monkeypatch.setattr(InstrumentCache, "_load_instruments_json", lambda self: [], raising=False)
    cache = InstrumentCache(str(tmp_path / "override.sqlite"), session_factory=lambda: session)
    try:
        with pytest.raises(RuntimeError):
            cache.list_expiries("NIFTY")
    finally:
        _reset_cache(cache)
        monkeypatch.delenv("APP_CONFIG_PATH", raising=False)
        CONFIG.reload()
        data_mod._reset_app_config_cache()
    assert len(session.calls) == 2
    assert session.calls[1][1] == "2024-07-25"
