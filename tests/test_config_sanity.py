import datetime as dt
from pathlib import Path

import pytest

from engine.config import (
    AlertConfig,
    BankNiftyConfig,
    BrokerConfig,
    DataConfig,
    EngineConfig,
    ExitConfig,
    MarketDataConfig,
    OMSConfig,
    ReplayConfig,
    RiskLimits,
    SecretsConfig,
    SmokeTestConfig,
    StrategyConfig,
    TelemetryConfig,
)
from engine.config_sanity import ConfigError, sanity_check_config


def _base_engine_config(**risk_overrides) -> EngineConfig:
    risk_kwargs = {
        "daily_pnl_stop": 1000.0,
        "per_symbol_loss_stop": 500.0,
        "max_open_lots": 1,
        "notional_premium_cap": 100000.0,
        "max_order_rate": 5,
        "no_new_entries_after": dt.time(15, 10),
        "square_off_by": dt.time(15, 20),
    }
    risk_kwargs.update(risk_overrides)
    risk = RiskLimits(**risk_kwargs)
    return EngineConfig(
        run_id="test",
        persistence_path=Path("engine_state.sqlite"),
        strategy_tag="test",
        strategy=StrategyConfig.from_dict({}),
        risk=risk,
        data=DataConfig.from_dict({}),
        market_data=MarketDataConfig.from_dict({}),
        broker=BrokerConfig.from_dict({}),
        oms=OMSConfig.from_dict({}),
        alerts=AlertConfig.from_dict({}),
        telemetry=TelemetryConfig.from_dict({}),
        replay=ReplayConfig.from_dict({}),
        smoke_test=SmokeTestConfig.from_dict({}),
        exit=ExitConfig.from_dict({}),
        banknifty=BankNiftyConfig.from_dict({}),
        capital_base=100000.0,
        allowed_ips=tuple(),
        secrets=SecretsConfig(),
    )


def test_negative_daily_pnl_stop_rejected():
    cfg = _base_engine_config(daily_pnl_stop=-1.0)
    with pytest.raises(ConfigError):
        sanity_check_config(cfg)


def test_negative_per_symbol_loss_stop_rejected():
    cfg = _base_engine_config(per_symbol_loss_stop=-1.0)
    with pytest.raises(ConfigError):
        sanity_check_config(cfg)
