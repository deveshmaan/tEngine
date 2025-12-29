import datetime as dt

from engine.config import EngineConfig, IST
from engine.strategy_manager import StrategyManager


def _app_cfg(*, enabled: bool = True, cooldown_seconds: int = 0, max_positions: int = 1) -> dict:
    return {
        "multi_strategy": {
            "enabled": True,
            "strategies": [
                {
                    "id": "orb",
                    "import": "strategy.strategies.orb:OpeningRangeBreakoutStrategy",
                    "enabled": enabled,
                    "params": {"range_minutes": 1, "breakout_margin_points": 0},
                    "risk": {"cooldown_seconds": cooldown_seconds, "max_positions": max_positions},
                }
            ],
        }
    }


def test_strategy_manager_load_and_enable_disable() -> None:
    cfg = EngineConfig.load()
    mgr = StrategyManager.from_app_config(cfg=cfg, app_config=_app_cfg(enabled=True))
    assert mgr.strategy_ids() == ["orb"]
    assert mgr.enabled_strategy_ids() == ["orb"]
    assert mgr.strategy("orb") is not None

    mgr2 = StrategyManager.from_app_config(cfg=cfg, app_config=_app_cfg(enabled=False))
    assert mgr2.strategy_ids() == ["orb"]
    assert mgr2.enabled_strategy_ids() == []


def test_strategy_manager_cooldown_and_position_tracking() -> None:
    cfg = EngineConfig.load()
    mgr = StrategyManager.from_app_config(cfg=cfg, app_config=_app_cfg(enabled=True, cooldown_seconds=60, max_positions=1))
    now = dt.datetime(2024, 1, 1, 9, 16, 0, tzinfo=IST)
    assert mgr.can_run("orb", now=now)

    symbol = "NIFTY-2024-01-01-100CE"
    mgr.record_order_submission(client_order_id="OID-1", strategy_id="orb", symbol=symbol)
    mgr.on_fill({"order_id": "OID-1", "symbol": symbol, "side": "BUY", "qty": 75, "price": 10.0, "ts": now.isoformat()})
    assert mgr.state_for("orb").open_positions == 1
    assert mgr.state_for("orb").cooldown_end is not None
    assert not mgr.can_run("orb", now=now)

    after = now + dt.timedelta(seconds=61)
    # Position still open, so max_positions blocks.
    assert not mgr.can_run("orb", now=after)

    # Close the position via a SELL fill; cooldown should be extended.
    mgr.on_fill({"order_id": "OID-EXIT", "symbol": symbol, "side": "SELL", "qty": 75, "price": 11.0, "ts": after.isoformat()})
    assert mgr.state_for("orb").open_positions == 0
    assert mgr.state_for("orb").cooldown_end is not None
    assert not mgr.can_run("orb", now=after)

    later = after + dt.timedelta(seconds=61)
    assert mgr.can_run("orb", now=later)

