import datetime as dt

from strategy.contracts import StrategyDecision, StrategyState, TradeIntent


def test_trade_intent_defaults() -> None:
    intent = TradeIntent(
        strategy_id="s1",
        underlying="NIFTY",
        direction="CALL",
        symbol="NIFTY-2024-01-01-22000CE",
    )
    assert intent.qty == 0
    assert intent.entry_style in {"MARKET", "LIMIT"}
    assert intent.time_in_force in {"DAY", "IOC"}
    assert isinstance(intent.created_at, dt.datetime)
    assert intent.created_at.tzinfo is not None


def test_strategy_decision_defaults() -> None:
    decision = StrategyDecision()
    assert decision.intents == []
    assert decision.debug == {}


def test_strategy_state_defaults() -> None:
    state = StrategyState()
    assert state.open_positions == 0
    assert state.last_signal_reason == ""
    assert state.extra == {}

