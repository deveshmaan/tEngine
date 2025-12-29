import datetime as dt

import pytest

from engine.config import IST, RiskLimits
from engine.intent_router import IntentRouter
from engine.signal_engine import SignalEngine
from engine.strategy_manager import StrategyManager, StrategySpec
from engine.risk import RiskManager
from persistence import SQLiteStore
from strategy.contracts import TradeIntent
from strategy.market_snapshot import MarketSnapshot, OptionQuote


def _risk_limits() -> RiskLimits:
    return RiskLimits(
        daily_pnl_stop=-10_000,
        per_symbol_loss_stop=5_000,
        max_open_lots=10,
        notional_premium_cap=200_000,
        max_order_rate=50,
        no_new_entries_after=dt.time(15, 0),
        square_off_by=dt.time(15, 20),
    )


def _snapshot(ts: dt.datetime) -> MarketSnapshot:
    options = [
        OptionQuote(
            instrument_key="OPTCE",
            symbol="NIFTY-2024-01-01-100CE",
            underlying="NIFTY",
            expiry="2024-01-01",
            strike=100,
            opt_type="CE",
            ltp=10.0,
            bid=9.0,
            ask=11.0,
            iv=0.2,
            oi=1000.0,
            ts=ts,
        ),
        OptionQuote(
            instrument_key="OPTPE",
            symbol="NIFTY-2024-01-01-100PE",
            underlying="NIFTY",
            expiry="2024-01-01",
            strike=100,
            opt_type="PE",
            ltp=10.0,
            bid=9.0,
            ask=11.0,
            iv=0.2,
            oi=1000.0,
            ts=ts,
        ),
    ]
    return MarketSnapshot.from_tick(ts=ts, underlying="NIFTY", spot=100.0, option_quotes=options, meta={"expiry": "2024-01-01"})


def test_router_resolves_call_put_conflict(tmp_path) -> None:
    cfg_stub = type("C", (), {"risk": _risk_limits()})()
    store = SQLiteStore(tmp_path / "risk.sqlite", run_id="risk")
    risk = RiskManager(_risk_limits(), store, clock=lambda: dt.datetime(2024, 1, 1, 10, 0, tzinfo=IST))
    signal = SignalEngine(delta_min=0.25, delta_max=0.55)
    strategies = StrategyManager(cfg=cfg_stub, specs=[StrategySpec("s1", "strategy.strategies.orb:OpeningRangeBreakoutStrategy"), StrategySpec("s2", "strategy.strategies.orb:OpeningRangeBreakoutStrategy")])  # type: ignore[arg-type]
    router = IntentRouter(risk=risk, signal_engine=signal, strategies=strategies, allow_straddle=False)
    ts = dt.datetime(2024, 1, 1, 9, 30, tzinfo=IST)
    snap = _snapshot(ts)
    call = TradeIntent(strategy_id="s1", underlying="NIFTY", direction="CALL", symbol="NIFTY-2024-01-01-100CE", signal_score=0.5)
    put = TradeIntent(strategy_id="s2", underlying="NIFTY", direction="PUT", symbol="NIFTY-2024-01-01-100PE", signal_score=0.9)
    routed = router.route([call, put], snap, now=ts)
    assert [i.direction for i in routed.accepted] == ["PUT"]


def test_router_allows_straddle_when_enabled(tmp_path) -> None:
    cfg_stub = type("C", (), {"risk": _risk_limits()})()
    store = SQLiteStore(tmp_path / "risk2.sqlite", run_id="risk2")
    risk = RiskManager(_risk_limits(), store, clock=lambda: dt.datetime(2024, 1, 1, 10, 0, tzinfo=IST))
    signal = SignalEngine(delta_min=0.25, delta_max=0.55)
    strategies = StrategyManager(cfg=cfg_stub, specs=[StrategySpec("s1", "strategy.strategies.orb:OpeningRangeBreakoutStrategy"), StrategySpec("s2", "strategy.strategies.orb:OpeningRangeBreakoutStrategy")])  # type: ignore[arg-type]
    router = IntentRouter(risk=risk, signal_engine=signal, strategies=strategies, allow_straddle=True)
    ts = dt.datetime(2024, 1, 1, 9, 30, tzinfo=IST)
    snap = _snapshot(ts)
    call = TradeIntent(strategy_id="s1", underlying="NIFTY", direction="CALL", symbol="NIFTY-2024-01-01-100CE", signal_score=0.5)
    put = TradeIntent(strategy_id="s2", underlying="NIFTY", direction="PUT", symbol="NIFTY-2024-01-01-100PE", signal_score=0.9)
    routed = router.route([call, put], snap, now=ts)
    assert len(routed.accepted) == 2
    assert routed.accepted[0].direction == "PUT"


def test_router_enforces_global_capacity(tmp_path) -> None:
    store = SQLiteStore(tmp_path / "risk3.sqlite", run_id="risk3")
    now = dt.datetime(2024, 1, 1, 10, 0, tzinfo=IST)
    risk = RiskManager(_risk_limits(), store, clock=lambda: now)
    risk.on_fill(symbol="S1", side="BUY", qty=50, price=100.0, lot_size=50)
    signal = SignalEngine(delta_min=0.25, delta_max=0.55)
    cfg_stub = type("C", (), {"risk": _risk_limits()})()
    strategies = StrategyManager(cfg=cfg_stub, specs=[StrategySpec("s1", "strategy.strategies.orb:OpeningRangeBreakoutStrategy")])  # type: ignore[arg-type]
    router = IntentRouter(risk=risk, signal_engine=signal, strategies=strategies, allow_straddle=False, max_global_positions=1)
    snap = _snapshot(now)
    call = TradeIntent(strategy_id="s1", underlying="NIFTY", direction="CALL", symbol="NIFTY-2024-01-01-100CE", signal_score=1.0)
    routed = router.route([call], snap, now=now)
    assert routed.accepted == []
    assert routed.rejected

