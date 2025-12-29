from __future__ import annotations

import datetime as dt
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Any, Dict, List, Literal, Optional, Protocol

from strategy.market_snapshot import MarketSnapshot

Direction = Literal["CALL", "PUT"]
EntryStyle = Literal["MARKET", "LIMIT"]
TimeInForce = Literal["DAY", "IOC"]


@dataclass(frozen=True)
class TradeIntent:
    strategy_id: str
    underlying: str
    direction: Direction  # CALL/PUT
    symbol: str  # human-friendly option symbol: UNDERLYING-YYYY-MM-DD-STRIKE(CE|PE)
    qty: int = 0  # 0 means "size via executor"
    instrument_key: Optional[str] = None
    entry_style: EntryStyle = "MARKET"
    limit_price: Optional[float] = None
    stop_loss: Optional[float] = None
    target: Optional[float] = None
    time_in_force: TimeInForce = "DAY"
    signal_score: float = 0.0
    reason: str = ""
    meta: Dict[str, Any] = field(default_factory=dict)
    created_at: dt.datetime = field(default_factory=lambda: dt.datetime.now(dt.timezone.utc))


@dataclass(frozen=True)
class StrategyDecision:
    intents: List[TradeIntent] = field(default_factory=list)
    debug: Dict[str, Any] = field(default_factory=dict)


@dataclass
class StrategyState:
    last_trade_time: Optional[dt.datetime] = None
    cooldown_end: Optional[dt.datetime] = None
    open_positions: int = 0
    last_signal_reason: str = ""
    extra: Dict[str, Any] = field(default_factory=dict)


class StrategyContext(Protocol):
    cfg: Any

    def now(self) -> dt.datetime: ...

    def state_for(self, strategy_id: str) -> StrategyState: ...


class BaseIntradayBuyStrategy(ABC):
    strategy_id: str

    def configure(self, params: Dict[str, Any]) -> None:
        return

    @abstractmethod
    def on_tick(self, market_snapshot: MarketSnapshot, ctx: StrategyContext) -> StrategyDecision:
        raise NotImplementedError

    def on_bar(self, market_snapshot: MarketSnapshot, ctx: StrategyContext) -> StrategyDecision:  # optional
        return StrategyDecision()

    def on_order_update(self, event: Dict[str, Any], ctx: StrategyContext) -> None:  # optional
        return

    @abstractmethod
    def end_of_day_flatten(self, ctx: StrategyContext) -> None:
        raise NotImplementedError


__all__ = [
    "BaseIntradayBuyStrategy",
    "Direction",
    "EntryStyle",
    "MarketSnapshot",
    "StrategyContext",
    "StrategyDecision",
    "StrategyState",
    "TimeInForce",
    "TradeIntent",
]

