from __future__ import annotations

import datetime as dt
from dataclasses import dataclass
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple

from engine.risk import OrderBudget, RiskManager
from engine.signal_engine import GateResult, SignalEngine
from engine.strategy_manager import StrategyManager
from strategy.contracts import TradeIntent
from strategy.market_snapshot import MarketSnapshot


@dataclass(frozen=True)
class Routed:
    accepted: List[TradeIntent]
    rejected: List[Tuple[TradeIntent, str, str]]


class IntentRouter:
    def __init__(
        self,
        *,
        risk: RiskManager,
        signal_engine: SignalEngine,
        strategies: StrategyManager,
        allow_straddle: bool = False,
        max_global_positions: int = 0,
    ) -> None:
        self._risk = risk
        self._signal = signal_engine
        self._strategies = strategies
        self._allow_straddle = bool(allow_straddle)
        self._max_global_positions = max(int(max_global_positions or 0), 0)

    def route(self, intents: Sequence[TradeIntent], snapshot: MarketSnapshot, *, now: dt.datetime) -> Routed:
        rejected: list[tuple[TradeIntent, str, str]] = []
        if not intents:
            return Routed(accepted=[], rejected=[])
        if self._risk.should_halt(now=now):
            for intent in intents:
                rejected.append((intent, "HALT", "Risk halt active"))
            return Routed(accepted=[], rejected=rejected)

        cleaned: list[TradeIntent] = []
        for intent in intents:
            if not intent.symbol or not intent.strategy_id or not intent.underlying:
                rejected.append((intent, "INVALID", "Missing symbol/strategy_id/underlying"))
                continue
            if intent.qty < 0:
                rejected.append((intent, "INVALID_QTY", "Negative qty"))
                continue
            cleaned.append(intent)

        if not cleaned:
            return Routed(accepted=[], rejected=rejected)

        # Conflict resolution (CALL vs PUT) per underlying.
        selected: list[TradeIntent] = []
        by_underlying: Dict[str, List[TradeIntent]] = {}
        for intent in cleaned:
            by_underlying.setdefault(intent.underlying.upper(), []).append(intent)
        for under, group in by_underlying.items():
            if self._allow_straddle:
                selected.extend(group)
                continue
            calls = [i for i in group if i.direction == "CALL"]
            puts = [i for i in group if i.direction == "PUT"]
            if calls and puts:
                best = max(group, key=lambda x: float(x.signal_score or 0.0))
                for other in group:
                    if other is best:
                        continue
                    rejected.append((other, "CONFLICT", "CALL/PUT conflict"))
                selected.append(best)
            else:
                selected.extend(group)

        # Enforce global capacity in terms of open positions (count, not lots).
        capacity = None
        if self._max_global_positions > 0:
            try:
                open_now = int(self._risk.open_position_count())
            except Exception:
                open_now = 0
            capacity = max(self._max_global_positions - open_now, 0)
            if capacity <= 0:
                for intent in selected:
                    rejected.append((intent, "MAX_POS", "Global max positions reached"))
                return Routed(accepted=[], rejected=rejected)

        gated: list[TradeIntent] = []
        for intent in selected:
            if not self._strategies.can_run(intent.strategy_id, now=now):
                rejected.append((intent, "STRATEGY_LIMIT", "Strategy cooldown/max positions"))
                continue
            gate = self._signal.evaluate(intent, snapshot)
            if not gate.ok:
                rejected.append((intent, gate.code or "GATE", gate.reason or "gate_reject"))
                continue
            gated.append(intent)

        gated.sort(key=lambda x: float(x.signal_score or 0.0), reverse=True)
        if capacity is not None:
            accepted = gated[:capacity]
            for intent in gated[capacity:]:
                rejected.append((intent, "CAPACITY", "Global capacity filled"))
        else:
            accepted = gated

        return Routed(accepted=accepted, rejected=rejected)

    def budget_ok(self, *, intent: TradeIntent, lot_size: int, premium: float) -> tuple[bool, str, str]:
        budget = OrderBudget(symbol=intent.symbol, qty=intent.qty, price=float(premium), lot_size=int(lot_size), side="BUY")
        if not self._risk.budget_ok_for(budget):
            return False, "RISK", "RiskManager budget rejected"
        return True, "OK", "ok"


__all__ = ["IntentRouter", "Routed"]
