from __future__ import annotations

import datetime as dt
import importlib
from dataclasses import dataclass, field
from typing import Any, Dict, Iterable, Mapping, Optional

from engine.config import EngineConfig
from engine.logging_utils import get_logger
from strategy.contracts import BaseIntradayBuyStrategy, StrategyState, TradeIntent


@dataclass(frozen=True)
class StrategyRiskBudget:
    cooldown_seconds: int = 0
    max_positions: int = 1
    max_premium_per_trade: float = 0.0
    risk_percent_per_trade: float = 0.0
    max_open_premium: float = 0.0


@dataclass(frozen=True)
class StrategySpec:
    strategy_id: str
    import_path: str
    enabled: bool = True
    params: Dict[str, Any] = field(default_factory=dict)
    risk: StrategyRiskBudget = field(default_factory=StrategyRiskBudget)


class StrategyManager:
    """Loads and tracks multiple stateless strategies + per-strategy runtime state."""

    def __init__(self, *, cfg: EngineConfig, specs: Iterable[StrategySpec]) -> None:
        self.cfg = cfg
        self._logger = get_logger("StrategyManager")
        self._specs: Dict[str, StrategySpec] = {spec.strategy_id: spec for spec in specs}
        self._strategies: Dict[str, BaseIntradayBuyStrategy] = {}
        self._state: Dict[str, StrategyState] = {}

        # Runtime trackers for per-strategy positions (symbol -> net qty).
        self._order_to_strategy: Dict[str, str] = {}
        self._symbol_owner: Dict[str, str] = {}
        self._net_qty_by_symbol: Dict[str, int] = {}

        self._load_all()

    @classmethod
    def from_app_config(cls, *, cfg: EngineConfig, app_config: Mapping[str, Any]) -> "StrategyManager":
        raw = (app_config.get("multi_strategy") or {}) if isinstance(app_config, Mapping) else {}
        entries = raw.get("strategies") or []
        specs: list[StrategySpec] = []
        if isinstance(entries, list):
            for entry in entries:
                if not isinstance(entry, Mapping):
                    continue
                strategy_id = str(entry.get("id") or entry.get("strategy_id") or "").strip()
                import_path = str(entry.get("import") or entry.get("import_path") or entry.get("path") or "").strip()
                if not strategy_id or not import_path:
                    continue
                enabled = bool(entry.get("enabled", True))
                params = entry.get("params") if isinstance(entry.get("params"), Mapping) else {}
                risk_raw = entry.get("risk") if isinstance(entry.get("risk"), Mapping) else {}
                budget = StrategyRiskBudget(
                    cooldown_seconds=max(int(risk_raw.get("cooldown_seconds", 0) or 0), 0),
                    max_positions=max(int(risk_raw.get("max_positions", 1) or 1), 0),
                    max_premium_per_trade=max(float(risk_raw.get("max_premium_per_trade", 0.0) or 0.0), 0.0),
                    risk_percent_per_trade=max(float(risk_raw.get("risk_percent_per_trade", 0.0) or 0.0), 0.0),
                    max_open_premium=max(float(risk_raw.get("max_open_premium", 0.0) or 0.0), 0.0),
                )
                specs.append(
                    StrategySpec(
                        strategy_id=strategy_id,
                        import_path=import_path,
                        enabled=enabled,
                        params=dict(params),
                        risk=budget,
                    )
                )
        return cls(cfg=cfg, specs=specs)

    def strategy_ids(self) -> list[str]:
        return sorted(self._specs.keys())

    def enabled_strategy_ids(self) -> list[str]:
        return [sid for sid in self.strategy_ids() if self._specs[sid].enabled]

    def strategy(self, strategy_id: str) -> Optional[BaseIntradayBuyStrategy]:
        return self._strategies.get(strategy_id)

    def specs(self) -> Dict[str, StrategySpec]:
        return dict(self._specs)

    def state_for(self, strategy_id: str) -> StrategyState:
        return self._state.setdefault(strategy_id, StrategyState())

    def budget_for(self, strategy_id: str) -> StrategyRiskBudget:
        return self._specs[strategy_id].risk

    def can_run(self, strategy_id: str, *, now: dt.datetime) -> bool:
        spec = self._specs.get(strategy_id)
        if not spec or not spec.enabled:
            return False
        state = self.state_for(strategy_id)
        if state.cooldown_end and now < state.cooldown_end:
            return False
        max_pos = max(int(spec.risk.max_positions), 0)
        if max_pos > 0 and state.open_positions >= max_pos:
            return False
        return True

    def record_order_submission(self, *, client_order_id: str, strategy_id: str, symbol: str) -> None:
        if client_order_id:
            self._order_to_strategy[client_order_id] = strategy_id
        if symbol and strategy_id:
            self._symbol_owner.setdefault(symbol, strategy_id)

    def on_fill(self, fill: Mapping[str, Any]) -> None:
        order_id = str(fill.get("order_id") or "")
        symbol = str(fill.get("symbol") or "")
        side = str(fill.get("side") or "").upper()
        try:
            qty = int(fill.get("qty") or 0)
        except Exception:
            qty = 0
        if not symbol or qty <= 0 or side not in {"BUY", "SELL"}:
            return
        strategy_id = self._order_to_strategy.get(order_id) or self._symbol_owner.get(symbol)
        if side == "BUY" and strategy_id:
            self._symbol_owner[symbol] = strategy_id
        signed = qty if side == "BUY" else -qty
        self._net_qty_by_symbol[symbol] = int(self._net_qty_by_symbol.get(symbol, 0) + signed)
        net = self._net_qty_by_symbol.get(symbol, 0)
        if net <= 0:
            self._net_qty_by_symbol.pop(symbol, None)
            self._symbol_owner.pop(symbol, None)
        self._recount_open_positions()

        if strategy_id:
            state = self.state_for(strategy_id)
            ts = _parse_ts(fill.get("ts"))
            if side == "BUY":
                state.last_trade_time = ts
                state.last_signal_reason = state.last_signal_reason or ""
                cooldown = self._specs[strategy_id].risk.cooldown_seconds
                if cooldown > 0:
                    state.cooldown_end = ts + dt.timedelta(seconds=cooldown)
            elif net <= 0:
                cooldown = self._specs[strategy_id].risk.cooldown_seconds
                if cooldown > 0:
                    state.cooldown_end = ts + dt.timedelta(seconds=cooldown)

    def apply_post_decision(self, decision_intent: TradeIntent, *, now: dt.datetime) -> None:
        sid = decision_intent.strategy_id
        if sid not in self._specs:
            return
        state = self.state_for(sid)
        state.last_signal_reason = str(decision_intent.reason or "")
        if decision_intent.signal_score:
            state.extra["last_score"] = float(decision_intent.signal_score)

    def _load_all(self) -> None:
        for sid, spec in self._specs.items():
            try:
                self._strategies[sid] = self._instantiate(spec)
            except Exception as exc:
                self._logger.log_event(30, "strategy_load_failed", strategy_id=sid, error=str(exc))

    def _instantiate(self, spec: StrategySpec) -> BaseIntradayBuyStrategy:
        cls = _load_symbol(spec.import_path)
        obj: Any = cls()
        if not isinstance(obj, BaseIntradayBuyStrategy):
            raise TypeError(f"{spec.import_path} does not implement BaseIntradayBuyStrategy")
        obj.strategy_id = spec.strategy_id
        try:
            obj.configure(dict(spec.params))
        except Exception:
            pass
        self._state.setdefault(spec.strategy_id, StrategyState())
        return obj

    def _recount_open_positions(self) -> None:
        counts: Dict[str, int] = {}
        for symbol, owner in self._symbol_owner.items():
            if symbol not in self._net_qty_by_symbol:
                continue
            counts[owner] = counts.get(owner, 0) + 1
        for sid in self._specs:
            self.state_for(sid).open_positions = int(counts.get(sid, 0))


def _load_symbol(import_path: str) -> Any:
    text = str(import_path or "").strip()
    if not text:
        raise ValueError("empty import path")
    if ":" in text:
        mod, name = text.split(":", 1)
    else:
        mod, _, name = text.rpartition(".")
        if not mod:
            raise ValueError(f"Invalid import path: {import_path}")
    module = importlib.import_module(mod)
    return getattr(module, name)


def _parse_ts(value: object) -> dt.datetime:
    if isinstance(value, dt.datetime):
        parsed = value
    else:
        try:
            parsed = dt.datetime.fromisoformat(str(value))
        except Exception:
            parsed = dt.datetime.now(dt.timezone.utc)
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=dt.timezone.utc)
    return parsed


__all__ = ["StrategyManager", "StrategyRiskBudget", "StrategySpec"]

