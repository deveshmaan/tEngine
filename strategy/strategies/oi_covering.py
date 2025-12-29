from __future__ import annotations

from typing import Any, Dict, Optional

from engine.config import IST
from strategy.contracts import BaseIntradayBuyStrategy, StrategyDecision, TradeIntent
from strategy.market_snapshot import MarketSnapshot, OptionQuote
from strategy.strategies.common import pick_best_option


class OICoveringMomentumStrategy(BaseIntradayBuyStrategy):
    strategy_id = "oi_covering"

    def configure(self, params: Dict[str, Any]) -> None:
        self.window_steps = max(int(params.get("window_steps", 6) or 6), 1)
        self.min_oi_drop = float(params.get("min_oi_drop", 0.0) or 0.0)
        self.delta_target = float(params.get("delta_target", 0.4) or 0.4)
        self.delta_min = float(params.get("delta_min", 0.25) or 0.25)
        self.delta_max = float(params.get("delta_max", 0.55) or 0.55)
        self.min_signal_interval_s = max(int(params.get("min_signal_interval_s", 20) or 20), 0)

    def on_tick(self, market_snapshot: MarketSnapshot, ctx) -> StrategyDecision:
        state = ctx.state_for(self.strategy_id)
        ts = market_snapshot.ts.astimezone(IST) if market_snapshot.ts.tzinfo else market_snapshot.ts.replace(tzinfo=IST)
        day_key = ts.date().isoformat()
        if state.extra.get("day") != day_key:
            state.extra.clear()
            state.extra["day"] = day_key

        last_signal_ts = float(state.extra.get("last_signal_ts", 0.0) or 0.0)
        if self.min_signal_interval_s and (ts.timestamp() - last_signal_ts) < self.min_signal_interval_s:
            return StrategyDecision()

        spot = float(market_snapshot.spot)
        prev_spot = state.extra.get("prev_spot")
        state.extra["prev_spot"] = spot
        if prev_spot is None:
            return StrategyDecision()
        prev_spot_f = float(prev_spot)
        momentum = spot - prev_spot_f
        if abs(momentum) <= 0:
            return StrategyDecision()

        step_map = getattr(ctx.cfg.data, "strike_steps", {}) or {}
        strike_step = int(step_map.get(market_snapshot.underlying, ctx.cfg.data.lot_step))
        atm = market_snapshot.nearest_strike(spot)
        if atm is None:
            return StrategyDecision()
        window = max(int(self.window_steps), 1) * max(int(strike_step), 1)

        prev_oi: Dict[str, float] = state.extra.setdefault("prev_oi", {})
        direction = "CALL" if momentum > 0 else "PUT"
        opt_type = "CE" if direction == "CALL" else "PE"

        best: Optional[tuple[OptionQuote, float]] = None
        for opt in market_snapshot.options:
            if opt.opt_type != opt_type or opt.oi is None:
                continue
            if abs(int(opt.strike) - int(atm)) > window:
                continue
            key = str(opt.instrument_key or opt.symbol)
            prev_val = prev_oi.get(key)
            prev_oi[key] = float(opt.oi)
            if prev_val is None:
                continue
            delta_oi = float(opt.oi) - float(prev_val)
            if delta_oi >= 0:
                continue
            drop = -delta_oi
            if self.min_oi_drop and drop < self.min_oi_drop:
                continue
            if best is None or drop > best[1]:
                best = (opt, drop)

        if best is None:
            return StrategyDecision()

        expiry = str(market_snapshot.meta.get("expiry") or "").strip()
        quote, symbol = pick_best_option(
            market_snapshot,
            direction=direction,  # type: ignore[arg-type]
            expiry=expiry,
            spot=spot,
            strike_step=strike_step,
            delta_target=self.delta_target,
            delta_min=self.delta_min,
            delta_max=self.delta_max,
        )
        oi_drop = float(best[1])
        reason = f"OI covering {direction} spotΔ={momentum:.2f} best_oi_drop={oi_drop:.0f} window={self.window_steps}"
        intent = TradeIntent(
            strategy_id=self.strategy_id,
            underlying=market_snapshot.underlying,
            direction=direction,  # type: ignore[arg-type]
            symbol=symbol,
            instrument_key=(quote.instrument_key if quote else None),
            qty=0,
            entry_style="MARKET",
            signal_score=float(min(5.0, oi_drop / 1_000.0)),
            reason=reason,
            meta={"oi_drop": oi_drop, "momentum": momentum, "atm": atm},
        )
        state.extra["last_signal_ts"] = ts.timestamp()
        return StrategyDecision(intents=[intent])

    def end_of_day_flatten(self, ctx) -> None:
        ctx.state_for(self.strategy_id).extra.clear()


__all__ = ["OICoveringMomentumStrategy"]

