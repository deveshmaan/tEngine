from __future__ import annotations

import datetime as dt
from dataclasses import replace
from typing import Any, Dict, Optional

from engine.config import IST
from strategy.contracts import BaseIntradayBuyStrategy, StrategyDecision, TradeIntent
from strategy.market_snapshot import MarketSnapshot
from strategy.strategies.common import pick_best_option, score_breakout


class OpeningRangeBreakoutStrategy(BaseIntradayBuyStrategy):
    strategy_id = "orb"

    def configure(self, params: Dict[str, Any]) -> None:
        self.range_minutes = max(int(params.get("range_minutes", 15) or 15), 1)
        self.margin_points = float(params.get("breakout_margin_points", params.get("margin_points", 0.0)) or 0.0)
        self.margin_pct = float(params.get("breakout_margin_pct", params.get("margin_pct", 0.0)) or 0.0)
        self.min_signal_interval_s = max(int(params.get("min_signal_interval_s", 10) or 10), 0)
        self.delta_target = float(params.get("delta_target", 0.4) or 0.4)
        self.delta_min = float(params.get("delta_min", 0.25) or 0.25)
        self.delta_max = float(params.get("delta_max", 0.55) or 0.55)

    def on_tick(self, market_snapshot: MarketSnapshot, ctx) -> StrategyDecision:
        state = ctx.state_for(self.strategy_id)
        ts = market_snapshot.ts.astimezone(IST) if market_snapshot.ts.tzinfo else market_snapshot.ts.replace(tzinfo=IST)
        day_key = ts.date().isoformat()
        if state.extra.get("day") != day_key:
            state.extra.clear()
            state.extra["day"] = day_key

        open_ts = ts.replace(hour=9, minute=15, second=0, microsecond=0)
        end_ts = open_ts + dt.timedelta(minutes=self.range_minutes)
        spot = float(market_snapshot.spot)

        hi = float(state.extra.get("orb_high", spot))
        lo = float(state.extra.get("orb_low", spot))

        if ts < end_ts:
            state.extra["orb_high"] = max(hi, spot)
            state.extra["orb_low"] = min(lo, spot)
            state.extra["prev_spot"] = spot
            return StrategyDecision()

        state.extra.setdefault("orb_high", hi)
        state.extra.setdefault("orb_low", lo)
        hi = float(state.extra.get("orb_high", spot))
        lo = float(state.extra.get("orb_low", spot))
        prev = state.extra.get("prev_spot")
        state.extra["prev_spot"] = spot

        last_signal_ts = float(state.extra.get("last_signal_ts", 0.0) or 0.0)
        if self.min_signal_interval_s and (ts.timestamp() - last_signal_ts) < self.min_signal_interval_s:
            return StrategyDecision()

        if prev is None:
            return StrategyDecision()
        prev_spot = float(prev)
        width = max(hi - lo, 1e-9)
        margin = self.margin_points if self.margin_points > 0 else (spot * self.margin_pct if self.margin_pct > 0 else 0.0)

        direction: Optional[str] = None
        level = None
        if spot > (hi + margin) and spot > prev_spot:
            direction = "CALL"
            level = hi + margin
        elif spot < (lo - margin) and spot < prev_spot:
            direction = "PUT"
            level = lo - margin
        if not direction:
            return StrategyDecision()

        expiry = str(market_snapshot.meta.get("expiry") or "").strip()
        step_map = getattr(ctx.cfg.data, "strike_steps", {}) or {}
        strike_step = int(step_map.get(market_snapshot.underlying, ctx.cfg.data.lot_step))
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
        score = score_breakout(spot - float(level or spot), width)
        reason = f"ORB {direction} breakout spot={spot:.2f} range=[{lo:.2f},{hi:.2f}] margin={margin:.2f}"
        intent = TradeIntent(
            strategy_id=self.strategy_id,
            underlying=market_snapshot.underlying,
            direction=direction,  # type: ignore[arg-type]
            symbol=symbol,
            instrument_key=(quote.instrument_key if quote else None),
            qty=0,
            entry_style="MARKET",
            limit_price=None,
            signal_score=float(score),
            reason=reason,
            meta={"orb_high": hi, "orb_low": lo, "margin": margin, "spot": spot},
        )
        state.extra["last_signal_ts"] = ts.timestamp()
        return StrategyDecision(intents=[intent], debug={"orb_high": hi, "orb_low": lo, "expiry": expiry})

    def end_of_day_flatten(self, ctx) -> None:
        state = ctx.state_for(self.strategy_id)
        state.extra.clear()


__all__ = ["OpeningRangeBreakoutStrategy"]

