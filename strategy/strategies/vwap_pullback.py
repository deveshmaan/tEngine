from __future__ import annotations

from dataclasses import replace
from typing import Any, Dict, Optional

from engine.config import IST
from strategy.contracts import BaseIntradayBuyStrategy, StrategyDecision, TradeIntent
from strategy.market_snapshot import MarketSnapshot
from strategy.strategies.common import ema, pick_best_option, safe_div


class VWAPPullbackContinuationStrategy(BaseIntradayBuyStrategy):
    strategy_id = "vwap_pullback"

    def configure(self, params: Dict[str, Any]) -> None:
        self.band_pct = float(params.get("band_pct", 0.001) or 0.001)
        self.fast_ema = max(int(params.get("fast_ema", 8) or 8), 1)
        self.slow_ema = max(int(params.get("slow_ema", 21) or 21), self.fast_ema + 1)
        self.delta_target = float(params.get("delta_target", 0.4) or 0.4)
        self.delta_min = float(params.get("delta_min", 0.25) or 0.25)
        self.delta_max = float(params.get("delta_max", 0.55) or 0.55)
        self.min_signal_interval_s = max(int(params.get("min_signal_interval_s", 15) or 15), 0)

    def on_tick(self, market_snapshot: MarketSnapshot, ctx) -> StrategyDecision:
        state = ctx.state_for(self.strategy_id)
        ts = market_snapshot.ts.astimezone(IST) if market_snapshot.ts.tzinfo else market_snapshot.ts.replace(tzinfo=IST)
        day_key = ts.date().isoformat()
        if state.extra.get("day") != day_key:
            state.extra.clear()
            state.extra["day"] = day_key

        spot = float(market_snapshot.spot)
        cum_pv = float(state.extra.get("cum_pv", 0.0) or 0.0) + spot
        cum_v = float(state.extra.get("cum_v", 0.0) or 0.0) + 1.0
        vwap = safe_div(cum_pv, cum_v, default=spot)
        state.extra["cum_pv"] = cum_pv
        state.extra["cum_v"] = cum_v
        state.extra["vwap"] = vwap

        alpha_f = 2.0 / (self.fast_ema + 1.0)
        alpha_s = 2.0 / (self.slow_ema + 1.0)
        ema_fast = ema(state.extra.get("ema_fast"), spot, alpha_f)
        ema_slow = ema(state.extra.get("ema_slow"), spot, alpha_s)
        state.extra["ema_fast"] = ema_fast
        state.extra["ema_slow"] = ema_slow

        last_signal_ts = float(state.extra.get("last_signal_ts", 0.0) or 0.0)
        if self.min_signal_interval_s and (ts.timestamp() - last_signal_ts) < self.min_signal_interval_s:
            return StrategyDecision()

        band = abs(float(self.band_pct)) * vwap
        touched = bool(state.extra.get("touched", False))
        direction: Optional[str] = None
        trend_up = spot > vwap and ema_fast > ema_slow
        trend_down = spot < vwap and ema_fast < ema_slow

        if trend_up:
            if spot <= (vwap - band):
                state.extra["touched"] = True
                return StrategyDecision(debug={"vwap": vwap, "trend": "up", "state": "pullback"})
            if touched and spot >= vwap:
                direction = "CALL"
        elif trend_down:
            if spot >= (vwap + band):
                state.extra["touched"] = True
                return StrategyDecision(debug={"vwap": vwap, "trend": "down", "state": "pullback"})
            if touched and spot <= vwap:
                direction = "PUT"

        if not direction:
            state.extra["touched"] = False
            return StrategyDecision(debug={"vwap": vwap, "trend": "flat"})

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
        score = abs(spot - vwap) / max(vwap, 1e-9)
        reason = f"VWAP {direction} pullback-resume spot={spot:.2f} vwap={vwap:.2f} band={band:.2f}"
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
            meta={"vwap": vwap, "band": band, "ema_fast": ema_fast, "ema_slow": ema_slow},
        )
        state.extra["touched"] = False
        state.extra["last_signal_ts"] = ts.timestamp()
        return StrategyDecision(intents=[intent], debug={"vwap": vwap, "trend": "resume"})

    def end_of_day_flatten(self, ctx) -> None:
        ctx.state_for(self.strategy_id).extra.clear()


__all__ = ["VWAPPullbackContinuationStrategy"]

