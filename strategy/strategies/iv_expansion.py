from __future__ import annotations

import statistics
from collections import deque
from typing import Any, Deque, Dict, Optional

from engine.config import IST
from strategy.contracts import BaseIntradayBuyStrategy, StrategyDecision, TradeIntent
from strategy.market_snapshot import MarketSnapshot
from strategy.strategies.common import pick_best_option, safe_div


class IVExpansionBreakoutStrategy(BaseIntradayBuyStrategy):
    strategy_id = "iv_expansion"

    def configure(self, params: Dict[str, Any]) -> None:
        self.lookback = max(int(params.get("lookback_ticks", 60) or 60), 10)
        self.iv_lookback = max(int(params.get("iv_lookback", 120) or 120), 20)
        self.iv_pct_max = float(params.get("iv_pct_max", 0.9) or 0.9)
        self.min_iv_slope = float(params.get("min_iv_slope", 0.0) or 0.0)
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

        spot_hist: Deque[float] = state.extra.setdefault("spot_hist", deque(maxlen=self.lookback))
        spot_hist.append(float(market_snapshot.spot))
        if len(spot_hist) < 5:
            return StrategyDecision()

        iv = _atm_iv(market_snapshot)
        iv_hist: Deque[float] = state.extra.setdefault("iv_hist", deque(maxlen=self.iv_lookback))
        if iv is not None and iv > 0:
            iv_hist.append(float(iv))
        if len(iv_hist) < 20:
            return StrategyDecision()

        curr_iv = float(iv_hist[-1])
        pct = _percentile(iv_hist, curr_iv)
        if pct is not None and pct > self.iv_pct_max:
            return StrategyDecision(debug={"iv_pct": pct, "iv": curr_iv, "skip": "iv_extreme"})

        # IV expansion proxy: latest iv - mean of recent window.
        tail = list(iv_hist)[-min(20, len(iv_hist)) :]
        mean_tail = statistics.mean(tail) if tail else curr_iv
        iv_slope = curr_iv - mean_tail
        if self.min_iv_slope and iv_slope < self.min_iv_slope:
            return StrategyDecision(debug={"iv_slope": iv_slope, "skip": "iv_flat"})

        prev = list(spot_hist)[:-1]
        curr = float(spot_hist[-1])
        hi = max(prev) if prev else curr
        lo = min(prev) if prev else curr
        direction: Optional[str] = None
        if curr > hi:
            direction = "CALL"
        elif curr < lo:
            direction = "PUT"
        if not direction:
            return StrategyDecision(debug={"hi": hi, "lo": lo, "iv_slope": iv_slope})

        expiry = str(market_snapshot.meta.get("expiry") or "").strip()
        step_map = getattr(ctx.cfg.data, "strike_steps", {}) or {}
        strike_step = int(step_map.get(market_snapshot.underlying, ctx.cfg.data.lot_step))
        quote, symbol = pick_best_option(
            market_snapshot,
            direction=direction,  # type: ignore[arg-type]
            expiry=expiry,
            spot=float(market_snapshot.spot),
            strike_step=strike_step,
            delta_target=self.delta_target,
            delta_min=self.delta_min,
            delta_max=self.delta_max,
        )
        score = abs(curr - (hi if direction == "CALL" else lo)) / max(abs(hi - lo), 1e-9)
        reason = f"IV-expansion {direction} breakout spot={curr:.2f} iv={curr_iv:.3f} iv_slope={iv_slope:.3f} iv_pct={pct if pct is not None else -1:.2f}"
        intent = TradeIntent(
            strategy_id=self.strategy_id,
            underlying=market_snapshot.underlying,
            direction=direction,  # type: ignore[arg-type]
            symbol=symbol,
            instrument_key=(quote.instrument_key if quote else None),
            qty=0,
            entry_style="MARKET",
            signal_score=float(min(5.0, score + max(iv_slope, 0.0))),
            reason=reason,
            meta={"iv": curr_iv, "iv_pct": pct, "iv_slope": iv_slope, "hi": hi, "lo": lo},
        )
        state.extra["last_signal_ts"] = ts.timestamp()
        return StrategyDecision(intents=[intent])

    def end_of_day_flatten(self, ctx) -> None:
        ctx.state_for(self.strategy_id).extra.clear()


def _atm_iv(snapshot: MarketSnapshot) -> Optional[float]:
    strike = snapshot.nearest_strike(snapshot.spot)
    if strike is None:
        return None
    ivs: list[float] = []
    for opt in snapshot.options:
        if opt.strike != strike or opt.iv is None:
            continue
        try:
            val = float(opt.iv)
        except Exception:
            continue
        if val > 3.0:
            val = val / 100.0
        if val > 0:
            ivs.append(val)
    if not ivs:
        return None
    return sum(ivs) / len(ivs)


def _percentile(series: Deque[float], value: float) -> Optional[float]:
    if not series or len(series) < 10:
        return None
    v = float(value)
    below = sum(1 for x in series if float(x) <= v)
    return below / len(series)


__all__ = ["IVExpansionBreakoutStrategy"]

