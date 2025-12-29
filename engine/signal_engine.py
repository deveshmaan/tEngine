from __future__ import annotations

import datetime as dt
import math
from collections import deque
from dataclasses import dataclass, field
from typing import Any, Deque, Dict, Optional

from strategy.contracts import TradeIntent
from strategy.market_snapshot import MarketSnapshot, OptionQuote


@dataclass(frozen=True)
class GateResult:
    ok: bool
    code: str = ""
    reason: str = ""
    details: Dict[str, Any] = field(default_factory=dict)


class SignalEngine:
    """Centralized entry gates for multi-strategy intents (BUY-side only)."""

    def __init__(
        self,
        *,
        delta_min: float = 0.25,
        delta_max: float = 0.55,
        spread_pct_max: float = 0.0,
        iv_zscore_max: float = 0.0,
        iv_percentile_min: float = 0.0,
        oi_percentile_min: float = 0.0,
        history_window: int = 240,
        min_samples: int = 20,
    ) -> None:
        self.delta_min = max(float(delta_min), 0.0)
        self.delta_max = max(float(delta_max), 0.0)
        self.spread_pct_max = max(float(spread_pct_max), 0.0)
        self.iv_zscore_max = max(float(iv_zscore_max), 0.0)
        self.iv_percentile_min = max(float(iv_percentile_min), 0.0)
        self.oi_percentile_min = max(float(oi_percentile_min), 0.0)
        self._min_samples = max(int(min_samples), 3)
        self._iv_history: Dict[str, Deque[float]] = {}
        self._oi_history: Dict[str, Deque[float]] = {}
        self._window = max(int(history_window), 10)

    def observe(self, snapshot: MarketSnapshot) -> None:
        """Update rolling IV/OI history from the latest snapshot."""

        atm_iv = self._atm_iv(snapshot)
        if atm_iv is not None:
            series = self._iv_history.setdefault(snapshot.underlying, deque(maxlen=self._window))
            series.append(float(atm_iv))
        for opt in snapshot.options:
            if opt.oi is None:
                continue
            key = str(opt.instrument_key or opt.symbol)
            if not key:
                continue
            series = self._oi_history.setdefault(key, deque(maxlen=self._window))
            series.append(float(opt.oi))

    def evaluate(self, intent: TradeIntent, snapshot: MarketSnapshot) -> GateResult:
        quote = self._resolve_quote(intent, snapshot)
        if quote is None:
            return GateResult(False, "NO_QUOTE", f"No quote for {intent.symbol}")

        if self.spread_pct_max > 0:
            spread_pct = quote.spread_pct()
            if spread_pct is not None and spread_pct > self.spread_pct_max:
                return GateResult(
                    False,
                    "SPREAD",
                    f"Spread {spread_pct:.4f} > {self.spread_pct_max:.4f}",
                    details={"spread_pct": spread_pct},
                )

        delta = quote.delta
        if delta is None:
            delta = self._estimate_delta(snapshot, quote)
        if delta is not None and self.delta_min > 0 and self.delta_max > 0:
            abs_delta = abs(float(delta))
            if abs_delta < self.delta_min or abs_delta > self.delta_max:
                return GateResult(
                    False,
                    "DELTA",
                    f"Delta {abs_delta:.3f} outside [{self.delta_min:.2f},{self.delta_max:.2f}]",
                    details={"delta": delta},
                )

        if self.iv_zscore_max > 0 or self.iv_percentile_min > 0:
            iv_val = self._atm_iv(snapshot)
            if iv_val is not None:
                iv_series = self._iv_history.get(snapshot.underlying)
                z = self._zscore(iv_series, float(iv_val))
                pct = self._percentile(iv_series, float(iv_val))
                if self.iv_zscore_max > 0 and z is not None and z > self.iv_zscore_max:
                    return GateResult(
                        False,
                        "IV_Z",
                        f"IV z-score {z:.2f} > {self.iv_zscore_max:.2f}",
                        details={"iv": iv_val, "iv_z": z, "iv_pct": pct},
                    )
                if self.iv_percentile_min > 0 and pct is not None and pct < self.iv_percentile_min:
                    return GateResult(
                        False,
                        "IV_PCT",
                        f"IV pct {pct:.2f} < {self.iv_percentile_min:.2f}",
                        details={"iv": iv_val, "iv_z": z, "iv_pct": pct},
                    )

        if self.oi_percentile_min > 0 and quote.oi is not None:
            series = self._oi_history.get(str(quote.instrument_key or quote.symbol))
            pct = self._percentile(series, float(quote.oi))
            if pct is not None and pct < self.oi_percentile_min:
                return GateResult(
                    False,
                    "OI_PCT",
                    f"OI pct {pct:.2f} < {self.oi_percentile_min:.2f}",
                    details={"oi": quote.oi, "oi_pct": pct},
                )

        return GateResult(True, "OK", "ok")

    # ----------------------------------------------------------------- helpers
    @staticmethod
    def _resolve_quote(intent: TradeIntent, snapshot: MarketSnapshot) -> Optional[OptionQuote]:
        by_symbol = snapshot.by_symbol()
        quote = by_symbol.get(intent.symbol)
        if quote:
            return quote
        if intent.instrument_key:
            for opt in snapshot.options:
                if opt.instrument_key == intent.instrument_key:
                    return opt
        return None

    @staticmethod
    def _normalize_iv(value: float) -> float:
        iv = float(value)
        if iv <= 0:
            return 0.0
        # Upstox payloads vary: some emit 0.18, some 18.0.
        if iv > 3.0:
            iv = iv / 100.0
        return max(iv, 0.0)

    def _atm_iv(self, snapshot: MarketSnapshot) -> Optional[float]:
        if not snapshot.options:
            return None
        strike = snapshot.nearest_strike(snapshot.spot)
        if strike is None:
            return None
        ivs: list[float] = []
        for opt in snapshot.options:
            if opt.strike != strike:
                continue
            if opt.iv is None:
                continue
            iv = self._normalize_iv(float(opt.iv))
            if iv > 0:
                ivs.append(iv)
        if not ivs:
            return None
        return float(sum(ivs) / len(ivs))

    def _estimate_delta(self, snapshot: MarketSnapshot, quote: OptionQuote) -> Optional[float]:
        if quote.iv is None or quote.strike <= 0 or snapshot.spot <= 0:
            return None
        try:
            expiry_date = dt.date.fromisoformat(str(quote.expiry))
        except Exception:
            return None
        tz = snapshot.ts.tzinfo or dt.timezone.utc
        expiry_dt = dt.datetime.combine(expiry_date, dt.time(15, 30), tzinfo=tz)
        t_sec = max((expiry_dt - snapshot.ts).total_seconds(), 0.0)
        t_years = t_sec / (365.0 * 24.0 * 3600.0)
        if t_years <= 0:
            return None
        sigma = self._normalize_iv(float(quote.iv))
        if sigma <= 0:
            return None
        s = float(snapshot.spot)
        k = float(quote.strike)
        try:
            d1 = (math.log(s / k) + 0.5 * sigma * sigma * t_years) / (sigma * math.sqrt(t_years))
        except Exception:
            return None
        nd1 = 0.5 * (1.0 + math.erf(d1 / math.sqrt(2.0)))
        if quote.opt_type == "PE":
            return float(nd1 - 1.0)
        return float(nd1)

    def _percentile(self, series: Optional[Deque[float]], value: float) -> Optional[float]:
        if not series or len(series) < self._min_samples:
            return None
        v = float(value)
        below = sum(1 for x in series if float(x) <= v)
        return below / len(series)

    def _zscore(self, series: Optional[Deque[float]], value: float) -> Optional[float]:
        if not series or len(series) < self._min_samples:
            return None
        values = [float(x) for x in series]
        mean = sum(values) / len(values)
        var = sum((x - mean) ** 2 for x in values) / max(len(values) - 1, 1)
        std = math.sqrt(var)
        if std <= 1e-9:
            return None
        return (float(value) - mean) / std


__all__ = ["GateResult", "SignalEngine"]

