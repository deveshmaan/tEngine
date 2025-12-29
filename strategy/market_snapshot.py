from __future__ import annotations

import datetime as dt
from dataclasses import dataclass
from typing import Dict, Iterable, List, Literal, Optional, Tuple

OptionType = Literal["CE", "PE"]


@dataclass(frozen=True)
class OptionQuote:
    instrument_key: str
    symbol: str
    underlying: str
    expiry: str
    strike: int
    opt_type: OptionType
    ltp: Optional[float] = None
    bid: Optional[float] = None
    ask: Optional[float] = None
    iv: Optional[float] = None
    oi: Optional[float] = None
    delta: Optional[float] = None
    gamma: Optional[float] = None
    theta: Optional[float] = None
    vega: Optional[float] = None
    ts: Optional[dt.datetime] = None

    def spread(self) -> Optional[float]:
        if self.bid is None or self.ask is None:
            return None
        return float(self.ask) - float(self.bid)

    def spread_pct(self) -> Optional[float]:
        ltp = self.ltp
        if ltp is None or ltp <= 0:
            return None
        spread = self.spread()
        if spread is None:
            return None
        return float(spread) / float(ltp)


@dataclass(frozen=True)
class MarketSnapshot:
    ts: dt.datetime
    underlying: str
    spot: float
    options: Tuple[OptionQuote, ...] = ()
    meta: Dict[str, object] = None  # type: ignore[assignment]

    def __post_init__(self) -> None:
        if self.meta is None:
            object.__setattr__(self, "meta", {})

    def options_by_type(self, opt_type: OptionType) -> List[OptionQuote]:
        wanted = opt_type.upper()
        return [opt for opt in self.options if opt.opt_type == wanted]

    def option_candidates(self, *, opt_type: OptionType, expiry: Optional[str] = None) -> List[OptionQuote]:
        candidates = self.options_by_type(opt_type)
        if expiry:
            candidates = [opt for opt in candidates if opt.expiry == expiry]
        return candidates

    def strikes(self) -> List[int]:
        return sorted({opt.strike for opt in self.options})

    def nearest_strike(self, target: float) -> Optional[int]:
        strikes = self.strikes()
        if not strikes:
            return None
        return min(strikes, key=lambda s: abs(float(s) - float(target)))

    def by_symbol(self) -> Dict[str, OptionQuote]:
        return {opt.symbol: opt for opt in self.options if opt.symbol}

    @staticmethod
    def coerce_ts(value: object, *, default_tz: dt.tzinfo = dt.timezone.utc) -> dt.datetime:
        if isinstance(value, dt.datetime):
            parsed = value
        else:
            try:
                parsed = dt.datetime.fromisoformat(str(value))
            except Exception:
                parsed = dt.datetime.now(default_tz)
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=default_tz)
        return parsed

    @staticmethod
    def normalize_underlying(value: str) -> str:
        return str(value or "").upper()

    @staticmethod
    def normalize_option_type(value: object) -> Optional[OptionType]:
        text = str(value or "").upper().strip()
        if text in {"CE", "PE"}:
            return text  # type: ignore[return-value]
        return None

    @staticmethod
    def from_tick(
        *,
        ts: dt.datetime,
        underlying: str,
        spot: float,
        option_quotes: Iterable[OptionQuote],
        meta: Optional[Dict[str, object]] = None,
    ) -> "MarketSnapshot":
        return MarketSnapshot(
            ts=ts,
            underlying=MarketSnapshot.normalize_underlying(underlying),
            spot=float(spot),
            options=tuple(option_quotes),
            meta=dict(meta or {}),
        )


__all__ = ["MarketSnapshot", "OptionQuote", "OptionType"]

