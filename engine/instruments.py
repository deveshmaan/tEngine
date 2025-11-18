from __future__ import annotations

import asyncio
import datetime as dt
import threading
from typing import Callable, Optional

from brokerage.upstox_client import UpstoxSession
from market.instrument_cache import InstrumentCache, InstrumentMeta


class InstrumentResolver:
    """Translate human-friendly symbols into Upstox instrument keys using the option chain cache."""

    def __init__(self, cache: InstrumentCache, session_factory: Optional[Callable[[], UpstoxSession]] = None, default_option: str = "CE"):
        self._cache = cache
        self._session_factory = session_factory or UpstoxSession
        self._session: Optional[UpstoxSession] = None
        self._lock = threading.Lock()
        self._default_option = default_option.upper()

    async def resolve_symbol(self, symbol: str) -> str:
        return await asyncio.to_thread(self._resolve_symbol_sync, symbol)

    def metadata_for(self, symbol: str) -> Optional[InstrumentMeta]:
        if "|" in symbol:
            return self._cache.get_meta(symbol)
        try:
            underlying, expiry, strike, option_type = self._parse_symbol(symbol)
        except ValueError:
            return None
        token = self._cache.lookup(underlying, expiry, strike, option_type)
        if not token:
            return None
        return self._cache.get_meta(token)

    # ------------------------------------------------------------------ internals
    def _resolve_symbol_sync(self, symbol: str) -> str:
        if "|" in symbol:
            return symbol
        underlying, expiry, strike, option_type = self._parse_symbol(symbol)
        token = self._cache.lookup(underlying, expiry, strike, option_type)
        if token:
            return token
        session = self._ensure_session()
        self._cache.refresh_option_chain(underlying, expiry, session)
        token = self._cache.lookup(underlying, expiry, strike, option_type)
        if not token:
            raise RuntimeError(f"Unable to resolve instrument for {symbol}")
        return token

    def _parse_symbol(self, symbol: str) -> tuple[str, str, float, str]:
        parts = symbol.split("-")
        if len(parts) < 3:
            raise ValueError(f"Unrecognized symbol format: {symbol}")
        underlying = parts[0].upper()
        raw_tail = parts[-1]
        option_type = self._default_option
        if raw_tail.endswith(("CE", "PE")):
            option_type = raw_tail[-2:].upper()
            raw_tail = raw_tail[:-2]
        expiry = "-".join(parts[1:-1])
        try:
            dt.date.fromisoformat(expiry)
        except ValueError as exc:
            raise ValueError(f"Invalid expiry in symbol {symbol}") from exc
        strike = float(raw_tail)
        return underlying, expiry, strike, option_type

    def _ensure_session(self) -> UpstoxSession:
        with self._lock:
            if self._session is None:
                self._session = self._session_factory()
            return self._session


__all__ = ["InstrumentResolver"]
