from __future__ import annotations

import asyncio
import datetime as dt
from abc import ABC, abstractmethod
from typing import Any, Callable, Optional

from engine.config import EngineConfig, IST
from engine.events import EventBus
from engine.exit import ExitEngine
from engine.logging_utils import get_logger
from engine.metrics import EngineMetrics
from engine.oms import OMS
from engine.risk import RiskManager
from engine.time_machine import now as engine_now
from market.instrument_cache import InstrumentCache


class BaseStrategy(ABC):
    """Common plumbing for event-driven strategies."""

    def __init__(
        self,
        config: EngineConfig,
        risk: RiskManager,
        oms: OMS,
        bus: EventBus,
        exit_engine: ExitEngine,
        instrument_cache: InstrumentCache,
        metrics: EngineMetrics,
        subscription_expiry_provider: Optional[Callable[[str], str]] = None,
    ) -> None:
        self.cfg = config
        self.risk = risk
        self.oms = oms
        self.bus = bus
        self.exit_engine = exit_engine
        self.instrument_cache = instrument_cache
        self.metrics = metrics
        self._subscription_expiry_provider = subscription_expiry_provider
        self._logger = get_logger(self.__class__.__name__)
        self._initialized = False
        self._app: Any = None

    async def init(self, app: Any) -> None:
        """One-time setup hook for derived strategies."""

        self._app = app
        self._initialized = True

    async def run(self, stop_event: asyncio.Event) -> None:
        """Subscribe to tick/fill streams and dispatch to handlers."""

        await self._ensure_initialized()
        market_q = await self.bus.subscribe("market/events", maxsize=1000)
        fills_q = await self.bus.subscribe("orders/fill", maxsize=500)
        tasks = [
            asyncio.create_task(self._consume_ticks(stop_event, market_q), name="strategy-ticks"),
            asyncio.create_task(self._consume_fills(stop_event, fills_q), name="strategy-fills"),
        ]
        try:
            await stop_event.wait()
        finally:
            for task in tasks:
                task.cancel()
            for task in tasks:
                try:
                    await task
                except asyncio.CancelledError:
                    continue

    async def _consume_ticks(self, stop_event: asyncio.Event, queue: asyncio.Queue) -> None:
        while not stop_event.is_set():
            try:
                event = await asyncio.wait_for(queue.get(), timeout=1.0)
            except asyncio.TimeoutError:
                continue
            try:
                await self.on_tick(event)
            except Exception as exc:  # pragma: no cover - defensive logging
                self._logger.warning("strategy_tick_error: %s", exc)

    async def _consume_fills(self, stop_event: asyncio.Event, queue: asyncio.Queue) -> None:
        while not stop_event.is_set():
            try:
                fill = await asyncio.wait_for(queue.get(), timeout=1.0)
            except asyncio.TimeoutError:
                continue
            try:
                await self.on_fill(fill)
            except Exception as exc:  # pragma: no cover - defensive logging
                self._logger.warning("strategy_fill_error: %s", exc)

    async def _ensure_initialized(self) -> None:
        if not self._initialized:
            await self.init(self._app)
            self._initialized = True

    def _event_ts(self, value: Optional[str]) -> dt.datetime:
        return _parse_ts(value)

    def _extract_price(self, payload: dict) -> Optional[float]:
        for key in ("ltp", "price", "close", "last"):
            if key in payload and payload[key] is not None:
                try:
                    return float(payload[key])
                except (TypeError, ValueError):
                    continue
        return None

    # ------------------------------------------------------------------ hooks
    @abstractmethod
    async def on_tick(self, event: dict) -> None:
        ...

    @abstractmethod
    async def on_fill(self, fill: dict) -> None:
        ...


def _parse_ts(value: Optional[str], tz: dt.tzinfo = IST) -> dt.datetime:
    if not value:
        return engine_now(tz)
    try:
        parsed = dt.datetime.fromisoformat(value)
    except ValueError:
        return engine_now(tz)
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=tz)
    return parsed.astimezone(tz)


__all__ = ["BaseStrategy"]
