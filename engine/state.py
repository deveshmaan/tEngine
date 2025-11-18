from __future__ import annotations

import asyncio
import datetime as dt
import logging
from dataclasses import dataclass
from enum import Enum
from typing import Dict, Optional

from .clock import MarketClock, MarketSchedule
from .events import EventBus, StateChange


class EngineState(str, Enum):
    STARTING = "STARTING"
    PREOPEN = "PREOPEN"
    OPEN = "OPEN"
    HALT = "HALT"
    FLATTEN = "FLATTEN"
    CLOSING = "CLOSING"
    CLOSED = "CLOSED"
    SHUTDOWN = "SHUTDOWN"


_TRANSITIONS: Dict[EngineState, set[EngineState]] = {
    EngineState.STARTING: {EngineState.PREOPEN, EngineState.HALT, EngineState.CLOSED},
    EngineState.PREOPEN: {EngineState.OPEN, EngineState.HALT, EngineState.CLOSED},
    EngineState.OPEN: {EngineState.FLATTEN, EngineState.HALT, EngineState.CLOSING},
    EngineState.FLATTEN: {EngineState.CLOSING, EngineState.HALT, EngineState.CLOSED},
    EngineState.CLOSING: {EngineState.CLOSED},
    EngineState.HALT: {EngineState.FLATTEN, EngineState.CLOSING, EngineState.CLOSED},
    EngineState.CLOSED: {EngineState.SHUTDOWN},
    EngineState.SHUTDOWN: set(),
}


@dataclass
class EngineFSM:
    """Async market-state finite state machine."""

    clock: MarketClock
    schedule: MarketSchedule
    bus: EventBus
    poll_interval: float = 5.0

    def __post_init__(self) -> None:
        self._state: EngineState = EngineState.STARTING
        self._task: Optional[asyncio.Task] = None
        self._stop_event = asyncio.Event()
        self._logger = logging.getLogger("EngineFSM")

    @property
    def state(self) -> EngineState:
        return self._state

    async def start(self) -> None:
        if self._task is None or self._task.done():
            self._stop_event.clear()
            self._task = asyncio.create_task(self._run(), name="engine-fsm")

    async def stop(self) -> None:
        self._stop_event.set()
        if self._task:
            await self._task
        await self._transition(EngineState.SHUTDOWN, force=True)

    async def _run(self) -> None:
        while not self._stop_event.is_set():
            desired = self._from_schedule()
            if desired and desired != self._state:
                await self._transition(desired)
            await asyncio.sleep(self.poll_interval)

    def _from_schedule(self) -> Optional[EngineState]:
        mapping = {
            "STARTING": EngineState.STARTING,
            "PREOPEN": EngineState.PREOPEN,
            "OPEN": EngineState.OPEN,
            "FLATTEN": EngineState.FLATTEN,
            "CLOSED": EngineState.CLOSED,
        }
        sched_state = self.schedule.state_for(self.clock.now())
        return mapping.get(sched_state)

    async def force_transition(self, target: EngineState) -> None:
        await self._transition(target)

    async def _transition(self, target: EngineState, *, force: bool = False) -> None:
        if target == self._state:
            return
        allowed = _TRANSITIONS.get(self._state, set())
        if not force and target not in allowed and self._state != target:
            self._logger.warning("Illegal transition %s -> %s", self._state, target)
            return
        prev = self._state
        self._state = target
        await self.bus.publish("state", StateChange(prev=prev.value, new=target.value, ts=self.clock.now()))
        self._logger.info("Engine state change: %s -> %s", prev.value, target.value)
