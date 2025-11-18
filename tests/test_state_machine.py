import asyncio
import datetime as dt
from typing import Optional

import pytest

from engine.clock import MarketClock, MarketSchedule
from engine.events import EventBus
from engine.state import EngineFSM, EngineState


class FakeClock(MarketClock):
    def __init__(self):
        super().__init__(tz=dt.timezone.utc)
        self._now = dt.datetime(2024, 1, 1, tzinfo=dt.timezone.utc)

    def set(self, new_now: dt.datetime) -> None:
        self._now = new_now

    def now(self) -> dt.datetime:
        return self._now


class MockSchedule:
    def __init__(self):
        self.state = "STARTING"

    def set_state(self, value: str):
        self.state = value

    def state_for(self, _: Optional[dt.datetime] = None) -> str:  # type: ignore[override]
        return self.state


@pytest.mark.asyncio
async def test_engine_fsm_transitions():
    bus = EventBus()
    clock = FakeClock()
    schedule = MockSchedule()
    fsm = EngineFSM(clock=clock, schedule=schedule, bus=bus, poll_interval=0.01)
    q = await bus.subscribe("state")

    await fsm.start()
    schedule.set_state("PREOPEN")
    evt = await asyncio.wait_for(q.get(), timeout=1)
    assert evt.new == EngineState.PREOPEN.value

    schedule.set_state("OPEN")
    evt = await asyncio.wait_for(q.get(), timeout=1)
    assert evt.new == EngineState.OPEN.value

    schedule.set_state("FLATTEN")
    evt = await asyncio.wait_for(q.get(), timeout=1)
    assert evt.new == EngineState.FLATTEN.value

    await fsm.stop()
    assert fsm.state == EngineState.SHUTDOWN


@pytest.mark.asyncio
async def test_event_bus_pub_sub():
    bus = EventBus()
    q1 = await bus.subscribe("ticks", maxsize=1)
    q2 = await bus.subscribe("ticks", maxsize=1)
    await bus.publish("ticks", {"value": 1})
    assert await q1.get() == {"value": 1}
    assert await q2.get() == {"value": 1}
