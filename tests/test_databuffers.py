import asyncio
import datetime as dt

import pytest

from engine.clock import MarketClock
from engine.databuffers import BarAggregator, RingBuffer, TickPipeline
from engine.events import EventBus, Tick


class FakeClock(MarketClock):
    def __init__(self):
        super().__init__(tz=dt.timezone.utc)
        self._now = dt.datetime(2024, 1, 1, tzinfo=dt.timezone.utc)

    def jump(self, seconds: float) -> None:
        self._now += dt.timedelta(seconds=seconds)

    def now(self) -> dt.datetime:  # type: ignore[override]
        return self._now


@pytest.mark.asyncio
async def test_ring_buffer_and_bar_flush():
    bus = EventBus()
    clock = FakeClock()
    agg = BarAggregator(interval_seconds=1, clock=clock, bus=bus)
    q = await bus.subscribe("bars/1")
    tick = Tick(instrument="X", ts=clock.now(), ltp=100.0, bid=99.5, ask=100.5, volume=10)
    agg.update_tick(tick)
    clock.jump(1)
    await agg.flush_now()
    bar = await asyncio.wait_for(q.get(), timeout=1)
    assert bar.instrument == "X"
    assert bar.close == pytest.approx(100.0)
    assert not bar.is_stale


@pytest.mark.asyncio
async def test_tick_pipeline_history_and_stale_bar():
    bus = EventBus()
    clock = FakeClock()
    pipeline = TickPipeline(bus=bus, clock=clock, history_size=3, bar_intervals=(1,))
    q = await bus.subscribe("bars/1")
    tick = Tick(instrument="Y", ts=clock.now(), ltp=50.0, bid=49.5, ask=50.5, volume=5)
    await pipeline.process_tick(tick)
    clock.jump(1)
    await pipeline.flush_bars()
    bar = await asyncio.wait_for(q.get(), timeout=1)
    assert not bar.is_stale
    clock.jump(1)
    await pipeline.flush_bars()
    bar2 = await asyncio.wait_for(q.get(), timeout=1)
    assert bar2.is_stale
    history = pipeline.history("Y")
    assert len(history) == 1
    await pipeline.stop()
