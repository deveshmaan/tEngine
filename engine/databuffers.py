from __future__ import annotations

import asyncio
import datetime as dt
import threading
from collections import defaultdict, deque
from dataclasses import dataclass
from typing import Deque, Dict, Iterable, List, Optional

from .clock import MarketClock
from .events import Bar1s, EventBus, Tick
from engine_metrics import EngineMetrics, NULL_METRICS


@dataclass
class _BarState:
    instrument: str
    start_ts: dt.datetime
    open: float
    high: float
    low: float
    close: float
    volume: float
    dirty: bool = False
    last_tick_ts: Optional[dt.datetime] = None


class RingBuffer:
    def __init__(self, capacity: int):
        self._capacity = capacity
        self._data: Deque[Tick] = deque(maxlen=capacity)
        self._lock = threading.Lock()

    def append(self, item: Tick) -> None:
        with self._lock:
            self._data.append(item)

    def snapshot(self) -> List[Tick]:
        with self._lock:
            return list(self._data)


class BarAggregator:
    def __init__(self, *, interval_seconds: int, clock: MarketClock, bus: EventBus):
        self.interval = interval_seconds
        self.clock = clock
        self.bus = bus
        self._state: Dict[str, _BarState] = {}
        self._last_close: Dict[str, float] = {}
        self._lock = threading.Lock()
        self._task: Optional[asyncio.Task] = None
        self._stop = asyncio.Event()

    def update_tick(self, tick: Tick) -> None:
        with self._lock:
            state = self._state.get(tick.instrument)
            if state is None:
                state = _BarState(
                    instrument=tick.instrument,
                    start_ts=self.clock.now(),
                    open=tick.ltp,
                    high=tick.ltp,
                    low=tick.ltp,
                    close=tick.ltp,
                    volume=tick.volume,
                    dirty=True,
                    last_tick_ts=tick.ts,
                )
                self._state[tick.instrument] = state
            else:
                if not state.dirty:
                    state.open = state.high = state.low = state.close = tick.ltp
                    state.volume = tick.volume
                else:
                    state.high = max(state.high, tick.ltp)
                    state.low = min(state.low, tick.ltp)
                    state.volume += tick.volume
                state.close = tick.ltp
                state.last_tick_ts = tick.ts
                state.dirty = True
            self._last_close[tick.instrument] = tick.ltp

    async def start(self) -> None:
        if self._task and not self._task.done():
            return
        self._stop.clear()
        self._task = asyncio.create_task(self._run(), name=f"bar-agg-{self.interval}s")

    async def stop(self) -> None:
        self._stop.set()
        if self._task:
            await self._task
            self._task = None

    async def _run(self) -> None:
        while not self._stop.is_set():
            await asyncio.sleep(self.interval)
            await self._flush()

    async def _flush(self) -> None:
        end_ts = self.clock.now()
        start_ts = end_ts - dt.timedelta(seconds=self.interval)
        bars: List[Bar1s] = []
        with self._lock:
            instruments = set(self._state.keys()) | set(self._last_close.keys())
            for instrument in instruments:
                state = self._state.get(instrument)
                if state and state.dirty:
                    bar = Bar1s(
                        instrument=instrument,
                        ts=end_ts,
                        open=state.open,
                        high=state.high,
                        low=state.low,
                        close=state.close,
                        volume=state.volume,
                        is_stale=False,
                    )
                else:
                    last_close = self._last_close.get(instrument)
                    if last_close is None:
                        continue
                    bar = Bar1s(
                        instrument=instrument,
                        ts=end_ts,
                        open=last_close,
                        high=last_close,
                        low=last_close,
                        close=last_close,
                        volume=0.0,
                        is_stale=True,
                    )
                bars.append(bar)
                self._state[instrument] = _BarState(
                    instrument=instrument,
                    start_ts=start_ts,
                    open=bar.close,
                    high=bar.close,
                    low=bar.close,
                    close=bar.close,
                    volume=0.0,
                    dirty=False,
                    last_tick_ts=state.last_tick_ts if state else None,
                )
        for bar in bars:
            await self.bus.publish(f"bars/{self.interval}", bar)
            await self.bus.publish("bars", bar)

    async def flush_now(self) -> None:
        await self._flush()


class TickPipeline:
    def __init__(self, *, bus: EventBus, clock: MarketClock, history_size: int = 600,
                 bar_intervals: Iterable[int] = (1, 5), metrics: EngineMetrics = NULL_METRICS):
        self.bus = bus
        self.clock = clock
        self._history = defaultdict(lambda: RingBuffer(history_size))
        self._aggregators = [BarAggregator(interval_seconds=i, clock=clock, bus=bus) for i in bar_intervals]
        self._started = False
        self.metrics = metrics

    async def start(self) -> None:
        if self._started:
            return
        for agg in self._aggregators:
            await agg.start()
        self._started = True

    async def stop(self) -> None:
        if not self._started:
            return
        for agg in self._aggregators:
            await agg.stop()
        self._started = False

    async def process_tick(self, tick: Tick) -> None:
        self._history[tick.instrument].append(tick)
        for agg in self._aggregators:
            agg.update_tick(tick)
        latency_ms = max(0.0, (self.clock.now() - tick.ts).total_seconds() * 1000.0)
        self.metrics.record_tick_latency(latency_ms)
        await self.bus.publish("ticks", tick)
        await self.bus.publish(f"ticks/{tick.instrument}", tick)

    def history(self, instrument: str) -> List[Tick]:
        return self._history[instrument].snapshot()

    async def flush_bars(self) -> None:
        for agg in self._aggregators:
            await agg.flush_now()
