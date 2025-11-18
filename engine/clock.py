from __future__ import annotations

import asyncio
import datetime as dt
from dataclasses import dataclass, field
from typing import Dict, Optional, Set

try:  # Python 3.9 fallback for ZoneInfo
    from zoneinfo import ZoneInfo
except ImportError:  # pragma: no cover
    ZoneInfo = None  # type: ignore

DEFAULT_TZ_NAME = "Asia/Kolkata"


def _ist_tz() -> dt.tzinfo:
    if ZoneInfo is not None:
        try:
            return ZoneInfo(DEFAULT_TZ_NAME)
        except Exception:  # pragma: no cover - fallback path
            pass
    return dt.timezone(dt.timedelta(hours=5, minutes=30))


class MarketClock:
    """Timezone-aware clock with async wait helpers for market workflows."""

    def __init__(self, tz: Optional[dt.tzinfo] = None):
        self._tz = tz or _ist_tz()

    @property
    def tz(self) -> dt.tzinfo:
        return self._tz

    def now(self) -> dt.datetime:
        return dt.datetime.now(self._tz)

    def session_now(self) -> dt.datetime:
        return self.now()

    async def wait_until(self, when: dt.datetime) -> None:
        if when.tzinfo is None:
            when = when.replace(tzinfo=self._tz)
        when = when.astimezone(self._tz)
        while True:
            now = self.now()
            remaining = (when - now).total_seconds()
            if remaining <= 0:
                break
            await asyncio.sleep(min(remaining, 60.0))


@dataclass
class MarketSchedule:
    """Determines session state based on configured market hours and holidays."""

    preopen_start: dt.time
    open_start: dt.time
    flatten_start: dt.time
    close_time: dt.time
    tz: dt.tzinfo = field(default_factory=_ist_tz)
    holidays: Set[dt.date] = field(default_factory=set)
    half_days: Dict[dt.date, dt.time] = field(default_factory=dict)

    @classmethod
    def from_dict(cls, data: Dict[str, str]) -> "MarketSchedule":
        def _parse(t: str) -> dt.time:
            return dt.time.fromisoformat(t)

        return cls(
            preopen_start=_parse(data.get("preopen", "09:00")),
            open_start=_parse(data.get("open", "09:15")),
            flatten_start=_parse(data.get("flatten", "15:20")),
            close_time=_parse(data.get("close", "15:30")),
            tz=_ist_tz(),
        )

    def is_holiday(self, day: dt.date) -> bool:
        return day in self.holidays

    def state_for(self, ts: Optional[dt.datetime] = None) -> str:
        now = (ts or dt.datetime.now(self.tz)).astimezone(self.tz)
        day = now.date()
        if self.is_holiday(day):
            return "CLOSED"
        close_time = self.half_days.get(day, self.close_time)
        if now.time() < self.preopen_start:
            return "STARTING"
        if now.time() < self.open_start:
            return "PREOPEN"
        if now.time() < self.flatten_start:
            return "OPEN"
        if now.time() < close_time:
            return "FLATTEN"
        return "CLOSED"
