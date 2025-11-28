from __future__ import annotations

import contextlib
import datetime as dt
from typing import Callable, Iterator, Optional

_NowCallable = Callable[[], dt.datetime]


def _default_now() -> dt.datetime:
    return dt.datetime.now(dt.timezone.utc)


_active_now: _NowCallable = _default_now


def now(tz: Optional[dt.tzinfo] = None) -> dt.datetime:
    """Return the current engine timestamp honoring any active travel."""

    current = _active_now()
    if tz:
        if current.tzinfo is None:
            current = current.replace(tzinfo=tz)
        else:
            current = current.astimezone(tz)
    return current


def utc_now() -> dt.datetime:
    return now(dt.timezone.utc)


@contextlib.contextmanager
def travel(frozen: dt.datetime | str) -> Iterator[None]:
    """
    Freeze the engine clock at ``frozen`` while the context is active.

    Accepts either a datetime or ISO string. Only monkey-patches the engine's own
    ``now()`` helper to satisfy determinism requirements.
    """

    target = _coerce_ts(frozen)
    global _active_now
    prev = _active_now
    _active_now = lambda: target  # noqa: E731 - simple lambda for speed
    try:
        yield
    finally:
        _active_now = prev


def install_now_provider(fn: _NowCallable) -> None:
    """Override the default provider; primarily intended for tests."""

    global _active_now
    _active_now = fn


def _coerce_ts(value: dt.datetime | str) -> dt.datetime:
    if isinstance(value, dt.datetime):
        ts = value
    else:
        ts = dt.datetime.fromisoformat(value)
    if ts.tzinfo is None:
        ts = ts.replace(tzinfo=dt.timezone.utc)
    return ts


__all__ = ["now", "utc_now", "travel", "install_now_provider"]
