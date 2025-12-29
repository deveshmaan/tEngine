from __future__ import annotations

import asyncio
import gzip
import json
import random
import datetime as dt
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional

from engine.data import record_tick_seen
from engine.events import EventBus
from engine.time_machine import install_now_provider
from persistence import SQLiteStore

try:  # Optional numpy seeding if available
    import numpy  # type: ignore
except Exception:  # pragma: no cover - numpy optional
    numpy = None  # type: ignore


@dataclass
class ReplayConfig:
    run_id: Optional[str]
    input_path: Optional[Path]
    speed: float = 1.0
    seed: Optional[int] = None


_BUS: Optional[EventBus] = None
_STORE: Optional[SQLiteStore] = None


def configure_runtime(*, bus: EventBus, store: SQLiteStore) -> None:
    global _BUS, _STORE
    _BUS = bus
    _STORE = store


async def replay(cfg: ReplayConfig) -> str:
    if _BUS is None or _STORE is None:
        raise RuntimeError("Replay runtime not configured")
    if (cfg.run_id is None) == (cfg.input_path is None):
        raise ValueError("Provide either run_id or input_path for replay")
    if cfg.seed is not None:
        random.seed(cfg.seed)
        if numpy:
            numpy.random.seed(cfg.seed)
    events = await _load_events(cfg)
    events.sort(key=lambda evt: evt["ts"])
    prev_ts: Optional[str] = None
    current_ts: Optional[dt.datetime] = None
    # Freeze engine clock to the last replayed timestamp so async consumers remain deterministic.
    install_now_provider(lambda: current_ts or dt.datetime.now(dt.timezone.utc))
    try:
        for event in events:
            ts_str = event["ts"]
            delay = 0.0
            if prev_ts:
                delta = _seconds_between(prev_ts, ts_str)
                delay = delta / cfg.speed if cfg.speed > 0 else 0.0
            prev_ts = ts_str
            if delay > 0:
                await asyncio.sleep(delay)
            payload = event.get("payload") or {}
            current_ts = _parse_ts(ts_str)
            try:
                record_tick_seen(
                    instrument_key=payload.get("instrument_key") or payload.get("instrument"),
                    underlying=payload.get("underlying") or payload.get("symbol"),
                    ts_seconds=current_ts.timestamp(),
                )
            except Exception:
                pass
            await _BUS.publish("market/events", event)
            _STORE.record_market_event(event["type"], payload, ts=current_ts)
            # Yield so consumers can process at the current frozen timestamp.
            await asyncio.sleep(0)
    finally:
        install_now_provider(lambda: dt.datetime.now(dt.timezone.utc))
    return _STORE.run_id


async def _load_events(cfg: ReplayConfig) -> List[Dict[str, Any]]:
    if cfg.run_id:
        assert _STORE is not None
        return _STORE.fetch_market_events_for(cfg.run_id)
    assert cfg.input_path is not None
    return list(_read_jsonl(cfg.input_path))


def _read_jsonl(path: Path) -> Iterable[Dict[str, Any]]:
    opener = gzip.open if path.suffix == ".gz" else open
    with opener(path, "rt", encoding="utf-8") as handle:
        for line in handle:
            line = line.strip()
            if not line:
                continue
            try:
                event = json.loads(line)
            except json.JSONDecodeError:
                continue
            if "ts" not in event or "type" not in event:
                continue
            payload = event.get("payload") or {}
            yield {"ts": event["ts"], "type": event["type"], "payload": payload}


def _parse_ts(ts: str):
    from datetime import datetime, timezone

    try:
        parsed = datetime.fromisoformat(ts)
    except ValueError:
        parsed = datetime.utcnow()
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed


def _seconds_between(prev: str, current: str) -> float:
    p = _parse_ts(prev)
    c = _parse_ts(current)
    return max((c - p).total_seconds(), 0.0)


__all__ = ["ReplayConfig", "configure_runtime", "replay"]
