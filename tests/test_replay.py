import asyncio
import json
from pathlib import Path
from typing import Dict, List

import pytest

from engine.events import EventBus
from engine.replay import ReplayConfig, configure_runtime, replay
from persistence import SQLiteStore

Captured = Dict[str, List[Dict[str, object]]]


@pytest.mark.asyncio
async def test_replay_is_deterministic_from_jsonl(tmp_path: Path) -> None:
    events = [
        {
            "ts": "2024-01-01T09:15:00+00:00",
            "type": "tick",
            "payload": {
                "symbol": "NIFTY",
                "ltp": 100.0,
                "order": {"id": "ord-1", "side": "BUY", "qty": 25},
                "execution": {"id": "exec-1", "price": 100.0, "qty": 25},
                "snapshot": {"net": 25, "pnl": 0.0},
            },
        },
        {
            "ts": "2024-01-01T09:15:01+00:00",
            "type": "tick",
            "payload": {
                "symbol": "NIFTY",
                "ltp": 101.5,
                "order": {"id": "ord-2", "side": "SELL", "qty": 10},
                "execution": {"id": "exec-2", "price": 101.5, "qty": 10},
                "snapshot": {"net": 15, "pnl": 15.0},
            },
        },
        {
            "ts": "2024-01-01T09:15:02+00:00",
            "type": "tick",
            "payload": {
                "symbol": "NIFTY",
                "ltp": 102.0,
                "order": {"id": "ord-3", "side": "SELL", "qty": 15},
                "execution": {"id": "exec-3", "price": 102.0, "qty": 15},
                "snapshot": {"net": 0, "pnl": 45.0},
            },
        },
    ]
    feed_path = tmp_path / "events.jsonl"
    _write_events(feed_path, events)

    async def run_once(run_name: str) -> Captured:
        store = SQLiteStore(tmp_path / f"{run_name}.sqlite", run_id=run_name)
        bus = EventBus()
        configure_runtime(bus=bus, store=store)
        queue = await bus.subscribe("market/events", maxsize=10)
        consumer = asyncio.create_task(_consume(queue, len(events)))
        cfg = ReplayConfig(run_id=None, input_path=feed_path, speed=0.0, seed=42)
        result_run_id = await replay(cfg)
        assert result_run_id == store.run_id
        captured = await consumer
        captured["events"] = [
            {"type": entry["type"], "payload": entry["payload"]} for entry in store.iter_market_events()
        ]
        return captured

    first = await run_once("replay-one")
    second = await run_once("replay-two")
    assert first == second


async def _consume(queue: asyncio.Queue[Dict[str, object]], expected: int) -> Captured:
    orders: List[Dict[str, object]] = []
    executions: List[Dict[str, object]] = []
    snapshots: List[Dict[str, object]] = []
    for _ in range(expected):
        event = await asyncio.wait_for(queue.get(), timeout=1.0)
        payload = event["payload"]
        orders.append(payload["order"])
        executions.append(payload["execution"])
        snapshots.append(payload["snapshot"])
    return {"orders": orders, "executions": executions, "snapshots": snapshots}


def _write_events(path: Path, events: List[Dict[str, object]]) -> None:
    with path.open("w", encoding="utf-8") as handle:
        for event in events:
            handle.write(json.dumps(event) + "\n")
