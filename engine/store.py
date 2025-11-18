from __future__ import annotations

import asyncio
import datetime as dt
import logging
import os
import sqlite3
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from engine.events import Bar1s, OrderAck, OrderFill, RiskReject


class SQLiteStore:
    """Async writer that persists ticks/orders/fills into SQLite with WAL enabled."""

    def __init__(self, db_path: Optional[str] = None, queue_size: int = 2000):
        self.db_path = Path(db_path or os.getenv("ENGINE_DB_PATH", "engine_state.sqlite"))
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self._conn = sqlite3.connect(self.db_path, check_same_thread=False)
        self._conn.execute("PRAGMA journal_mode=WAL;")
        self._conn.execute("PRAGMA synchronous=NORMAL;")
        self._conn.execute("PRAGMA foreign_keys=ON;")
        self._init_schema()
        self._queue: asyncio.Queue = asyncio.Queue(maxsize=queue_size)
        self._writer_task: Optional[asyncio.Task] = None
        self._stop = asyncio.Event()
        self.logger = logging.getLogger("SQLiteStore")

    def _init_schema(self) -> None:
        cur = self._conn.cursor()
        cur.executescript(
            """
            CREATE TABLE IF NOT EXISTS ticks (
                instrument TEXT,
                ts TEXT,
                open REAL,
                high REAL,
                low REAL,
                close REAL,
                volume REAL,
                is_stale INTEGER
            );
            CREATE TABLE IF NOT EXISTS orders (
                internal_id TEXT PRIMARY KEY,
                broker_id TEXT,
                ts TEXT,
                symbol TEXT,
                qty INTEGER,
                side TEXT,
                price REAL,
                status TEXT,
                message TEXT
            );
            CREATE TABLE IF NOT EXISTS fills (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                order_internal_id TEXT,
                ts TEXT,
                price REAL,
                qty INTEGER,
                FOREIGN KEY(order_internal_id) REFERENCES orders(internal_id)
            );
            CREATE TABLE IF NOT EXISTS risk_events (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                ts TEXT,
                instrument TEXT,
                reason TEXT,
                code TEXT
            );
            """
        )
        self._conn.commit()

    async def start(self) -> None:
        if self._writer_task and not self._writer_task.done():
            return
        self._stop.clear()
        self._writer_task = asyncio.create_task(self._writer(), name="sqlite-writer")

    async def stop(self) -> None:
        self._stop.set()
        if self._writer_task:
            await self._queue.put(("__STOP__", None))
            await self._writer_task
            self._writer_task = None
        self._conn.close()

    async def log_bar(self, bar: Bar1s) -> None:
        await self._queue.put(("bar", bar))

    async def log_order_ack(self, ack: OrderAck, intent_symbol: str, qty: int, side: str, price: Optional[float]) -> None:
        payload = {
            "internal_id": ack.internal_id,
            "broker_id": ack.broker_id,
            "ts": ack.ts.isoformat(),
            "symbol": intent_symbol,
            "qty": qty,
            "side": side,
            "price": price,
            "status": ack.status,
            "message": ack.message,
        }
        await self._queue.put(("order", payload))

    async def log_fill(self, fill: OrderFill) -> None:
        payload = {
            "order_internal_id": fill.internal_id,
            "ts": fill.ts.isoformat(),
            "price": fill.price,
            "qty": fill.qty,
        }
        await self._queue.put(("fill", payload))

    async def log_risk_event(self, event: RiskReject) -> None:
        payload = {
            "ts": event.ts.isoformat(),
            "instrument": event.signal.instrument,
            "reason": event.reason,
            "code": event.code,
        }
        await self._queue.put(("risk", payload))

    async def _writer(self) -> None:
        while True:
            kind, payload = await self._queue.get()
            if kind == "__STOP__":
                break
            batch = [(kind, payload)]
            while len(batch) < 50 and not self._queue.empty():
                batch.append(self._queue.get_nowait())
            await asyncio.to_thread(self._flush, batch)

    def _flush(self, batch: List[Tuple[str, Any]]) -> None:
        cur = self._conn.cursor()
        for kind, payload in batch:
            if kind == "bar" and isinstance(payload, Bar1s):
                cur.execute(
                    "INSERT INTO ticks(instrument, ts, open, high, low, close, volume, is_stale) VALUES (?,?,?,?,?,?,?,?)",
                    (
                        payload.instrument,
                        payload.ts.isoformat(),
                        payload.open,
                        payload.high,
                        payload.low,
                        payload.close,
                        payload.volume,
                        1 if payload.is_stale else 0,
                    ),
                )
            elif kind == "order":
                cur.execute(
                    """
                    INSERT INTO orders(internal_id, broker_id, ts, symbol, qty, side, price, status, message)
                    VALUES (:internal_id, :broker_id, :ts, :symbol, :qty, :side, :price, :status, :message)
                    ON CONFLICT(internal_id) DO UPDATE SET status=excluded.status, message=excluded.message, broker_id=excluded.broker_id
                    """,
                    payload,
                )
            elif kind == "fill":
                cur.execute(
                    "INSERT INTO fills(order_internal_id, ts, price, qty) VALUES (:order_internal_id, :ts, :price, :qty)",
                    payload,
                )
            elif kind == "risk":
                cur.execute(
                    "INSERT INTO risk_events(ts, instrument, reason, code) VALUES (:ts, :instrument, :reason, :code)",
                    payload,
                )
        self._conn.commit()
