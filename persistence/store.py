from __future__ import annotations

import datetime as dt
import json
import sqlite3
import threading
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple, TYPE_CHECKING

if TYPE_CHECKING:  # pragma: no cover - for type checkers only
    from engine.oms import Order, OrderState


class SQLiteStore:
    """SQLite persistence layer with schema migrations and monotonic timestamps."""

    def __init__(self, path: str | Path, run_id: str):
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._conn = sqlite3.connect(self.path, detect_types=sqlite3.PARSE_DECLTYPES, check_same_thread=False)
        self._conn.row_factory = sqlite3.Row
        self._conn.execute("PRAGMA foreign_keys=ON;")
        self._conn.execute("PRAGMA journal_mode=WAL;")
        self._lock = threading.Lock()
        self.run_id = run_id
        self._latest_ts = dt.datetime.min.replace(tzinfo=dt.timezone.utc)
        self._migrate()
        self._ensure_run()

    # ------------------------------------------------------------------ schema
    def _migrate(self) -> None:
        cur = self._conn.cursor()
        version = cur.execute("PRAGMA user_version").fetchone()[0]
        if version < 1:
            cur.executescript(
                """
                CREATE TABLE IF NOT EXISTS runs (
                    run_id TEXT PRIMARY KEY,
                    started_at TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS orders (
                    run_id TEXT NOT NULL,
                    client_order_id TEXT NOT NULL,
                    strategy TEXT NOT NULL,
                    symbol TEXT NOT NULL,
                    side TEXT NOT NULL,
                    qty INTEGER NOT NULL,
                    price REAL,
                    state TEXT NOT NULL,
                    last_update TEXT NOT NULL,
                    broker_order_id TEXT,
                    UNIQUE(run_id, client_order_id)
                );
                CREATE TABLE IF NOT EXISTS order_transitions (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    run_id TEXT NOT NULL,
                    client_order_id TEXT NOT NULL,
                    prev_state TEXT,
                    new_state TEXT NOT NULL,
                    ts TEXT NOT NULL,
                    reason TEXT
                );
                CREATE TABLE IF NOT EXISTS executions (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    run_id TEXT NOT NULL,
                    client_order_id TEXT NOT NULL,
                    ts TEXT NOT NULL,
                    qty INTEGER NOT NULL,
                    price REAL NOT NULL,
                    liquidity TEXT
                );
                CREATE TABLE IF NOT EXISTS risk_events (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    run_id TEXT NOT NULL,
                    ts TEXT NOT NULL,
                    code TEXT NOT NULL,
                    message TEXT NOT NULL,
                    symbol TEXT,
                    context TEXT
                );
                CREATE TABLE IF NOT EXISTS incidents (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    run_id TEXT NOT NULL,
                    ts TEXT NOT NULL,
                    code TEXT NOT NULL,
                    payload TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS cost_ledger (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    run_id TEXT NOT NULL,
                    ts TEXT NOT NULL,
                    symbol TEXT,
                    amount REAL NOT NULL,
                    kind TEXT NOT NULL,
                    note TEXT
                );
                """
            )
            cur.execute("PRAGMA user_version = 1;")
        self._conn.commit()

    def _ensure_run(self) -> None:
        with self._lock:
            now = self._ts()
            self._conn.execute(
                "INSERT OR IGNORE INTO runs(run_id, started_at) VALUES (?, ?)",
                (self.run_id, now),
            )
            self._conn.commit()

    # ----------------------------------------------------------------- helpers
    def _ts(self, ts: Optional[dt.datetime] = None) -> str:
        current = (ts or dt.datetime.now(dt.timezone.utc)).astimezone(dt.timezone.utc)
        if current <= self._latest_ts:
            current = self._latest_ts + dt.timedelta(microseconds=1)
        self._latest_ts = current
        return current.isoformat()

    def _json(self, payload: Optional[Dict[str, Any]]) -> Optional[str]:
        if payload is None:
            return None
        return json.dumps(payload, separators=(",", ":"))

    # -------------------------------------------------------------- order logs
    def record_order_snapshot(self, order: "Order") -> None:
        with self._lock:
            ts = self._ts(order.updated_at)
            self._conn.execute(
                """
                INSERT INTO orders(run_id, client_order_id, strategy, symbol, side, qty, price, state, last_update, broker_order_id)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(run_id, client_order_id) DO UPDATE SET
                    strategy=excluded.strategy,
                    symbol=excluded.symbol,
                    side=excluded.side,
                    qty=excluded.qty,
                    price=excluded.price,
                    state=excluded.state,
                    last_update=excluded.last_update,
                    broker_order_id=excluded.broker_order_id
                """,
                (
                    self.run_id,
                    order.client_order_id,
                    order.strategy,
                    order.symbol,
                    order.side,
                    order.qty,
                    order.limit_price,
                    order.state.value,
                    ts,
                    order.broker_order_id,
                ),
            )
            self._conn.commit()

    def record_transition(self, client_order_id: str, prev: Optional["OrderState"], new: "OrderState", reason: Optional[str] = None, ts: Optional[dt.datetime] = None) -> None:
        with self._lock:
            stamp = self._ts(ts)
            self._conn.execute(
                """
                INSERT INTO order_transitions(run_id, client_order_id, prev_state, new_state, ts, reason)
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                (self.run_id, client_order_id, getattr(prev, "value", prev), new.value, stamp, reason),
            )
            self._conn.commit()

    # ------------------------------------------------------------- executions
    def record_execution(self, client_order_id: str, qty: int, price: float, liquidity: str = "UNKNOWN", ts: Optional[dt.datetime] = None) -> None:
        with self._lock:
            stamp = self._ts(ts)
            self._conn.execute(
                """
                INSERT INTO executions(run_id, client_order_id, ts, qty, price, liquidity)
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                (self.run_id, client_order_id, stamp, qty, price, liquidity),
            )
            self._conn.commit()

    # -------------------------------------------------------------- risk events
    def record_risk_event(self, *, code: str, message: str, symbol: Optional[str], context: Optional[Dict[str, Any]] = None, ts: Optional[dt.datetime] = None) -> None:
        with self._lock:
            stamp = self._ts(ts)
            self._conn.execute(
                """
                INSERT INTO risk_events(run_id, ts, code, message, symbol, context)
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                (self.run_id, stamp, code, message, symbol, self._json(context)),
            )
            self._conn.commit()

    # --------------------------------------------------------------- incidents
    def record_incident(self, code: str, payload: Dict[str, Any], ts: Optional[dt.datetime] = None) -> None:
        with self._lock:
            stamp = self._ts(ts)
            self._conn.execute(
                """
                INSERT INTO incidents(run_id, ts, code, payload)
                VALUES (?, ?, ?, ?)
                """,
                (self.run_id, stamp, code, self._json(payload) or "{}"),
            )
            self._conn.commit()

    # -------------------------------------------------------------- cost ledger
    def record_cost(self, symbol: Optional[str], amount: float, kind: str, note: Optional[str] = None, ts: Optional[dt.datetime] = None) -> None:
        with self._lock:
            stamp = self._ts(ts)
            self._conn.execute(
                """
                INSERT INTO cost_ledger(run_id, ts, symbol, amount, kind, note)
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                (self.run_id, stamp, symbol, amount, kind, note),
            )
            self._conn.commit()

    # -------------------------------------------------------------- query utils
    def list_risk_events(self) -> List[Dict[str, Any]]:
        cur = self._conn.execute(
            "SELECT ts, code, message, symbol, context FROM risk_events WHERE run_id=? ORDER BY id",
            (self.run_id,),
        )
        rows = []
        for row in cur.fetchall():
            ctx = row["context"]
            rows.append(
                {
                    "ts": row["ts"],
                    "code": row["code"],
                    "message": row["message"],
                    "symbol": row["symbol"],
                    "context": json.loads(ctx) if ctx else {},
                }
            )
        return rows

    def list_order_transitions(self) -> List[Tuple[str, str, str]]:
        cur = self._conn.execute(
            "SELECT client_order_id, prev_state, new_state FROM order_transitions WHERE run_id=? ORDER BY id",
            (self.run_id,),
        )
        return [(row[0], row[1], row[2]) for row in cur.fetchall()]


__all__ = ["SQLiteStore"]
