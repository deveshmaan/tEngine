from __future__ import annotations

import datetime as dt
import json
import threading
import time
from collections import deque
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple, TYPE_CHECKING

from persistence.db import connect_db, run_migrations

try:  # Optional dependency on engine.time_machine without creating strong cycle
    from engine.time_machine import utc_now as engine_utc_now
except Exception:  # pragma: no cover - fallback when engine module unavailable
    engine_utc_now = lambda: dt.datetime.now(dt.timezone.utc)  # type: ignore

if TYPE_CHECKING:  # pragma: no cover - for type checkers only
    from engine.oms import Order, OrderState


class SQLiteStore:
    """SQLite persistence layer with schema migrations and monotonic timestamps."""

    def __init__(self, path: str | Path, run_id: str):
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._conn = connect_db(self.path)
        self._lock = threading.Lock()
        self.run_id = run_id
        self._latest_ts = dt.datetime.min.replace(tzinfo=dt.timezone.utc)
        run_migrations(self._conn)
        self._ensure_run()
        self._event_queue: deque[Tuple[str, Dict[str, Any], dt.datetime]] = deque()
        self._stop_event = threading.Event()
        self._worker = threading.Thread(target=self._drain_events, name="store-event-worker", daemon=True)
        self._worker.start()

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
        current = (ts or engine_utc_now()).astimezone(dt.timezone.utc)
        if current <= self._latest_ts:
            current = self._latest_ts + dt.timedelta(microseconds=1)
        self._latest_ts = current
        return current.isoformat()

    def _json(self, payload: Optional[Dict[str, Any]]) -> Optional[str]:
        if payload is None:
            return None
        return json.dumps(payload, separators=(",", ":"))

    def close(self) -> None:
        self._stop_event.set()
        if self._worker.is_alive():
            self._worker.join(timeout=2.0)
        with self._lock:
            self._flush_event_queue()
            try:
                self._conn.commit()
            except Exception:
                pass

    # -------------------------------------------------------------- order logs
    def record_order_snapshot(self, order: "Order") -> None:
        with self._lock:
            ts = self._ts(order.updated_at)
            self._conn.execute(
                """
                INSERT INTO orders(run_id, client_order_id, strategy, symbol, side, qty, price, state, last_update, broker_order_id, idempotency_key)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(run_id, client_order_id) DO UPDATE SET
                    strategy=excluded.strategy,
                    symbol=excluded.symbol,
                    side=excluded.side,
                    qty=excluded.qty,
                    price=excluded.price,
                    state=excluded.state,
                    last_update=excluded.last_update,
                    broker_order_id=excluded.broker_order_id,
                    idempotency_key=excluded.idempotency_key
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
                    order.idempotency_key,
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
    def record_execution(
        self,
        order_id: str,
        *,
        exec_id: Optional[str] = None,
        symbol: str,
        side: str,
        qty: int,
        price: float,
        raw_price: Optional[float] = None,
        effective_price: Optional[float] = None,
        venue: Optional[str] = None,
        idempotency_key: Optional[str] = None,
        ts: Optional[dt.datetime] = None,
    ) -> None:
        with self._lock:
            stamp = self._ts(ts)
            exec_id = str(exec_id or order_id)
            eff = float(effective_price if effective_price is not None else price)
            self._conn.execute(
                """
                INSERT INTO executions(run_id, order_id, exec_id, symbol, side, qty, price, raw_price, effective_price, ts, venue, idempotency_key)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (self.run_id, order_id, exec_id, symbol, side, qty, eff, raw_price, eff, stamp, venue, idempotency_key),
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

    # ---------------------------------------------------------- market events
    def record_market_event(self, event_type: str, payload: Dict[str, Any], ts: Optional[dt.datetime] = None) -> None:
        stamp = self._ts(ts)
        self._event_queue.append((event_type, dict(payload), dt.datetime.fromisoformat(stamp)))

    # -------------------------------------------------------------- cost ledger
    def record_cost(
        self,
        exec_id: str,
        *,
        category: str,
        amount: float,
        currency: str = "INR",
        note: Optional[str] = None,
        ts: Optional[dt.datetime] = None,
    ) -> None:
        with self._lock:
            stamp = self._ts(ts)
            self._conn.execute(
                """
                INSERT INTO cost_ledger(run_id, exec_id, category, amount, currency, ts, note)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                (self.run_id, exec_id, category, amount, currency, stamp, note),
            )
            self._conn.commit()

    def record_pnl_snapshot(
        self,
        *,
        ts: dt.datetime,
        realized: float,
        unrealized: float,
        fees: float,
        per_symbol: Dict[str, Any],
    ) -> None:
        net = realized + unrealized - fees
        payload = json.dumps(per_symbol, separators=(",", ":"))
        with self._lock:
            self._conn.execute(
                """
                INSERT INTO pnl_snapshots(run_id, ts, realized, unrealized, fees, net, per_symbol)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                (self.run_id, ts.isoformat(), realized, unrealized, fees, net, payload),
            )
            self._conn.commit()

    def upsert_position(
        self,
        *,
        symbol: str,
        expiry: Optional[str],
        strike: Optional[float],
        opt_type: Optional[str],
        qty: int,
        avg_price: float,
        opened_at: dt.datetime,
        closed_at: Optional[dt.datetime],
    ) -> None:
        with self._lock:
            self._conn.execute(
                """
                INSERT INTO positions(run_id, symbol, expiry, strike, opt_type, qty, avg_price, opened_at, closed_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(run_id, symbol, expiry, strike, opt_type) DO UPDATE SET
                    qty=excluded.qty,
                    avg_price=excluded.avg_price,
                    opened_at=excluded.opened_at,
                    closed_at=excluded.closed_at
                """,
                (
                    self.run_id,
                    symbol,
                    expiry,
                    strike,
                    opt_type,
                    qty,
                    avg_price,
                    opened_at.isoformat(),
                    closed_at.isoformat() if closed_at else None,
                ),
            )
            self._conn.commit()

    def load_open_position(self, instrument: str) -> Optional[Dict[str, Any]]:
        with self._lock:
            cur = self._conn.execute(
                """
                SELECT qty, avg_price, opened_at, closed_at, expiry, strike, opt_type
                FROM positions
                WHERE run_id=? AND symbol=? AND (closed_at IS NULL OR closed_at='')
                ORDER BY id DESC
                LIMIT 1
                """,
                (self.run_id, instrument),
            )
            row = cur.fetchone()
        if not row:
            return None
        try:
            opened_at = dt.datetime.fromisoformat(row["opened_at"])
        except Exception:
            opened_at = None
        try:
            closed_at = dt.datetime.fromisoformat(row["closed_at"]) if row["closed_at"] else None
        except Exception:
            closed_at = None
        return {
            "qty": row["qty"],
            "avg_price": row["avg_price"],
            "opened_at": opened_at,
            "closed_at": closed_at,
            "expiry": row["expiry"],
            "strike": row["strike"],
            "opt_type": row["opt_type"],
        }

    def list_open_positions(self) -> List[Dict[str, Any]]:
        with self._lock:
            cur = self._conn.execute(
                """
                SELECT symbol, qty, avg_price, opened_at, expiry, strike, opt_type
                FROM positions
                WHERE run_id=? AND (closed_at IS NULL OR closed_at='') AND ABS(qty) > 0
                """,
                (self.run_id,),
            )
            rows = cur.fetchall()
        positions: List[Dict[str, Any]] = []
        for row in rows:
            try:
                opened_at = dt.datetime.fromisoformat(row["opened_at"]) if row["opened_at"] else None
            except Exception:
                opened_at = None
            positions.append(
                {
                    "symbol": row["symbol"],
                    "qty": row["qty"],
                    "avg_price": row["avg_price"],
                    "opened_at": opened_at,
                    "expiry": row["expiry"],
                    "strike": row["strike"],
                    "opt_type": row["opt_type"],
                }
            )
        return positions

    def upsert_exit_plan(self, instrument: str, plan: Dict[str, Any], ts: Optional[dt.datetime] = None) -> None:
        payload = self._json(plan) or "{}"
        with self._lock:
            stamp = self._ts(ts)
            self._conn.execute(
                """
                INSERT INTO exit_plans(run_id, instrument, plan_json, updated_at)
                VALUES (?, ?, ?, ?)
                ON CONFLICT(run_id, instrument) DO UPDATE SET
                    plan_json=excluded.plan_json,
                    updated_at=excluded.updated_at
                """,
                (self.run_id, instrument, payload, stamp),
            )
            self._conn.commit()

    def load_exit_plan(self, instrument: str) -> Optional[Dict[str, Any]]:
        with self._lock:
            cur = self._conn.execute(
                "SELECT plan_json FROM exit_plans WHERE run_id=? AND instrument=?",
                (self.run_id, instrument),
            )
            row = cur.fetchone()
        if not row:
            return None
        try:
            return json.loads(row["plan_json"] or "{}")
        except Exception:
            return None

    def delete_exit_plan(self, instrument: str) -> None:
        with self._lock:
            self._conn.execute(
                "DELETE FROM exit_plans WHERE run_id=? AND instrument=?",
                (self.run_id, instrument),
            )
            self._conn.commit()

    def clear_exit_plans(self) -> None:
        with self._lock:
            self._conn.execute("DELETE FROM exit_plans WHERE run_id=?", (self.run_id,))
            self._conn.commit()

    def insert_control_intent(self, action: str, payload: Optional[Dict[str, Any]] = None, ts: Optional[dt.datetime] = None) -> None:
        with self._lock:
            stamp = self._ts(ts)
            self._conn.execute(
                """
                INSERT INTO control_intents(run_id, ts, action, payload_json)
                VALUES (?, ?, ?, ?)
                """,
                (self.run_id, stamp, action, self._json(payload)),
            )
            self._conn.commit()

    # ----------------------------------------------------------- async helpers
    def _drain_events(self) -> None:
        """Background worker to flush non-critical market events in batches."""

        while not self._stop_event.is_set():
            time.sleep(0.5)
            self._flush_event_queue()
        self._flush_event_queue()

    def _flush_event_queue(self) -> None:
        if not self._event_queue:
            return
        rows: list[tuple[str, str, str]] = []
        while self._event_queue:
            event_type, payload, ts = self._event_queue.popleft()
            rows.append((self.run_id, ts.isoformat(), event_type, self._json(payload) or "{}"))
        if not rows:
            return
        with self._lock:
            self._conn.executemany(
                """
                INSERT INTO event_log(run_id, ts, event_type, payload)
                VALUES (?, ?, ?, ?)
                """,
                rows,
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

    def list_cost_ledger(self) -> List[Dict[str, Any]]:
        cur = self._conn.execute(
            "SELECT exec_id, category, amount, currency, ts, note FROM cost_ledger WHERE run_id=? ORDER BY ts",
            (self.run_id,),
        )
        return [dict(row) for row in cur.fetchall()]

    def list_pnl_snapshots(self) -> List[Dict[str, Any]]:
        cur = self._conn.execute(
            "SELECT ts, realized, unrealized, fees, net, per_symbol FROM pnl_snapshots WHERE run_id=? ORDER BY ts",
            (self.run_id,),
        )
        rows = []
        for row in cur.fetchall():
            rows.append(
                {
                    "ts": row["ts"],
                    "realized": row["realized"],
                    "unrealized": row["unrealized"],
                    "fees": row["fees"],
                    "net": row["net"],
                    "per_symbol": json.loads(row["per_symbol"] or "{}"),
                }
            )
        return rows

    def iter_market_events(self) -> Iterable[Dict[str, Any]]:
        cur = self._conn.execute(
            "SELECT ts, event_type, payload FROM event_log WHERE run_id=? ORDER BY ts",
            (self.run_id,),
        )
        for row in cur.fetchall():
            yield {
                "ts": row["ts"],
                "type": row["event_type"],
                "payload": json.loads(row["payload"] or "{}"),
            }

    def fetch_market_events_for(self, source_run_id: str) -> List[Dict[str, Any]]:
        cur = self._conn.execute(
            "SELECT ts, event_type, payload FROM event_log WHERE run_id=? ORDER BY ts",
            (source_run_id,),
        )
        events: List[Dict[str, Any]] = []
        for row in cur.fetchall():
            events.append(
                {
                    "ts": row["ts"],
                    "type": row["event_type"],
                    "payload": json.loads(row["payload"] or "{}"),
                }
            )
        return events

    def load_active_orders(self) -> List[Dict[str, Any]]:
        cur = self._conn.execute(
            """
            SELECT client_order_id, strategy, symbol, side, qty, price, state, last_update, broker_order_id, idempotency_key
            FROM orders
            WHERE run_id=? AND state NOT IN ('FILLED','REJECTED','CANCELED')
            """,
            (self.run_id,),
        )
        return [dict(row) for row in cur.fetchall()]

    def find_order_by_idempotency(self, key: str) -> Optional[Dict[str, Any]]:
        cur = self._conn.execute(
            """
            SELECT client_order_id, strategy, symbol, side, qty, price, state, last_update, broker_order_id, idempotency_key
            FROM orders WHERE run_id=? AND idempotency_key=?
            """,
            (self.run_id, key),
        )
        row = cur.fetchone()
        return dict(row) if row else None

    def find_order_by_client_id(self, client_order_id: str) -> Optional[Dict[str, Any]]:
        cur = self._conn.execute(
            """
            SELECT client_order_id, strategy, symbol, side, qty, price, state, last_update, broker_order_id, idempotency_key
            FROM orders WHERE run_id=? AND client_order_id=?
            """,
            (self.run_id, client_order_id),
        )
        row = cur.fetchone()
        return dict(row) if row else None

    def control_intents_since(self, since_ts: Optional[str]) -> List[Dict[str, Any]]:
        if since_ts:
            cur = self._conn.execute(
                "SELECT ts, action, payload_json FROM control_intents WHERE run_id=? AND ts>? ORDER BY ts",
                (self.run_id, since_ts),
            )
        else:
            cur = self._conn.execute(
                "SELECT ts, action, payload_json FROM control_intents WHERE run_id=? ORDER BY ts",
                (self.run_id,),
            )
        intents = []
        for row in cur.fetchall():
            intents.append(
                {
                    "ts": row["ts"],
                    "action": row["action"],
                    "payload": json.loads(row["payload_json"] or "{}"),
                }
            )
        return intents


__all__ = ["SQLiteStore"]
