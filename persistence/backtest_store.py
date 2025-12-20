from __future__ import annotations

import datetime as dt
import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence

from persistence.db import connect_db, run_migrations


def _epoch_seconds(value: Any) -> Optional[int]:
    if value is None:
        return None
    if isinstance(value, (int, float)):
        return int(value)
    if isinstance(value, dt.datetime):
        if value.tzinfo is None:
            value = value.replace(tzinfo=dt.timezone.utc)
        return int(value.astimezone(dt.timezone.utc).timestamp())
    text = str(value).strip()
    if not text:
        return None
    try:
        ts = dt.datetime.fromisoformat(text)
    except Exception:
        return None
    if ts.tzinfo is None:
        ts = ts.replace(tzinfo=dt.timezone.utc)
    return int(ts.astimezone(dt.timezone.utc).timestamp())


def _json_dumps(payload: Any) -> Optional[str]:
    if payload is None:
        return None
    try:
        return json.dumps(payload, separators=(",", ":"), sort_keys=True, default=str)
    except Exception:
        try:
            return json.dumps(str(payload), separators=(",", ":"), sort_keys=True)
        except Exception:
            return None


@dataclass(frozen=True)
class BacktestRunRow:
    run_id: str
    created_ts: int
    start_date: Optional[str]
    end_date: Optional[str]
    interval: Optional[str]
    underlying_key: Optional[str]
    strategy: Optional[str]
    spec_json: Optional[str]
    execution: Dict[str, Any]
    costs: Dict[str, Any]


class BacktestStore:
    """
    Persist backtest runs inside the standard `engine_state.sqlite` file.

    This is intentionally separate from `SQLiteStore`: it stores UI-facing run history
    and cached renderables (summary/equity/trades/orders) so Streamlit can reload and
    compare runs without re-running the backtest.
    """

    def __init__(self, path: str | Path = "engine_state.sqlite") -> None:
        self.path = Path(path)
        self._conn = connect_db(self.path)
        run_migrations(self._conn)

    def close(self) -> None:
        try:
            self._conn.close()
        except Exception:
            pass

    def __enter__(self) -> "BacktestStore":
        return self

    def __exit__(self, exc_type, exc, tb) -> None:  # type: ignore[no-untyped-def]
        self.close()

    # ----------------------------------------------------------------- writes
    def save_run(
        self,
        *,
        run_id: str,
        results: Dict[str, Any],
        created_ts: Optional[int] = None,
        spec_json: Optional[str] = None,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        interval: Optional[str] = None,
        underlying_key: Optional[str] = None,
        strategy: Optional[str] = None,
    ) -> None:
        run_id = str(run_id or "").strip()
        if not run_id:
            raise ValueError("run_id is required")

        created_ts = int(created_ts if created_ts is not None else dt.datetime.now(dt.timezone.utc).timestamp())
        execution = results.get("execution") or {}
        costs = results.get("costs") or {}
        errors = results.get("errors") or []
        strategy = strategy or str(results.get("strategy") or "")
        interval = interval or str(results.get("interval") or "")
        underlying_key = underlying_key or str(results.get("underlying") or "")

        # Prefer explicit inputs; fallback to parse from results.period.
        start_date = start_date or None
        end_date = end_date or None
        if (not start_date or not end_date) and results.get("period"):
            try:
                period = str(results["period"])
                parts = [p.strip() for p in period.split("to")]
                if len(parts) == 2:
                    start_date = start_date or parts[0]
                    end_date = end_date or parts[1]
            except Exception:
                pass

        with self._conn:
            self._conn.execute("DELETE FROM backtest_summary WHERE run_id=?", (run_id,))
            self._conn.execute("DELETE FROM backtest_equity WHERE run_id=?", (run_id,))
            self._conn.execute("DELETE FROM backtest_trades WHERE run_id=?", (run_id,))
            self._conn.execute("DELETE FROM backtest_orders WHERE run_id=?", (run_id,))

            self._conn.execute(
                """
                INSERT INTO backtest_runs(
                    run_id, created_ts, spec_json, start_date, end_date, interval, underlying_key,
                    strategy, execution_json, costs_json, errors_json
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(run_id) DO UPDATE SET
                    created_ts=excluded.created_ts,
                    spec_json=excluded.spec_json,
                    start_date=excluded.start_date,
                    end_date=excluded.end_date,
                    interval=excluded.interval,
                    underlying_key=excluded.underlying_key,
                    strategy=excluded.strategy,
                    execution_json=excluded.execution_json,
                    costs_json=excluded.costs_json,
                    errors_json=excluded.errors_json
                """,
                (
                    run_id,
                    int(created_ts),
                    spec_json,
                    start_date,
                    end_date,
                    interval,
                    underlying_key,
                    strategy,
                    _json_dumps(execution),
                    _json_dumps(costs),
                    _json_dumps(errors),
                ),
            )

            self._insert_summary(run_id=run_id, results=results)
            self._insert_equity(run_id=run_id, equity_curve=results.get("equity_curve") or [])
            self._insert_trades(run_id=run_id, trades=results.get("trade_log") or [])
            # Prefer already-materialized orders list (it may include derived fill prices).
            orders = results.get("orders")
            if isinstance(orders, list) and orders:
                self._insert_orders(run_id=run_id, orders=orders)
            else:
                self._insert_orders_from_engine_tables(run_id=run_id)

    def _insert_summary(self, *, run_id: str, results: Dict[str, Any]) -> None:
        metrics: Dict[str, Any] = {
            "net_pnl": results.get("net_pnl"),
            "gross_pnl": results.get("gross_pnl"),
            "realized_pnl": results.get("realized_pnl"),
            "unrealized_pnl": results.get("unrealized_pnl"),
            "fees": results.get("total_fees") if results.get("total_fees") is not None else results.get("fees"),
            "trades": results.get("trades"),
            "wins": results.get("wins"),
            "win_rate": results.get("win_rate"),
            "candles": results.get("candles"),
            "ticks": results.get("ticks"),
        }
        for metric, value in metrics.items():
            if value is None:
                continue
            value_real: Optional[float]
            value_text: Optional[str]
            if isinstance(value, (int, float)):
                value_real = float(value)
                value_text = None
            else:
                try:
                    value_real = float(value)  # type: ignore[arg-type]
                    value_text = None
                except Exception:
                    value_real = None
                    value_text = str(value)
            self._conn.execute(
                "INSERT OR REPLACE INTO backtest_summary(run_id, metric, value_real, value_text) VALUES (?, ?, ?, ?)",
                (run_id, str(metric), value_real, value_text),
            )

    def _insert_equity(self, *, run_id: str, equity_curve: Sequence[Dict[str, Any]]) -> None:
        peak = float("-inf")
        for row in equity_curve:
            ts_raw = row.get("ts")
            ts = _epoch_seconds(ts_raw)
            if ts is None:
                continue
            equity = float(row.get("net") if row.get("net") is not None else row.get("equity") or 0.0)
            peak = max(peak, equity)
            drawdown = float(equity - peak)
            self._conn.execute(
                "INSERT OR REPLACE INTO backtest_equity(run_id, ts, equity, drawdown) VALUES (?, ?, ?, ?)",
                (run_id, int(ts), float(equity), float(drawdown)),
            )

    def _insert_trades(self, *, run_id: str, trades: Sequence[Dict[str, Any]]) -> None:
        for idx, trade in enumerate(trades):
            trade_id = str(trade.get("trade_id") or f"{run_id}:{idx+1}")
            opened_ts = _epoch_seconds(trade.get("opened_at"))
            closed_ts = _epoch_seconds(trade.get("closed_at"))
            trade_date = None
            if closed_ts is not None:
                trade_date = dt.datetime.fromtimestamp(int(closed_ts), tz=dt.timezone.utc).date().isoformat()
            symbol = str(trade.get("symbol") or "")
            pnl_gross = trade.get("gross_pnl") if trade.get("gross_pnl") is not None else trade.get("realized_pnl")
            fees = trade.get("fees")
            pnl_net = trade.get("net_pnl")
            exit_reason = trade.get("exit_reason")
            self._conn.execute(
                """
                INSERT OR REPLACE INTO backtest_trades(
                    run_id, trade_id, trade_date, opened_ts, closed_ts, symbol,
                    pnl_gross, fees, pnl_net, exit_reason, raw_json
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    run_id,
                    trade_id,
                    trade_date,
                    opened_ts,
                    closed_ts,
                    symbol,
                    (None if pnl_gross is None else float(pnl_gross)),
                    (None if fees is None else float(fees)),
                    (None if pnl_net is None else float(pnl_net)),
                    (None if exit_reason is None else str(exit_reason)),
                    _json_dumps(trade),
                ),
            )

    def _insert_orders(self, *, run_id: str, orders: Sequence[Dict[str, Any]]) -> None:
        for row in orders:
            order_id = str(row.get("order_id") or row.get("client_order_id") or "")
            if not order_id:
                continue
            ts = _epoch_seconds(row.get("last_update") or row.get("ts"))
            self._conn.execute(
                """
                INSERT OR REPLACE INTO backtest_orders(
                    run_id, order_id, ts, strategy, symbol, side, qty, fill_qty, price, eff_price, status, raw_json
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    run_id,
                    order_id,
                    ts,
                    row.get("strategy"),
                    row.get("symbol"),
                    row.get("side"),
                    (None if row.get("qty") is None else int(row.get("qty") or 0)),
                    (None if row.get("fill_qty") is None else int(row.get("fill_qty") or 0)),
                    (None if row.get("avg_raw_price") is None else float(row.get("avg_raw_price") or 0.0)),
                    (None if row.get("avg_effective_price") is None else float(row.get("avg_effective_price") or 0.0)),
                    (row.get("state") or row.get("status")),
                    _json_dumps(row),
                ),
            )

    def _insert_orders_from_engine_tables(self, *, run_id: str) -> None:
        rows = self._conn.execute(
            """
            SELECT
                o.client_order_id AS order_id,
                o.strategy,
                o.symbol,
                o.side,
                o.qty,
                o.state,
                o.last_update,
                COALESCE(SUM(e.qty), 0) AS fill_qty,
                CASE WHEN COALESCE(SUM(e.qty), 0) > 0 THEN SUM(e.qty * COALESCE(e.raw_price, e.price)) / SUM(e.qty) END AS avg_raw_price,
                CASE WHEN COALESCE(SUM(e.qty), 0) > 0 THEN SUM(e.qty * COALESCE(e.effective_price, e.price)) / SUM(e.qty) END AS avg_effective_price
            FROM orders o
            LEFT JOIN executions e
                ON e.run_id=o.run_id AND e.order_id=o.client_order_id
            WHERE o.run_id=?
            GROUP BY o.client_order_id
            ORDER BY o.last_update ASC
            """,
            (run_id,),
        ).fetchall()
        for r in rows:
            payload = dict(r)
            ts = _epoch_seconds(payload.get("last_update"))
            self._conn.execute(
                """
                INSERT OR REPLACE INTO backtest_orders(
                    run_id, order_id, ts, strategy, symbol, side, qty, fill_qty, price, eff_price, status, raw_json
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    run_id,
                    payload.get("order_id"),
                    ts,
                    payload.get("strategy"),
                    payload.get("symbol"),
                    payload.get("side"),
                    (None if payload.get("qty") is None else int(payload.get("qty") or 0)),
                    int(payload.get("fill_qty") or 0),
                    payload.get("avg_raw_price"),
                    payload.get("avg_effective_price"),
                    payload.get("state"),
                    _json_dumps(payload),
                ),
            )

    # ------------------------------------------------------------------ reads
    def list_runs(self, *, limit: int = 200) -> List[BacktestRunRow]:
        cur = self._conn.execute(
            """
            SELECT run_id, created_ts, start_date, end_date, interval, underlying_key, strategy, spec_json, execution_json, costs_json
            FROM backtest_runs
            ORDER BY created_ts DESC
            LIMIT ?
            """,
            (int(limit),),
        )
        rows: List[BacktestRunRow] = []
        for r in cur.fetchall():
            try:
                execution = json.loads(r["execution_json"] or "{}")
            except Exception:
                execution = {}
            try:
                costs = json.loads(r["costs_json"] or "{}")
            except Exception:
                costs = {}
            rows.append(
                BacktestRunRow(
                    run_id=r["run_id"],
                    created_ts=int(r["created_ts"]),
                    start_date=r["start_date"],
                    end_date=r["end_date"],
                    interval=r["interval"],
                    underlying_key=r["underlying_key"],
                    strategy=r["strategy"],
                    spec_json=r["spec_json"],
                    execution=execution,
                    costs=costs,
                )
            )
        return rows

    def load_run(self, run_id: str) -> Dict[str, Any]:
        run_id = str(run_id or "").strip()
        if not run_id:
            raise ValueError("run_id is required")

        run = self._conn.execute(
            """
            SELECT run_id, created_ts, spec_json, start_date, end_date, interval, underlying_key, strategy, execution_json, costs_json, errors_json
            FROM backtest_runs
            WHERE run_id=?
            """,
            (run_id,),
        ).fetchone()
        if not run:
            raise KeyError(f"unknown_run_id:{run_id}")

        summary = self._load_summary(run_id)
        equity = self._load_equity(run_id)
        trades = self._load_trades(run_id)
        orders = self._load_orders(run_id)
        executions = self._load_executions(run_id)

        try:
            execution = json.loads(run["execution_json"] or "{}")
        except Exception:
            execution = {}
        try:
            costs = json.loads(run["costs_json"] or "{}")
        except Exception:
            costs = {}
        try:
            errors = json.loads(run["errors_json"] or "[]")
        except Exception:
            errors = []

        period = None
        if run["start_date"] and run["end_date"]:
            period = f"{run['start_date']} to {run['end_date']}"

        out: Dict[str, Any] = {
            "run_id": run["run_id"],
            "strategy": run["strategy"] or "",
            "period": period or "",
            "interval": run["interval"] or "",
            "underlying": run["underlying_key"] or "",
            "execution": execution,
            "costs": costs,
            "errors": errors if isinstance(errors, list) else [],
            "equity_curve": equity,
            "trade_log": trades,
            "orders": orders,
            "executions": executions,
        }

        # Mirror BacktestingEngine result keys used by Streamlit.
        for key in ("net_pnl", "gross_pnl", "realized_pnl", "unrealized_pnl", "fees", "total_fees", "trades", "wins", "win_rate", "candles", "ticks"):
            if key in summary:
                out[key] = summary[key]
        if out.get("total_fees") is None and out.get("fees") is not None:
            out["total_fees"] = out.get("fees")
        return out

    def _load_summary(self, run_id: str) -> Dict[str, Any]:
        cur = self._conn.execute(
            "SELECT metric, value_real, value_text FROM backtest_summary WHERE run_id=?",
            (run_id,),
        )
        out: Dict[str, Any] = {}
        for r in cur.fetchall():
            metric = str(r["metric"])
            if r["value_real"] is not None:
                out[metric] = float(r["value_real"])
            else:
                out[metric] = r["value_text"]
        return out

    def _load_equity(self, run_id: str) -> List[Dict[str, Any]]:
        cur = self._conn.execute(
            "SELECT ts, equity, drawdown FROM backtest_equity WHERE run_id=? ORDER BY ts ASC",
            (run_id,),
        )
        rows: List[Dict[str, Any]] = []
        for r in cur.fetchall():
            ts = int(r["ts"])
            ts_dt = dt.datetime.fromtimestamp(ts, tz=dt.timezone.utc)
            rows.append({"ts": ts_dt.isoformat(), "net": float(r["equity"]), "drawdown": float(r["drawdown"])})
        return rows

    def _load_trades(self, run_id: str) -> List[Dict[str, Any]]:
        cur = self._conn.execute(
            """
            SELECT trade_id, trade_date, opened_ts, closed_ts, symbol, pnl_gross, fees, pnl_net, exit_reason, raw_json
            FROM backtest_trades
            WHERE run_id=?
            ORDER BY COALESCE(closed_ts, opened_ts) ASC
            """,
            (run_id,),
        )
        out: List[Dict[str, Any]] = []
        for r in cur.fetchall():
            try:
                raw = json.loads(r["raw_json"] or "{}")
            except Exception:
                raw = {}
            raw.setdefault("trade_id", r["trade_id"])
            raw.setdefault("symbol", r["symbol"])
            raw.setdefault("gross_pnl", r["pnl_gross"])
            raw.setdefault("fees", r["fees"])
            raw.setdefault("net_pnl", r["pnl_net"])
            out.append(raw)
        return out

    def _load_orders(self, run_id: str) -> List[Dict[str, Any]]:
        cur = self._conn.execute(
            """
            SELECT order_id, ts, strategy, symbol, side, qty, fill_qty, price, eff_price, status, raw_json
            FROM backtest_orders
            WHERE run_id=?
            ORDER BY ts ASC
            """,
            (run_id,),
        )
        out: List[Dict[str, Any]] = []
        for r in cur.fetchall():
            try:
                raw = json.loads(r["raw_json"] or "{}")
            except Exception:
                raw = {}
            raw.setdefault("order_id", r["order_id"])
            raw.setdefault("last_update", dt.datetime.fromtimestamp(int(r["ts"] or 0), tz=dt.timezone.utc).isoformat() if r["ts"] else None)
            raw.setdefault("fill_qty", r["fill_qty"])
            raw.setdefault("avg_raw_price", r["price"])
            raw.setdefault("avg_effective_price", r["eff_price"])
            raw.setdefault("state", r["status"])
            out.append(raw)
        return out

    def _load_executions(self, run_id: str) -> List[Dict[str, Any]]:
        # Read directly from the engine executions + cost ledger for full fidelity.
        cur = self._conn.execute(
            """
            SELECT
                e.exec_id,
                e.order_id,
                e.symbol,
                e.side,
                e.qty,
                COALESCE(e.raw_price, e.price) AS raw_price,
                COALESCE(e.effective_price, e.price) AS effective_price,
                e.price,
                e.ts,
                e.venue,
                COALESCE(SUM(c.amount), 0.0) AS fees
            FROM executions e
            LEFT JOIN cost_ledger c
                ON c.run_id=e.run_id AND c.exec_id=e.exec_id
            WHERE e.run_id=?
            GROUP BY e.id
            ORDER BY e.ts ASC
            """,
            (run_id,),
        )
        return [dict(r) for r in cur.fetchall()]


__all__ = ["BacktestRunRow", "BacktestStore"]
