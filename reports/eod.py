from __future__ import annotations

import argparse
import csv
import json
import sqlite3
from pathlib import Path
from typing import Any, Dict, List


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Generate end-of-day PnL report")
    parser.add_argument("--db", type=Path, default=Path("engine_state.sqlite"), help="SQLite DB path")
    parser.add_argument("--run-id", required=True, help="Run identifier to report")
    parser.add_argument("--out", type=Path, required=True, help="Output CSV path for PnL summary")
    return parser.parse_args()


def _latest_snapshot(conn: sqlite3.Connection, run_id: str) -> Dict[str, Any]:
    conn.row_factory = sqlite3.Row
    row = conn.execute(
        "SELECT ts, realized, unrealized, fees, net, per_symbol FROM pnl_snapshots WHERE run_id=? ORDER BY ts DESC LIMIT 1",
        (run_id,),
    ).fetchone()
    if not row:
        raise SystemExit(f"No snapshots recorded for run {run_id}")
    payload = json.loads(row["per_symbol"]) if row["per_symbol"] else {}
    return {
        "ts": row["ts"],
        "realized": row["realized"],
        "unrealized": row["unrealized"],
        "fees": row["fees"],
        "net": row["net"],
        "per_symbol": payload,
    }


def _cost_ledger(conn: sqlite3.Connection, run_id: str) -> List[sqlite3.Row]:
    conn.row_factory = sqlite3.Row
    cur = conn.execute(
        "SELECT exec_id, category, amount, currency, ts, note FROM cost_ledger WHERE run_id=? ORDER BY ts",
        (run_id,),
    )
    return cur.fetchall()


def _write_pnl(path: Path, snapshot: Dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.writer(handle)
        writer.writerow(["symbol", "quantity", "realized_pnl", "unrealized_pnl", "gross_fees", "net_pnl"])
        for symbol, data in snapshot["per_symbol"].items():
            net = float(data["realized"]) + float(data["unrealized"]) - float(data["fees"])
            writer.writerow(
                [
                    symbol,
                    data["qty"],
                    f"{data['realized']:.2f}",
                    f"{data['unrealized']:.2f}",
                    f"{data['fees']:.2f}",
                    f"{net:.2f}",
                ]
            )


def _write_costs(path: Path, rows: List[sqlite3.Row]) -> None:
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.writer(handle)
        writer.writerow(["exec_id", "category", "amount", "currency", "ts", "note"])
        for row in rows:
            writer.writerow([row["exec_id"], row["category"], f"{row['amount']:.2f}", row["currency"], row["ts"], row["note"] or ""])


def main() -> None:
    args = _parse_args()
    conn = sqlite3.connect(args.db)
    snapshot = _latest_snapshot(conn, args.run_id)
    costs = _cost_ledger(conn, args.run_id)
    _write_pnl(args.out, snapshot)
    cost_path = args.out.with_name(f"{args.out.stem}_costs{args.out.suffix}")
    _write_costs(cost_path, costs)
    conn.close()


if __name__ == "__main__":  # pragma: no cover
    main()
