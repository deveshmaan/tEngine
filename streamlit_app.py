from __future__ import annotations

import datetime as dt
import json
import os
import sqlite3
import time
from pathlib import Path
from typing import Any, Dict, Optional

import pandas as pd
import requests
import streamlit as st
from prometheus_client.parser import text_string_to_metric_families
from yaml import safe_load

DEFAULT_DB = Path(os.getenv("ENGINE_DB_PATH", "engine_state.sqlite"))
PROM_URL = os.getenv("PROM_URL", "http://127.0.0.1:9103/metrics")
ENGINE_CONFIG = Path(os.getenv("ENGINE_CONFIG", "engine_config.yaml"))


def load_risk_limits() -> Dict[str, float]:
    if not ENGINE_CONFIG.exists():
        return {}
    with ENGINE_CONFIG.open("r", encoding="utf-8") as handle:
        raw = safe_load(handle) or {}
    return raw.get("risk", {})


def fetch_metrics() -> Dict[str, float]:
    try:
        resp = requests.get(PROM_URL, timeout=2)
        resp.raise_for_status()
    except Exception:
        return {}
    parsed: Dict[str, float] = {}
    for fam in text_string_to_metric_families(resp.text):
        for sample in fam.samples:
            name, labels, value = sample[:3]
            key = name
            if labels:
                label_text = ",".join(f'{k}="{v}"' for k, v in labels.items())
                key = f"{name}{{{label_text}}}"
            parsed[key] = value
    return parsed


def fetch_df(conn: sqlite3.Connection, query: str, params: tuple[Any, ...]) -> pd.DataFrame:
    return pd.read_sql_query(query, conn, params=params)


def fetch_latest_snapshot(conn: sqlite3.Connection, run_id: str) -> Optional[Dict[str, Any]]:
    row = conn.execute(
        "SELECT ts, realized, unrealized, fees, net, per_symbol FROM pnl_snapshots WHERE run_id=? ORDER BY ts DESC LIMIT 1",
        (run_id,),
    ).fetchone()
    if not row:
        return None
    entry = dict(row)
    entry["per_symbol"] = json.loads(entry["per_symbol"] or "{}")
    return entry


def fetch_pnl_history(conn: sqlite3.Connection, run_id: str, limit: int = 200) -> pd.DataFrame:
    rows = conn.execute(
        "SELECT ts, net FROM pnl_snapshots WHERE run_id=? ORDER BY ts DESC LIMIT ?",
        (run_id, limit),
    ).fetchall()
    if not rows:
        return pd.DataFrame(columns=["ts", "net"])
    df = pd.DataFrame(rows, columns=["ts", "net"])
    df["ts"] = pd.to_datetime(df["ts"])
    return df.sort_values("ts")


def insert_control_intent(conn: sqlite3.Connection, run_id: str, action: str, payload: Optional[Dict[str, Any]] = None) -> None:
    ts = dt.datetime.utcnow().isoformat()
    conn.execute(
        "INSERT INTO control_intents(run_id, ts, action, payload_json) VALUES (?, ?, ?, ?)",
        (run_id, ts, action, json.dumps(payload) if payload else None),
    )
    conn.commit()


st.set_page_config(page_title="Trading Engine Console", layout="wide")
st.title("Trading Engine Console")

db_path = Path(st.sidebar.text_input("State DB", value=str(DEFAULT_DB)))
run_id = st.sidebar.text_input("Run ID", value="dev-run")
auto = st.sidebar.checkbox("Auto refresh", value=True)
interval = st.sidebar.slider("Refresh seconds", min_value=1, max_value=10, value=2)
if auto:
    st.sidebar.caption("Streaming updates enabled")

metrics_data = fetch_metrics()
limits = load_risk_limits()

with sqlite3.connect(db_path) as conn:
    conn.row_factory = sqlite3.Row
    positions = fetch_df(conn, "SELECT symbol, qty, avg_price, opened_at, closed_at FROM positions WHERE run_id=?", (run_id,))
    orders = fetch_df(
        conn,
        "SELECT client_order_id, symbol, side, qty, state, last_update FROM orders WHERE run_id=? ORDER BY last_update DESC LIMIT 100",
        (run_id,),
    )
    incidents = fetch_df(conn, "SELECT ts, code, payload FROM incidents WHERE run_id=? ORDER BY ts DESC LIMIT 50", (run_id,))
    snapshot = fetch_latest_snapshot(conn, run_id)
    pnl_history = fetch_pnl_history(conn, run_id)

run_col1, run_col2, run_col3 = st.columns(3)
heartbeat = metrics_data.get("heartbeat_ts", 0.0)
lag = time.time() - heartbeat if heartbeat else float("nan")
run_col1.metric("Heartbeat", f"{heartbeat:.0f}")
run_col2.metric("Lag (s)", f"{lag:.1f}" if heartbeat else "n/a")
run_col3.metric("Open Orders", int(metrics_data.get("order_queue_depth", 0)))

st.subheader("Risk Dials")
risk_cols = st.columns(3)
realized = snapshot["realized"] if snapshot else 0.0
unrealized = snapshot["unrealized"] if snapshot else 0.0
fees = snapshot["fees"] if snapshot else 0.0
net = realized + unrealized - fees
daily_limit = limits.get("daily_pnl_stop", 0.0)
risk_cols[0].metric("Realized PnL", f"{realized:,.0f}", delta=None)
risk_cols[1].metric("Unrealized PnL", f"{unrealized:,.0f}", delta=None)
risk_cols[2].metric("Net PnL vs Stop", f"{net:,.0f}", delta=f"-{daily_limit:,.0f}" if daily_limit else None)

st.subheader("Open Positions")
st.dataframe(positions, use_container_width=True)

st.subheader("Orders")
st.dataframe(orders, use_container_width=True)

st.subheader("Incidents")
st.dataframe(incidents, use_container_width=True)

st.subheader("PnL Trend")
if not pnl_history.empty:
    st.line_chart(pnl_history.set_index("ts"))
else:
    st.write("No PnL snapshots yet.")

st.subheader("Controls")
ctrl_cols = st.columns(2)
with sqlite3.connect(db_path) as conn:
    conn.row_factory = sqlite3.Row
    if ctrl_cols[0].button("Kill Switch"):
        insert_control_intent(conn, run_id, "KILL", {"source": "ui"})
        st.success("Kill intent posted")
    if ctrl_cols[1].button("Square-Off"):
        insert_control_intent(conn, run_id, "SQUARE_OFF", {"source": "ui"})
        st.success("Square-off intent posted")

if auto:
    time.sleep(interval)
    st.rerun()
