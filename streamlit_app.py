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

IST = dt.timezone(dt.timedelta(hours=5, minutes=30), name="Asia/Kolkata")
DEFAULT_DB = Path(os.getenv("ENGINE_DB_PATH", "engine_state.sqlite"))
PROM_URL = os.getenv("PROM_URL", "http://127.0.0.1:9103/metrics")
APP_CONFIG = Path(os.getenv("APP_CONFIG_PATH", "config/app.yml"))


def _load_app_config() -> Dict[str, Any]:
    if not APP_CONFIG.exists():
        return {}
    with APP_CONFIG.open("r", encoding="utf-8") as handle:
        raw = safe_load(handle) or {}
    return raw


def _data_config() -> Dict[str, Any]:
    return _load_app_config().get("data", {})


def load_risk_limits() -> Dict[str, float]:
    cfg = _load_app_config()
    return (cfg.get("risk") or {})


def load_index_symbol() -> str:
    data_cfg = _data_config()
    return str(data_cfg.get("index_symbol", "NIFTY")).upper()


def load_weekly_weekday() -> int:
    data_cfg = _data_config()
    try:
        return int(data_cfg.get("weekly_expiry_weekday", 3))
    except (TypeError, ValueError):
        return 3


def load_holidays() -> set[dt.date]:
    data_cfg = _data_config()
    holidays: set[dt.date] = set()
    for raw in data_cfg.get("holidays", []):
        try:
            holidays.add(dt.date.fromisoformat(str(raw)))
        except ValueError:
            continue
    return holidays


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


def _extract_symbol_expiry(symbol: Optional[str]) -> Optional[str]:
    if not symbol:
        return None
    parts = symbol.split("-")
    if len(parts) < 3:
        return None
    return "-".join(parts[1:-1])


def fetch_latest_contract_ticks(
    conn: sqlite3.Connection,
    run_id: str,
    limit: int = 500,
    *,
    expiry_filter: Optional[str] = None,
    underlying_filter: Optional[str] = None,
) -> pd.DataFrame:
    rows = conn.execute(
        "SELECT ts, payload FROM event_log WHERE run_id=? AND event_type='tick' ORDER BY ts DESC LIMIT ?",
        (run_id, limit),
    ).fetchall()
    latest: Dict[str, Dict[str, Any]] = {}
    for row in rows:
        payload = json.loads(row["payload"] or "{}")
        symbol = payload.get("symbol")
        if not symbol or symbol in latest:
            continue
        latest[symbol] = {"ts": row["ts"], "payload": payload}
    records: list[Dict[str, Any]] = []
    for symbol, entry in latest.items():
        payload = entry["payload"]
        expiry = _extract_symbol_expiry(symbol) or ""
        if expiry_filter and expiry != expiry_filter:
            continue
        underlying = (payload.get("underlying") or payload.get("name") or symbol).upper()
        if underlying_filter and underlying_filter.upper() not in {underlying, symbol.split("-")[0].upper()}:
            continue
        greeks = payload.get("optionGreeks") or payload.get("greeks") or {}
        records.append(
            {
                "Symbol": symbol,
                "Name": payload.get("name") or payload.get("display_name") or payload.get("underlying") or symbol,
                "Expiry": expiry,
                "LTP": payload.get("ltp") or payload.get("price"),
                "IV": payload.get("iv") or greeks.get("iv"),
                "Delta": payload.get("delta") or greeks.get("delta"),
                "Gamma": payload.get("gamma") or greeks.get("gamma"),
                "Theta": payload.get("theta") or greeks.get("theta"),
                "Vega": payload.get("vega") or greeks.get("vega"),
            }
        )
    if not records:
        return pd.DataFrame(columns=["Symbol", "Name", "Expiry", "LTP", "IV", "Delta", "Gamma", "Theta", "Vega"])
    df = pd.DataFrame(records)
    return df.sort_values("Symbol")


def insert_control_intent(conn: sqlite3.Connection, run_id: str, action: str, payload: Optional[Dict[str, Any]] = None) -> None:
    ts = dt.datetime.utcnow().isoformat()
    conn.execute(
        "INSERT INTO control_intents(run_id, ts, action, payload_json) VALUES (?, ?, ?, ?)",
        (run_id, ts, action, json.dumps(payload) if payload else None),
    )
    conn.commit()


def resolve_next_weekly_expiry(now: Optional[dt.datetime] = None) -> str:
    now_ist = (now or dt.datetime.now(IST)).astimezone(IST)
    weekday = load_weekly_weekday()
    holidays = load_holidays()
    delta = (weekday - now_ist.weekday()) % 7
    if delta == 0 and now_ist.time() >= dt.time(15, 30):
        delta = 7
    expiry = now_ist.date() + dt.timedelta(days=delta)
    while expiry.weekday() >= 5 or expiry in holidays:
        expiry -= dt.timedelta(days=1)
    return expiry.isoformat()


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
    index_symbol = load_index_symbol()
    weekly_expiry = resolve_next_weekly_expiry()
    positions = fetch_df(conn, "SELECT symbol, qty, avg_price, opened_at, closed_at FROM positions WHERE run_id=?", (run_id,))
    orders = fetch_df(
        conn,
        "SELECT client_order_id, symbol, side, qty, state, last_update FROM orders WHERE run_id=? ORDER BY last_update DESC LIMIT 100",
        (run_id,),
    )
    incidents = fetch_df(conn, "SELECT ts, code, payload FROM incidents WHERE run_id=? ORDER BY ts DESC LIMIT 50", (run_id,))
    snapshot = fetch_latest_snapshot(conn, run_id)
    pnl_history = fetch_pnl_history(conn, run_id)
    contracts_df = fetch_latest_contract_ticks(
        conn,
        run_id,
        expiry_filter=weekly_expiry,
        underlying_filter=index_symbol,
    )

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

st.subheader("Subscribed Contracts (Next Weekly)")
if not contracts_df.empty:
    expiry_label = weekly_expiry or "n/a"
    display_df = contracts_df[["Symbol", "Name", "Expiry", "LTP", "IV", "Delta", "Gamma", "Theta", "Vega"]]
else:
    expiry_label = weekly_expiry or "n/a"
    display_df = contracts_df
st.markdown(f"**Expiry:** {expiry_label}")
st.dataframe(display_df, use_container_width=True)

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
