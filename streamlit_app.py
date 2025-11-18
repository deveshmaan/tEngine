
# streamlit_app.py — live UI for the BUY engine (Python 3.12.11)
import datetime as dt
import os
import time
import re
import sqlite3
import pandas as pd
import streamlit as st
from prometheus_client.parser import text_string_to_metric_families

from brokerage.upstox_client import IST

from brokerage.upstox_client import UpstoxSession as BrokerSession
from market.instrument_cache import InstrumentCache

PROM_URL = os.getenv("PROM_URL", "http://127.0.0.1:9103/metrics")

st.set_page_config(page_title="Intraday BUY Options — Engine UI", layout="wide")
st.title("Intraday BUY Options — Engine UI")

def fetch_metrics(url: str) -> dict:
    r = requests.get(url, timeout=3)
    r.raise_for_status()
    out = {}
    for fam in text_string_to_metric_families(r.text):
        for s in fam.samples:
            # sample: (name, labels, value, timestamp, exemplar)
            name, labels, value, *_ = s
            key = name + ("{" + ",".join([f'{k}="{v}"' for k,v in labels.items()]) + "}" if labels else "")
            out[key] = value
    return out


def fetch_risk_events(limit: int = 10):
    path = os.getenv("ENGINE_DB_PATH", "engine_state.sqlite")
    if not os.path.exists(path):
        return []
    try:
        with sqlite3.connect(path) as conn:
            rows = conn.execute(
                "SELECT ts, instrument, reason, code FROM risk_events ORDER BY id DESC LIMIT ?",
                (limit,),
            ).fetchall()
        return rows
    except Exception:
        return []

if "pnl_history" not in st.session_state:
    st.session_state["pnl_history"] = []

auto_refresh = st.sidebar.checkbox("Auto-refresh", value=True)
interval = st.sidebar.slider("Refresh (sec)", 1, 10, 2)
if st.sidebar.button("Warm Option Cache (Phase 1)"):
    try:
        session = BrokerSession()
        cache = InstrumentCache(os.getenv("ENGINE_DB_PATH", "engine_state.sqlite"))
        expiry, _ = cache.nearest_weekly_and_monthly_expiries("NIFTY", session)
        count = cache.refresh_option_chain("NIFTY", expiry, session)
        cache.close()
        st.sidebar.success(f"Warmed {count} rows for {expiry}")
    except Exception as exc:
        st.sidebar.error(f"Cache warm failed: {exc}")

while True:
    try:
        m = fetch_metrics(PROM_URL)
    except Exception as e:
        st.error(f"Failed to fetch metrics from {PROM_URL}: {e}")
        time.sleep(3)
        continue

    pnl_val = float(m.get("pnl_net_rupees", 0.0))
    st.session_state["pnl_history"].append({"ts": time.time(), "pnl": pnl_val})
    st.session_state["pnl_history"] = st.session_state["pnl_history"][-240:]

    runtime_tab, metrics_tab = st.tabs(["Runtime", "Metrics"])

    def _label_value(key: str, label: str) -> str | None:
        needle = f'{label}="'
        if needle not in key:
            return None
        return key.split(needle)[1].split('"')[0]

    with runtime_tab:
        state = "UNKNOWN"
        for k, v in m.items():
            if k.startswith("engine_state_indicator{") and v >= 1:
                state = _label_value(k, "state") or state
                break
        hb_ts = float(m.get("engine_heartbeat_ts", 0.0))
        hb_age = time.time() - hb_ts if hb_ts else 0.0
        cols = st.columns(3)
        cols[0].metric("Engine State", state)
        cols[1].metric("Session Clock (IST)", dt.datetime.now(IST).strftime("%H:%M:%S"))
        cols[2].metric("Heartbeat Age (s)", f"{hb_age:.1f}")
        cols = st.columns(3)
        cols[0].metric("Open Positions", int(m.get("open_positions", 0)))
        cols[1].metric("Tick latency (ms)", f"{m.get('tick_to_bus_ms_last', 0):.2f}")
        cols[2].metric("Signal→Post (ms)", f"{m.get('signal_to_post_ms_last', 0):.2f}")
        cols = st.columns(2)
        cols[0].metric("Post→Ack (ms)", f"{m.get('post_to_ack_ms_last', 0):.2f}")
        cols[1].metric("Ack→State (ms)", f"{m.get('ack_to_state_ms_last', 0):.2f}")
        st.subheader("Recent Risk Rejects")
        risk_rows = fetch_risk_events()
        if risk_rows:
            risk_df = pd.DataFrame(risk_rows, columns=["ts", "instrument", "reason", "code"])
            st.table(risk_df)
        else:
            st.write("No risk rejects logged yet.")

    with metrics_tab:
        cols = st.columns(4)
        cols[0].metric("Signals", int(sum(v for k, v in m.items() if k.startswith("buy_signals_total"))))
        cols[1].metric("Orders Submitted", int(sum(v for k, v in m.items() if k.startswith("orders_submitted_total"))))
        cols[2].metric("Fills", int(sum(v for k, v in m.items() if k.startswith("orders_filled_total"))))
        cols[3].metric("Rejects", int(sum(v for k, v in m.items() if k.startswith("orders_rejected_total"))))

        cols = st.columns(3)
        cols[0].metric("PnL (₹)", f"{pnl_val:,.0f}")
        cols[1].metric("Avail Margin (₹)", f"{m.get('margin_available_rupees', 0):,.0f}")
        cols[2].metric("Open Positions", int(m.get("open_positions", 0)))

        cols = st.columns(3)
        risk_state = "HALTED" if m.get("risk_halt_state", 0) >= 1 else "ACTIVE"
        cols[0].metric("Risk State", risk_state)
        hb_age = time.time() - float(m.get("engine_heartbeat_ts", time.time()))
        cols[1].metric("Heartbeat Age (s)", f"{hb_age:.1f}")
        cols[2].metric("Last Latency (ms)", f"{m.get('last_execution_latency_ms', 0):,.1f}")

        st.subheader("PnL trend (session)")
        hist_df = pd.DataFrame(st.session_state["pnl_history"])
        if not hist_df.empty:
            hist_df["ts"] = pd.to_datetime(hist_df["ts"], unit="s")
            hist_df = hist_df.set_index("ts")
            st.line_chart(hist_df, width="stretch")

        # Per-instrument table
        rows = []
        for k, v in m.items():
            if not k.startswith("ltp_underlying{"):
                continue
            inst = _label_value(k, "instrument")
            if not inst:
                continue
            symbol = None
            for sym_key in m.keys():
                if sym_key.startswith("instrument_trading_symbol{") and inst in sym_key:
                    symbol = sym_key.split('symbol="')[1].split('"')[0]

            rows.append({
                "instrument": inst,
                "symbol": symbol,
                "ltp": v,
                "iv": m.get(f'option_iv{{instrument="{inst}"}}', float("nan")),
                "iv_z": m.get(f'option_iv_zscore{{instrument="{inst}"}}', float("nan")),
            })
        df = pd.DataFrame(rows).sort_values("instrument") if rows else pd.DataFrame(columns=["instrument","symbol","ltp","iv","iv_z"])
        st.subheader("Per-instrument snapshot")
        st.dataframe(df, width="stretch")

        # Health bar
        health_rows = []
        for k, v in m.items():
            if not k.startswith("instrument_health_state{"):
                continue
            inst_match = re.search(r'instrument="([^"]+)"', k)
            reason_match = re.search(r'reason="([^"]+)"', k)
            health_rows.append({
                "instrument": inst_match.group(1) if inst_match else "unknown",
                "reason": reason_match.group(1) if reason_match else "unknown",
                "value": v,
            })
        health_df = pd.DataFrame(health_rows)
        if not health_df.empty:
        st.subheader("Feed health (reason counts)")
        reason_summary = health_df.groupby("reason")["value"].sum().reset_index()
        st.dataframe(reason_summary, width="stretch")
            bad = health_df[health_df["reason"] != "ok"]
            if not bad.empty:
                st.subheader("Contracts needing attention")
                st.dataframe(bad, width="stretch")

    if not auto_refresh:
        break
    time.sleep(interval)
