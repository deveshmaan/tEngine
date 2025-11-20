from __future__ import annotations

import datetime as dt
import json
import os
import re
import sqlite3
import time
from pathlib import Path
from typing import Any, Dict, List, Optional

import pandas as pd
import requests
import streamlit as st
from prometheus_client.parser import text_string_to_metric_families

from engine import data as engine_data
from engine.config import CONFIG, read_config
from market.instrument_cache import InstrumentCache
from brokerage.upstox_client import UpstoxSession

IST = dt.timezone(dt.timedelta(hours=5, minutes=30), name="Asia/Kolkata")
DEFAULT_DB = Path(os.getenv("ENGINE_DB_PATH", "engine_state.sqlite"))
PROM_URL = os.getenv("PROM_URL", "http://127.0.0.1:9103/metrics")


def _load_app_config() -> Dict[str, Any]:
    try:
        return read_config()
    except Exception:
        return {}


def _data_config() -> Dict[str, Any]:
    return _load_app_config().get("data", {})


def load_risk_limits() -> Dict[str, float]:
    cfg = _load_app_config()
    return (cfg.get("risk") or {})


def _get_cache(db_path: Path) -> InstrumentCache:
    cache = st.session_state.get("_instrument_cache")
    cache_path = st.session_state.get("_instrument_cache_path")
    if cache is None or cache_path != str(db_path):
        if cache is not None:
            try:
                cache.close()
            except Exception:
                pass
        cache = InstrumentCache(str(db_path))
        st.session_state["_instrument_cache"] = cache
        st.session_state["_instrument_cache_path"] = str(db_path)
    return cache


def _data_index_symbol() -> str:
    data_cfg = _data_config()
    return str(data_cfg.get("index_symbol", "NIFTY")).upper()


def load_weekly_weekday() -> int:
    data_cfg = _data_config()
    try:
        return int(data_cfg.get("weekly_expiry_weekday", 1))
    except (TypeError, ValueError):
        return 1


def load_holidays() -> set[dt.date]:
    data_cfg = _data_config()
    holidays: set[dt.date] = set()
    for raw in data_cfg.get("holidays", []):
        try:
            holidays.add(dt.date.fromisoformat(str(raw)))
        except ValueError:
            continue
    return holidays


def _read_underlying_spot(instrument: str) -> Optional[float]:
    """
    Fetch underlying LTP from metrics; returns None if unavailable.
    """

    target = f'instrument="{instrument.upper()}"'
    try:
        resp = requests.get(PROM_URL, timeout=2.5)
        resp.raise_for_status()
        for line in resp.text.splitlines():
            if not line.startswith("ltp_underlying{"):
                continue
            if target not in line:
                continue
            try:
                return float(line.rsplit("}", 1)[1].strip())
            except Exception:
                continue
    except Exception:
        return None
    return None


def resolve_next_weekly_expiry(now: Optional[dt.datetime] = None) -> str:
    """Legacy helper retained for compatibility; UI now relies on live metrics."""

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



def _scrape_text(url: str) -> str:
    resp = requests.get(url, timeout=2)
    resp.raise_for_status()
    return resp.text


def _pull_metrics_text() -> Optional[str]:
    try:
        return _scrape_text(PROM_URL)
    except Exception:
        return None


def fetch_metrics(raw_text: Optional[str] = None) -> Dict[str, float]:
    if raw_text is None:
        raw_text = _pull_metrics_text()
    if not raw_text:
        return {}
    parsed: Dict[str, float] = {}
    for fam in text_string_to_metric_families(raw_text):
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


_OPTION_METRICS = {
    "md_subscription",
    "option_ltp",
    "option_bid",
    "option_ask",
    "option_iv",
    "option_oi",
    "option_last_ts_seconds",
    "option_bid_depth_price",
    "option_bid_depth_qty",
    "option_ask_depth_price",
    "option_ask_depth_qty",
}
_METRIC_LINE_RE = re.compile(r'^(?P<name>[a-zA-Z_:][\w:]*)(?:\{(?P<labels>[^}]*)\})?\s+(?P<value>[-+]?\d+(?:\.\d+)?(?:[eE][-+]?\d+)?)$')
_LABEL_RE = re.compile(r'(?P<key>[a-zA-Z_][\w:]*)="(?P<value>(?:\\.|[^"])*)"')


def _parse_prom_labels(raw: str) -> Dict[str, str]:
    labels: Dict[str, str] = {}
    for match in _LABEL_RE.finditer(raw):
        value = match.group("value")
        value = value.replace('\\"', '"').replace("\\\\", "\\")
        labels[match.group("key")] = value
    return labels


def _norm_labels(lbl: dict) -> dict:
    out = dict(lbl)
    # unify instrument key
    if "instrument" not in out and "instrument_key" in out:
        out["instrument"] = out["instrument_key"]
    # unify type
    if "type" not in out and "opt_type" in out:
        out["type"] = out["opt_type"]
    # symbol may be wrong (contract name). If it looks like 'NIFTY-YYYY-MM-DD-...'
    # try to recover underlying from instrument if missing/odd.
    import re

    sym_re = r"^(NIFTY|BANKNIFTY)-\d{4}-\d{2}-\d{2}-\d+(?:[CP]E)?$"
    if "symbol" not in out or re.match(sym_re, out.get("symbol", "")):
        # Try deriving underlying from instrument; fallback to parsing symbol itself.
        m = re.match(r"^(NIFTY|BANKNIFTY)-\d{4}-\d{2}-\d{2}-\d+(?:-)?[CP]E$", out.get("instrument", ""))
        if m:
            out["symbol"] = m.group(1)
        else:
            m = re.match(sym_re, out.get("symbol", ""))
            if m:
                out["symbol"] = m.group(1)
    return out


def _parse_metric(text: str, metric: str):
    import re

    pattern = re.compile(r'^%s\{([^}]*)\}\s+([0-9.eE+-]+)$' % re.escape(metric))
    lblre = re.compile(r'([a-zA-Z_][a-zA-Z0-9_]*)="([^"]*)"')
    out = []
    for line in text.splitlines():
        m = pattern.match(line.strip())
        if not m:
            continue
        labels, val = m.group(1), m.group(2)
        d = {mm.group(1): mm.group(2) for mm in lblre.finditer(labels)}
        out.append((_norm_labels(d), float(val)))
    return out


def _parse_option_metrics(raw_text: Optional[str]) -> Dict[str, Dict[str, Any]]:
    if not raw_text:
        return {}
    rows: Dict[str, Dict[str, Any]] = {}
    for line in raw_text.splitlines():
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        match = _METRIC_LINE_RE.match(line)
        if not match:
            continue
        name = match.group("name")
        if name not in _OPTION_METRICS:
            continue
        labels = _parse_prom_labels(match.group("labels") or "")
        key = labels.get("instrument_key")
        if not key:
            continue
        key = str(key)
        row = rows.setdefault(
            key,
            {
                "instrument_key": key,
                "symbol": "",
                "expiry": "",
                "opt_type": "",
                "strike": "",
                "subscribed": False,
                "ltp": None,
                "bid": None,
                "ask": None,
                "iv": None,
                "oi": None,
                "last_ts": None,
                "bid_depth": {},
                "ask_depth": {},
            },
        )
        if labels.get("symbol"):
            row["symbol"] = labels["symbol"]
        if labels.get("expiry"):
            row["expiry"] = labels["expiry"]
        if labels.get("opt_type"):
            row["opt_type"] = labels["opt_type"]
        if labels.get("strike"):
            row["strike"] = labels["strike"]
        try:
            value = float(match.group("value"))
        except ValueError:
            continue
        if name == "md_subscription":
            row["subscribed"] = value >= 0.5
        elif name == "option_ltp":
            row["ltp"] = value
        elif name == "option_bid":
            row["bid"] = value
        elif name == "option_ask":
            row["ask"] = value
        elif name == "option_iv":
            row["iv"] = value
        elif name == "option_oi":
            row["oi"] = value
        elif name == "option_bid_depth_price":
            level = int(labels.get("level", 0) or 0)
            if level:
                row["bid_depth"].setdefault(level, {})["price"] = value
        elif name == "option_bid_depth_qty":
            level = int(labels.get("level", 0) or 0)
            if level:
                row["bid_depth"].setdefault(level, {})["qty"] = value
        elif name == "option_ask_depth_price":
            level = int(labels.get("level", 0) or 0)
            if level:
                row["ask_depth"].setdefault(level, {})["price"] = value
        elif name == "option_ask_depth_qty":
            level = int(labels.get("level", 0) or 0)
            if level:
                row["ask_depth"].setdefault(level, {})["qty"] = value
        elif name == "option_last_ts_seconds":
            row["last_ts"] = value
    for row in rows.values():
        bids: list[dict[str, Any]] = []
        asks: list[dict[str, Any]] = []
        for lvl in sorted(row.get("bid_depth", {}).keys()):
            entry = row["bid_depth"].get(lvl) or {}
            bids.append({"price": entry.get("price"), "qty": entry.get("qty")})
        for lvl in sorted(row.get("ask_depth", {}).keys()):
            entry = row["ask_depth"].get(lvl) or {}
            asks.append({"price": entry.get("price"), "qty": entry.get("qty")})
        row["depth"] = {"bids": bids, "asks": asks}
        row.pop("bid_depth", None)
        row.pop("ask_depth", None)
    return rows


def _format_depth_levels(levels: List[Dict[str, Any]], depth_cap: int = 5) -> List[str]:
    formatted: List[str] = []
    limit = max(1, int(depth_cap or 1))
    for entry in (levels or [])[:limit]:
        price = entry.get("price")
        qty = entry.get("qty")
        if price is None and qty is None:
            continue
        px_text = f"{float(price):.2f}" if price is not None else ""
        if qty is None:
            formatted.append(px_text)
        else:
            formatted.append(f"{px_text}@{int(qty)}")
    return formatted


def _strike_sort_key(value: Optional[str]) -> float:
    if value is None or value == "":
        return float("inf")
    try:
        return float(value)
    except (TypeError, ValueError):
        return float("inf")


def _format_strike(value: Optional[str]) -> str:
    if value is None or value == "":
        return ""
    try:
        strike = float(value)
    except (TypeError, ValueError):
        return str(value)
    if strike.is_integer():
        return f"{int(strike)}"
    return f"{strike:.2f}".rstrip("0").rstrip(".")


def _build_option_table(rows: list[Dict[str, Any]]) -> pd.DataFrame:
    now = time.time()
    sorted_rows = sorted(rows, key=lambda row: (_strike_sort_key(row.get("strike")), row["instrument_key"]))
    records: list[Dict[str, Any]] = []
    for idx, row in enumerate(sorted_rows, start=1):
        bid = row.get("bid")
        ask = row.get("ask")
        spread = None
        if bid is not None and ask is not None:
            spread = max(ask - bid, 0.0)
        last_ts = row.get("last_ts")
        age = max(0.0, now - last_ts) if last_ts is not None else None
        records.append(
            {
                "Idx": idx,
                "Instrument": row["instrument_key"],
                "Symbol": row.get("symbol") or "",
                "Expiry": row.get("expiry") or "",
                "Strike": _format_strike(row.get("strike")),
                "Type": (row.get("opt_type") or "").upper(),
                "LTP": row.get("ltp"),
                "Bid": bid,
                "Ask": ask,
                "Spread": spread,
                "IV": row.get("iv"),
                "OI": row.get("oi"),
                "Age(s)": age,
            }
        )
    return pd.DataFrame(records)


def render_table(rows: List[Dict[str, Any]], title: str) -> None:
    st.caption(title)
    if not rows:
        st.warning("No contracts to display.")
        return
    if rows and "Instrument" in rows[0]:
        df = pd.DataFrame(rows)
        preferred = ["Instrument", "Symbol", "Expiry", "Strike", "Type", "LTP", "Bid", "Ask", "Spread", "IV", "OI", "Age(s)"]
        order = [col for col in preferred if col in df.columns]
        order += [col for col in df.columns if col not in order]
        st.dataframe(df[order], use_container_width=True)
        return
    st.dataframe(_build_option_table(rows), use_container_width=True)


def _coerce_float(value: object) -> Optional[float]:
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def insert_control_intent(conn: sqlite3.Connection, run_id: str, action: str, payload: Optional[Dict[str, Any]] = None) -> None:
    ts = dt.datetime.utcnow().isoformat()
    conn.execute(
        "INSERT INTO control_intents(run_id, ts, action, payload_json) VALUES (?, ?, ?, ?)",
        (run_id, ts, action, json.dumps(payload) if payload else None),
    )
    conn.commit()


def _active_metric_rows(rows: Dict[str, Dict[str, Any]], symbol: str, expiry: str) -> List[Dict[str, Any]]:
    filtered: List[Dict[str, Any]] = []
    for record in rows.values():
        sym = (record.get("symbol") or "").upper()
        exp = record.get("expiry") or ""
        if sym != symbol or exp != expiry:
            continue
        if int(record.get("subscribed", 0)) != 1:
            continue
        filtered.append(record)
    return filtered


def _chunked(seq: List[str], size: int) -> List[List[str]]:
    span = max(size, 1)
    return [seq[i : i + span] for i in range(0, len(seq), span)]


def fetch_rest_quotes(keys: List[str], batch_size: int = 15) -> Dict[str, Dict[str, Any]]:
    if not keys:
        return {}
    try:
        session = UpstoxSession()
    except Exception as exc:
        return {k: {"instrument_key": k, "error": f"Upstox session unavailable: {exc}"} for k in keys}
    quotes: Dict[str, Dict[str, Any]] = {}
    for batch in _chunked(keys, batch_size):
        try:
            resp = session.get_ltp(batch)
        except Exception as exc:
            msg = str(exc)
            for key in batch:
                quotes[key] = {"instrument_key": key, "error": msg}
            continue
        data = resp
        if isinstance(resp, dict):
            data = resp.get("data") or resp.get("ltp") or resp
        if isinstance(data, dict):
            entries = list(data.values())
        else:
            entries = data or []
        for entry in entries:
            if not isinstance(entry, dict):
                continue
            key = str(entry.get("instrument_key") or entry.get("instrument") or "")
            if not key:
                continue
            depth = entry.get("depth") or {}
            quotes[key] = {
                "instrument_key": key,
                "ltp": entry.get("ltp") or entry.get("last_price") or entry.get("close"),
                "bid": entry.get("bid") or entry.get("best_bid") or depth.get("buy_price") or depth.get("buy_price_1"),
                "ask": entry.get("ask") or entry.get("best_ask") or depth.get("sell_price") or depth.get("sell_price_1"),
                "iv": entry.get("iv"),
                "oi": entry.get("oi") or entry.get("open_interest"),
                "last_ts": entry.get("timestamp") or entry.get("ts"),
            }
    return quotes


def get_live_rows(symbol: str, engine_expiry: str):
    try:
        text = _scrape_text(PROM_URL)
    except Exception:
        return [], None, "metrics_scrape_failed"

    parsed = _parse_option_metrics(text)
    if not parsed:
        return [], None, "no_metrics"
    filtered = [row for row in parsed.values() if (row.get("symbol") or "").upper() == symbol.upper() and row.get("expiry")]
    if not filtered:
        return [], None, "no_metrics_for_symbol"
    expiries = sorted({row["expiry"] for row in filtered if row.get("expiry")})
    metrics_expiry = expiries[0] if expiries else None
    rows = []
    now = time.time()
    for row in filtered:
        if metrics_expiry and row.get("expiry") != metrics_expiry:
            continue
        bid_val = row.get("bid")
        ask_val = row.get("ask")
        spread = (ask_val - bid_val) if (ask_val is not None and bid_val is not None) else None
        last_ts = row.get("last_ts")
        depth = row.get("depth") or {}
        rows.append(
            {
                "Instrument": row["instrument_key"],
                "Symbol": row.get("symbol") or "",
                "Expiry": row.get("expiry") or "",
                "Strike": int(float(row["strike"])) if row.get("strike") else None,
                "Type": row.get("opt_type"),
                "LTP": row.get("ltp"),
                "Bid": bid_val,
                "Ask": ask_val,
                "Spread": spread,
                "IV": row.get("iv"),
                "OI": row.get("oi"),
                "Age(s)": (now - last_ts) if last_ts is not None else None,
                "Depth": depth,
            }
        )
    rows.sort(key=lambda r: (r["Type"] or "", r["Strike"] or 0))
    return rows, metrics_expiry, "metrics_ok"


st.set_page_config(page_title="Trading Engine Console", layout="wide")
st.title("Trading Engine Console")

db_path = Path(st.sidebar.text_input("State DB", value=str(DEFAULT_DB)))
run_id = st.sidebar.text_input("Run ID", value="dev-run")
auto = st.sidebar.checkbox("Auto refresh", value=True)
interval = st.sidebar.slider("Refresh seconds", min_value=1, max_value=10, value=2)
fallback_refresh = False
if auto:
    st.sidebar.caption("Streaming updates enabled")
    auto_fn = getattr(st, "autorefresh", None)
    if callable(auto_fn):
        auto_fn(interval=int(interval * 1000), key="refresh")
    else:
        fallback_refresh = True
        st.sidebar.caption("Streamlit build lacks autorefresh; using timer fallback.")

cache = _get_cache(db_path)
cfg_data = getattr(CONFIG, "data", None)
default_symbol = cfg_data.index_symbol if cfg_data else _data_index_symbol()
underlying = os.getenv("UNDERLYING", default_symbol).upper()
st.sidebar.markdown(f"**Underlying:** `{underlying}`")
try:
    expiries = engine_data.resolve_expiries_with_fallback(underlying)
except Exception as exc:
    st.error(f"Failed to resolve expiries: {exc}")
    st.stop()
if not expiries:
    st.error("No expiries available via engine resolver.")
    st.stop()
active_expiry = expiries[0]
st.sidebar.markdown(f"**Detected expiry (engine):** `{active_expiry}`")
window = st.sidebar.slider("ATM window (steps)", 1, 5, 2, 1)
opt_type_choice = st.sidebar.selectbox("Option type", ["CE", "PE"], index=0)
spot_fallback_text = st.sidebar.text_input("Spot fallback", value="23000")
spot_metric = _read_underlying_spot(underlying)
spot_guess = spot_metric if spot_metric is not None else (_coerce_float(spot_fallback_text) or 0.0)
try:
    ladder = cache.nearest_strikes(underlying, active_expiry, spot_guess, window=window, opt_type=opt_type_choice)
except Exception as exc:
    st.warning(f"Cache ladder failed: {exc}")
    try:
        ladder = cache.list_contracts_for_expiry(underlying, active_expiry, opt_type=opt_type_choice)
    except Exception:
        ladder = []
use_rest_fallback = st.sidebar.checkbox("Enable REST fallback when metrics missing", value=False)
if spot_metric is not None:
    st.caption(f"Detected spot (metrics): {spot_metric:,.2f}")
else:
    st.caption("Detected spot (metrics): n/a (using fallback)")

metrics_text = _pull_metrics_text()
metrics_data = fetch_metrics(metrics_text)
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

st.subheader("Subscribed Contracts (Live)")
st.caption(f"Data source: {PROM_URL}")
rows, metrics_expiry, mode = get_live_rows(underlying, active_expiry)
depth_cap = getattr(CONFIG.market_data, "depth_levels", 5)

if mode == "metrics_ok":
    st.success("Live metrics OK — showing subscribed contracts.")
    st.caption("Badge: metrics")
    max_age = max((row.get("Age(s)") or 0.0) for row in rows) if rows else 0.0
    st.caption(f"Detected expiry (metrics): `{metrics_expiry or 'n/a'}` • Detected expiry (engine): `{active_expiry}` • Tick age ≤ {max_age:.1f}s")
    render_table([{k: v for k, v in row.items() if k != "Depth"} for row in rows], "Live (metrics)")
    depth_rows = [row for row in rows if row.get("Depth")]
    if depth_rows:
        with st.expander("Depth (Top5)", expanded=False):
            depth_table: List[Dict[str, Any]] = []
            for row in depth_rows:
                depth = row.get("Depth") or {}
                bids = _format_depth_levels(depth.get("bids", []), depth_cap=depth_cap if depth_cap else 5)
                asks = _format_depth_levels(depth.get("asks", []), depth_cap=depth_cap if depth_cap else 5)
                depth_table.append(
                    {
                        "Instrument": row["Instrument"],
                        "Strike": row["Strike"],
                        "Type": row["Type"],
                        "Bid Depth": " | ".join(bids) if bids else "",
                        "Ask Depth": " | ".join(asks) if asks else "",
                    }
                )
            st.dataframe(pd.DataFrame(depth_table), use_container_width=True)
else:
    if mode in ("no_metrics", "metrics_scrape_failed", "no_metrics_for_symbol"):
        st.info("Metrics unavailable; showing cache ladder / REST fallback.")
    else:
        st.warning(f"Unexpected mode: {mode}; falling back to cache.")
    st.caption("Badge: REST fallback")
    fallback_rows = ladder
    if not fallback_rows:
        st.warning("No contracts available from the instrument cache for the detected expiry.")
    else:
        if use_rest_fallback:
            quote_map = fetch_rest_quotes([row["instrument_key"] for row in fallback_rows])
            errors = [info["error"] for info in quote_map.values() if info.get("error")]
            enriched: List[Dict[str, Any]] = []
            for row in fallback_rows:
                merged = dict(row)
                quote = quote_map.get(row["instrument_key"])
                if quote:
                    for field in ("ltp", "bid", "ask", "iv", "oi", "last_ts"):
                        if quote.get(field) is not None:
                            merged[field] = quote[field]
                enriched.append(merged)
            fallback_rows = enriched
            if errors:
                st.warning(f"REST fallback errors: {set(errors)}")
        render_table(fallback_rows, "Cache / REST fallback")

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

if auto and fallback_refresh:
    rerun_fn = getattr(st, "experimental_rerun", None)
    time.sleep(interval)
    if callable(rerun_fn):
        rerun_fn()
