# Cell

import os, time, json, threading, math, statistics, queue, uuid, gzip, io, sys
from collections import deque, defaultdict
from dataclasses import dataclass
import numpy as np
import pandas as pd

# Upstox SDK
import upstox_client
from upstox_client.rest import ApiException

# Notebook display helpers
pd.set_option("display.max_rows", 200)
pd.set_option("display.width", 120)
print("SDK version:", getattr(upstox_client, "__version__", "unknown"))

# Cell

# ---- Core toggles ----
UNDERLYING = os.getenv("UNDERLYING", "NIFTY").upper()     # "NIFTY" or "BANKNIFTY"
DRY_RUN     = os.getenv("DRY_RUN", "false").lower() in ("1","true","yes")
APP_TAG     = os.getenv("UPSTOX_APP_NAME", "BUY-ENGINE-NB")
USE_SANDBOX = os.getenv("UPSTOX_SANDBOX", "false").lower() in ("1","true","yes")

# ---- Risk & execution ----
MAX_CONCURRENT_POS   = int(os.getenv("MAX_CONCURRENT_POS", "2"))
CAPITAL_PER_TRADE    = float(os.getenv("CAPITAL_PER_TRADE", "10000"))  # in ₹
EXIT_TARGET_PCT      = float(os.getenv("EXIT_TARGET_PCT_BUY", "0.08")) # 8% target
EXIT_STOP_PCT        = float(os.getenv("EXIT_STOP_PCT_BUY", "0.04"))   # 4% stop
INSTRUMENTS_TO_SUB   = int(os.getenv("INSTRUMENTS_TO_SUB", "24"))      # subscribe top 24 strikes around ATM

# ---- BUY gating (buyers pay spread & decay) ----
DELTA_MIN_BUY        = float(os.getenv("DELTA_MIN_BUY", "0.20"))
DELTA_MAX_BUY        = float(os.getenv("DELTA_MAX_BUY", "0.55"))
SPREAD_MAX_BUY       = float(os.getenv("SPREAD_MAX_BUY", "3.0"))       # ₹ absolute max spread
IMBALANCE_ABS_MIN    = float(os.getenv("IMBALANCE_ABS_MIN", "0.15"))   # |(bidQ-askQ)/(bidQ+askQ)|
IV_Z_MAX             = float(os.getenv("IV_Z_MAX", "3.0"))
IV_Z_FLOOR           = float(os.getenv("IV_Z_FLOOR", "-0.20"))
IV_Z_LOOKBACK        = int(os.getenv("IV_Z_LOOKBACK", "120"))          # ~minutes at 1-tick/second

# ---- Time & session ----
SQUARE_OFF_HHMM      = os.getenv("SQUARE_OFF_HHMM", "15:20")           # force exit before close
ENTRY_CUTOFF_HHMM    = os.getenv("ENTRY_CUTOFF_HHMM", "15:00")         # no new entries after this

print("Config:", dict(UNDERLYING=UNDERLYING, DRY_RUN=DRY_RUN, TARGET=EXIT_TARGET_PCT, STOP=EXIT_STOP_PCT))

# Cell

def make_configuration():
    cfg = upstox_client.Configuration(sandbox=USE_SANDBOX)
    cfg.access_token = os.environ.get("UPSTOX_ACCESS_TOKEN", "eyJ0eXAiOiJKV1QiLCJrZXlfaWQiOiJza192MS4wIiwiYWxnIjoiSFMyNTYifQ.eyJzdWIiOiIzR0NMM1kiLCJqdGkiOiI2OTI3ZDJhMzhjZDgwMjRlMjQ1NWVlYjgiLCJpc011bHRpQ2xpZW50IjpmYWxzZSwiaXNQbHVzUGxhbiI6dHJ1ZSwiaWF0IjoxNzY0MjE3NTA3LCJpc3MiOiJ1ZGFwaS1nYXRld2F5LXNlcnZpY2UiLCJleHAiOjE3NjQyODA4MDB9.I75IlguJ2rPEu9razXGvEIn2WfPCuWJjYDDyfPmivKQ").strip()
    if not cfg.access_token:
        raise RuntimeError("UPSTOX_ACCESS_TOKEN is not set.")
    return cfg

def get_order_api(cfg=None):
    cfg = cfg or make_configuration()
    return upstox_client.OrderApiV3(upstox_client.ApiClient(cfg))

def get_quote_api(cfg=None):
    cfg = cfg or make_configuration()
    return upstox_client.MarketQuoteApi(upstox_client.ApiClient(cfg))

def make_market_streamer(cfg=None):
    cfg = cfg or make_configuration()
    return upstox_client.MarketDataStreamerV3(upstox_client.ApiClient(cfg))

def make_portfolio_streamer(cfg=None):
    cfg = cfg or make_configuration()
    return upstox_client.PortfolioDataStreamer(upstox_client.ApiClient(cfg))

_apis = {"cfg": make_configuration()}
_apis["order"]  = get_order_api(_apis["cfg"])
_apis["quote"]  = get_quote_api(_apis["cfg"])
print("REST clients ready.")

# Cell

import requests

INSTR_URL = "https://assets.upstox.com/market-quote/instruments/exchange/NSE.json.gz"

def load_instruments_df():
    resp = requests.get(INSTR_URL, timeout=30)
    resp.raise_for_status()
    buf = io.BytesIO(resp.content)
    with gzip.GzipFile(fileobj=buf) as gz:
        raw = gz.read().decode("utf-8")
    df = pd.read_json(io.StringIO(raw))
    return df

def filter_index_options(df: pd.DataFrame, underlying: str):
    u = underlying.upper()
    # Upstox uses name field like 'NIFTY'/'BANKNIFTY' for index options in NSE_FO
    f = (df["segment"].eq("NSE_FO") & df["instrument_type"].isin(["CE","PE"]) & df["name"].str.upper().eq(u))
    out = df.loc[f, ["instrument_key","trading_symbol","name","expiry","strike_price","option_type","lot_size","tick_size"]].copy()
    out.rename(columns={"strike_price":"strike","option_type":"option_type"}, inplace=True)
    out["expiry"] = pd.to_datetime(out["expiry"]).dt.tz_localize(None)
    out.sort_values(["expiry","strike","option_type"], inplace=True)
    return out.reset_index(drop=True)

def next_expiry(chain: pd.DataFrame):
    future = chain.loc[chain["expiry"]>=pd.Timestamp.today().normalize(), "expiry"].sort_values().unique()
    return future[0] if len(future) else chain["expiry"].max()

def chain_for_underlying(underlying: str):
    df = load_instruments_df()
    ch = filter_index_options(df, underlying)
    exp = next_expiry(ch)
    ch = ch[ch["expiry"].eq(exp)].reset_index(drop=True)
    return ch, exp

chain_df, current_expiry = chain_for_underlying(UNDERLYING)
print(UNDERLYING, "expiry:", current_expiry.date(), "contracts:", len(chain_df))
display(chain_df.head())

# Cell

IDX_KEY = "NSE_INDEX|Nifty 50" if UNDERLYING=="NIFTY" else "NSE_INDEX|Nifty Bank"

def get_index_ltp(idx_key: str)->float:
    q = _apis["quote"].get_ltp(instrument_key=[idx_key])
    data = q.data or {}
    row = data.get(idx_key) or {}
    return float(row.get("last_price") or row.get("ltp") or 0.0)

spot = get_index_ltp(IDX_KEY)
if spot <= 0:
    raise RuntimeError(f"Failed to fetch spot for {IDX_KEY}")
print(f"{UNDERLYING} spot ~ {spot:.2f}")

# strike step
STRIKE_STEP = 50 if UNDERLYING=="NIFTY" else 100

def nearest_strike(x, step): 
    return int(round(x/step)*step)

atm = nearest_strike(spot, STRIKE_STEP)
print("ATM:", atm)

# Cell

# Select a balanced window of strikes around ATM and cap by INSTRUMENTS_TO_SUB
window = max(4, INSTRUMENTS_TO_SUB//2//2)  # CE+PE → times two
sel = chain_df[(chain_df["strike"].between(atm - window*STRIKE_STEP, atm + window*STRIKE_STEP))]
# keep both CE/PE
sel = sel.sort_values(["strike","option_type"]).reset_index(drop=True)

# If too many, trim equally from edges
if len(sel) > INSTRUMENTS_TO_SUB:
    center = sel.index[abs(sel["strike"]-atm).sort_values().index]
    sel = sel.loc[sorted(center[:INSTRUMENTS_TO_SUB])]

subscribe_keys = sel["instrument_key"].tolist()
print("Subscribing instruments:", len(subscribe_keys))
display(sel.head(8))

# Cell

# Shared state updated by feed callbacks
feed_lock = threading.Lock()
ticks_store = {}           # instrument_key -> last parsed payload (dict)
iv_history = defaultdict(lambda: deque(maxlen=IV_Z_LOOKBACK))  # token -> iv series
df_feed = pd.DataFrame()   # rolling snapshot (for display/logic)

def _update_from_tick(key: str, payload: dict):
    global df_feed
    ltpc = payload.get("ltpc") or payload.get("ltp") or {}
    ltp = float(ltpc.get("ltp") or 0.0)
    cp  = float(ltpc.get("cp") or 0.0)
    first = payload.get("firstLevelWithGreeks") or payload.get("firstLevel") or {}
    depth = first.get("firstDepth") or {}
    bidp, askp = float(depth.get("bidP") or np.nan), float(depth.get("askP") or np.nan)
    bidq, askq = float(depth.get("bidQ") or 0.0), float(depth.get("askQ") or 0.0)
    greeks = first.get("optionGreeks") or {}
    delta = greeks.get("delta")
    iv    = first.get("iv") if "iv" in first else greeks.get("iv")

    mid  = np.nanmean([bidp, askp]) if np.isfinite(bidp) and np.isfinite(askp) else (ltp if ltp>0 else np.nan)
    spr  = (askp - bidp) if (np.isfinite(askp) and np.isfinite(bidp)) else np.nan
    imb  = ((bidq-askq)/(bidq+askq)) if (bidq+askq)>0 else 0.0

    if isinstance(iv, (int, float)):
        iv_history[key].append(float(iv))

    rec = dict(Token=key, LTP=ltp, Mid=mid, Spread=spr, BidP1=bidp, AskP1=askp, BidQ1=bidq, AskQ1=askq,
               Imb=imb, Delta=float(delta) if delta is not None else np.nan, IV=float(iv) if isinstance(iv,(int,float)) else np.nan)
    ticks_store[key] = rec

    # rebuild small dataframe (avoid excessive reindex)
    df_feed = pd.DataFrame.from_records(list(ticks_store.values()))
    # enrich with chain fields (lot_size, strike, option_type)
    if not df_feed.empty:
        df_feed = df_feed.merge(chain_df[["instrument_key","strike","option_type","lot_size","tick_size"]],
                                left_on="Token", right_on="instrument_key", how="left").drop(columns=["instrument_key"])

def start_market_stream(keys, mode="full"):
    streamer = make_market_streamer(_apis["cfg"])

    def on_open():
        print("[MD] opened")
        # subscribe
        payload = {"guid": str(uuid.uuid4()).replace("-",""), "method":"sub", "data":{"mode":mode, "instrumentKeys": keys}}
        streamer.send(json.dumps(payload).encode("utf-8"))

    def on_message(msg):
        # SDK may pass dict or object with to_dict()
        if hasattr(msg, "to_dict"):
            msg = msg.to_dict()
        if isinstance(msg, (bytes, bytearray)):
            try:
                msg = json.loads(msg)
            except Exception:
                return
        if not isinstance(msg, dict):
            return
        t = msg.get("type")
        if t == "live_feed":
            feeds = msg.get("feeds") or {}
            with feed_lock:
                for k, v in feeds.items():
                    # v may hold 'ltpc' or 'firstLevelWithGreeks' etc.
                    payload = v.get("firstLevelWithGreeks") or v.get("fullFeed") or v.get("ltpc") or v
                    _update_from_tick(k, payload)
        elif t == "market_info":
            print("[MD] market status:", msg.get("marketInfo",{}).get("segmentStatus",{}))

    def on_close():
        print("[MD] closed")

    streamer.on_open    = on_open
    streamer.on_message = on_message
    streamer.on_close   = on_close
    # non-blocking start
    threading.Thread(target=streamer.connect, kwargs={"auto_reconnect": True}, daemon=True).start()
    return streamer

market_stream = start_market_stream(subscribe_keys, mode="full")  # or "full_d30" if Plus
time.sleep(2.0)
print("Market stream started.")

# Cell

def iv_z_for(token: str):
    series = iv_history.get(token)
    if not series or len(series) < max(30, IV_Z_LOOKBACK//4):
        return np.nan
    vals = np.array(series, dtype=float)
    mu, sd = float(np.nanmean(vals)), float(np.nanstd(vals))
    if sd <= 0: 
        return 0.0
    return float((vals[-1]-mu)/sd)

# Cell

def hhmm_now():
    return dt.datetime.now().strftime("%H:%M")

def within_entry_window():
    return hhmm_now() < ENTRY_CUTOFF_HHMM

def lot_round(qty, lot):
    if lot <= 0:
        return int(qty)
    return int(math.ceil(qty/lot)*lot)

def plan_buy_entries(snapshot: pd.DataFrame):
    if snapshot is None or snapshot.empty:
        return []

    if not within_entry_window():
        return []

    # candidates near ATM first
    snapshot = snapshot.copy()
    snapshot["strike_gap"] = (snapshot["strike"] - atm).abs()
    snapshot.sort_values(["strike_gap","Spread"], inplace=True)

    plans = []
    for _, r in snapshot.iterrows():
        token = r["Token"]
        if not isinstance(token, str): 
            continue
        delta = r.get("Delta")
        if not (np.isfinite(delta) and DELTA_MIN_BUY <= delta <= DELTA_MAX_BUY):
            continue
        spread_ok = (np.isfinite(r["Spread"]) and r["Spread"] <= SPREAD_MAX_BUY)
        if not spread_ok:
            continue
        imb_ok = (abs(float(r.get("Imb") or 0.0)) >= IMBALANCE_ABS_MIN)
        if not imb_ok:
            continue
        z = iv_z_for(token)
        if not (np.isfinite(z) and IV_Z_FLOOR <= z <= IV_Z_MAX):
            continue
        mid = float(r.get("Mid") or r.get("LTP") or 0.0)
        if not np.isfinite(mid) or mid <= 0:
            continue
        lot = int(r.get("lot_size") or 1)
        qty = lot_round(max(1, int(CAPITAL_PER_TRADE//max(1.0, mid))), lot)
        if qty <= 0:
            continue

        plans.append({
            "instrument_key": token,
            "side": "BUY",
            "order_type": "MARKET",     # buyers avoid missing fill; change to LIMIT if you prefer
            "validity": "DAY",
            "product": "I",
            "qty": int(qty),
            "mid": mid,
            "delta": float(delta),
            "spread": float(r["Spread"]),
            "imb": float(r["Imb"]),
            "iv_z": float(z),
            "strike": int(r["strike"]),
            "option_type": str(r["option_type"])
        })
        if len(plans) >= (MAX_CONCURRENT_POS):
            break
    return plans

# Small helper to view top candidates
def live_snapshot():
    with feed_lock:
        snap = df_feed.copy()
    if snap.empty:
        return snap
    snap["iv_z"] = [iv_z_for(t) for t in snap["Token"]]
    return snap.sort_values(["strike","option_type"])

# Cell

open_positions = {}   # token -> dict(qty, avg_price, side)
order_ids_map  = {}   # token -> last order_id

def place_market_buy(order_api, plan):
    body = upstox_client.PlaceOrderV3Request(
        quantity=plan["qty"],
        product="I",
        validity="DAY",
        price=0.0,
        tag=APP_TAG,
        instrument_token=plan["instrument_key"],
        order_type="MARKET",
        transaction_type="BUY",
        disclosed_quantity=0,
        trigger_price=0.0,
        is_amo=False,
        slice=True
    )
    if DRY_RUN:
        print("[DRY] BUY", plan["instrument_key"], plan["qty"])
        oid = "SIM-" + str(uuid.uuid4())[:8]
        order_ids_map[plan["instrument_key"]] = oid
        # simulate position
        open_positions.setdefault(plan["instrument_key"], {"qty":0,"avg_price":plan["mid"],"side":"BUY"})
        p = open_positions[plan["instrument_key"]]
        # naive avg
        p["avg_price"] = (p["avg_price"]*p["qty"] + plan["mid"]*plan["qty"]) / max(1, (p["qty"]+plan["qty"]))
        p["qty"] += plan["qty"]
        p["side"] = "BUY"
        return {"status":"success","order_id":oid}
    else:
        try:
            resp = order_api.place_order(body, algo_id=APP_TAG)
            if hasattr(resp, "to_dict"):
                resp = resp.to_dict()
            oid = (resp.get("data") or {}).get("order_id") or resp.get("order_id")
            print("LIVE BUY placed:", oid, plan["instrument_key"], "qty", plan["qty"])
            order_ids_map[plan["instrument_key"]] = oid
            return resp
        except ApiException as e:
            print("OrderApiV3.place_order error:", e)
            return {"status":"error","error":str(e)}

# Portfolio streamer
portfolio_updates = queue.Queue()

def start_portfolio_stream():
    pstream = make_portfolio_streamer(_apis["cfg"])

    def on_open():
        print("[PF] opened")

    def on_message(msg):
        if hasattr(msg, "to_dict"):
            msg = msg.to_dict()
        if isinstance(msg, (bytes, bytearray)):
            try:
                msg = json.loads(msg)
            except Exception:
                return
        portfolio_updates.put(msg)

    def on_close():
        print("[PF] closed")

    pstream.on_open = on_open
    pstream.on_message = on_message
    pstream.on_close = on_close
    threading.Thread(target=pstream.connect, kwargs={"auto_reconnect": True}, daemon=True).start()
    return pstream

portfolio_stream = start_portfolio_stream()

# Cell

def current_price(token: str):
    with feed_lock:
        snap = df_feed
        if snap is None or snap.empty:
            return np.nan
        row = snap.loc[snap["Token"]==token]
        if row.empty:
            return np.nan
        r = row.iloc[0]
        return float(r.get("Mid") or r.get("LTP") or 0.0)

def try_exit(order_api, token: str, side: str, qty: int, reason: str):
    if qty <= 0: 
        return None
    opp_side = "SELL" if side=="BUY" else "BUY"
    if DRY_RUN:
        print(f"[DRY EXIT] {token} qty={qty} reason={reason}")
        p = open_positions.get(token, {})
        if p:
            p["qty"] = max(0, int(p["qty"]-qty))
        return {"status":"success","reason":reason}
    else:
        body = upstox_client.PlaceOrderV3Request(
            quantity=int(qty),
            product="I",
            validity="DAY",
            price=0.0,
            tag=f"{APP_TAG}-EXIT-{reason}",
            instrument_token=token,
            order_type="MARKET",
            transaction_type=opp_side,
            disclosed_quantity=0,
            trigger_price=0.0,
            is_amo=False,
            slice=True
        )
        try:
            resp = order_api.place_order(body, algo_id=APP_TAG)
            if hasattr(resp, "to_dict"):
                resp = resp.to_dict()
            print(f"[LIVE EXIT] {token} qty={qty} reason={reason}")
            return resp
        except ApiException as e:
            print("OrderApiV3.place_order EXIT error:", e)
            return {"status":"error","error":str(e)}

def exit_worker():
    api = _apis["order"]
    while True:
        time.sleep(1.0)
        now = hhmm_now()
        # square-off window
        if now >= SQUARE_OFF_HHMM:
            for tok, pos in list(open_positions.items()):
                if pos.get("qty", 0) > 0 and pos.get("side") == "BUY":
                    try_exit(api, tok, "BUY", pos["qty"], reason="SQUARE-OFF")
            break

        # target/stop
        for tok, pos in list(open_positions.items()):
            qty = int(pos.get("qty") or 0)
            if qty <= 0:
                continue
            px = current_price(tok)
            if not np.isfinite(px) or px <= 0:
                continue
            entry = float(pos.get("avg_price") or 0.0)
            if entry <= 0:
                continue
            if px >= entry * (1.0 + EXIT_TARGET_PCT):
                try_exit(api, tok, "BUY", qty, reason="TARGET")
                open_positions[tok]["qty"] = 0
            elif px <= entry * (1.0 - EXIT_STOP_PCT):
                try_exit(api, tok, "BUY", qty, reason="STOP")
                open_positions[tok]["qty"] = 0

# kick exit manager
threading.Thread(target=exit_worker, daemon=True).start()
print("Exit manager started.")

# Cell

def trade_loop(duration_s=120):
    api = _apis["order"]
    t0 = time.time()
    while time.time() - t0 < duration_s:
        time.sleep(2.0)
        snap = live_snapshot()
        if snap.empty:
            continue
        # skip if positions already at limit
        active = sum(1 for p in open_positions.values() if p.get("qty",0)>0 and p.get("side")=="BUY")
        if active >= MAX_CONCURRENT_POS:
            continue
        plans = plan_buy_entries(snap)
        for plan in plans:
            # check again for count
            active = sum(1 for p in open_positions.values() if p.get("qty",0)>0 and p.get("side")=="BUY")
            if active >= MAX_CONCURRENT_POS:
                break
            resp = place_market_buy(api, plan)
            # open_positions updated on DRY; for LIVE it will be updated by portfolio updates below

trade_loop(30)  # run short demo loop; increase for full session
print("Entry scan loop finished (demo window).")

# Cell

def handle_pf_update(msg: dict):
    # Example messages vary; we look for 'type' and 'data' payloads
    try:
        typ = (msg.get("type") or "").lower()
        data = msg.get("data") or {}
        if typ == "order":
            st = (data.get("status") or "").upper()
            tok = data.get("instrument_token")
            if st in ("TRADED","COMPLETED","FILLED") and tok:
                qty = int(data.get("filled_quantity") or data.get("quantity") or 0)
                avg = float(data.get("average_price") or data.get("price") or 0.0)
                side = (data.get("transaction_type") or "").upper()
                if side == "BUY":
                    p = open_positions.setdefault(tok, {"qty":0,"avg_price":avg,"side":"BUY"})
                    p["avg_price"] = (p["avg_price"]*p["qty"] + avg*qty) / max(1, (p["qty"]+qty))
                    p["qty"] += qty
                    p["side"] = "BUY"
                elif side == "SELL":
                    p = open_positions.setdefault(tok, {"qty":0,"avg_price":avg,"side":"BUY"})
                    p["qty"] = max(0, p["qty"] - qty)
        elif typ == "position":
            # Could reconcile net positions here if desired
            pass
    except Exception as e:
        print("PF handler error:", e)

def pf_listener(duration_s=10):
    t0 = time.time()
    while time.time() - t0 < duration_s:
        try:
            msg = portfolio_updates.get(timeout=1.0)
            handle_pf_update(msg)
        except queue.Empty:
            pass

pf_listener(5)  # drain a few seconds
print("Portfolio listener drained (demo).")

# Cell

print("Open positions:")
print(open_positions)
print("Sample live snapshot (top 12 rows):")
display(live_snapshot().head(12))

# Cell

# ---- Risk controls (daily caps) ----
DAILY_LOSS_LIMIT_R = float(os.getenv("DAILY_LOSS_LIMIT_R", "2500"))   # Max daily loss in ₹; engine hard-stops below -limit
DAILY_PROFIT_CAP_R = float(os.getenv("DAILY_PROFIT_CAP_R", "5000"))   # Optional profit cap to stop trading when reached

# ---- Persistence ----
DB_PATH = os.getenv("UPSTOX_ENGINE_DB", "/mnt/data/upstox_engine.sqlite")

# ---- Backtest / Replay ----
REPLAY_CSV = os.getenv("REPLAY_CSV", "").strip()         # path to CSV of ticks; if set, use replay harness
REPLAY_SPEED = float(os.getenv("REPLAY_SPEED", "1.0"))   # 1.0 = realtime, 2.0 = 2x faster, etc.

# ---- Execution styles ----
PEG_OFFSET = float(os.getenv("PEG_OFFSET", "0.05"))      # ₹ offset for pegged-limit from best bid for BUY
PEG_REPRICE_MS = int(os.getenv("PEG_REPRICE_MS", "250")) # milliseconds between reprice attempts
PEG_MAX_REPRICES = int(os.getenv("PEG_MAX_REPRICES", "40"))


# Cell

import sqlite3
from contextlib import closing

def init_db(db_path=DB_PATH):
    con = sqlite3.connect(db_path, isolation_level=None, check_same_thread=False)
    cur = con.cursor()
    # Pragmas for durability + speed balance
    cur.execute("PRAGMA journal_mode=WAL;")
    cur.execute("PRAGMA synchronous=NORMAL;")
    # Schema: minimal but complete for intraday BUY engine
    cur.executescript("""CREATE TABLE IF NOT EXISTS orders (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    created_at TEXT DEFAULT CURRENT_TIMESTAMP,
    updated_at TEXT,
    client_order_id TEXT,
    upstox_order_id TEXT,
    instrument_token TEXT,
    transaction_type TEXT,
    product TEXT,
    order_type TEXT,
    quantity INTEGER,
    price REAL,
    trigger_price REAL,
    status TEXT,
    tag TEXT,
    raw_json TEXT
);
CREATE INDEX IF NOT EXISTS idx_orders_upstox_id ON orders(upstox_order_id);

CREATE TABLE IF NOT EXISTS fills (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    order_id INTEGER,
    upstox_order_id TEXT,
    trade_id TEXT,
    instrument_token TEXT,
    transaction_type TEXT,
    quantity INTEGER,
    price REAL,
    fill_time TEXT,
    brokerage REAL,
    taxes REAL,
    net_amount REAL,
    raw_json TEXT,
    FOREIGN KEY(order_id) REFERENCES orders(id)
);
CREATE INDEX IF NOT EXISTS idx_fills_order ON fills(order_id);

CREATE TABLE IF NOT EXISTS positions (
    instrument_token TEXT PRIMARY KEY,
    net_qty INTEGER,
    avg_price REAL,
    last_price REAL,
    updated_at TEXT
);

CREATE TABLE IF NOT EXISTS pnl_day (
    trade_date TEXT PRIMARY KEY,
    realized REAL DEFAULT 0.0,
    unrealized REAL DEFAULT 0.0,
    charges REAL DEFAULT 0.0,
    net REAL DEFAULT 0.0,
    hit_loss_cap INTEGER DEFAULT 0,
    hit_profit_cap INTEGER DEFAULT 0,
    updated_at TEXT DEFAULT CURRENT_TIMESTAMP
);
""")
    con.commit()
    return con

db_con = init_db()
print(f"SQLite ready at {DB_PATH}")

def db_execute(sql, params=()):
    with closing(db_con.cursor()) as cur:
        cur.execute(sql, params)
        db_con.commit()
        return cur.lastrowid

def db_query(sql, params=()):
    with closing(db_con.cursor()) as cur:
        cur.execute(sql, params)
        cols = [d[0] for d in cur.description]
        rows = cur.fetchall()
        return [dict(zip(cols, r)) for r in rows]

def record_order(local_order: dict, upstox_resp: dict):
    """Persist a newly placed order (or an update)."""
    order_id = db_execute(
        """INSERT INTO orders
               (updated_at, client_order_id, upstox_order_id, instrument_token, transaction_type,
                product, order_type, quantity, price, trigger_price, status, tag, raw_json)
               VALUES (CURRENT_TIMESTAMP,?,?,?,?,?,?,?,?,?,?,?,?)""",
        (
            local_order.get("client_order_id"),
            str(upstox_resp.get("order_id") or upstox_resp.get("data",{}).get("order_id") or ""),
            local_order.get("instrument_token"),
            local_order.get("transaction_type"),
            local_order.get("product"),
            local_order.get("order_type"),
            int(local_order.get("quantity") or 0),
            float(local_order.get("price") or 0.0),
            float(local_order.get("trigger_price") or 0.0),
            str(upstox_resp.get("status") or upstox_resp.get("message") or ""),
            local_order.get("tag"),
            json.dumps(upstox_resp)[:200000],
        ))
    return order_id

def record_fill(order_row_id: int, fill_msg: dict, brokerage: float=0.0, taxes: float=0.0):
    upstox_order_id = str(fill_msg.get("order_id") or fill_msg.get("orderId") or "")
    trade_id = str(fill_msg.get("trade_id") or fill_msg.get("tradeId") or "")
    instrument_token = fill_msg.get("instrument_token") or fill_msg.get("instrumentToken")
    side = (fill_msg.get("transaction_type") or fill_msg.get("transactionType") or "").upper()
    qty = int(fill_msg.get("filled_quantity") or fill_msg.get("quantity") or 0)
    price = float(fill_msg.get("average_price") or fill_msg.get("price") or 0.0)
    fill_time = fill_msg.get("filled_at") or fill_msg.get("trade_time") or fill_msg.get("time") or datetime.datetime.utcnow().isoformat()

    db_execute("""INSERT INTO fills
        (order_id, upstox_order_id, trade_id, instrument_token, transaction_type, quantity, price, fill_time, brokerage, taxes, net_amount, raw_json)
        VALUES (?,?,?,?,?,?,?,?,?,?,?,?)""",
        (order_row_id, upstox_order_id, trade_id, instrument_token, side, qty, price, fill_time, brokerage, taxes, (qty*price) + brokerage + taxes, json.dumps(fill_msg)[:200000])
    )

def update_position_from_fill(fill_msg: dict):
    tok = fill_msg.get("instrument_token") or fill_msg.get("instrumentToken")
    side = (fill_msg.get("transaction_type") or fill_msg.get("transactionType") or "").upper()
    qty  = int(fill_msg.get("filled_quantity") or fill_msg.get("quantity") or 0)
    avg  = float(fill_msg.get("average_price") or fill_msg.get("price") or 0.0)
    if not tok or qty <= 0: 
        return
    row = db_query("SELECT instrument_token, net_qty, avg_price FROM positions WHERE instrument_token=?", (tok,))
    if side == "BUY":
        if not row:
            db_execute("INSERT INTO positions VALUES (?,?,?,?,CURRENT_TIMESTAMP)", (tok, qty, avg, avg))
        else:
            r = row[0]
            new_qty = (r["net_qty"] or 0) + qty
            new_avg = ((r["avg_price"] or 0.0)*(r["net_qty"] or 0) + avg*qty) / max(1, new_qty)
            db_execute("UPDATE positions SET net_qty=?, avg_price=?, updated_at=CURRENT_TIMESTAMP WHERE instrument_token=?", (new_qty, new_avg, tok))
    elif side == "SELL":
        if not row:
            return
        r = row[0]
        new_qty = (r["net_qty"] or 0) - qty
        if new_qty <= 0:
            db_execute("DELETE FROM positions WHERE instrument_token=?", (tok,))
        else:
            db_execute("UPDATE positions SET net_qty=?, updated_at=CURRENT_TIMESTAMP WHERE instrument_token=?", (new_qty, tok))

def calc_brokerage_for(instrument_token: str, quantity: int, product: str, transaction_type: str, price: float) -> float:
    """Use Upstox ChargeApi.get_brokerage to estimate brokerage for a leg.
    Returns brokerage + all charges as a positive float (₹)."""
    try:
        cfg = _apis.get("cfg") or make_configuration()
        api = upstox_client.ChargeApi(upstox_client.ApiClient(cfg))
        # 'api_version' is mandatory on this endpoint
        resp = api.get_brokerage(instrument_token, int(quantity), product, transaction_type, float(price), "2.0")
        # Depending on SDK version, resp may be model or dict
        data = resp.to_dict() if hasattr(resp, "to_dict") else (resp or {})
        total = float(data.get("total_charges") or data.get("charges",{}).get("total") or 0.0)
        return abs(total)
    except Exception as e:
        print("Brokerage calc error:", repr(e))
        return 0.0


# Cell

class RiskGuard:
    """Tracks P&L in SQLite and enforces daily loss/profit caps."""
    def __init__(self, db_path=DB_PATH, loss_limit=DAILY_LOSS_LIMIT_R, profit_cap=DAILY_PROFIT_CAP_R):
        self.db_path = db_path
        self.loss_limit = float(loss_limit or 0.0)
        self.profit_cap = float(profit_cap or 0.0)
        self._trading_halted = False

    @staticmethod
    def _today():
        return datetime.datetime.now().strftime("%Y-%m-%d")

    def realized_pnl(self):
        # Approx realized: sum of SELL fills vs position avg would be more accurate; here we aggregate by leg signs
        rows = db_query("""
            SELECT transaction_type, SUM(quantity*price) AS gross
            FROM fills
            WHERE date(fill_time)=?
            GROUP BY transaction_type
        """, (self._today(),))
        buy = next((r['gross'] for r in rows if (r['transaction_type'] or '').upper()=='BUY'), 0.0) or 0.0
        sell = next((r['gross'] for r in rows if (r['transaction_type'] or '').upper()=='SELL'), 0.0) or 0.0
        # Charges
        ch_rows = db_query("""SELECT COALESCE(SUM(brokerage+taxes),0.0) AS charges FROM fills WHERE date(fill_time)=?""", (self._today(),))
        charges = (ch_rows[0]['charges'] if ch_rows else 0.0) or 0.0
        return float(sell - buy - charges)

    def unrealized_pnl(self):
        # MTM using last_price from cache (ticks_store) vs avg_price*qty for open positions
        rows = db_query("SELECT instrument_token, net_qty, avg_price FROM positions", ())
        total = 0.0
        for r in rows:
            tok = r['instrument_token']
            qty = r['net_qty'] or 0
            avg = r['avg_price'] or 0.0
            if qty == 0:
                continue
            # Get LTP from shared cache if any
            with feed_lock:
                md = ticks_store.get(tok, {})
            ltp = 0.0
            if md:
                ltpc = md.get("ltpc") or md.get("ltp") or {}
                ltp = float(ltpc.get("ltp") or 0.0)
            total += (ltp - avg) * qty
        return float(total)

    def net_pnl(self):
        return self.realized_pnl() + self.unrealized_pnl()

    def update_day_row(self):
        rp, up = self.realized_pnl(), self.unrealized_pnl()
        net = rp + up
        today = self._today()
        row = db_query("SELECT trade_date FROM pnl_day WHERE trade_date=?", (today,))
        flags = db_query("SELECT hit_loss_cap, hit_profit_cap FROM pnl_day WHERE trade_date=?", (today,))
        hit_loss_cap = 1 if net <= -abs(self.loss_limit) and self.loss_limit>0 else (flags[0]['hit_loss_cap'] if flags else 0)
        hit_profit_cap = 1 if net >= abs(self.profit_cap) and self.profit_cap>0 else (flags[0]['hit_profit_cap'] if flags else 0)
        if row:
            db_execute("UPDATE pnl_day SET realized=?, unrealized=?, charges=(SELECT COALESCE(SUM(brokerage+taxes),0.0) FROM fills WHERE date(fill_time)=?), net=?, hit_loss_cap=?, hit_profit_cap=?, updated_at=CURRENT_TIMESTAMP WHERE trade_date=?",
                       (rp, up, today, net, hit_loss_cap, hit_profit_cap, today))
        else:
            db_execute("INSERT INTO pnl_day(trade_date, realized, unrealized, charges, net, hit_loss_cap, hit_profit_cap) VALUES (?,?,?,?,?,?,?)",
                       (today, rp, up, 0.0, net, hit_loss_cap, hit_profit_cap))

        # Update trading halt flag
        self._trading_halted = bool(hit_loss_cap or hit_profit_cap)

    def trading_halted(self) -> bool:
        self.update_day_row()
        return self._trading_halted

risk_guard = RiskGuard()
print("RiskGuard activated.")


# Cell

def start_market_stream_v2(instrument_keys: list, mode: str="full"):
    cfg = _apis.get("cfg") or make_configuration()
    streamer = upstox_client.MarketDataStreamerV3(upstox_client.ApiClient(cfg))

    def on_open():
        try:
            if instrument_keys:
                streamer.subscribe(instrument_keys, mode)
            print(f"[MD] open; subscribed {len(instrument_keys)} keys, mode={mode}")
        except Exception as e:
            print("Subscribe error:", e)

    def on_message(message):
        try:
            # message is already decoded by SDK (dict); normalize by instrument_key
            if isinstance(message, dict):
                if 'type' in message and 'data' in message:
                    # bulk frame with list under data?
                    data = message.get('data')
                    if isinstance(data, list):
                        for pkt in data:
                            k = pkt.get('instrument_key') or pkt.get('instrumentKey')
                            if k: _update_from_tick(k, pkt)
                    else:
                        k = data.get('instrument_key') or data.get('instrumentKey')
                        if k: _update_from_tick(k, data)
                else:
                    # single instrument tick
                    k = message.get('instrument_key') or message.get('instrumentKey')
                    if k: _update_from_tick(k, message)
        except Exception as e:
            print("Market message error:", e)

    def on_error(err):
        print("[MD] error:", err)

    def on_close():
        print("[MD] close")

    streamer.on("open", on_open)
    streamer.on("message", on_message)
    streamer.on("error", on_error)
    streamer.on("close", on_close)
    streamer.auto_reconnect(True, 5, 20)
    threading.Thread(target=streamer.connect, daemon=True).start()
    return streamer

def start_portfolio_stream_v2(order_update=True, position_update=True):
    cfg = _apis.get("cfg") or make_configuration()
    pstream = upstox_client.PortfolioDataStreamer(upstox_client.ApiClient(cfg), order_update=order_update, position_update=position_update)

    def on_message(message):
        try:
            if isinstance(message, dict):
                portfolio_updates.put(message)
        except Exception as e:
            print("PF message error:", e)

    pstream.on("message", on_message)
    pstream.auto_reconnect(True, 5, 20)
    threading.Thread(target=pstream.connect, daemon=True).start()
    return pstream

# Replace earlier streamers with v2
market_stream = start_market_stream_v2(subscribe_keys, mode="full")
portfolio_stream = start_portfolio_stream_v2()
print("Websocket v2 clients started.")


# Cell

def best_bid_ask(token: str):
    with feed_lock:
        md = ticks_store.get(token, {})
    first = (md.get("firstLevelWithGreeks") or md.get("firstLevel") or {}) if md else {}
    depth = first.get("firstDepth") or {}
    bidp, askp = float(depth.get("bidP") or 0.0), float(depth.get("askP") or 0.0)
    bidq, askq = float(depth.get("bidQ") or 0.0), float(depth.get("askQ") or 0.0)
    return bidp, askp, bidq, askq

def _tick_round(price: float, tick=0.05):
    # NSE FO option tick is ₹0.05
    return round(round(price / tick) * tick, 2)

def place_pegged_limit_buy(order_api, instrument_token: str, qty: int, tag: str=APP_TAG,
                           offset: float=PEG_OFFSET, max_reprices: int=PEG_MAX_REPRICES,
                           reprice_ms: int=PEG_REPRICE_MS, timeout_s: float=5.0, algo_id: str=None):
    if risk_guard.trading_halted():
        print("[RISK] Trading halted by RiskGuard; skipping order.")
        return None

    client_order_id = str(uuid.uuid4())
    t_end = time.time() + float(timeout_s)
    order_id = None
    attempts = 0

    # Initial price peg to best bid + offset
    bidp, askp, _, _ = best_bid_ask(instrument_token)
    if askp > 0 and bidp > 0:
        px = _tick_round(max(bidp + float(offset), 0.0))
    else:
        # Fallback to LTP
        with feed_lock:
            md = ticks_store.get(instrument_token, {})
        ltpc = (md.get("ltpc") or md.get("ltp") or {}) if md else {}
        ltp = float(ltpc.get("ltp") or 0.0)
        px = _tick_round(max(ltp - 0.05, 0.0))

    body = upstox_client.PlaceOrderV3Request(
        quantity=int(qty),
        product="I",
        validity="DAY",
        price=float(px),
        tag=tag,
        instrument_token=instrument_token,
        order_type="LIMIT",
        transaction_type="BUY",
        disclosed_quantity=0,
        trigger_price=0.0,
        is_amo=False,
        slice=True
    )
    try:
        resp = order_api.place_order(body, algo_id=algo_id) if algo_id else order_api.place_order(body)
        order_id = str(getattr(resp, "order_id", None) or getattr(resp, "data", {}).get("order_id") or "")
        record_order({
            "client_order_id": client_order_id,
            "instrument_token": instrument_token,
            "transaction_type": "BUY",
            "product": "I",
            "order_type": "LIMIT",
            "quantity": int(qty),
            "price": float(px),
            "trigger_price": 0.0,
            "tag": tag
        }, resp if hasattr(resp, "to_dict") else (resp or {}))
        order_ids_map[instrument_token] = order_id
        print(f"[PEG] Placed LIMIT BUY {instrument_token} qty={qty} px={px} order_id={order_id}")
    except ApiException as e:
        print("place_order error:", e)
        return None

    # Reprice loop until filled/timeout
    while time.time() < t_end and attempts < max_reprices:
        attempts += 1
        time.sleep(reprice_ms/1000.0)
        # Check latest BBO
        bidp, askp, _, _ = best_bid_ask(instrument_token)
        new_px = _tick_round(max(bidp + float(offset), 0.0)) if bidp>0 else px
        if new_px != px:
            try:
                mod_req = upstox_client.ModifyOrderV3Request(
                    order_id=order_id,
                    price=float(new_px),
                    order_type="LIMIT",
                    validity="DAY"
                )
            except AttributeError:
                # SDK older name fallback
                mod_req = {"order_id": order_id, "price": float(new_px), "order_type": "LIMIT", "validity": "DAY"}
            try:
                _ = order_api.modify_order(mod_req)
                px = new_px
                print(f"[PEG] Repriced order {order_id} -> {px}")
            except Exception as e:
                print("modify_order error:", e)
                break
        # Risk check mid-flight
        if risk_guard.trading_halted():
            print("[RISK] Halt triggered mid-order; consider cancel here.")
            break

    return order_id


# Cell

def handle_pf_update_v2(msg: dict):
    try:
        typ = (msg.get("type") or "").lower()
        data = msg.get("data") or {}
        if typ == "order":
            st = (data.get("status") or "").upper()
            tok = data.get("instrument_token") or data.get("instrumentToken")
            if st in ("TRADED","COMPLETED","FILLED","PARTIAL") and tok:
                qty = int(data.get("filled_quantity") or data.get("quantity") or 0)
                avg = float(data.get("average_price") or data.get("price") or 0.0)
                side = (data.get("transaction_type") or data.get("transactionType") or "").upper()

                # Persist order (idempotent-ish via upstox_order_id)
                local_order = {
                    "client_order_id": data.get("client_order_id") or data.get("clientOrderId"),
                    "instrument_token": tok,
                    "transaction_type": side,
                    "product": data.get("product") or "I",
                    "order_type": data.get("order_type") or "MARKET",
                    "quantity": qty,
                    "price": avg,
                    "trigger_price": data.get("trigger_price") or 0.0,
                    "tag": data.get("tag") or APP_TAG
                }
                ord_row_id = record_order(local_order, data)

                # Brokerage on the fill
                brokerage = calc_brokerage_for(tok, qty, local_order["product"], side, avg)
                taxes = 0.0  # can be part of brokerage if API returns all-in total; left as 0 to avoid double counting
                record_fill(ord_row_id, data, brokerage=brokerage, taxes=taxes)

                # Update positions
                update_position_from_fill(data)

                # Update P&L day row and potentially halt trading
                risk_guard.update_day_row()
                if risk_guard.trading_halted():
                    print(f"[RISK] P&L cap hit. Net today = ₹{risk_guard.net_pnl():.2f}. No new entries will be allowed.")
        elif typ in ("position","positions"):
            # Could persist live MTM here as last_price
            payloads = data if isinstance(data, list) else [data]
            for p in payloads:
                tok = p.get("instrument_token") or p.get("instrumentToken")
                ltp = float(p.get("ltp") or 0.0)
                if tok:
                    db_execute("UPDATE positions SET last_price=?, updated_at=CURRENT_TIMESTAMP WHERE instrument_token=?", (ltp, tok))
    except Exception as e:
        print("PF handler error:", e)

# Replace old listener with new one
def pf_listener_v2(duration_s=10):
    t0 = time.time()
    while time.time() - t0 < duration_s:
        try:
            msg = portfolio_updates.get(timeout=1.0)
            handle_pf_update_v2(msg)
        except queue.Empty:
            pass

# Start a lightweight background drain (non-blocking demo)
threading.Thread(target=pf_listener_v2, kwargs={"duration_s": 3}, daemon=True).start()
print("Portfolio listener v2 running.")


# Cell

import csv

class ReplayStreamer:
    """Feed ticks from a CSV file to the same _update_from_tick() handler.
    CSV columns expected: ts_ms, instrument_key, ltp, bidP, askP, bidQ, askQ
    """
    def __init__(self, path: str, speed: float=1.0):
        self.path = path
        self.speed = max(0.1, float(speed))
        self._thread = None
        self._stop = threading.Event()

    def start(self):
        if not os.path.exists(self.path):
            raise FileNotFoundError(self.path)
        self._thread = threading.Thread(target=self._run, daemon=True)
        self._thread.start()
        print(f"[REPLAY] streaming from {self.path} at {self.speed}x")

    def stop(self):
        self._stop.set()

    def _run(self):
        prev_ts = None
        with open(self.path, 'r') as f:
            rdr = csv.DictReader(f)
            for row in rdr:
                if self._stop.is_set():
                    break
                key = row.get("instrument_key")
                if not key:
                    continue
                # Build a message that looks like SDK message
                payload = {
                    "instrument_key": key,
                    "ltpc": {"ltp": float(row.get("ltp") or 0.0), "cp": 0.0},
                    "firstLevel": {"firstDepth": {
                        "bidP": float(row.get("bidP") or 0.0),
                        "askP": float(row.get("askP") or 0.0),
                        "bidQ": float(row.get("bidQ") or 0.0),
                        "askQ": float(row.get("askQ") or 0.0),
                    }}
                }
                _update_from_tick(key, payload)

                # pace timing
                ts_ms = int(row.get("ts_ms") or 0)
                if prev_ts is not None and ts_ms>0:
                    sleep_s = max(0.0, (ts_ms - prev_ts)/1000.0) / self.speed
                    if sleep_s > 0: time.sleep(min(sleep_s, 1.0))
                prev_ts = ts_ms

replay = None
if REPLAY_CSV:
    replay = ReplayStreamer(REPLAY_CSV, REPLAY_SPEED)
    replay.start()


# Cell

def place_buy(order_api, plan, execution_style: str="market"):
    """execution_style: 'market' or 'pegged'"""
    if risk_guard.trading_halted():
        print("[RISK] Trading halted; skip entry.")
        return None
    qty = int(plan["qty"]) if isinstance(plan, dict) else int(plan)
    tok = plan["instrument_key"]
    if execution_style == "pegged":
        return place_pegged_limit_buy(order_api, tok, qty, tag=APP_TAG)
    else:
        return place_market_buy(order_api, plan)

print("Entry helper ready (market/pegged)." )


# Cell

def get_order_api_v2(cfg=None):
    cfg = cfg or make_configuration()
    return upstox_client.OrderApi(upstox_client.ApiClient(cfg))

def cancel_all_open_orders():
    try:
        api = get_order_api_v2(_apis.get("cfg"))
        book = api.get_order_book()
        data = book.to_dict() if hasattr(book, "to_dict") else (book or {})
        orders = data.get("data") or data.get("orders") or []
        for o in orders:
            st = (o.get("status") or "").upper()
            oid = o.get("order_id") or o.get("orderId")
            if st in ("OPEN","TRIGGER PENDING","TRIGGER_PENDING","NEW"):
                try:
                    api.cancel_order({"order_id": oid})  # v2 signature allows body dict
                    print(f"[RISK] Cancelled open order {oid}")
                except Exception as e:
                    print("cancel_order error:", e)
    except Exception as e:
        print("get_order_book error:", e)

def exit_all_positions():
    try:
        api = get_order_api_v2(_apis.get("cfg"))
        resp = api.exit_positions()  # exits all intraday positions
        print("[RISK] Exit all positions invoked.")
        return resp
    except Exception as e:
        print("exit_positions error:", e)

def risk_enforcer_loop(poll_s: float=1.0):
    while True:
        try:
            if risk_guard.trading_halted():
                cancel_all_open_orders()
                exit_all_positions()
                break
        except Exception as e:
            print("risk_enforcer error:", e)
        time.sleep(poll_s)

# Fire a background watchdog
threading.Thread(target=risk_enforcer_loop, daemon=True).start()
print("Risk enforcer loop started.")


# Cell

def place_pegged_limit_buy(order_api, instrument_token: str, qty: int, tag: str=APP_TAG,
                           offset: float=PEG_OFFSET, max_reprices: int=PEG_MAX_REPRICES,
                           reprice_ms: int=PEG_REPRICE_MS, timeout_s: float=5.0, algo_id: str=None):
    if risk_guard.trading_halted():
        print("[RISK] Trading halted by RiskGuard; skipping order.")
        return None

    client_order_id = str(uuid.uuid4())
    t_end = time.time() + float(timeout_s)
    order_id = None
    attempts = 0

    bidp, askp, _, _ = best_bid_ask(instrument_token)
    if askp > 0 and bidp > 0:
        px = _tick_round(max(bidp + float(offset), 0.0))
    else:
        with feed_lock:
            md = ticks_store.get(instrument_token, {})
        ltpc = (md.get("ltpc") or md.get("ltp") or {}) if md else {}
        ltp = float(ltpc.get("ltp") or 0.0)
        px = _tick_round(max(ltp - 0.05, 0.0))

    body = upstox_client.PlaceOrderV3Request(
        quantity=int(qty),
        product="I",
        validity="DAY",
        price=float(px),
        tag=tag,
        instrument_token=instrument_token,
        order_type="LIMIT",
        transaction_type="BUY",
        disclosed_quantity=0,
        trigger_price=0.0,
        is_amo=False,
        slice=True
    )
    try:
        resp = order_api.place_order(body, algo_id=algo_id) if algo_id else order_api.place_order(body)
        order_id = str(getattr(resp, "order_id", None) or getattr(resp, "data", {}).get("order_id") or "")
        record_order({
            "client_order_id": client_order_id,
            "instrument_token": instrument_token,
            "transaction_type": "BUY",
            "product": "I",
            "order_type": "LIMIT",
            "quantity": int(qty),
            "price": float(px),
            "trigger_price": 0.0,
            "tag": tag
        }, resp if hasattr(resp, "to_dict") else (resp or {}))
        order_ids_map[instrument_token] = order_id
        print(f"[PEG] Placed LIMIT BUY {instrument_token} qty={qty} px={px} order_id={order_id}")
    except ApiException as e:
        print("place_order error:", e)
        return None

    while time.time() < t_end and attempts < max_reprices:
        attempts += 1
        time.sleep(reprice_ms/1000.0)
        bidp, askp, _, _ = best_bid_ask(instrument_token)
        new_px = _tick_round(max(bidp + float(offset), 0.0)) if bidp>0 else px
        if new_px != px:
            modified = False
            try:
                # Try v3 model class first
                try:
                    mod_req = upstox_client.ModifyOrderV3Request(
                        order_id=order_id, price=float(new_px), order_type="LIMIT", validity="DAY"
                    )
                except AttributeError:
                    # Fallback: another autogenerated name
                    mod_req = upstox_client.ModifyOrderRequest(order_id=order_id, price=float(new_px), order_type="LIMIT", validity="DAY")
                _ = order_api.modify_order(mod_req)
                modified = True
            except Exception as e:
                print("modify_order error, trying cancel/replace:", e)
                try:
                    # Cancel existing order and replace
                    get_order_api_v2(_apis.get("cfg")).cancel_order({"order_id": order_id})
                    body.price = float(new_px)
                    resp = order_api.place_order(body, algo_id=algo_id) if algo_id else order_api.place_order(body)
                    order_id = str(getattr(resp, "order_id", None) or getattr(resp, "data", {}).get("order_id") or "")
                    modified = True
                except Exception as e2:
                    print("cancel/replace failed:", e2)
                    break
            if modified:
                px = new_px
                print(f"[PEG] Repriced -> {px} (order {order_id})")
        if risk_guard.trading_halted():
            print("[RISK] Halt triggered mid-order; attempting exit/cancel.")
            try:
                get_order_api_v2(_apis.get("cfg")).cancel_order({"order_id": order_id})
            except Exception:
                pass
            break

    return order_id


# Cell

EXECUTION_STYLE = os.getenv("EXECUTION_STYLE", "pegged").lower()  # 'market' or 'pegged'

# Keep original impl name but delegate to place_buy for risk gating and pegged flow
def place_market_buy(order_api, plan):
    return place_buy(order_api, plan, execution_style=('pegged' if EXECUTION_STYLE=='pegged' else 'market'))


# Cell

def show_daily_pnl():
    today = datetime.datetime.now().strftime("%Y-%m-%d")
    rows = db_query("SELECT * FROM pnl_day WHERE trade_date=?", (today,))
    print(pd.DataFrame(rows))

print("Use show_daily_pnl() to view current day P&L caps and totals.")


# Cell

if 'portfolio_updates' not in globals():
    portfolio_updates = queue.Queue()

