# %% [markdown]
# # Upstox Intraday BUY Options Trading Engine (Refined)
#
# This notebook is a clean-room rebuild of the intraday **BUY-only** options engine for NIFTY/BANKNIFTY. It focuses on:
#
# - Deterministic wiring with the official **Upstox Python SDK** (REST + MarketDataStreamerV3 + PortfolioDataStreamer).
# - Robust data-integrity gates for NSE index options (lot size, tick size, spread, IV sanity, liveness).
# - A BUY-biased signal stack inspired by NSE/NFO microstructure guidance, Zerodha Varsity (Options & Price-Action modules), and breakout best-practices from Kaushik & Gurjar.
# - Production guardrails: SQLite ledger, portfolio reconciliation, RiskGuard with daily loss/profit caps, and UI-ready state snapshots.
#
# > **Note:** Execute the cells sequentially after exporting `UPSTOX_ACCESS_TOKEN`. The notebook is side-effect free in DRY-RUN mode and can be toggled to live trading once validated.

# %% [markdown]
# ## Reference Stack
# - Upstox Open API v2/v3 docs: https://upstox.com/developer/api-documentation/open-api
# - Official Upstox Python SDK: https://github.com/upstox/upstox-python (WebSocket v3, OrderApiV3, ChargeApi, PortfolioDataStreamer)
# - NSE India option chain, contract specifications & trading circulars: https://www.nseindia.com/option-chain and product notes on equity derivatives.
# - Zerodha Varsity, **Options Strategies** + **Price Action Trading** modules for directional buy-side frameworks.
# - Books: *Options Trading Handbook* (Mahesh Chandra Kaushik), *Price Action Trading* & *Technical Analysis Made Easy* (Sunil Gurjar), *How to Make Money with Breakout Trading* (Indrazith Shantharaj).
#
# These sources drove the signal/risk heuristics (delta windows, spread caps, breakout confirmation, and the focus on disciplined exits for option buyers).

# %% [markdown]
# ## 0. Environment & Installation
#
# ```bash
# pip install --upgrade upstox-python-sdk pandas numpy requests
# export UPSTOX_ACCESS_TOKEN="<oauth_access_token>"
# # Optional toggles
# export UNDERLYING="NIFTY"          # or BANKNIFTY
# export DRY_RUN="true"              # switch to false only after paper-testing
# export USE_SANDBOX="false"         # set true for Upstox sandbox account
# export ENABLE_METRICS="true"       # disable to turn off Prometheus server
# export METRICS_PORT="9103"         # Prometheus exporter port
# ```
#
# The engine writes lightweight state to `./engine_state.sqlite` and keeps cache files under `./cache`. Adjust the directories below if needed.

# %%
import os
import sys
import time
import math
import json
import gzip
import io
import queue
import sqlite3
import threading
import datetime as dt
from pathlib import Path
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Deque, Tuple
from collections import deque, defaultdict

import numpy as np
import pandas as pd
import requests

from engine_metrics import EngineMetrics, NULL_METRICS

try:
    import upstox_client
    from upstox_client.rest import ApiException
except ImportError as exc:  # pragma: no cover
    raise ImportError("Install the official Upstox SDK via `pip install upstox-python-sdk`.") from exc

pd.options.display.max_rows = 200
pd.options.display.width = 140

# %% [markdown]
# ## 1. Configuration & Helpers
#
# We capture all tunables in dataclasses so the notebook can be versioned like code. Every value can be overridden by environment variables at runtime.

# %%
@dataclass
class StrategyConfig:
    option_window: int = int(os.getenv("INSTRUMENTS_TO_SUB", 20))
    delta_min: float = float(os.getenv("DELTA_MIN_BUY", 0.25))
    delta_max: float = float(os.getenv("DELTA_MAX_BUY", 0.55))
    spread_max: float = float(os.getenv("SPREAD_MAX_BUY", 3.0))
    imbalance_min: float = float(os.getenv("IMBALANCE_ABS_MIN", 0.2))
    iv_z_floor: float = float(os.getenv("IV_Z_FLOOR", -0.2))
    iv_z_ceiling: float = float(os.getenv("IV_Z_MAX", 2.5))
    lookback_ticks: int = int(os.getenv("LOOKBACK_TICKS", 180))
    spot_momentum_min: float = float(os.getenv("SPOT_MOMENTUM_MIN", 0.0008))
    max_candidates: int = int(os.getenv("MAX_CONCURRENT_POS", 2))
    execution_style: str = os.getenv("EXECUTION_STYLE", "pegged").lower()

@dataclass
class RiskConfig:
    max_positions: int = int(os.getenv("MAX_CONCURRENT_POS", 2))
    capital_per_trade: float = float(os.getenv("CAPITAL_PER_TRADE", 12500))
    target_pct: float = float(os.getenv("EXIT_TARGET_PCT_BUY", 0.07))
    stop_pct: float = float(os.getenv("EXIT_STOP_PCT_BUY", 0.035))
    daily_loss_limit: float = float(os.getenv("DAILY_LOSS_LIMIT_R", 3000))
    daily_profit_cap: float = float(os.getenv("DAILY_PROFIT_CAP_R", 6000))
    square_off_hhmm: str = os.getenv("SQUARE_OFF_HHMM", "15:20")
    entry_cutoff_hhmm: str = os.getenv("ENTRY_CUTOFF_HHMM", "15:00")

@dataclass
class DataIntegrityConfig:
    master_cache: Path = Path(os.getenv("INSTRUMENT_CACHE", "cache/nse_master.json.gz"))
    cache_ttl_hours: int = int(os.getenv("MASTER_CACHE_TTL_H", 4))
    max_tick_age: float = float(os.getenv("MAX_TICK_AGE_S", 5.0))
    max_iv_z: float = float(os.getenv("IV_Z_ABS_MAX", 5.0))
    min_iv: float = float(os.getenv("IV_MIN", 5.0))

@dataclass
class EngineConfig:
    underlying: str = os.getenv("UNDERLYING", "NIFTY").upper()
    dry_run: bool = os.getenv("DRY_RUN", "true").lower() in ("1","true","yes")
    use_sandbox: bool = os.getenv("USE_SANDBOX", "false").lower() in ("1","true","yes")
    app_tag: str = os.getenv("UPSTOX_APP_NAME", "BUY-ENGINE-NB")
    db_path: Path = Path(os.getenv("UPSTOX_ENGINE_DB", "engine_state.sqlite"))
    metrics_port: int = int(os.getenv("METRICS_PORT", "9103"))
    enable_metrics: bool = os.getenv("ENABLE_METRICS", "true").lower() in ("1","true","yes")
    market_stream_mode: str = os.getenv("MARKET_STREAM_MODE", "full")
    strategy: StrategyConfig = field(default_factory=StrategyConfig)
    risk: RiskConfig = field(default_factory=RiskConfig)
    data: DataIntegrityConfig = field(default_factory=DataIntegrityConfig)
    lot_size_map: Dict[str, int] = field(default_factory=dict)

    @staticmethod
    def load() -> "EngineConfig":
        cfg = EngineConfig()
        cfg.data.master_cache.parent.mkdir(parents=True, exist_ok=True)
        cfg.db_path.parent.mkdir(parents=True, exist_ok=True)
        return cfg

CONFIG = EngineConfig.load()
print(CONFIG)

# %%
IDX_MAP = {
    "NIFTY": "NSE_INDEX|Nifty 50",
    "BANKNIFTY": "NSE_INDEX|Nifty Bank",
}
STRIKE_STEP = 50 if CONFIG.underlying == "NIFTY" else 100

def hhmm_now() -> str:
    return dt.datetime.now().strftime("%H:%M")

def within_entry_window(risk: RiskConfig) -> bool:
    return hhmm_now() < risk.entry_cutoff_hhmm

def nearest_strike(price: float, step: int = STRIKE_STEP) -> int:
    return int(round(price/step) * step)

def pct_move(old: float, new: float) -> float:
    if old <= 0:
        return 0.0
    return (new - old) / old

# %% [markdown]
# ## 2. Upstox Session & API clients
#
# Wraps authentication, REST clients, and websocket factories. This keeps token handling in one place and ensures the correct sandbox/live base URL is used (per Upstox Open-API recommendations).

# %%
class UpstoxSession:
    def __init__(self, config: EngineConfig):
        self.config = config
        self._api_client: Optional[upstox_client.ApiClient] = None
        self.order_api: Optional[upstox_client.OrderApiV3] = None
        self.quote_api: Optional[upstox_client.MarketQuoteApi] = None
        self.charge_api: Optional[upstox_client.ChargeApi] = None

    def connect(self):
        token = os.getenv("UPSTOX_ACCESS_TOKEN", "").strip()
        if not token:
            raise RuntimeError("UPSTOX_ACCESS_TOKEN not set. Complete OAuth flow and export the token.")
        cfg = upstox_client.Configuration(sandbox=self.config.use_sandbox)
        cfg.access_token = token
        self._api_client = upstox_client.ApiClient(cfg)
        self.order_api = upstox_client.OrderApiV3(self._api_client)
        self.quote_api = upstox_client.MarketQuoteApi(self._api_client)
        self.charge_api = upstox_client.ChargeApi(self._api_client)
        print("Connected to Upstox; sandbox=", self.config.use_sandbox)

    def market_streamer(self):
        if not self._api_client:
            raise RuntimeError("Call connect() first")
        return upstox_client.MarketDataStreamerV3(self._api_client)

    def portfolio_streamer(self, order_update=True, position_update=True):
        if not self._api_client:
            raise RuntimeError("Call connect() first")
        return upstox_client.PortfolioDataStreamer(self._api_client, order_update=order_update, position_update=position_update)

    def index_key(self) -> str:
        return IDX_MAP.get(self.config.underlying, "NSE_INDEX|Nifty 50")

SESSION = UpstoxSession(CONFIG)

# %% [markdown]
# ## 3. Instrument Master & Option Chain Builder
#
# We rely on the official NSE instrument dump hosted by Upstox. The loader enforces contract metadata (lot size, tick size) so downstream sizing respects NSE specs.

# %%
MASTER_URL = "https://assets.upstox.com/market-quote/instruments/exchange/NSE.json.gz"

def _normalize_master_columns(df: pd.DataFrame) -> pd.DataFrame:
    rename = {}
    replacements = {
        "instrumentkey": "instrument_key",
        "trading_symbol": "trading_symbol",
        "trading_symbol_name": "trading_symbol",
        "strikeprice": "strike_price",
        "optiontype": "option_type",
        "ticksize": "tick_size",
        "lotsize": "lot_size",
    }
    for col in df.columns:
        key = str(col).lower().replace(" ", "_").replace("-", "_")
        key = replacements.get(key, key)
        rename[col] = key
    df = df.rename(columns=rename)
    if "option_type" not in df.columns and "instrument_type" in df.columns:
        df["option_type"] = df["instrument_type"]
    if "strike_price" not in df.columns and "strike" in df.columns:
        df["strike_price"] = df["strike"]
    return df

def load_instrument_master(cache_path: Path, ttl_hours: int) -> pd.DataFrame:
    if cache_path.exists():
        age_h = (time.time() - cache_path.stat().st_mtime) / 3600
        if age_h <= ttl_hours:
            with gzip.open(cache_path, "rt") as fp:
                return _normalize_master_columns(pd.read_json(fp))
    resp = requests.get(MASTER_URL, timeout=30)
    resp.raise_for_status()
    cache_path.parent.mkdir(parents=True, exist_ok=True)
    with open(cache_path, "wb") as fh:
        fh.write(resp.content)
    with gzip.open(io.BytesIO(resp.content), "rt") as fp:
        return _normalize_master_columns(pd.read_json(fp))

def option_chain(df: pd.DataFrame, underlying: str, window: int) -> pd.DataFrame:
    u = underlying.upper()
    flt = (
        df.segment.eq("NSE_FO") &
        df.instrument_type.isin(["CE","PE"]) &
        df.name.str.upper().eq(u)
    )
    required_cols = ["instrument_key","trading_symbol","name","expiry","strike_price","option_type","lot_size","tick_size"]
    missing = [c for c in required_cols if c not in df.columns]
    if missing:
        raise KeyError(f"Instrument master missing columns {missing}. Check API response format.")
    chain = df.loc[flt, required_cols].copy()
    chain.rename(columns={"strike_price":"strike"}, inplace=True)
    chain["expiry"] = pd.to_datetime(chain["expiry"], unit="ms", errors="coerce").dt.tz_localize(None)
    today = pd.Timestamp.today().normalize()
    future = chain.loc[chain["expiry"] >= today, "expiry"]
    if future.empty:
        if chain["expiry"].empty:
            raise RuntimeError(f"No expiry data available for {underlying}. Refresh instrument master.")
        next_exp = chain["expiry"].max()
        print(f"[WARN] No future expiry in master; defaulting to latest available {next_exp.date()}.")
    else:
        next_exp = future.min()
    chain = chain[chain.expiry.eq(next_exp)].sort_values(["strike","option_type"])
    if chain.empty:
        raise RuntimeError(f"No contracts found for {underlying} and expiry {next_exp}. Verify master data.")
    print(f"Loaded {len(chain)} contracts for {underlying} expiry {next_exp.date()}. Live ATM filtering happens at runtime.")
    return chain.reset_index(drop=True)

MASTER_DF = load_instrument_master(CONFIG.data.master_cache, CONFIG.data.cache_ttl_hours)
CHAIN_DF = option_chain(MASTER_DF, CONFIG.underlying, CONFIG.strategy.option_window)
CONFIG.lot_size_map = dict(zip(CHAIN_DF.instrument_key, CHAIN_DF.lot_size))
CHAIN_DF.head()

# %% [markdown]
# ## 4. Market Data Hub & Integrity Monitors
#
# The hub fans out MarketDataStreamerV3 ticks, keeps rolling stats, and flags stale/invalid feeds. Buyers must avoid stale or wide markets per NSE liquidity advisories.

# %%
@dataclass
class TickState:
    instrument_key: str
    last_price: float = np.nan
    bid: float = np.nan
    ask: float = np.nan
    bid_qty: float = 0.0
    ask_qty: float = 0.0
    delta: float = np.nan
    iv: float = np.nan
    last_ts: float = 0.0
    spread: float = np.nan
    imbalance: float = 0.0

class MarketDataHub:
    def __init__(self, session: UpstoxSession, cfg: EngineConfig, chain_df: pd.DataFrame,
                 metrics: EngineMetrics = NULL_METRICS):
        self.session = session
        self.cfg = cfg
        self.chain_df = chain_df
        self.metrics = metrics
        self.index_key = session.index_key()
        self.lock = threading.Lock()
        self.state: Dict[str, TickState] = {}
        self.price_history: Dict[str, Deque[float]] = defaultdict(lambda: deque(maxlen=cfg.strategy.lookback_ticks))
        self.iv_history: Dict[str, Deque[float]] = defaultdict(lambda: deque(maxlen=cfg.strategy.lookback_ticks))
        self.streamer = None
        self.health: Dict[str, str] = {}
        self._stop = threading.Event()
        self.subscribed_keys: List[str] = []

    def start(self):
        keys = self._subscription_keys()
        idx_key = self.index_key
        if idx_key not in keys:
            keys.append(idx_key)
        self.subscribed_keys = keys
        self.streamer = self.session.market_streamer()

        def on_open():
            try:
                mode = self.cfg.market_stream_mode
                self.streamer.subscribe(keys, mode)
                print(f"[MD] subscribed {len(keys)} keys mode={mode}")
            except Exception as exc:
                print("Subscribe error:", exc)

        def on_message(message):
            payload = message.to_dict() if hasattr(message, "to_dict") else message
            if not isinstance(payload, dict):
                return
            typ = payload.get("type")
            if typ == "live_feed":
                feeds = payload.get("feeds") or {}
                for key, data in feeds.items():
                    feed = data.get("firstLevelWithGreeks") or data.get("fullFeed") or data
                    self._ingest_tick(key, feed)
            elif "instrument_key" in payload:
                self._ingest_tick(payload.get("instrument_key"), payload)

        def on_close():
            if not self._stop.is_set():
                print("[MD] connection closed; auto-reconnect armed")

        self.streamer.on("open", lambda: on_open())
        self.streamer.on("message", lambda msg: on_message(msg))
        self.streamer.on("close", lambda: on_close())
        self.streamer.auto_reconnect(True, 5, 20)
        threading.Thread(target=self.streamer.connect, daemon=True).start()

    def _subscription_keys(self) -> List[str]:
        idx_key = self.index_key
        spot = self._fetch_spot(idx_key)
        if spot <= 0:
            spot = float(self.chain_df["strike"].median())
        atm = nearest_strike(spot, STRIKE_STEP)
        window = max(1, self.cfg.strategy.option_window)
        subset = self.chain_df[
            self.chain_df["strike"].between(
                atm - window * STRIKE_STEP,
                atm + window * STRIKE_STEP
            )
        ]
        if subset.empty:
            subset = self.chain_df
        print(f"[MD] subscribing {len(subset)} contracts around ATM {atm}")
        return subset["instrument_key"].tolist()

    def _fetch_spot(self, idx_key: str) -> float:
        try:
            quote = self.session.quote_api.ltp(symbol=idx_key, api_version="2.0")
            data = getattr(quote, "data", None) or {}
            row = (data.get(idx_key)
                   or data.get(idx_key.replace("|", ":"))
                   or data.get(idx_key.replace(":", "|")))
            if not row:
                return 0.0
            if hasattr(row, "last_price"):
                return float(row.last_price or 0.0)
            if isinstance(row, dict):
                return float(row.get("last_price") or row.get("ltp") or 0.0)
            return 0.0
        except Exception as exc:
            print(f"[WARN] Failed to fetch spot for {idx_key}: {exc}")
            return 0.0

    def stop(self):
        self._stop.set()
        if self.streamer:
            try:
                self.streamer.disconnect()
            except Exception:
                pass

    def _ingest_tick(self, key: str, feed: dict):
        if not key:
            return
        if "marketFF" in feed:
            feed = feed.get("marketFF") or feed
        ltpc = feed.get("ltpc") or feed.get("ltp") or {}
        ltp = float(ltpc.get("ltp") or feed.get("last_price") or 0.0)
        first = feed.get("firstLevelWithGreeks") or feed.get("firstLevel") or {}
        depth = first.get("firstDepth") or {}
        if not depth and feed.get("marketLevel"):
            quotes = feed["marketLevel"].get("bidAskQuote") or []
            depth = quotes[0] if quotes else {}
        bid = float(depth.get("bidP") or depth.get("bid") or 0.0)
        ask = float(depth.get("askP") or depth.get("ask") or 0.0)
        bidq = float(depth.get("bidQ") or depth.get("bid_quantity") or 0.0)
        askq = float(depth.get("askQ") or depth.get("ask_quantity") or 0.0)
        greeks = first.get("optionGreeks") or feed.get("optionGreeks") or {}
        delta = greeks.get("delta")
        iv = greeks.get("iv") or first.get("iv") or feed.get("iv")
        ts = time.time()
        spread = (ask - bid) if (ask > 0 and bid > 0) else np.nan
        imbalance = ((bidq - askq) / (bidq + askq)) if (bidq + askq) > 0 else 0.0

        with self.lock:
            state = self.state.get(key) or TickState(key)
            state.last_price = ltp
            state.bid = bid if bid > 0 else state.bid
            state.ask = ask if ask > 0 else state.ask
            state.bid_qty = bidq
            state.ask_qty = askq
            state.delta = float(delta) if delta is not None else state.delta
            state.iv = float(iv) if isinstance(iv, (int, float)) else state.iv
            state.last_ts = ts
            state.spread = spread
            state.imbalance = imbalance
            self.state[key] = state
            if np.isfinite(ltp) and ltp > 0:
                self.price_history[key].append(ltp)
            if isinstance(state.iv, float):
                self.iv_history[key].append(state.iv)
            self.health[key] = self._validate_tick(state)
            health_state = self.health.get(key, "unknown")
            iv_z = self.iv_zscore(key)
            if key != self.index_key:
                self.metrics.update_market(
                    instrument=key,
                    ltp=ltp if np.isfinite(ltp) else None,
                    iv=state.iv if isinstance(state.iv, float) else None,
                    iv_z=iv_z if np.isfinite(iv_z) else None,
                )
                self.metrics.set_health(key, health_state, 1.0 if health_state == "ok" else 0.0)

    def _validate_tick(self, state: TickState) -> str:
        if state.last_price <= 0:
            return "no_ltp"
        if state.spread and state.spread > self.cfg.strategy.spread_max * 2:
            return "wide_spread"
        if state.delta and not (0 <= state.delta <= 1):
            return "bad_delta"
        age = time.time() - state.last_ts
        if age > self.cfg.data.max_tick_age:
            return "stale"
        return "ok"

    def iv_zscore(self, key: str) -> float:
        series = self.iv_history.get(key)
        if not series or len(series) < 20:
            return np.nan
        arr = np.array(series, dtype=float)
        if not np.isfinite(arr).any():
            return np.nan
        mu = np.nanmean(arr)
        sd = np.nanstd(arr)
        if sd <= 0:
            return 0.0
        return float((arr[-1] - mu) / sd)

    def snapshot(self) -> pd.DataFrame:
        with self.lock:
            if not self.state:
                return pd.DataFrame()
            rows = []
            for key, st in self.state.items():
                rows.append({
                    "instrument_key": key,
                    "ltp": st.last_price,
                    "bid": st.bid,
                    "ask": st.ask,
                    "spread": st.spread,
                    "imbalance": st.imbalance,
                    "delta": st.delta,
                    "iv": st.iv,
                    "tick_age": time.time() - st.last_ts,
                    "iv_z": self.iv_zscore(key),
                    "health": self.health.get(key, "unknown")
                })
        snap = pd.DataFrame(rows)
        if snap.empty:
            return snap
        snap = snap.merge(self.chain_df, on="instrument_key", how="left")
        snap = snap[~snap["instrument_key"].eq(self.index_key)]
        return snap.sort_values(["strike","option_type"])

MARKET_DATA = None

# %% [markdown]
# ## 5. Signal Engine (BUY-side logic)
#
# We follow a conservative breakout template: take CE when spot momentum is positive and delta-rich contracts show healthy depth, and take PE only when the index momentum flips negative. Filters come from Zerodha Varsity guidance (delta sweet spots, spread discipline) and breakout texts (waiting for confirmation via order-book imbalance + short-term momentum).

# %%
@dataclass
class TradePlan:
    instrument_key: str
    option_type: str
    strike: int
    qty: int
    entry_price: float
    stop_price: float
    target_price: float
    reason: str

class SpotMomentum:
    def __init__(self, lookback: int = 60):
        self.window = deque(maxlen=lookback)

    def update(self, price: float):
        if price > 0:
            self.window.append(price)

    def slope(self) -> float:
        if len(self.window) < 5:
            return 0.0
        first = self.window[0]
        last = self.window[-1]
        return pct_move(first, last)

class SignalEngine:
    def __init__(self, cfg: EngineConfig, market: MarketDataHub, metrics: EngineMetrics = NULL_METRICS):
        self.cfg = cfg
        self.market = market
        self.spot_state = SpotMomentum(lookback=cfg.strategy.lookback_ticks)
        self.metrics = metrics

    def update_spot(self):
        st = self.market.state.get(self.market.session.index_key()) if self.market else None
        if st and st.last_price:
            self.spot_state.update(st.last_price)
            return st.last_price
        return np.nan

    def _position_count(self, open_positions: Dict[str, dict]) -> int:
        return sum(1 for pos in open_positions.values() if pos.get("qty",0)>0)

    def generate(self, snapshot: pd.DataFrame, open_positions: Dict[str, dict]) -> List[TradePlan]:
        t_start = time.perf_counter()
        if snapshot is None or snapshot.empty:
            return []
        if not within_entry_window(self.cfg.risk):
            return []
        spot = self.update_spot()
        trend = self.spot_state.slope()
        if abs(trend) < self.cfg.strategy.spot_momentum_min:
            return []
        side_filter = "CE" if trend > 0 else "PE"
        eligible = snapshot.copy()
        eligible = eligible[eligible.option_type.str.upper().eq(side_filter)]
        eligible = eligible[eligible.health.eq("ok")]
        eligible = eligible[np.isfinite(eligible["delta"])]
        eligible = eligible[(eligible["delta"]>=self.cfg.strategy.delta_min) & (eligible["delta"]<=self.cfg.strategy.delta_max)]
        eligible = eligible[(eligible["spread"]<=self.cfg.strategy.spread_max)]
        eligible = eligible[eligible["imbalance"].abs()>=self.cfg.strategy.imbalance_min]
        eligible = eligible[eligible["iv_z"].between(self.cfg.strategy.iv_z_floor, self.cfg.strategy.iv_z_ceiling)]
        if eligible.empty:
            return []
        eligible["atm_gap"] = (eligible["strike"] - nearest_strike(spot, STRIKE_STEP)).abs()
        eligible = eligible.sort_values(["atm_gap","spread"])
        plans: List[TradePlan] = []
        for _, row in eligible.iterrows():
            if self._position_count(open_positions) + len(plans) >= self.cfg.risk.max_positions:
                break
            price = float(row.ltp or row.bid or 0.0)
            if price <= 0:
                continue
            lot = self.cfg.lot_size_map.get(row.instrument_key, 1)
            lots = max(1, int(self.cfg.risk.capital_per_trade // max(price * lot, 1)))
            qty = lots * lot
            stop = price * (1 - self.cfg.risk.stop_pct)
            target = price * (1 + self.cfg.risk.target_pct)
            plans.append(TradePlan(
                instrument_key=row.instrument_key,
                option_type=row.option_type,
                strike=int(row.strike or 0),
                qty=qty,
                entry_price=price,
                stop_price=stop,
                target_price=target,
                reason=f"{side_filter}-trend {trend:.4f}"
            ))
        duration_ms = (time.perf_counter() - t_start) * 1000.0
        for plan in plans:
            self.metrics.record_signal(plan.instrument_key, duration_ms)
        return plans

SIGNALS = None

# %% [markdown]
# ## 6. Risk Ledger & PnL Tracking
#
# Upstox strongly recommends real-time reconciliation via PortfolioDataStreamer. We persist fills into SQLite for auditability, compute MTM using the latest ticks, and hard-stop when the daily loss/profit guardrails are breached.

# %%
class PnLLedger:
    def __init__(self, db_path: Path):
        self.db_path = db_path
        self._conn = sqlite3.connect(db_path, check_same_thread=False)
        self._init_schema()
        self.lock = threading.Lock()

    def _init_schema(self):
        cur = self._conn.cursor()
        cur.executescript(
            """
            CREATE TABLE IF NOT EXISTS fills (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                trade_time TEXT,
                instrument_key TEXT,
                side TEXT,
                quantity INTEGER,
                price REAL,
                charges REAL DEFAULT 0.0
            );
            CREATE TABLE IF NOT EXISTS day_pnl (
                trade_date TEXT PRIMARY KEY,
                realized REAL DEFAULT 0.0,
                unrealized REAL DEFAULT 0.0,
                net REAL DEFAULT 0.0,
                hit_loss_cap INTEGER DEFAULT 0,
                hit_profit_cap INTEGER DEFAULT 0
            );
            """
        )
        self._conn.commit()

    def record_fill(self, instrument_key: str, side: str, quantity: int, price: float, charges: float = 0.0):
        with self.lock:
            self._conn.execute(
                "INSERT INTO fills(trade_time,instrument_key,side,quantity,price,charges) VALUES (?,?,?,?,?,?)",
                (dt.datetime.utcnow().isoformat(), instrument_key, side, quantity, price, charges)
            )
            self._conn.commit()

    def realized_today(self) -> float:
        rows = self._conn.execute(
            "SELECT side, SUM(quantity*price) val FROM fills WHERE date(trade_time)=date('now','localtime') GROUP BY side"
        ).fetchall()
        buy = next((r[1] for r in rows if (r[0] or '').upper()=="BUY"), 0.0)
        sell = next((r[1] for r in rows if (r[0] or '').upper()=="SELL"), 0.0)
        return float(sell - buy)

class RiskManager:
    def __init__(self, cfg: EngineConfig, ledger: PnLLedger, metrics: EngineMetrics = NULL_METRICS):
        self.cfg = cfg
        self.ledger = ledger
        self.open_positions: Dict[str, dict] = {}
        self._trading_halted = False
        self.metrics = metrics

    def update_position(self, instrument_key: str, qty: int, avg_price: float, side: str):
        pos = self.open_positions.get(instrument_key, {"qty":0,"avg_price":0.0})
        if side == "BUY":
            new_qty = pos["qty"] + qty
            new_avg = ((pos["avg_price"] * pos["qty"]) + avg_price * qty) / max(1, new_qty)
            self.open_positions[instrument_key] = {"qty": new_qty, "avg_price": new_avg}
        else:
            new_qty = pos["qty"] - qty
            self.open_positions[instrument_key] = {"qty": max(0, new_qty), "avg_price": pos["avg_price"]}
        self.metrics.set_open_positions(sum(1 for p in self.open_positions.values() if p.get("qty", 0) > 0))

    def should_block(self) -> bool:
        if self._trading_halted:
            self.metrics.set_risk_state(True)
            return True
        pnl = self.ledger.realized_today()
        if pnl <= -abs(self.cfg.risk.daily_loss_limit):
            self._trading_halted = True
        if self.cfg.risk.daily_profit_cap > 0 and pnl >= self.cfg.risk.daily_profit_cap:
            self._trading_halted = True
        self.metrics.set_risk_state(self._trading_halted)
        return self._trading_halted

    def register_fill(self, instrument_key: str, side: str, qty: int, price: float, charges: float = 0.0):
        self.ledger.record_fill(instrument_key, side, qty, price, charges)
        self.update_position(instrument_key, qty, price, side)
        self.metrics.order_filled(instrument_key)
        self.metrics.set_pnl(self.ledger.realized_today())

LEDGER = PnLLedger(CONFIG.db_path)
RISK = RiskManager(CONFIG, LEDGER, metrics=NULL_METRICS)

# %% [markdown]
# ## 7. Execution Helpers
#
# Implements pegged-limit BUY orders (Upstox `OrderApiV3`) with safe fallbacks. Buyers typically lift the best ask, but we attempt a bid+offset peg per Zerodha/Varsity liquidity tips.

# %%
class OrderManager:
    def __init__(self, session: UpstoxSession, cfg: EngineConfig, market: MarketDataHub,
                 risk: RiskManager, metrics: EngineMetrics = NULL_METRICS):
        self.session = session
        self.cfg = cfg
        self.market = market
        self.risk = risk
        self.metrics = metrics

    def _best_quote(self, token: str) -> Tuple[float, float]:
        st = self.market.state.get(token)
        if not st:
            return (np.nan, np.nan)
        return st.bid, st.ask

    def _pegged_price(self, token: str, fallback: float) -> float:
        bid, ask = self._best_quote(token)
        px = fallback
        if bid and bid > 0:
            px = bid + 0.05
        elif ask and ask > 0:
            px = ask
        return round(max(px, 0.05) / 0.05) * 0.05

    def _place_order(self, plan: TradePlan) -> Optional[str]:
        start = time.perf_counter()
        if self.cfg.dry_run:
            print(f"[DRY BUY] {plan.instrument_key} qty={plan.qty} @~{plan.entry_price:.2f} reason={plan.reason}")
            self.risk.register_fill(plan.instrument_key, "BUY", plan.qty, plan.entry_price)
            latency_ms = (time.perf_counter() - start) * 1000.0
            self.metrics.order_submitted(plan.instrument_key, latency_ms)
            return "SIM-ORDER"
        price = self._pegged_price(plan.instrument_key, plan.entry_price) if self.cfg.strategy.execution_style == "pegged" else 0.0
        body = upstox_client.PlaceOrderV3Request(
            quantity=int(plan.qty),
            product="I",
            validity="DAY",
            price=float(price),
            tag=self.cfg.app_tag,
            instrument_token=plan.instrument_key,
            order_type="LIMIT" if self.cfg.strategy.execution_style == "pegged" else "MARKET",
            transaction_type="BUY",
            disclosed_quantity=0,
            trigger_price=0.0,
            is_amo=False,
            slice=True
        )
        resp = self.session.order_api.place_order(body, algo_id=self.cfg.app_tag)
        data = resp.to_dict() if hasattr(resp, "to_dict") else resp
        order_id = (data.get("data") or {}).get("order_id") or data.get("order_id")
        print("[LIVE BUY]", order_id, plan.instrument_key, plan.qty)
        latency_ms = (time.perf_counter() - start) * 1000.0
        self.metrics.order_submitted(plan.instrument_key, latency_ms)
        return order_id

    def execute(self, plan: TradePlan):
        if self.risk.should_block():
            print("[RISK] Trading halted; order skipped")
            self.metrics.order_rejected(plan.instrument_key, "risk_halt")
            return None
        return self._place_order(plan)

ORDERS = None

# %% [markdown]
# ## 8. Portfolio Stream Listener
#
# Consumes `PortfolioDataStreamer` for order/fill callbacks so RiskGuard stays in sync without polling.

# %%
class PortfolioWatcher:
    def __init__(self, session: UpstoxSession, risk: RiskManager):
        self.session = session
        self.risk = risk
        self.stream = None
        self.thread = None

    def start(self):
        self.stream = self.session.portfolio_streamer()

        def on_message(message):
            payload = message.to_dict() if hasattr(message, "to_dict") else message
            if not isinstance(payload, dict):
                return
            typ = (payload.get("type") or "").lower()
            data = payload.get("data") or {}
            if typ == "order":
                status = (data.get("status") or "").upper()
                if status in {"TRADED","COMPLETED","FILLED","PARTIAL"}:
                    tok = data.get("instrument_token") or data.get("instrumentKey")
                    qty = int(data.get("filled_quantity") or data.get("quantity") or 0)
                    price = float(data.get("average_price") or data.get("price") or 0.0)
                    side = (data.get("transaction_type") or data.get("transactionType") or "").upper()
                    if tok and qty>0:
                        self.risk.register_fill(tok, side, qty, price)

        self.stream.on("message", lambda msg: on_message(msg))
        self.stream.auto_reconnect(True, 5, 20)
        self.thread = threading.Thread(target=self.stream.connect, daemon=True)
        self.thread.start()
        print("Portfolio watcher running.")

PORTFOLIO = None

# %% [markdown]
# ## 9. Engine Orchestrator
#
# The `TradingEngine` wires everything: session, market data, signals, execution, and diagnostics.

# %%
class TradingEngine:
    def __init__(self, cfg: EngineConfig):
        self.cfg = cfg
        self.metrics = EngineMetrics(cfg.metrics_port, cfg.enable_metrics)
        self.session = UpstoxSession(cfg)
        self.market: Optional[MarketDataHub] = None
        self.signals: Optional[SignalEngine] = None
        self.order_mgr: Optional[OrderManager] = None
        self.ledger = PnLLedger(cfg.db_path)
        self.risk = RiskManager(cfg, self.ledger, metrics=self.metrics)
        self.portfolio: Optional[PortfolioWatcher] = None

    def bootstrap(self):
        self.session.connect()
        self.market = MarketDataHub(self.session, self.cfg, CHAIN_DF, metrics=self.metrics)
        self.signals = SignalEngine(self.cfg, self.market, metrics=self.metrics)
        self.order_mgr = OrderManager(self.session, self.cfg, self.market, self.risk, metrics=self.metrics)
        self.portfolio = PortfolioWatcher(self.session, self.risk)
        self.market.start()
        self.portfolio.start()
        print("Engine bootstrap complete.")

    def run(self, runtime_minutes: float = 60):
        if not all([self.market, self.signals, self.order_mgr]):
            raise RuntimeError("Call bootstrap() first")
        t_end = time.time() + runtime_minutes * 60
        while time.time() < t_end:
            time.sleep(1.5)
            snap = self.market.snapshot()
            if snap.empty:
                continue
            plans = self.signals.generate(snap, self.risk.open_positions)
            for plan in plans:
                self.order_mgr.execute(plan)
            if hhmm_now() >= self.cfg.risk.square_off_hhmm:
                print("Square-off window reached; stop initiating new trades.")
                break
            self.metrics.heartbeat()
        print("Engine loop finished.")

ENGINE = TradingEngine(CONFIG)

# %% [markdown]
# ## 10. Notebook Dashboards & Utilities
#
# Lightweight helpers to inspect market snapshots, open risk, and PnL without spinning up the Streamlit UI.

# %%
def show_live_snapshot(market: MarketDataHub, top: int = 12):
    snap = market.snapshot() if market else pd.DataFrame()
    if snap.empty:
        print("snapshot empty")
        return
    display(snap.head(top))

def show_positions(risk: RiskManager):
    if not risk.open_positions:
        print("no open positions")
        return
    print(pd.DataFrame.from_dict(risk.open_positions, orient='index'))

def show_day_pnl(ledger: PnLLedger):
    print({"realized_today": ledger.realized_today()})

# %% [markdown]
# ## 11. Execution Instructions
#
# ```python
# # 1. Bootstrap (after tokens and configs are set)
# ENGINE.bootstrap()
#
# # 2. Inspect market health before arming the loop
# show_live_snapshot(ENGINE.market)
#
# # 3. Run the engine during market hours (paper trade first)
# ENGINE.run(runtime_minutes=90)
#
# # 4. Monitor risk & PnL
# show_positions(ENGINE.risk)
# show_day_pnl(ENGINE.order_mgr.risk.ledger if ENGINE.order_mgr else LEDGER)
# ```
#
# The DRY-RUN flag should remain `true` until:
# 1. Market data feed is stable (`health == ok`) for subscribed contracts.
# 2. Portfolio watcher receives fills correctly (verify SQLite ledger rows).
# 3. Strategy performance has been validated against NSE option-chain data and back-tests (you can reuse the `ReplayStreamer` from earlier notebooks with this architecture).
