from __future__ import annotations

import datetime as dt
import gzip
import io
import json
import logging
import os
import sqlite3
from pathlib import Path
from dataclasses import dataclass
from typing import Dict, List, Optional, Tuple

import requests

from brokerage.upstox_client import INDEX_INSTRUMENT_KEYS, IST, UpstoxSession

CACHE_TTL_SECONDS = 300
MASTER_URL = os.getenv("UPSTOX_MASTER_URL", "https://assets.upstox.com/market-quote/instruments/exchange/NSE.json.gz")
MASTER_CACHE_PATH = Path(os.getenv("UPSTOX_MASTER_CACHE", "cache/nse_master.json.gz"))


@dataclass(frozen=True)
class InstrumentMeta:
    instrument_key: str
    symbol: str
    expiry_date: str
    option_type: str
    strike: float
    lot_size: Optional[float]
    tick_size: Optional[float]
    freeze_qty: Optional[float]
    price_band_low: Optional[float]
    price_band_high: Optional[float]


class InstrumentCache:
    """Persists option metadata for quick lookup of CE/PE contracts."""

    def __init__(self, db_path: Optional[str] = None):
        path = Path(db_path or os.getenv("ENGINE_DB_PATH", "engine_state.sqlite"))
        path.parent.mkdir(parents=True, exist_ok=True)
        self._conn = sqlite3.connect(path, check_same_thread=False)
        self._conn.row_factory = sqlite3.Row
        self._conn.execute("PRAGMA journal_mode=WAL;")
        self._conn.execute("PRAGMA synchronous=NORMAL;")
        self._init_schema()

    def _init_schema(self) -> None:
        cur = self._conn.cursor()
        cur.executescript(
            """
            CREATE TABLE IF NOT EXISTS instrument_cache (
                symbol TEXT NOT NULL,
                expiry_date TEXT NOT NULL,
                strike REAL NOT NULL,
                option_type TEXT NOT NULL CHECK(option_type IN ('CE','PE')),
                instrument_key TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                PRIMARY KEY(symbol, expiry_date, strike, option_type)
            );
            CREATE TABLE IF NOT EXISTS instrument_meta (
                instrument_key TEXT PRIMARY KEY,
                symbol TEXT NOT NULL,
                expiry_date TEXT NOT NULL,
                option_type TEXT NOT NULL,
                strike REAL NOT NULL,
                lot_size REAL,
                tick_size REAL,
                freeze_qty REAL,
                price_band_low REAL,
                price_band_high REAL,
                updated_at TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS cache_meta (
                k TEXT PRIMARY KEY,
                v TEXT NOT NULL,
                updated_at TEXT NOT NULL
            );
            """
        )
        self._conn.commit()

    def close(self) -> None:
        self._conn.close()

    def _now_str(self) -> str:
        return dt.datetime.now(IST).isoformat()

    def resolve_index_key(self, symbol: str) -> str:
        try:
            return INDEX_INSTRUMENT_KEYS[symbol.upper()]
        except KeyError as exc:
            raise ValueError(f"Unsupported symbol {symbol}") from exc

    # ----- Expiry helpers --------------------------------------------------
    def _get_cached_expiries(self, symbol: str) -> Optional[Tuple[str, ...]]:
        cur = self._conn.execute("SELECT v, updated_at FROM cache_meta WHERE k=?", (f"expiries:{symbol.upper()}",))
        row = cur.fetchone()
        if not row:
            return None
        updated_at = dt.datetime.fromisoformat(row["updated_at"]).astimezone(IST)
        if (dt.datetime.now(IST) - updated_at).total_seconds() > CACHE_TTL_SECONDS:
            return None
        try:
            payload = json.loads(row["v"])
            return tuple(payload)
        except json.JSONDecodeError:
            return None

    def _set_cached_expiries(self, symbol: str, expiries: Tuple[str, ...]) -> None:
        key = f"expiries:{symbol.upper()}"
        self._conn.execute(
            "INSERT INTO cache_meta(k,v,updated_at) VALUES (?,?,?) ON CONFLICT(k) DO UPDATE SET v=excluded.v, updated_at=excluded.updated_at",
            (key, json.dumps(list(expiries)), self._now_str()),
        )
        self._conn.commit()

    def _fetch_expiries(self, symbol: str, session: UpstoxSession) -> Tuple[str, ...]:
        cached = self._get_cached_expiries(symbol)
        if cached:
            return cached
        try:
            payload = session.get_option_contracts(
                self.resolve_index_key(symbol),
                expiry_date=dt.date.today().isoformat(),
            )
            expiries = self._extract_expiries(payload)
        except Exception as exc:
            logging.warning("Option contract lookup failed (%s); using instrument master", exc)
            expiries = self._expiries_from_master(symbol)
        if not expiries:
            expiries = self._expiries_from_master(symbol)
        if not expiries:
            raise RuntimeError(f"Unable to determine expiries for {symbol}")
        self._set_cached_expiries(symbol, expiries)
        return expiries

    def _extract_expiries(self, payload: dict) -> Tuple[str, ...]:
        data = payload.get("data") if isinstance(payload, dict) else None
        expiries = []
        if isinstance(data, dict):
            if isinstance(data.get("expiries"), list):
                expiries = [str(e) for e in data.get("expiries") if e]
            elif isinstance(data.get("contracts"), list):
                expiries = [c.get("expiry") for c in data["contracts"] if c.get("expiry")]
        expiries = sorted({e for e in expiries if e})
        return tuple(expiries)

    def _load_master_data(self) -> List[dict]:
        if MASTER_CACHE_PATH.exists():
            with gzip.open(MASTER_CACHE_PATH, "rt") as fh:
                return json.load(fh)
        try:
            resp = requests.get(MASTER_URL, timeout=30)
            resp.raise_for_status()
            MASTER_CACHE_PATH.parent.mkdir(parents=True, exist_ok=True)
            MASTER_CACHE_PATH.write_bytes(resp.content)
            with gzip.open(io.BytesIO(resp.content), "rt") as fh:
                return json.load(fh)
        except Exception as exc:
            logging.error("Failed to fetch instrument master: %s", exc)
            return []

    def _expiries_from_master(self, symbol: str) -> Tuple[str, ...]:
        data = self._load_master_data()
        if not data:
            return tuple()
        symbol = symbol.upper()
        expiries = set()
        for row in data:
            segment = str(row.get("segment") or row.get("Segment") or "").upper()
            if segment != "NSE_FO":
                continue
            inst_type = str(row.get("instrument_type") or row.get("instrumentType") or "").upper()
            if inst_type not in {"CE", "PE", "OPTIDX"}:
                continue
            name = str(row.get("name") or row.get("tradingsymbol") or row.get("trading_symbol") or "").upper()
            if symbol not in name:
                continue
            expiry_val = row.get("expiry") or row.get("expiry_date") or row.get("expiryDate")
            if not expiry_val:
                continue
            if isinstance(expiry_val, (int, float)):
                exp_dt = dt.datetime.fromtimestamp(float(expiry_val) / 1000.0).date()
            else:
                try:
                    exp_dt = dt.datetime.fromisoformat(str(expiry_val)).date()
                except ValueError:
                    continue
            expiries.add(exp_dt.isoformat())
        return tuple(sorted(expiries))

    def nearest_weekly_and_monthly_expiries(self, symbol: str, session: UpstoxSession) -> Tuple[str, Optional[str]]:
        expiries = self._fetch_expiries(symbol, session)
        weekly = expiries[0]
        weekly_dt = dt.date.fromisoformat(weekly)
        monthly = None
        for exp in expiries[1:]:
            exp_dt = dt.date.fromisoformat(exp)
            if exp_dt.month != weekly_dt.month or exp_dt.day >= 25:
                monthly = exp
                break
        if monthly is None and len(expiries) > 1:
            monthly = expiries[1]
        return weekly, monthly

    # ----- Option chain refresh -------------------------------------------
    def refresh_option_chain(self, symbol: str, expiry_date: str, session: UpstoxSession) -> int:
        underlying_key = self.resolve_index_key(symbol)
        payload = session.get_option_chain(underlying_key, expiry_date)
        contracts = self._parse_option_chain(payload)
        if not contracts:
            contracts = self._contracts_from_master(symbol, expiry_date)
        if not contracts:
            logging.warning("Option chain empty for %s %s", symbol, expiry_date)
            return 0
        now = self._now_str()
        rows = []
        meta_rows = []
        for item in contracts:
            rows.append(
                (
                    symbol.upper(),
                    expiry_date,
                    float(item["strike"]),
                    item["type"],
                    item["instrument_key"],
                    now,
                )
            )
            meta_rows.append(
                (
                    item["instrument_key"],
                    symbol.upper(),
                    expiry_date,
                    item["type"],
                    float(item["strike"]),
                    item.get("lot_size"),
                    item.get("tick_size"),
                    item.get("freeze_qty"),
                    item.get("price_band_low"),
                    item.get("price_band_high"),
                    now,
                )
            )
        with self._conn:
            self._conn.executemany(
                """
                INSERT INTO instrument_cache(symbol, expiry_date, strike, option_type, instrument_key, updated_at)
                VALUES (?,?,?,?,?,?)
                ON CONFLICT(symbol, expiry_date, strike, option_type)
                DO UPDATE SET instrument_key=excluded.instrument_key, updated_at=excluded.updated_at
                """,
                rows,
            )
            self._conn.executemany(
                """
                INSERT INTO instrument_meta(instrument_key, symbol, expiry_date, option_type, strike, lot_size, tick_size, freeze_qty, price_band_low, price_band_high, updated_at)
                VALUES (?,?,?,?,?,?,?,?,?,?,?)
                ON CONFLICT(instrument_key)
                DO UPDATE SET symbol=excluded.symbol, expiry_date=excluded.expiry_date, option_type=excluded.option_type,
                              strike=excluded.strike, lot_size=excluded.lot_size, tick_size=excluded.tick_size,
                              freeze_qty=excluded.freeze_qty, price_band_low=excluded.price_band_low,
                              price_band_high=excluded.price_band_high, updated_at=excluded.updated_at
                """,
                meta_rows,
            )
        logging.info("Cached %s contracts for %s %s", len(rows), symbol, expiry_date)
        return len(rows)

    def _parse_option_chain(self, payload: dict) -> Tuple[Dict[str, str], ...]:
        data = payload.get("data") if isinstance(payload, dict) else None
        if not isinstance(data, dict):
            return tuple()
        contracts: list[Dict[str, str]] = []

        def _collect(entries, opt_type):
            if not isinstance(entries, list):
                return
            for item in entries:
                if not isinstance(item, dict):
                    continue
                key = item.get("instrument_key") or item.get("instrumentKey")
                strike = item.get("strike") or item.get("strike_price") or item.get("strikePrice")
                if key and strike:
                    contracts.append({
                        "instrument_key": key,
                        "strike": float(strike),
                        "type": opt_type,
                        "lot_size": item.get("lot_size") or item.get("lotSize"),
                        "tick_size": item.get("tick_size") or item.get("tickSize"),
                        "freeze_qty": item.get("freeze_quantity") or item.get("freezeQuantity"),
                        "price_band_low": item.get("price_band_lower") or item.get("priceBandLower"),
                        "price_band_high": item.get("price_band_upper") or item.get("priceBandUpper"),
                    })

        calls = data.get("call") or data.get("CALL") or data.get("ce") or data.get("CE")
        puts = data.get("put") or data.get("PUT") or data.get("pe") or data.get("PE")
        if isinstance(calls, dict) and "data" in calls:
            calls = calls["data"]
        if isinstance(puts, dict) and "data" in puts:
            puts = puts["data"]
        _collect(calls, "CE")
        _collect(puts, "PE")

        if not contracts and isinstance(data.get("contracts"), list):
            for item in data["contracts"]:
                if not isinstance(item, dict):
                    continue
                opt_type = (item.get("option_type") or item.get("optionType") or "").upper()
                if opt_type not in {"CE", "PE"}:
                    continue
                key = item.get("instrument_key") or item.get("instrumentKey")
                strike = item.get("strike") or item.get("strike_price") or item.get("strikePrice")
                if key and strike:
                    contracts.append({
                        "instrument_key": key,
                        "strike": float(strike),
                        "type": opt_type,
                        "lot_size": item.get("lot_size") or item.get("lotSize"),
                        "tick_size": item.get("tick_size") or item.get("tickSize"),
                        "freeze_qty": item.get("freeze_quantity") or item.get("freezeQuantity"),
                        "price_band_low": item.get("price_band_lower") or item.get("priceBandLower"),
                        "price_band_high": item.get("price_band_upper") or item.get("priceBandUpper"),
                    })

        return tuple(contracts)

    def _contracts_from_master(self, symbol: str, expiry_date: str) -> Tuple[Dict[str, str], ...]:
        data = self._load_master_data()
        if not data:
            return tuple()
        target = dt.date.fromisoformat(expiry_date)
        symbol_upper = symbol.upper()
        contracts: list[Dict[str, str]] = []
        for row in data:
            segment = str(row.get("segment") or row.get("Segment") or "").upper()
            if segment != "NSE_FO":
                continue
            inst_type = str(row.get("instrument_type") or row.get("instrumentType") or "").upper()
            if inst_type not in {"CE", "PE"}:
                continue
            name = str(row.get("name") or row.get("tradingsymbol") or row.get("trading_symbol") or "").upper()
            if symbol_upper not in name:
                continue
            expiry_val = row.get("expiry") or row.get("expiry_date") or row.get("expiryDate")
            if not expiry_val:
                continue
            if isinstance(expiry_val, (int, float)):
                exp_dt = dt.datetime.fromtimestamp(float(expiry_val) / 1000.0).date()
            else:
                try:
                    exp_dt = dt.datetime.fromisoformat(str(expiry_val)).date()
                except ValueError:
                    continue
            if exp_dt != target:
                continue
            strike = row.get("strike_price") or row.get("strikePrice") or row.get("strike")
            instrument_key = row.get("instrument_key") or row.get("instrumentKey")
            if not strike or not instrument_key:
                continue
            contracts.append(
                {
                    "instrument_key": str(instrument_key),
                    "strike": float(strike),
                    "type": inst_type,
                    "lot_size": row.get("lot_size") or row.get("lotSize"),
                    "tick_size": row.get("tick_size") or row.get("tickSize"),
                    "freeze_qty": row.get("freeze_quantity") or row.get("freezeQuantity"),
                    "price_band_low": row.get("price_band_lower") or row.get("priceBandLower"),
                    "price_band_high": row.get("price_band_upper") or row.get("priceBandUpper"),
                }
            )
        return tuple(contracts)

    # ----- Lookup helpers -------------------------------------------------
    def lookup(self, symbol: str, expiry_date: str, strike: float, option_type: str) -> Optional[str]:
        cur = self._conn.execute(
            "SELECT instrument_key FROM instrument_cache WHERE symbol=? AND expiry_date=? AND strike=? AND option_type=?",
            (symbol.upper(), expiry_date, float(strike), option_type.upper()),
        )
        row = cur.fetchone()
        return row[0] if row else None

    def get_meta(self, instrument_key: str) -> Optional[InstrumentMeta]:
        cur = self._conn.execute(
            "SELECT instrument_key, symbol, expiry_date, option_type, strike, lot_size, tick_size, freeze_qty, price_band_low, price_band_high "
            "FROM instrument_meta WHERE instrument_key=?",
            (instrument_key,),
        )
        row = cur.fetchone()
        if not row:
            return None
        return InstrumentMeta(
            instrument_key=row["instrument_key"],
            symbol=row["symbol"],
            expiry_date=row["expiry_date"],
            option_type=row["option_type"],
            strike=row["strike"],
            lot_size=row["lot_size"],
            tick_size=row["tick_size"],
            freeze_qty=row["freeze_qty"],
            price_band_low=row["price_band_low"],
            price_band_high=row["price_band_high"],
        )

    def nearest_strikes(self, symbol: str, ltp: float, step: int = 50) -> Tuple[int, int]:
        atm = int(round(ltp / step) * step)
        return atm, atm

    def select_default_buy_contracts(self, symbol: str, ltp: float, expiry_date: str, step: int = 50, session: Optional[UpstoxSession] = None) -> Dict[str, Optional[str]]:
        atm_ce, atm_pe = self.nearest_strikes(symbol, ltp, step)
        ce = self.lookup(symbol, expiry_date, atm_ce, "CE")
        pe = self.lookup(symbol, expiry_date, atm_pe, "PE")
        if (not ce or not pe) and session is not None:
            self.refresh_option_chain(symbol, expiry_date, session)
            ce = ce or self.lookup(symbol, expiry_date, atm_ce, "CE")
            pe = pe or self.lookup(symbol, expiry_date, atm_pe, "PE")
        return {"CE": ce, "PE": pe}
