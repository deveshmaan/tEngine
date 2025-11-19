from __future__ import annotations

import datetime as dt
import logging
import os
import sqlite3
from dataclasses import dataclass
from pathlib import Path
from threading import Lock
from typing import Callable, ClassVar, Dict, List, Optional, Sequence, Tuple

from brokerage.upstox_client import INDEX_INSTRUMENT_KEYS, IST, UpstoxSession

SUPPORTED_OPT_TYPES = ("CE", "PE")
EXPIRY_TTL_SECONDS = int(os.getenv("OPTION_EXPIRY_TTL_SECONDS", "300"))
CONTRACT_TTL_SECONDS = int(os.getenv("OPTION_CONTRACT_TTL_SECONDS", "300"))


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
    """SQLite-backed option contract cache with expiry discovery and metadata."""

    _runtime: ClassVar[Optional["InstrumentCache"]] = None
    _runtime_lock: ClassVar[Lock] = Lock()

    def __init__(self, db_path: Optional[str] = None, *, session_factory: Optional[Callable[[], UpstoxSession]] = None):
        path = Path(db_path or os.getenv("ENGINE_DB_PATH", "engine_state.sqlite"))
        path.parent.mkdir(parents=True, exist_ok=True)
        self._conn = sqlite3.connect(path, check_same_thread=False)
        self._conn.row_factory = sqlite3.Row
        self._conn.execute("PRAGMA journal_mode=WAL;")
        self._conn.execute("PRAGMA synchronous=NORMAL;")
        self._logger = logging.getLogger("InstrumentCache")
        self._session_factory = session_factory or UpstoxSession
        self._session: Optional[UpstoxSession] = None
        self._init_schema()
        with self._runtime_lock:
            InstrumentCache._runtime = self

    # ------------------------------------------------------------------ runtime
    @classmethod
    def runtime_cache(cls) -> Optional["InstrumentCache"]:
        return cls._runtime

    # --------------------------------------------------------------------- schema
    def _init_schema(self) -> None:
        cur = self._conn.cursor()
        cur.executescript(
            """
            CREATE TABLE IF NOT EXISTS option_contracts (
                symbol TEXT NOT NULL,
                expiry TEXT NOT NULL,
                strike REAL NOT NULL,
                opt_type TEXT NOT NULL CHECK(opt_type IN ('CE','PE')),
                instrument_key TEXT NOT NULL,
                lot_size INT,
                tick_size REAL,
                band_low REAL,
                band_high REAL,
                updated_at TEXT NOT NULL,
                PRIMARY KEY(symbol, expiry, strike, opt_type)
            );
            CREATE TABLE IF NOT EXISTS expiries (
                symbol TEXT NOT NULL,
                expiry TEXT NOT NULL,
                kind TEXT NOT NULL CHECK(kind IN ('weekly','monthly')),
                updated_at TEXT NOT NULL,
                PRIMARY KEY(symbol, expiry)
            );
            CREATE TABLE IF NOT EXISTS meta (
                k TEXT PRIMARY KEY,
                v TEXT,
                updated_at TEXT NOT NULL
            );
            """
        )
        self._conn.commit()

    def close(self) -> None:
        self._conn.close()

    # -------------------------------------------------------------------- helpers
    def _now_str(self) -> str:
        return dt.datetime.now(IST).isoformat()

    def _ensure_session(self) -> UpstoxSession:
        if self._session is None:
            self._session = self._session_factory()
        return self._session

    def resolve_index_key(self, symbol: str) -> str:
        try:
            return INDEX_INSTRUMENT_KEYS[symbol.upper()]
        except KeyError as exc:
            raise ValueError(f"Unsupported symbol {symbol}") from exc

    # -------------------------------------------------------------- expiry cache
    def list_expiries(self, symbol: str, kind: Optional[str] = None) -> List[str]:
        symbol = symbol.upper()
        if not self._is_meta_fresh(self._expiry_meta(symbol), EXPIRY_TTL_SECONDS):
            self._refresh_expiries(symbol)
        query = "SELECT expiry FROM expiries WHERE symbol=?"
        params: List[object] = [symbol]
        if kind:
            query += " AND kind=?"
            params.append(kind.lower())
        query += " ORDER BY expiry"
        cur = self._conn.execute(query, tuple(params))
        return [str(row["expiry"]) for row in cur.fetchall()]

    def refresh_expiry(self, symbol: str, expiry: str, session: Optional[UpstoxSession] = None) -> int:
        """Fetch and persist all contracts for a given expiry."""

        session = session or self._ensure_session()
        payload = session.get_option_chain(self.resolve_index_key(symbol), expiry)
        contracts = self._parse_option_chain(payload)
        if not contracts:
            return 0
        now = self._now_str()
        rows = []
        expiry_rows = []
        for item in contracts:
            rows.append(
                (
                    symbol.upper(),
                    expiry,
                    float(item["strike"]),
                    item["type"],
                    item["instrument_key"],
                    item.get("lot_size"),
                    item.get("tick_size"),
                    item.get("price_band_low"),
                    item.get("price_band_high"),
                    now,
                )
            )
        expiry_rows.append((symbol.upper(), expiry, self._classify_expiry(expiry), now))
        with self._conn:
            self._conn.executemany(
                """
                INSERT INTO option_contracts(symbol, expiry, strike, opt_type, instrument_key, lot_size, tick_size, band_low, band_high, updated_at)
                VALUES (?,?,?,?,?,?,?,?,?,?)
                ON CONFLICT(symbol, expiry, strike, opt_type)
                DO UPDATE SET instrument_key=excluded.instrument_key,
                              lot_size=excluded.lot_size,
                              tick_size=excluded.tick_size,
                              band_low=excluded.band_low,
                              band_high=excluded.band_high,
                              updated_at=excluded.updated_at
                """,
                rows,
            )
            self._conn.executemany(
                """
                INSERT INTO expiries(symbol, expiry, kind, updated_at)
                VALUES (?,?,?,?)
                ON CONFLICT(symbol, expiry)
                DO UPDATE SET kind=excluded.kind, updated_at=excluded.updated_at
                """,
                expiry_rows,
            )
            self._upsert_meta(self._contracts_meta(symbol, expiry), now)
        return len(rows)

    def get_contract(self, symbol: str, expiry: str, strike: float, opt_type: str) -> Optional[Dict[str, object]]:
        symbol = symbol.upper()
        opt_type = opt_type.upper()
        if opt_type not in SUPPORTED_OPT_TYPES:
            return None
        self._ensure_contracts(symbol, expiry)
        cur = self._conn.execute(
            "SELECT * FROM option_contracts WHERE symbol=? AND expiry=? AND strike=? AND opt_type=?",
            (symbol, expiry, float(strike), opt_type),
        )
        row = cur.fetchone()
        if not row:
            return None
        return dict(row)

    def get_meta(self, key: str, expiry: Optional[str] = None):
        """Return contract meta for an instrument key or an expiry (tick, lot, bands)."""

        if expiry is None or "|" in key:
            return self._get_instrument_meta(key)
        return self._get_expiry_meta(key, expiry)

    def _get_expiry_meta(self, symbol: str, expiry: str) -> Optional[Tuple[float, int, float, float]]:
        symbol = symbol.upper()
        self._ensure_contracts(symbol, expiry)
        cur = self._conn.execute(
            "SELECT tick_size, lot_size, band_low, band_high FROM option_contracts WHERE symbol=? AND expiry=? ORDER BY updated_at DESC LIMIT 1",
            (symbol, expiry),
        )
        row = cur.fetchone()
        if not row:
            return None
        tick = float(row["tick_size"] or 0.0)
        lot = int(row["lot_size"] or 0)
        band_low = float(row["band_low"] or 0.0)
        band_high = float(row["band_high"] or 0.0)
        return (tick, lot, band_low, band_high)

    def _get_instrument_meta(self, instrument_key: str) -> Optional[InstrumentMeta]:
        cur = self._conn.execute(
            "SELECT * FROM option_contracts WHERE instrument_key=?",
            (instrument_key,),
        )
        row = cur.fetchone()
        if not row:
            return None
        return InstrumentMeta(
            instrument_key=row["instrument_key"],
            symbol=row["symbol"],
            expiry_date=row["expiry"],
            option_type=row["opt_type"],
            strike=row["strike"],
            lot_size=row["lot_size"],
            tick_size=row["tick_size"],
            freeze_qty=None,
            price_band_low=row["band_low"],
            price_band_high=row["band_high"],
        )

    # -------------------------------------------------------------- compat funcs
    def refresh_option_chain(self, symbol: str, expiry_date: str, session: Optional[UpstoxSession] = None) -> int:
        return self.refresh_expiry(symbol, expiry_date, session=session)

    def lookup(self, symbol: str, expiry_date: str, strike: float, option_type: str) -> Optional[str]:
        row = self.get_contract(symbol, expiry_date, strike, option_type)
        if row:
            return str(row.get("instrument_key"))
        return None

    def nearest_weekly_and_monthly_expiries(self, symbol: str, session: UpstoxSession) -> Tuple[str, Optional[str]]:
        expiries = self.list_expiries(symbol)
        if not expiries:
            raise RuntimeError(f"No expiries cached for {symbol}")
        weekly = expiries[0]
        monthly = None
        for exp in expiries:
            if exp != weekly and self._classify_expiry(exp) == "monthly":
                monthly = exp
                break
        return weekly, monthly

    def nearest_strikes(self, symbol: str, ltp: float, step: int = 50) -> Tuple[int, int]:
        atm = int(round(ltp / step) * step)
        return atm, atm

    def select_default_buy_contracts(self, symbol: str, ltp: float, expiry_date: str, step: int = 50, session: Optional[UpstoxSession] = None) -> Dict[str, Optional[str]]:
        atm_ce, atm_pe = self.nearest_strikes(symbol, ltp, step)
        ce = self.lookup(symbol, expiry_date, atm_ce, "CE")
        pe = self.lookup(symbol, expiry_date, atm_pe, "PE")
        if (not ce or not pe) and session is not None:
            self.refresh_expiry(symbol, expiry_date, session=session)
            ce = ce or self.lookup(symbol, expiry_date, atm_ce, "CE")
            pe = pe or self.lookup(symbol, expiry_date, atm_pe, "PE")
        return {"CE": ce, "PE": pe}

    # ------------------------------------------------------------------ internals
    def _expiry_meta(self, symbol: str) -> str:
        return f"expiries:{symbol}"

    def _contracts_meta(self, symbol: str, expiry: str) -> str:
        return f"contracts:{symbol}:{expiry}"

    def _is_meta_fresh(self, key: str, ttl_seconds: int) -> bool:
        cur = self._conn.execute("SELECT updated_at FROM meta WHERE k=?", (key,))
        row = cur.fetchone()
        if not row:
            return False
        updated = dt.datetime.fromisoformat(row["updated_at"]).astimezone(IST)
        return (dt.datetime.now(IST) - updated).total_seconds() < ttl_seconds

    def _upsert_meta(self, key: str, now: Optional[str] = None) -> None:
        ts = now or self._now_str()
        self._conn.execute(
            "INSERT INTO meta(k, v, updated_at) VALUES(?,?,?) ON CONFLICT(k) DO UPDATE SET v=excluded.v, updated_at=excluded.updated_at",
            (key, "1", ts),
        )

    def _refresh_expiries(self, symbol: str) -> None:
        session = self._ensure_session()
        payload = session.get_option_contracts(self.resolve_index_key(symbol))
        expiries = self._extract_expiries(payload)
        rows = [(symbol.upper(), expiry, self._classify_expiry(expiry), self._now_str()) for expiry in expiries]
        if not rows:
            raise RuntimeError(f"Unable to discover expiries for {symbol}")
        with self._conn:
            self._conn.execute("DELETE FROM expiries WHERE symbol=?", (symbol.upper(),))
            self._conn.executemany(
                "INSERT INTO expiries(symbol, expiry, kind, updated_at) VALUES (?,?,?,?)",
                rows,
            )
            self._upsert_meta(self._expiry_meta(symbol.upper()))

    def _ensure_contracts(self, symbol: str, expiry: str) -> None:
        if self._is_meta_fresh(self._contracts_meta(symbol, expiry), CONTRACT_TTL_SECONDS):
            return
        self.refresh_expiry(symbol, expiry)

    def _extract_expiries(self, payload: Dict[str, object]) -> List[str]:
        data = payload.get("data") if isinstance(payload, dict) else None
        expiries: List[str] = []
        if isinstance(data, dict) and isinstance(data.get("expiries"), Sequence):
            expiries = [str(item) for item in data.get("expiries", []) if item]
        if not expiries and isinstance(data, dict) and isinstance(data.get("contracts"), Sequence):
            for entry in data.get("contracts", []):
                if isinstance(entry, dict) and entry.get("expiry"):
                    expiries.append(str(entry["expiry"]))
        uniq = sorted({exp for exp in expiries})
        return uniq

    def _parse_option_chain(self, payload: Dict[str, object]) -> Tuple[Dict[str, object], ...]:
        data = payload.get("data") if isinstance(payload, dict) else None
        if not isinstance(data, dict):
            return tuple()
        contracts: List[Dict[str, object]] = []

        def _collect(entries: Optional[Sequence[Dict[str, object]]], opt_type: str) -> None:
            if not isinstance(entries, Sequence):
                return
            for item in entries:
                if not isinstance(item, dict):
                    continue
                key = item.get("instrument_key") or item.get("instrumentKey")
                strike = item.get("strike") or item.get("strike_price") or item.get("strikePrice")
                if not key or strike is None:
                    continue
                contracts.append(
                    {
                        "instrument_key": str(key),
                        "strike": float(strike),
                        "type": opt_type,
                        "lot_size": item.get("lot_size") or item.get("lotSize"),
                        "tick_size": item.get("tick_size") or item.get("tickSize"),
                        "price_band_low": item.get("price_band_lower") or item.get("priceBandLower"),
                        "price_band_high": item.get("price_band_upper") or item.get("priceBandUpper"),
                    }
                )

        calls = data.get("call") or data.get("CALL") or data.get("ce") or data.get("CE")
        puts = data.get("put") or data.get("PUT") or data.get("pe") or data.get("PE")
        if isinstance(calls, dict) and "data" in calls:
            calls = calls["data"]
        if isinstance(puts, dict) and "data" in puts:
            puts = puts["data"]
        _collect(calls, "CE")
        _collect(puts, "PE")
        if not contracts and isinstance(data.get("contracts"), Sequence):
            for item in data["contracts"]:
                if not isinstance(item, dict):
                    continue
                opt_type = (item.get("option_type") or item.get("optionType") or "").upper()
                if opt_type not in SUPPORTED_OPT_TYPES:
                    continue
                key = item.get("instrument_key") or item.get("instrumentKey")
                strike = item.get("strike") or item.get("strike_price") or item.get("strikePrice")
                if not key or strike is None:
                    continue
                contracts.append(
                    {
                        "instrument_key": str(key),
                        "strike": float(strike),
                        "type": opt_type,
                        "lot_size": item.get("lot_size") or item.get("lotSize"),
                        "tick_size": item.get("tick_size") or item.get("tickSize"),
                        "price_band_low": item.get("price_band_lower") or item.get("priceBandLower"),
                        "price_band_high": item.get("price_band_upper") or item.get("priceBandUpper"),
                    }
                )
        return tuple(contracts)

    def _classify_expiry(self, expiry: str) -> str:
        exp_date = dt.date.fromisoformat(expiry)
        next_week = exp_date + dt.timedelta(days=7)
        return "monthly" if next_week.month != exp_date.month else "weekly"


__all__ = ["InstrumentCache", "InstrumentMeta"]
