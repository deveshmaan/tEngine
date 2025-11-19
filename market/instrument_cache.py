from __future__ import annotations

import datetime as dt
import json
import logging
import os
import sqlite3
import time
from dataclasses import dataclass
from pathlib import Path
from threading import Lock
from typing import Any, Callable, ClassVar, Dict, Iterable, List, Optional, Sequence, Tuple

from upstox_client.rest import ApiException

from brokerage.upstox_client import IST, InvalidDateError, UpstoxSession
from engine.config import CONFIG
from engine.data import normalize_date
from engine.metrics import inc_expiry_attempt, inc_expiry_override_used, inc_expiry_success, set_expiry_source

SUPPORTED_OPT_TYPES = ("CE", "PE")
EXPIRY_TTL_SECONDS = int(os.getenv("OPTION_EXPIRY_TTL_SECONDS", "300"))
CONTRACT_TTL_SECONDS = int(os.getenv("OPTION_CONTRACT_TTL_SECONDS", "300"))
LOG = logging.getLogger(__name__)
_EXPIRY_SOURCE_CODES = {"contracts": 0, "instruments": 1, "override": 2}


def _underlying_key(symbol: str) -> str:
    symbol_upper = symbol.upper()
    if symbol_upper == "NIFTY":
        return "NSE_INDEX|Nifty 50"
    if symbol_upper == "BANKNIFTY":
        return "NSE_INDEX|Nifty Bank"
    raise ValueError("Unsupported symbol: %r" % symbol)


def _extract_expiries_from_contracts_payload(payload: Any) -> List[str]:
    data = getattr(payload, "data", None)
    if data is None and isinstance(payload, dict):
        data = payload.get("data")
    if not data:
        return []
    expiries: List[str] = []
    for row in data:
        exp = getattr(row, "expiry", None)
        if exp is None and isinstance(row, dict):
            exp = row.get("expiry")
        if not exp:
            continue
        try:
            expiries.append(normalize_date(exp))
        except Exception:
            continue
    return sorted(set(expiries))


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

    def __init__(
        self,
        db_path: Optional[str] = None,
        *,
        session_factory: Optional[Callable[[], UpstoxSession]] = None,
        weekly_expiry_weekday: int = 3,
        holidays: Optional[Iterable[dt.date]] = None,
        enable_remote_expiry_probe: bool = False,
        expiry_ttl_minutes: Optional[int] = None,
    ):
        path = Path(db_path or os.getenv("ENGINE_DB_PATH", "engine_state.sqlite"))
        path.parent.mkdir(parents=True, exist_ok=True)
        self._conn = sqlite3.connect(path, check_same_thread=False)
        self._conn.row_factory = sqlite3.Row
        self._conn.execute("PRAGMA journal_mode=WAL;")
        self._conn.execute("PRAGMA synchronous=NORMAL;")
        self._conn.execute("PRAGMA busy_timeout=5000;")
        self._logger = logging.getLogger("InstrumentCache")
        self._session_factory = session_factory or UpstoxSession
        self._session: Optional[UpstoxSession] = None
        self._weekly_expiry_weekday = weekly_expiry_weekday if 0 <= weekly_expiry_weekday <= 6 else 3
        self._holidays = {dt.date.fromisoformat(str(day)) if isinstance(day, str) else day for day in (holidays or [])}
        self._enable_remote_expiry_probe = enable_remote_expiry_probe
        cfg_data = getattr(CONFIG, "data", None)
        cfg_get = getattr(cfg_data, "get", None)
        cfg_ttl = None
        if cfg_get:
            try:
                cfg_ttl = int(cfg_get("expiry_ttl_minutes", 5))
            except (TypeError, ValueError):
                cfg_ttl = 5
        ttl_minutes = expiry_ttl_minutes if expiry_ttl_minutes is not None else cfg_ttl or 5
        self._cfg_allow_override = bool(cfg_get("allow_expiry_override", False)) if cfg_get else False
        override_path = cfg_get("expiry_override_path", None) if cfg_get else None
        self._cfg_override_path = str(override_path).strip() if override_path else ""
        try:
            self._cfg_override_max_age = int(cfg_get("expiry_override_max_age_minutes", 90)) if cfg_get else 90
        except (TypeError, ValueError):
            self._cfg_override_max_age = 90
        self._expiry_ttl_seconds = max(60, int(ttl_minutes) * 60)
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
        self._ensure_expiry_schema()
        self._conn.commit()

    def close(self) -> None:
        self._conn.close()

    def _ensure_expiry_schema(self) -> None:
        info = self._conn.execute("PRAGMA table_info(expiries)").fetchall()
        if not info:
            return
        columns = {row[1] for row in info}
        if "kind" not in columns:
            return
        now = self._now_str()
        with self._conn:
            self._conn.execute("ALTER TABLE expiries RENAME TO expiries_legacy")
            self._conn.execute(
                """
                CREATE TABLE expiries (
                    symbol TEXT NOT NULL,
                    expiry TEXT NOT NULL,
                    updated_at TEXT NOT NULL,
                    PRIMARY KEY(symbol, expiry)
                )
                """
            )
            self._conn.execute(
                "INSERT OR IGNORE INTO expiries(symbol, expiry, updated_at) SELECT symbol, expiry, COALESCE(updated_at, ?) FROM expiries_legacy",
                (now,),
            )
            self._conn.execute("DROP TABLE expiries_legacy")

    # -------------------------------------------------------------------- helpers
    def _now_str(self) -> str:
        return dt.datetime.now(IST).isoformat()

    def _ensure_session(self) -> UpstoxSession:
        if self._session is None:
            self._session = self._session_factory()
        return self._session

    def upstox_session(self) -> UpstoxSession:
        return self._ensure_session()

    def resolve_index_key(self, symbol: str) -> str:
        return _underlying_key(symbol)

    # -------------------------------------------------------------- expiry cache
    def list_expiries(self, symbol: str, kind: Optional[str] = None) -> List[str]:
        symbol = symbol.upper()
        self._ensure_expiries(symbol)
        cur = self._conn.execute("SELECT expiry FROM expiries WHERE symbol=? ORDER BY expiry", (symbol,))
        expiries: List[str] = []
        for row in cur.fetchall():
            try:
                expiries.append(self._normalize_date_str(str(row["expiry"])))
            except ValueError:
                continue
        if kind:
            target = kind.lower()
            expiries = [exp for exp in expiries if self._classify_expiry(exp, symbol=symbol) == target]
        return sorted(expiries)

    def _ensure_expiries(self, symbol: str) -> None:
        if self._is_meta_fresh(self._expiry_meta(symbol), self._expiry_ttl_seconds):
            return
        self._refresh_expiries(symbol)

    def refresh_expiry(self, symbol: str, expiry: str, session: Optional[UpstoxSession] = None) -> int:
        """Fetch and persist all contracts for a given expiry."""

        from engine import data as data_mod

        expiry = data_mod.assert_valid_expiry(symbol, expiry)
        session = session or self._ensure_session()
        key = self.resolve_index_key(symbol)
        self._logger.debug("Refreshing option chain for %s expiry %s (key=%s)", symbol, expiry, key)
        contracts: Tuple[Dict[str, object], ...] = tuple()
        payload: Optional[Dict[str, object]] = None
        try:
            payload = session.get_option_contracts(key, expiry)
        except ApiException:
            payload = None
        if payload:
            contracts = self._parse_option_chain(payload)
        if not contracts:
            payload = session.get_option_chain(key, expiry)
            contracts = self._parse_option_chain(payload)
        if not contracts:
            return 0
        now = self._now_str()
        rows = []
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
            self._conn.execute(
                """
                INSERT INTO expiries(symbol, expiry, updated_at)
                VALUES (?,?,?)
                ON CONFLICT(symbol, expiry)
                DO UPDATE SET updated_at=excluded.updated_at
                """,
                (symbol.upper(), expiry, now),
            )
            self._upsert_meta(self._contracts_meta(symbol, expiry), now)
        return len(rows)

    def get_contract(self, symbol: str, expiry: str, strike: float, opt_type: str) -> Optional[Dict[str, object]]:
        from engine import data as data_mod

        symbol = symbol.upper()
        opt_type = opt_type.upper()
        expiry = data_mod.assert_valid_expiry(symbol, expiry)
        if opt_type not in SUPPORTED_OPT_TYPES:
            return None
        self._ensure_contracts(symbol, expiry)
        cur = self._conn.execute(
            "SELECT * FROM option_contracts WHERE symbol=? AND expiry=? AND strike=? AND opt_type=?",
            (symbol, expiry, float(strike), opt_type),
        )
        row = cur.fetchone()
        if not row:
            try:
                self.refresh_expiry(symbol, expiry)
            except Exception:
                return None
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
            if exp != weekly and self._classify_expiry(exp, symbol=symbol) == "monthly":
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
        key = _underlying_key(symbol)
        if os.getenv("UPSTOX_SDK_DEBUG", "").lower() in {"1", "true", "yes"}:
            LOG.debug("refresh_expiries(discovery): symbol=%s key=%s", symbol, key)
        expiries: List[str] = []
        source_code: Optional[int] = None
        inc_expiry_attempt("contracts")
        try:
            payload = session.get_option_contracts(key)
        except InvalidDateError:
            LOG.warning("OptionContracts discovery failed for %s; falling back to instruments", symbol)
        else:
            expiries = _extract_expiries_from_contracts_payload(payload)
            if expiries:
                inc_expiry_success("contracts")
                source_code = _EXPIRY_SOURCE_CODES["contracts"]

        if not expiries:
            LOG.warning("OptionContracts discovery returned empty for %s. Falling back to Instruments.", symbol)
            inc_expiry_attempt("instruments")
            expiries = self._fallback_expiries_via_instruments(symbol)
            if expiries:
                inc_expiry_success("instruments")
                source_code = _EXPIRY_SOURCE_CODES["instruments"]

        if not expiries and self._cfg_allow_override:
            inc_expiry_attempt("override")
            override = self._load_expiry_override(symbol)
            if override:
                earliest = override[0]
                if self._validate_expiry_with_broker(symbol, earliest):
                    LOG.warning("Using local expiry override for %s: %s", symbol, earliest)
                    expiries = override
                    inc_expiry_success("override")
                    inc_expiry_override_used()
                    source_code = _EXPIRY_SOURCE_CODES["override"]

        if not expiries:
            raise RuntimeError("Broker returned no expiries for %s" % symbol)

        self._upsert_expiries(symbol, expiries)
        set_expiry_source(symbol.upper(), source_code if source_code is not None else _EXPIRY_SOURCE_CODES["contracts"])

    def _upsert_expiries(self, symbol: str, expiries: Sequence[str]) -> None:
        now = self._now_str()
        rows = [(symbol.upper(), expiry, now) for expiry in expiries]
        with self._conn:
            self._conn.execute("DELETE FROM expiries WHERE symbol=?", (symbol.upper(),))
            self._conn.executemany(
                "INSERT INTO expiries(symbol, expiry, updated_at) VALUES (?,?,?)",
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
        if not expiries and isinstance(data, Sequence):
            for entry in data:
                if isinstance(entry, dict) and entry.get("expiry"):
                    expiries.append(str(entry["expiry"]))
        normalized: List[str] = []
        for exp in expiries:
            try:
                normalized.append(self._normalize_date_str(exp))
            except ValueError:
                continue
        uniq = sorted(set(normalized))
        return uniq

    def _normalize_date_str(self, value: str) -> str:
        return normalize_date(value)

    def _parse_option_chain(self, payload: Dict[str, object]) -> Tuple[Dict[str, object], ...]:
        data = payload.get("data") if isinstance(payload, dict) else None
        if not isinstance(data, dict):
            if isinstance(data, Sequence):
                return self._parse_contract_sequence(data)
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
        if not contracts:
            sequence = data.get("contracts") if isinstance(data.get("contracts"), Sequence) else None
            if sequence:
                contracts.extend(self._parse_contract_sequence(sequence))
        return tuple(contracts)

    def _parse_contract_sequence(self, entries: Sequence[Dict[str, object]]) -> List[Dict[str, object]]:
        contracts: List[Dict[str, object]] = []
        for item in entries:
            if not isinstance(item, dict):
                continue
            opt_type = (item.get("option_type") or item.get("optionType") or item.get("instrument_type") or "").upper()
            if opt_type not in SUPPORTED_OPT_TYPES:
                continue
            key = item.get("instrument_key") or item.get("instrumentKey")
            strike = item.get("strike") or item.get("strike_price") or item.get("strikePrice") or item.get("strike_price_value")
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
        return contracts

    def _classify_expiry(self, expiry: str, *, symbol: Optional[str] = None) -> str:
        if symbol and symbol.upper() == "BANKNIFTY":
            return "monthly"
        exp_date = dt.date.fromisoformat(expiry)
        next_week = exp_date + dt.timedelta(days=7)
        return "monthly" if next_week.month != exp_date.month else "weekly"

    def _fallback_expiries_via_instruments(self, symbol: str) -> List[str]:
        try:
            rows = self._load_instruments_json()
        except Exception:
            return []
        key = _underlying_key(symbol)
        expiries: List[str] = []
        for row in rows:
            if not isinstance(row, dict):
                continue
            if row.get("instrument_type") != "OPTIDX":
                continue
            if row.get("underlying_key") != key:
                continue
            expiry = row.get("expiry")
            if not expiry:
                continue
            try:
                expiries.append(normalize_date(expiry))
            except Exception:
                continue
        return sorted(set(expiries))

    def _validate_expiry_with_broker(self, symbol: str, expiry: str) -> bool:
        session = self._ensure_session()
        key = _underlying_key(symbol)
        try:
            payload = session.get_option_contracts(key, expiry_date=expiry)
        except Exception as exc:  # pragma: no cover - network dependent
            LOG.warning("validate_expiry broker call failed: %s", exc)
            return False
        exps = _extract_expiries_from_contracts_payload(payload)
        return bool(exps)

    def _load_expiry_override(self, symbol: str) -> List[str]:
        allow = bool(self._cfg_allow_override)
        path = self._cfg_override_path
        max_age_min = int(self._cfg_override_max_age or 90)
        if not allow or not path or not os.path.exists(path):
            return []
        try:
            with open(path, "r", encoding="utf-8") as handle:
                payload = json.load(handle)
        except Exception:
            LOG.warning("Failed to read expiry override file %s", path)
            return []
        generated_at = payload.get("generated_at")
        if not generated_at:
            return []
        try:
            ts = str(generated_at).replace("Z", "+00:00")
            epoch = int(dt.datetime.fromisoformat(ts).timestamp())
            if time.time() - epoch > max_age_min * 60:
                LOG.warning("Expiry override stale: generated_at=%s (> %s min)", generated_at, max_age_min)
                return []
        except Exception:
            return []
        block = payload.get("underlyings", {}).get(symbol.upper(), [])
        expiries: List[str] = []
        for exp in block:
            try:
                expiries.append(normalize_date(exp))
            except Exception:
                continue
        return sorted(set(expiries))

    def _load_instruments_json(self) -> List[Dict[str, Any]]:
        path = Path(os.getenv("INSTRUMENTS_JSON_PATH", "universe.json"))
        if not path.exists():
            return []
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
        except Exception:
            LOG.warning("Failed to load fallback instruments JSON from %s", path)
            return []
        if isinstance(payload, list):
            return [row for row in payload if isinstance(row, dict)]
        if isinstance(payload, dict):
            entries = payload.get("instruments")
            if isinstance(entries, list):
                return [row for row in entries if isinstance(row, dict)]
            return [payload]
        return []


__all__ = ["InstrumentCache", "InstrumentMeta"]
