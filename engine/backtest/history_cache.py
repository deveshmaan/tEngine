from __future__ import annotations

import datetime as dt
import os
import sqlite3
from dataclasses import dataclass
from pathlib import Path
from typing import Callable, Iterable, List, Optional, Sequence, Tuple

from engine.config import IST


class HistoryCacheError(RuntimeError):
    """Raised when the on-disk history cache cannot be read/written."""


@dataclass(frozen=True)
class MissingRange:
    start_ts: int
    end_ts: int


MARKET_OPEN = dt.time(9, 15)
MARKET_CLOSE = dt.time(15, 30)


def _coerce_date(value: object) -> Optional[dt.date]:
    if value is None:
        return None
    if isinstance(value, dt.datetime):
        return value.date()
    if isinstance(value, dt.date):
        return value
    text = str(value).strip()
    if not text:
        return None
    try:
        return dt.date.fromisoformat(text)
    except Exception:
        return None


def _holiday_set(holiday_calendar: Optional[Iterable[object]]) -> set[dt.date]:
    out: set[dt.date] = set()
    for item in holiday_calendar or ():
        d = _coerce_date(item)
        if d is not None:
            out.add(d)
    return out


def _iter_dates(start: dt.date, end: dt.date) -> Iterable[dt.date]:
    cursor = start
    while cursor <= end:
        yield cursor
        cursor = cursor + dt.timedelta(days=1)


def _coalesce_date_ranges(ranges: Sequence[Tuple[dt.date, dt.date]]) -> List[Tuple[dt.date, dt.date]]:
    ordered = sorted((s, e) for s, e in ranges if isinstance(s, dt.date) and isinstance(e, dt.date) and e >= s)
    if not ordered:
        return []
    out: List[Tuple[dt.date, dt.date]] = [ordered[0]]
    for start, end in ordered[1:]:
        prev_start, prev_end = out[-1]
        if start <= (prev_end + dt.timedelta(days=1)):
            out[-1] = (prev_start, max(prev_end, end))
        else:
            out.append((start, end))
    return out


def normalize_interval(interval: str) -> str:
    text = str(interval or "").strip().lower()
    if not text:
        raise ValueError("interval must be non-empty")
    if text in {"1minute", "minute", "1min", "min", "m", "1m"}:
        return "1minute"
    if text in {"5minute", "5min", "5m"}:
        return "5minute"
    if text in {"15minute", "15min", "15m"}:
        return "15minute"
    if text in {"30minute", "30min", "30m"}:
        return "30minute"
    if text in {"day", "1day", "d", "1d"}:
        return "1day"
    raise ValueError(f"Unsupported interval: {interval!r}")


def interval_seconds(interval: str) -> int:
    key = normalize_interval(interval)
    if key.endswith("minute"):
        return int(key.removesuffix("minute")) * 60
    if key == "1day":
        return 24 * 60 * 60
    raise ValueError(f"Unsupported interval: {interval!r}")


def _ensure_tz(dt_value: dt.datetime, tz: dt.tzinfo) -> dt.datetime:
    if not isinstance(dt_value, dt.datetime):
        raise TypeError(f"Expected datetime, got {type(dt_value)!r}")
    if dt_value.tzinfo is None:
        return dt_value.replace(tzinfo=tz)
    return dt_value.astimezone(tz)


def _coalesce_ranges(ranges: Sequence[Tuple[int, int]]) -> List[Tuple[int, int]]:
    ordered = sorted((int(s), int(e)) for s, e in ranges if e >= s)
    if not ordered:
        return []
    out: List[Tuple[int, int]] = [ordered[0]]
    for start, end in ordered[1:]:
        prev_start, prev_end = out[-1]
        if start <= prev_end + 1:
            out[-1] = (prev_start, max(prev_end, end))
        else:
            out.append((start, end))
    return out


def _missing_ranges(covered: Sequence[Tuple[int, int]], start_ts: int, end_ts: int) -> List[MissingRange]:
    if end_ts < start_ts:
        return []
    covered_merged = _coalesce_ranges(covered)
    cursor = int(start_ts)
    out: List[MissingRange] = []
    for seg_start, seg_end in covered_merged:
        if seg_end < cursor:
            continue
        if seg_start > end_ts:
            break
        if seg_start > cursor:
            out.append(MissingRange(start_ts=cursor, end_ts=min(end_ts, seg_start - 1)))
        cursor = max(cursor, seg_end + 1)
        if cursor > end_ts:
            break
    if cursor <= end_ts:
        out.append(MissingRange(start_ts=cursor, end_ts=end_ts))
    return out


class HistoryCache:
    """
    SQLite-backed cache for historical candles (underlyings + options).

    Schema (v3):
      candles(instrument_key TEXT, interval TEXT, ts INTEGER, open REAL, high REAL, low REAL, close REAL,
              volume REAL, oi REAL NULL, source TEXT, PRIMARY KEY(instrument_key, interval, ts))
      coverage(instrument_key TEXT, interval TEXT, start_ts INTEGER, end_ts INTEGER)

    Backward compatibility:
      Migrates older schemas that used `key` and/or lacked `source`.
    """

    def __init__(self, path: Optional[Path | str] = None, *, tz: dt.tzinfo = IST) -> None:
        self._tz = tz
        default_path = os.getenv("HISTORY_CACHE_PATH", "cache/history.sqlite")
        self._path = Path(path) if path is not None else Path(default_path)
        self._path.parent.mkdir(parents=True, exist_ok=True)
        self._init_db()

    @property
    def path(self) -> Path:
        return self._path

    def ensure_range(
        self,
        instrument_key: Optional[str],
        interval: str,
        start_ts: int,
        end_ts: int,
        fetch_fn: Callable[[str, str, dt.datetime, dt.datetime], "pd.DataFrame"],
        *,
        key: Optional[str] = None,  # deprecated alias
        source: str = "upstox",
        holiday_calendar: Optional[Iterable[object]] = None,
    ) -> List[MissingRange]:
        import pandas as pd  # type: ignore

        instrument_key = str(instrument_key or key or "").strip()
        if not instrument_key:
            raise ValueError("instrument_key must be non-empty")
        interval_key = normalize_interval(interval)
        start_ts_i = int(start_ts)
        end_ts_i = int(end_ts)
        if end_ts_i < start_ts_i:
            raise ValueError("Invalid range: end_ts < start_ts")

        gap = self.gap_info(
            instrument_key=instrument_key,
            interval=interval_key,
            start_ts=start_ts_i,
            end_ts=end_ts_i,
            holiday_calendar=holiday_calendar,
        )
        missing_ranges: List[MissingRange] = list(gap.get("missing_ranges") or [])
        if not missing_ranges:
            return []

        # Convert missing ranges -> date windows and chunk for Upstox compatibility.
        missing_date_ranges: List[Tuple[dt.date, dt.date]] = []
        for seg in missing_ranges:
            seg_start_date = dt.datetime.fromtimestamp(int(seg.start_ts), tz=dt.timezone.utc).astimezone(self._tz).date()
            seg_end_date = dt.datetime.fromtimestamp(int(seg.end_ts), tz=dt.timezone.utc).astimezone(self._tz).date()
            missing_date_ranges.append((seg_start_date, seg_end_date))
        merged_date_ranges = _coalesce_date_ranges(missing_date_ranges)

        fetch_windows: List[Tuple[dt.date, dt.date]] = []
        try:
            bar_secs = interval_seconds(interval_key)
        except Exception:
            bar_secs = 0

        def _month_end(d: dt.date) -> dt.date:
            next_month = (d.replace(day=1) + dt.timedelta(days=32)).replace(day=1)
            return next_month - dt.timedelta(days=1)

        for start_d, end_d in merged_date_ranges:
            if bar_secs > 0 and bar_secs <= 15 * 60 and interval_key.endswith("minute"):
                cursor = start_d
                while cursor <= end_d:
                    chunk_end = min(end_d, _month_end(cursor))
                    fetch_windows.append((cursor, chunk_end))
                    cursor = chunk_end + dt.timedelta(days=1)
            else:
                fetch_windows.append((start_d, end_d))

        for start_d, end_d in fetch_windows:
            if bar_secs > 0 and interval_key.endswith("minute") and interval_key != "1day":
                seg_start = dt.datetime.combine(start_d, MARKET_OPEN, tzinfo=self._tz)
                seg_end = dt.datetime.combine(end_d, MARKET_CLOSE, tzinfo=self._tz)
            else:
                seg_start = dt.datetime.combine(start_d, dt.time(0, 0), tzinfo=self._tz)
                seg_end = dt.datetime.combine(end_d, dt.time(23, 59, 59), tzinfo=self._tz)
            df = fetch_fn(instrument_key, interval_key, seg_start, seg_end)
            if df is None:
                df = pd.DataFrame()
            self.store(df, instrument_key=instrument_key, interval=interval_key, source=source)

        return missing_ranges

    def expected_timestamps(
        self,
        *,
        interval: str,
        start_ts: int,
        end_ts: int,
        holiday_calendar: Optional[Iterable[object]] = None,
    ) -> List[int]:
        """
        Return expected bar open timestamps (UTC epoch seconds) for the interval
        during NSE market hours between [start_ts, end_ts].
        """

        interval_key = normalize_interval(interval)
        start_ts_i = int(start_ts)
        end_ts_i = int(end_ts)
        if end_ts_i < start_ts_i:
            return []

        start_dt = dt.datetime.fromtimestamp(start_ts_i, tz=dt.timezone.utc).astimezone(self._tz)
        end_dt = dt.datetime.fromtimestamp(end_ts_i, tz=dt.timezone.utc).astimezone(self._tz)
        holidays = _holiday_set(holiday_calendar)

        expected: List[int] = []
        if interval_key == "1day":
            for day in _iter_dates(start_dt.date(), end_dt.date()):
                if day.weekday() >= 5 or day in holidays:
                    continue
                bar_dt = dt.datetime.combine(day, dt.time(0, 0), tzinfo=self._tz)
                ts_epoch = int(bar_dt.astimezone(dt.timezone.utc).timestamp())
                if start_ts_i <= ts_epoch <= end_ts_i:
                    expected.append(ts_epoch)
            return expected

        bar_secs = interval_seconds(interval_key)
        for day in _iter_dates(start_dt.date(), end_dt.date()):
            if day.weekday() >= 5 or day in holidays:
                continue
            session_start = dt.datetime.combine(day, MARKET_OPEN, tzinfo=self._tz)
            session_end = dt.datetime.combine(day, MARKET_CLOSE, tzinfo=self._tz)
            cursor = session_start
            while cursor < session_end:
                ts_epoch = int(cursor.astimezone(dt.timezone.utc).timestamp())
                if start_ts_i <= ts_epoch <= end_ts_i:
                    expected.append(ts_epoch)
                cursor = cursor + dt.timedelta(seconds=int(bar_secs))
        return expected

    def gap_info(
        self,
        *,
        instrument_key: str,
        interval: str,
        start_ts: int,
        end_ts: int,
        holiday_calendar: Optional[Iterable[object]] = None,
    ) -> Dict[str, object]:
        """
        Compute expected vs present candles and return missing segments.

        Returned dict contains:
          - expected_bars: int
          - present_bars: int (present among expected)
          - missing_bars: int
          - missing_ranges: list[MissingRange]
        """

        instrument_key = str(instrument_key or "").strip()
        if not instrument_key:
            raise ValueError("instrument_key must be non-empty")
        interval_key = normalize_interval(interval)
        start_ts_i = int(start_ts)
        end_ts_i = int(end_ts)
        expected = self.expected_timestamps(interval=interval_key, start_ts=start_ts_i, end_ts=end_ts_i, holiday_calendar=holiday_calendar)
        expected_set = set(int(x) for x in expected)
        if not expected_set:
            return {"expected_bars": 0, "present_bars": 0, "missing_bars": 0, "missing_ranges": []}

        with self._connect() as conn:
            rows = conn.execute(
                "SELECT ts FROM candles WHERE instrument_key=? AND interval=? AND ts>=? AND ts<=?",
                (instrument_key, interval_key, start_ts_i, end_ts_i),
            ).fetchall()
        present_set = {int(r[0]) for r in rows if r and r[0] is not None}

        missing_ts = sorted(expected_set - present_set)
        try:
            step_secs = interval_seconds(interval_key)
        except Exception:
            step_secs = 0
        missing_ranges: List[MissingRange] = []
        if missing_ts and step_secs > 0:
            start_seg = int(missing_ts[0])
            prev = int(missing_ts[0])
            for ts_val in missing_ts[1:]:
                ts_i = int(ts_val)
                if ts_i == prev + int(step_secs):
                    prev = ts_i
                    continue
                missing_ranges.append(MissingRange(start_ts=start_seg, end_ts=prev))
                start_seg = ts_i
                prev = ts_i
            missing_ranges.append(MissingRange(start_ts=start_seg, end_ts=prev))
        elif missing_ts:
            # Fallback: treat as a single coarse segment.
            missing_ranges = [MissingRange(start_ts=int(missing_ts[0]), end_ts=int(missing_ts[-1]))]

        missing_bars = len(missing_ts)
        present_bars = len(expected_set) - missing_bars
        return {
            "expected_bars": int(len(expected_set)),
            "present_bars": int(present_bars),
            "missing_bars": int(missing_bars),
            "missing_ranges": missing_ranges,
        }

    def load(
        self,
        *,
        instrument_key: Optional[str] = None,
        key: Optional[str] = None,  # deprecated alias
        interval: str,
        start_ts: int,
        end_ts: int,
    ) -> "pd.DataFrame":
        import pandas as pd  # type: ignore

        instrument_key = str(instrument_key or key or "").strip()
        if not instrument_key:
            raise ValueError("instrument_key must be non-empty")
        interval_key = normalize_interval(interval)
        start_ts_i = int(start_ts)
        end_ts_i = int(end_ts)
        if end_ts_i < start_ts_i:
            raise ValueError("Invalid range: end_ts < start_ts")

        with self._connect() as conn:
            cur = conn.execute(
                """
                SELECT ts, open, high, low, close, volume, oi, source
                FROM candles
                WHERE instrument_key = ? AND interval = ? AND ts >= ? AND ts <= ?
                ORDER BY ts ASC
                """,
                (instrument_key, interval_key, start_ts_i, end_ts_i),
            )
            rows = cur.fetchall()

        if not rows:
            return pd.DataFrame(
                columns=["ts", "open", "high", "low", "close", "volume", "oi", "source", "instrument_key", "key", "interval"]
            )

        df = pd.DataFrame(rows, columns=["ts", "open", "high", "low", "close", "volume", "oi", "source"])
        df["ts"] = pd.to_datetime(df["ts"], unit="s", utc=True).dt.tz_convert(self._tz)
        df["instrument_key"] = instrument_key
        df["key"] = instrument_key  # compatibility
        df["interval"] = interval_key
        df = df.drop_duplicates(subset=["ts"], keep="last").sort_values("ts").reset_index(drop=True)
        return df

    def store(
        self,
        df: "pd.DataFrame",
        *,
        instrument_key: Optional[str] = None,
        key: Optional[str] = None,  # deprecated alias
        interval: Optional[str] = None,
        source: str = "upstox",
    ) -> None:
        import pandas as pd  # type: ignore

        if df is None or df.empty:
            return

        df_local = df.copy()
        if instrument_key is not None or key is not None:
            df_local["instrument_key"] = str(instrument_key or key)
            df_local["key"] = str(instrument_key or key)  # compatibility
        if interval is not None:
            df_local["interval"] = normalize_interval(str(interval))
        if "instrument_key" not in df_local.columns and "key" in df_local.columns:
            df_local["instrument_key"] = df_local["key"].astype(str)
        required = {"ts", "open", "high", "low", "close", "volume", "instrument_key", "interval"}
        missing = required - set(df_local.columns)
        if missing:
            raise ValueError(f"Cannot store candles; missing columns: {sorted(missing)}")

        # Normalize key columns.
        cache_key = str(df_local["instrument_key"].iloc[0])
        interval_key = normalize_interval(str(df_local["interval"].iloc[0]))

        ts_series = pd.to_datetime(df_local["ts"], utc=True, errors="coerce")
        if ts_series.isna().any():
            raise ValueError("Cannot store candles; some timestamps could not be parsed")

        # Normalize timestamps to bar open (IST) to avoid off-by-one boundary drift.
        try:
            bar_secs = interval_seconds(interval_key)
        except Exception:
            bar_secs = 0

        ts_list_ist: List[dt.datetime] = [t.to_pydatetime().astimezone(self._tz) for t in ts_series.tolist()]

        if bar_secs > 0 and interval_key != "1day" and interval_key.endswith("minute"):
            close_labeled_days = {
                t.date() for t in ts_list_ist if t.replace(second=0, microsecond=0).time() == MARKET_CLOSE
            }
            normalized: List[dt.datetime] = []
            for t in ts_list_ist:
                t = _ensure_tz(t, self._tz).replace(second=0, microsecond=0)
                if t.date() in close_labeled_days:
                    t = t - dt.timedelta(seconds=int(bar_secs))
                session_start = dt.datetime.combine(t.date(), MARKET_OPEN, tzinfo=self._tz)
                session_end = dt.datetime.combine(t.date(), MARKET_CLOSE, tzinfo=self._tz)
                if t >= session_end:
                    t = session_end - dt.timedelta(seconds=int(bar_secs))
                if t < session_start:
                    t = session_start
                delta = int((t - session_start).total_seconds())
                aligned = session_start + dt.timedelta(seconds=(delta // int(bar_secs)) * int(bar_secs))
                if aligned >= session_end:
                    aligned = session_end - dt.timedelta(seconds=int(bar_secs))
                normalized.append(aligned)
            ts_list_ist = normalized

        ts_epoch_list = [int(t.astimezone(dt.timezone.utc).timestamp()) for t in ts_list_ist]
        df_local["_ts_epoch"] = ts_epoch_list
        df_local = df_local.drop_duplicates(subset=["_ts_epoch"], keep="last").reset_index(drop=True)
        ts_epoch = df_local["_ts_epoch"].astype("int64")

        oi_values: Iterable[Optional[float]]
        if "oi" in df_local.columns:
            oi_values = [None if pd.isna(v) else float(v) for v in df_local["oi"].tolist()]
        else:
            oi_values = [None] * len(df_local)

        src = str(source or "upstox")
        records = list(
            zip(
                [cache_key] * len(df_local),
                [interval_key] * len(df_local),
                ts_epoch.tolist(),
                df_local["open"].astype(float).tolist(),
                df_local["high"].astype(float).tolist(),
                df_local["low"].astype(float).tolist(),
                df_local["close"].astype(float).tolist(),
                df_local["volume"].astype(float).tolist(),
                list(oi_values),
                [src] * len(df_local),
            )
        )

        with self._connect() as conn:
            conn.executemany(
                """
                INSERT OR REPLACE INTO candles
                (instrument_key, interval, ts, open, high, low, close, volume, oi, source)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                records,
            )

    # ------------------------------------------------------------------ internals
    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(str(self._path))
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA journal_mode=WAL;")
        conn.execute("PRAGMA synchronous=NORMAL;")
        conn.execute("PRAGMA busy_timeout=5000;")
        return conn

    def _table_columns(self, conn: sqlite3.Connection, table: str) -> List[str]:
        info = conn.execute(f"PRAGMA table_info({table})").fetchall()
        return [str(row[1]) for row in info]

    def _init_db(self) -> None:
        try:
            with self._connect() as conn:
                conn.execute("BEGIN")
                # Create tables if missing, else migrate legacy schema.
                existing = conn.execute(
                    "SELECT name FROM sqlite_master WHERE type='table' AND name IN ('candles','coverage')"
                ).fetchall()
                tables = {row[0] for row in existing}
                if "candles" in tables:
                    self._migrate_if_needed(conn)
                self._create_schema_if_missing(conn)
                conn.execute("COMMIT")
        except Exception as exc:
            raise HistoryCacheError(f"Failed to initialize history cache at {self._path}") from exc

    def _create_schema_if_missing(self, conn: sqlite3.Connection) -> None:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS candles (
                instrument_key TEXT NOT NULL,
                interval TEXT NOT NULL,
                ts INTEGER NOT NULL,
                open REAL NOT NULL,
                high REAL NOT NULL,
                low REAL NOT NULL,
                close REAL NOT NULL,
                volume REAL NOT NULL,
                oi REAL NULL,
                source TEXT NOT NULL,
                PRIMARY KEY (instrument_key, interval, ts)
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS coverage (
                instrument_key TEXT NOT NULL,
                interval TEXT NOT NULL,
                start_ts INTEGER NOT NULL,
                end_ts INTEGER NOT NULL
            )
            """
        )
        conn.execute("CREATE INDEX IF NOT EXISTS idx_candles_instrument_ts ON candles(instrument_key, interval, ts)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_coverage_instrument ON coverage(instrument_key, interval, start_ts, end_ts)")

    def _migrate_if_needed(self, conn: sqlite3.Connection) -> None:
        cols = set(self._table_columns(conn, "candles"))
        if "key" in cols:
            self._migrate_key_to_instrument_key(conn)
            cols = set(self._table_columns(conn, "candles"))
        if "instrument_key" in cols and "source" not in cols:
            conn.execute("ALTER TABLE candles ADD COLUMN source TEXT")
            conn.execute("UPDATE candles SET source='legacy' WHERE source IS NULL")

        if "coverage" in {row[0] for row in conn.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()}:
            cov_cols = set(self._table_columns(conn, "coverage"))
            if "key" in cov_cols:
                conn.execute("ALTER TABLE coverage RENAME TO coverage_legacy")
                conn.execute(
                    """
                    CREATE TABLE coverage (
                        instrument_key TEXT NOT NULL,
                        interval TEXT NOT NULL,
                        start_ts INTEGER NOT NULL,
                        end_ts INTEGER NOT NULL
                    )
                    """
                )
                conn.execute(
                    "INSERT INTO coverage(instrument_key, interval, start_ts, end_ts) SELECT key, interval, start_ts, end_ts FROM coverage_legacy"
                )
                conn.execute("DROP TABLE coverage_legacy")

    def _migrate_key_to_instrument_key(self, conn: sqlite3.Connection) -> None:
        # Migrate `candles(key, ...)` -> `candles(instrument_key, ...)`
        conn.execute("ALTER TABLE candles RENAME TO candles_legacy")
        conn.execute(
            """
            CREATE TABLE candles (
                instrument_key TEXT NOT NULL,
                interval TEXT NOT NULL,
                ts INTEGER NOT NULL,
                open REAL NOT NULL,
                high REAL NOT NULL,
                low REAL NOT NULL,
                close REAL NOT NULL,
                volume REAL NOT NULL,
                oi REAL NULL,
                source TEXT NOT NULL,
                PRIMARY KEY (instrument_key, interval, ts)
            )
            """
        )
        legacy_cols = set(self._table_columns(conn, "candles_legacy"))
        if "source" in legacy_cols:
            conn.execute(
                """
                INSERT OR REPLACE INTO candles
                (instrument_key, interval, ts, open, high, low, close, volume, oi, source)
                SELECT key, interval, ts, open, high, low, close, volume, oi, COALESCE(source, 'legacy')
                FROM candles_legacy
                """
            )
        else:
            conn.execute(
                """
                INSERT OR REPLACE INTO candles
                (instrument_key, interval, ts, open, high, low, close, volume, oi, source)
                SELECT key, interval, ts, open, high, low, close, volume, oi, 'legacy'
                FROM candles_legacy
                """
            )
        conn.execute("DROP TABLE candles_legacy")

    def _covered_ranges(self, instrument_key: str, interval: str, *, start_ts: int, end_ts: int) -> List[Tuple[int, int]]:
        with self._connect() as conn:
            cur = conn.execute(
                """
                SELECT start_ts, end_ts
                FROM coverage
                WHERE instrument_key = ? AND interval = ? AND end_ts >= ? AND start_ts <= ?
                ORDER BY start_ts ASC
                """,
                (instrument_key, interval, int(start_ts), int(end_ts)),
            )
            rows = cur.fetchall()
        return [(int(row[0]), int(row[1])) for row in rows]

    def _add_coverage(self, instrument_key: str, interval: str, start_ts: int, end_ts: int) -> None:
        start_ts_i = int(start_ts)
        end_ts_i = int(end_ts)
        if end_ts_i < start_ts_i:
            return
        with self._connect() as conn:
            conn.execute(
                "INSERT INTO coverage(instrument_key, interval, start_ts, end_ts) VALUES (?, ?, ?, ?)",
                (instrument_key, interval, start_ts_i, end_ts_i),
            )
            self._coalesce_coverage(conn, instrument_key, interval)

    def _coalesce_coverage(self, conn: sqlite3.Connection, instrument_key: str, interval: str) -> None:
        cur = conn.execute(
            "SELECT start_ts, end_ts FROM coverage WHERE instrument_key = ? AND interval = ? ORDER BY start_ts ASC",
            (instrument_key, interval),
        )
        merged = _coalesce_ranges([(int(s), int(e)) for s, e in cur.fetchall()])
        conn.execute("DELETE FROM coverage WHERE instrument_key = ? AND interval = ?", (instrument_key, interval))
        conn.executemany(
            "INSERT INTO coverage(instrument_key, interval, start_ts, end_ts) VALUES (?, ?, ?, ?)",
            [(instrument_key, interval, s, e) for s, e in merged],
        )


__all__ = [
    "HistoryCache",
    "HistoryCacheError",
    "MissingRange",
    "interval_seconds",
    "normalize_interval",
]
