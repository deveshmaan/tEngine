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

    Schema (v2):
      candles(key TEXT, interval TEXT, ts INTEGER, open REAL, high REAL, low REAL, close REAL,
              volume REAL, oi REAL NULL, source TEXT, PRIMARY KEY(key, interval, ts))
      coverage(key TEXT, interval TEXT, start_ts INTEGER, end_ts INTEGER)

    Backward compatibility:
      Migrates legacy schema that used `instrument_key` and lacked `source`.
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
        key: str,
        interval: str,
        start_ts: int,
        end_ts: int,
        fetch_fn: Callable[[str, str, dt.datetime, dt.datetime], "pd.DataFrame"],
        *,
        source: str = "upstox",
    ) -> List[MissingRange]:
        import pandas as pd  # type: ignore

        key = str(key or "").strip()
        if not key:
            raise ValueError("key must be non-empty")
        interval_key = normalize_interval(interval)
        start_ts_i = int(start_ts)
        end_ts_i = int(end_ts)
        if end_ts_i < start_ts_i:
            raise ValueError("Invalid range: end_ts < start_ts")

        covered = self._covered_ranges(key, interval_key, start_ts=start_ts_i, end_ts=end_ts_i)
        missing = _missing_ranges(covered, start_ts_i, end_ts_i)
        for seg in missing:
            seg_start = dt.datetime.fromtimestamp(seg.start_ts, tz=dt.timezone.utc).astimezone(self._tz)
            seg_end = dt.datetime.fromtimestamp(seg.end_ts, tz=dt.timezone.utc).astimezone(self._tz)
            df = fetch_fn(key, interval_key, seg_start, seg_end)
            if df is None:
                df = pd.DataFrame()
            self.store(df, key=key, interval=interval_key, source=source)
            self._add_coverage(key, interval_key, seg.start_ts, seg.end_ts)
        return missing

    def load(self, *, key: str, interval: str, start_ts: int, end_ts: int) -> "pd.DataFrame":
        import pandas as pd  # type: ignore

        key = str(key or "").strip()
        if not key:
            raise ValueError("key must be non-empty")
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
                WHERE key = ? AND interval = ? AND ts >= ? AND ts <= ?
                ORDER BY ts ASC
                """,
                (key, interval_key, start_ts_i, end_ts_i),
            )
            rows = cur.fetchall()

        if not rows:
            return pd.DataFrame(columns=["ts", "open", "high", "low", "close", "volume", "oi", "source", "key", "interval"])

        df = pd.DataFrame(rows, columns=["ts", "open", "high", "low", "close", "volume", "oi", "source"])
        df["ts"] = pd.to_datetime(df["ts"], unit="s", utc=True).dt.tz_convert(self._tz)
        df["key"] = key
        df["interval"] = interval_key
        df = df.drop_duplicates(subset=["ts"], keep="last").sort_values("ts").reset_index(drop=True)
        return df

    def store(self, df: "pd.DataFrame", *, key: Optional[str] = None, interval: Optional[str] = None, source: str = "upstox") -> None:
        import pandas as pd  # type: ignore

        if df is None or df.empty:
            return

        df_local = df.copy()
        if key is not None:
            df_local["key"] = str(key)
        if interval is not None:
            df_local["interval"] = normalize_interval(str(interval))
        required = {"ts", "open", "high", "low", "close", "volume", "key", "interval"}
        missing = required - set(df_local.columns)
        if missing:
            raise ValueError(f"Cannot store candles; missing columns: {sorted(missing)}")

        # Normalize key columns.
        cache_key = str(df_local["key"].iloc[0])
        interval_key = normalize_interval(str(df_local["interval"].iloc[0]))

        ts_series = pd.to_datetime(df_local["ts"], utc=True, errors="coerce")
        if ts_series.isna().any():
            raise ValueError("Cannot store candles; some timestamps could not be parsed")
        ts_epoch = (ts_series.astype("int64") // 1_000_000_000).astype("int64")

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
                (key, interval, ts, open, high, low, close, volume, oi, source)
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
                key TEXT NOT NULL,
                interval TEXT NOT NULL,
                ts INTEGER NOT NULL,
                open REAL NOT NULL,
                high REAL NOT NULL,
                low REAL NOT NULL,
                close REAL NOT NULL,
                volume REAL NOT NULL,
                oi REAL NULL,
                source TEXT NOT NULL,
                PRIMARY KEY (key, interval, ts)
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS coverage (
                key TEXT NOT NULL,
                interval TEXT NOT NULL,
                start_ts INTEGER NOT NULL,
                end_ts INTEGER NOT NULL
            )
            """
        )
        conn.execute("CREATE INDEX IF NOT EXISTS idx_candles_key_ts ON candles(key, interval, ts)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_coverage_key ON coverage(key, interval, start_ts, end_ts)")

    def _migrate_if_needed(self, conn: sqlite3.Connection) -> None:
        cols = set(self._table_columns(conn, "candles"))
        if "instrument_key" in cols:
            self._migrate_legacy_instrument_key(conn)
            return
        if "key" in cols and "source" not in cols:
            conn.execute("ALTER TABLE candles ADD COLUMN source TEXT")
            conn.execute("UPDATE candles SET source='legacy' WHERE source IS NULL")

        if "coverage" in {row[0] for row in conn.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()}:
            cov_cols = set(self._table_columns(conn, "coverage"))
            if "instrument_key" in cov_cols:
                conn.execute("ALTER TABLE coverage RENAME TO coverage_legacy")
                conn.execute(
                    """
                    CREATE TABLE coverage (
                        key TEXT NOT NULL,
                        interval TEXT NOT NULL,
                        start_ts INTEGER NOT NULL,
                        end_ts INTEGER NOT NULL
                    )
                    """
                )
                conn.execute(
                    "INSERT INTO coverage(key, interval, start_ts, end_ts) SELECT instrument_key, interval, start_ts, end_ts FROM coverage_legacy"
                )
                conn.execute("DROP TABLE coverage_legacy")

    def _migrate_legacy_instrument_key(self, conn: sqlite3.Connection) -> None:
        # Migrate `candles(instrument_key, ...)` -> `candles(key, ..., source)`
        conn.execute("ALTER TABLE candles RENAME TO candles_legacy")
        conn.execute(
            """
            CREATE TABLE candles (
                key TEXT NOT NULL,
                interval TEXT NOT NULL,
                ts INTEGER NOT NULL,
                open REAL NOT NULL,
                high REAL NOT NULL,
                low REAL NOT NULL,
                close REAL NOT NULL,
                volume REAL NOT NULL,
                oi REAL NULL,
                source TEXT NOT NULL,
                PRIMARY KEY (key, interval, ts)
            )
            """
        )
        conn.execute(
            """
            INSERT OR REPLACE INTO candles
            (key, interval, ts, open, high, low, close, volume, oi, source)
            SELECT instrument_key, interval, ts, open, high, low, close, volume, oi, 'legacy'
            FROM candles_legacy
            """
        )
        conn.execute("DROP TABLE candles_legacy")

        # coverage(instrument_key, ...) -> coverage(key, ...)
        has_cov = conn.execute(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name='coverage' LIMIT 1"
        ).fetchone()
        if has_cov:
            conn.execute("ALTER TABLE coverage RENAME TO coverage_legacy")
            conn.execute(
                """
                CREATE TABLE coverage (
                    key TEXT NOT NULL,
                    interval TEXT NOT NULL,
                    start_ts INTEGER NOT NULL,
                    end_ts INTEGER NOT NULL
                )
                """
            )
            conn.execute(
                "INSERT INTO coverage(key, interval, start_ts, end_ts) SELECT instrument_key, interval, start_ts, end_ts FROM coverage_legacy"
            )
            conn.execute("DROP TABLE coverage_legacy")

    def _covered_ranges(self, key: str, interval: str, *, start_ts: int, end_ts: int) -> List[Tuple[int, int]]:
        with self._connect() as conn:
            cur = conn.execute(
                """
                SELECT start_ts, end_ts
                FROM coverage
                WHERE key = ? AND interval = ? AND end_ts >= ? AND start_ts <= ?
                ORDER BY start_ts ASC
                """,
                (key, interval, int(start_ts), int(end_ts)),
            )
            rows = cur.fetchall()
        return [(int(row[0]), int(row[1])) for row in rows]

    def _add_coverage(self, key: str, interval: str, start_ts: int, end_ts: int) -> None:
        start_ts_i = int(start_ts)
        end_ts_i = int(end_ts)
        if end_ts_i < start_ts_i:
            return
        with self._connect() as conn:
            conn.execute("INSERT INTO coverage(key, interval, start_ts, end_ts) VALUES (?, ?, ?, ?)", (key, interval, start_ts_i, end_ts_i))
            self._coalesce_coverage(conn, key, interval)

    def _coalesce_coverage(self, conn: sqlite3.Connection, key: str, interval: str) -> None:
        cur = conn.execute(
            "SELECT start_ts, end_ts FROM coverage WHERE key = ? AND interval = ? ORDER BY start_ts ASC",
            (key, interval),
        )
        merged = _coalesce_ranges([(int(s), int(e)) for s, e in cur.fetchall()])
        conn.execute("DELETE FROM coverage WHERE key = ? AND interval = ?", (key, interval))
        conn.executemany(
            "INSERT INTO coverage(key, interval, start_ts, end_ts) VALUES (?, ?, ?, ?)",
            [(key, interval, s, e) for s, e in merged],
        )


__all__ = [
    "HistoryCache",
    "HistoryCacheError",
    "MissingRange",
    "interval_seconds",
    "normalize_interval",
]
