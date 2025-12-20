from __future__ import annotations

import datetime as dt
import sqlite3
from pathlib import Path

import pandas as pd

from engine.backtest.history_cache import HistoryCache


IST = dt.timezone(dt.timedelta(hours=5, minutes=30), name="Asia/Kolkata")


def test_history_cache_migrates_legacy_schema(tmp_path: Path) -> None:
    db_path = tmp_path / "history.sqlite"
    with sqlite3.connect(db_path) as conn:
        conn.executescript(
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
                oi REAL,
                PRIMARY KEY (instrument_key, interval, ts)
            );
            CREATE TABLE coverage (
                instrument_key TEXT NOT NULL,
                interval TEXT NOT NULL,
                start_ts INTEGER NOT NULL,
                end_ts INTEGER NOT NULL
            );
            """
        )
        conn.execute(
            "INSERT INTO candles(instrument_key, interval, ts, open, high, low, close, volume, oi) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
            ("NSE_INDEX|Nifty 50", "1minute", 1700000000, 1.0, 2.0, 0.5, 1.5, 100.0, None),
        )
        conn.execute(
            "INSERT INTO coverage(instrument_key, interval, start_ts, end_ts) VALUES (?, ?, ?, ?)",
            ("NSE_INDEX|Nifty 50", "1minute", 1700000000, 1700000060),
        )

    cache = HistoryCache(path=db_path, tz=IST)

    with sqlite3.connect(db_path) as conn:
        cols = [row[1] for row in conn.execute("PRAGMA table_info(candles)").fetchall()]
        assert "key" in cols
        assert "source" in cols
        cov_cols = [row[1] for row in conn.execute("PRAGMA table_info(coverage)").fetchall()]
        assert "key" in cov_cols

    df = cache.load(key="NSE_INDEX|Nifty 50", interval="1minute", start_ts=1699990000, end_ts=1700010000)
    assert not df.empty
    assert set(df.columns) >= {"ts", "open", "high", "low", "close", "volume", "oi", "source", "key", "interval"}
    assert df.iloc[0]["key"] == "NSE_INDEX|Nifty 50"
    assert df.iloc[0]["source"] == "legacy"


def test_history_cache_ensure_range_fetches_once(tmp_path: Path) -> None:
    db_path = tmp_path / "history.sqlite"
    cache = HistoryCache(path=db_path, tz=IST)

    calls: list[tuple[str, str, dt.datetime, dt.datetime]] = []

    def fetch_fn(key: str, interval: str, start: dt.datetime, end: dt.datetime) -> pd.DataFrame:
        calls.append((key, interval, start, end))
        ts = start.replace(second=0, microsecond=0)
        df = pd.DataFrame(
            [
                {"ts": ts, "open": 1.0, "high": 1.0, "low": 1.0, "close": 1.0, "volume": 10.0, "oi": None, "key": key, "interval": interval},
                {"ts": ts + dt.timedelta(minutes=1), "open": 2.0, "high": 2.0, "low": 2.0, "close": 2.0, "volume": 11.0, "oi": None, "key": key, "interval": interval},
            ]
        )
        return df

    start_dt = dt.datetime(2024, 1, 2, 9, 15, tzinfo=IST)
    end_dt = dt.datetime(2024, 1, 2, 9, 16, tzinfo=IST)
    start_ts = int(start_dt.astimezone(dt.timezone.utc).timestamp())
    end_ts = int(end_dt.astimezone(dt.timezone.utc).timestamp())

    missing = cache.ensure_range("TEST|KEY", "1minute", start_ts, end_ts, fetch_fn, source="upstox")
    assert missing
    assert len(calls) == 1

    missing2 = cache.ensure_range("TEST|KEY", "1minute", start_ts, end_ts, fetch_fn, source="upstox")
    assert missing2 == []
    assert len(calls) == 1

    loaded = cache.load(key="TEST|KEY", interval="1minute", start_ts=start_ts, end_ts=end_ts)
    assert len(loaded) == 2

