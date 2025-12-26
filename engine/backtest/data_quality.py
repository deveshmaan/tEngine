from __future__ import annotations

import datetime as dt
import sqlite3
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple

from brokerage.upstox_client import INDEX_INSTRUMENT_KEYS
from engine.backtest.history_cache import HistoryCache, MissingRange, interval_seconds, normalize_interval
from engine.backtest.instrument_master import InstrumentMaster
from engine.config import IST


@dataclass(frozen=True)
class InstrumentRef:
    instrument_key: str
    kind: str  # "UNDERLYING" | "OPTION"
    symbol: Optional[str] = None
    expiry: Optional[str] = None  # YYYY-MM-DD for options (IST)


_OPT_SUFFIXES = ("CE", "PE")


def parse_option_symbol(symbol: str) -> Tuple[str, str, int, str]:
    """
    Parse app option symbols of form: `NIFTY-YYYY-MM-DD-<strike><CE|PE>`.

    Returns: (underlying_symbol, expiry, strike, opt_type)
    """

    text = str(symbol or "").strip()
    if not text:
        raise ValueError("empty symbol")
    parts = text.split("-")
    if len(parts) < 3:
        raise ValueError(f"not_option_symbol:{symbol}")
    underlying = parts[0].strip().upper()
    expiry = parts[1].strip()
    tail = parts[2].strip().upper()
    if not underlying or not expiry or not tail:
        raise ValueError(f"not_option_symbol:{symbol}")
    opt_type = None
    for suf in _OPT_SUFFIXES:
        if tail.endswith(suf):
            opt_type = suf
            strike_text = tail[: -len(suf)]
            break
    if opt_type is None:
        raise ValueError(f"not_option_symbol:{symbol}")
    try:
        strike = int(float(strike_text))
    except (TypeError, ValueError) as exc:
        raise ValueError(f"bad_strike:{symbol}") from exc
    return underlying, expiry, int(strike), str(opt_type)


def _safe_date(value: object) -> Optional[dt.date]:
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
        return dt.date.fromisoformat(text[:10])
    except Exception:
        return None


def _range_ts(*, start_date: dt.date, end_date: dt.date) -> Tuple[int, int]:
    start = dt.datetime.combine(start_date, dt.time(0, 0), tzinfo=IST)
    end = dt.datetime.combine(end_date, dt.time(23, 59, 59), tzinfo=IST)
    return int(start.astimezone(dt.timezone.utc).timestamp()), int(end.astimezone(dt.timezone.utc).timestamp())


def collect_instruments_from_results(
    results: Dict[str, Any],
    *,
    instrument_master: Optional[InstrumentMaster] = None,
    underlying_key: Optional[str] = None,
) -> List[InstrumentRef]:
    """
    Best-effort extraction of instrument keys used by a backtest run.

    - Always includes the underlying instrument_key.
    - Resolves option symbols seen in orders/executions/trades via InstrumentMaster.
    """

    if underlying_key is None:
        underlying_key = str(results.get("underlying_key") or results.get("underlying") or "").strip()
    if not underlying_key:
        underlying_key = INDEX_INSTRUMENT_KEYS.get(str(results.get("underlying") or "").strip().upper(), "")

    refs: Dict[str, InstrumentRef] = {}
    if underlying_key:
        refs[underlying_key] = InstrumentRef(instrument_key=underlying_key, kind="UNDERLYING", symbol=None, expiry=None)

    master = instrument_master or InstrumentMaster()

    def _visit_symbol(sym: object) -> None:
        s = str(sym or "").strip()
        if not s:
            return
        try:
            _u_sym, expiry, strike, opt = parse_option_symbol(s)
        except Exception:
            return
        if not underlying_key:
            return
        try:
            key = master.resolve_option_key(underlying_key=underlying_key, expiry=str(expiry), opt_type=str(opt), strike=int(strike))
        except Exception:
            return
        key = str(key)
        refs.setdefault(key, InstrumentRef(instrument_key=key, kind="OPTION", symbol=s, expiry=str(expiry)))

    for bucket in ("orders", "executions", "trade_log"):
        rows = results.get(bucket) or []
        if not isinstance(rows, list):
            continue
        for row in rows:
            if not isinstance(row, dict):
                continue
            _visit_symbol(row.get("symbol"))

    # Preserve stable ordering: underlying first, then options by instrument_key.
    ordered: List[InstrumentRef] = []
    if underlying_key and underlying_key in refs:
        ordered.append(refs[underlying_key])
    for k in sorted(refs.keys()):
        if k == underlying_key:
            continue
        ordered.append(refs[k])
    return ordered


def compute_data_quality(
    *,
    history_cache_path: str | Path,
    instruments: Sequence[InstrumentRef],
    interval: str,
    start_date: dt.date,
    end_date: dt.date,
    holiday_calendar: Optional[Iterable[object]] = None,
) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
    """
    Return:
      - per_instrument_rows: list of dicts suitable for a DataFrame
      - missing_segments_rows: list of dicts (for CSV export)
    """

    cache_path = Path(history_cache_path)
    interval_key = normalize_interval(interval)
    range_start_ts, range_end_ts = _range_ts(start_date=start_date, end_date=end_date)

    cache = HistoryCache(path=cache_path, tz=IST)
    conn = sqlite3.connect(str(cache_path))
    conn.row_factory = sqlite3.Row
    try:
        out: List[Dict[str, Any]] = []
        missing_rows: List[Dict[str, Any]] = []

        for ref in instruments:
            instrument_key = str(ref.instrument_key or "").strip()
            if not instrument_key:
                continue

            end_ts = int(range_end_ts)
            expiry_dt = _safe_date(ref.expiry)
            if expiry_dt is not None:
                exp_end = dt.datetime.combine(expiry_dt, dt.time(23, 59, 59), tzinfo=IST)
                end_ts = min(end_ts, int(exp_end.astimezone(dt.timezone.utc).timestamp()))

            gap = cache.gap_info(
                instrument_key=instrument_key,
                interval=interval_key,
                start_ts=int(range_start_ts),
                end_ts=int(end_ts),
                holiday_calendar=holiday_calendar,
            )
            expected_bars = int(gap.get("expected_bars", 0) or 0)
            missing_bars = int(gap.get("missing_bars", 0) or 0)
            actual_bars = int(gap.get("present_bars", 0) or (expected_bars - missing_bars))
            missing_ranges: List[MissingRange] = list(gap.get("missing_ranges") or [])

            expected_set = set(
                cache.expected_timestamps(interval=interval_key, start_ts=int(range_start_ts), end_ts=int(end_ts), holiday_calendar=holiday_calendar)
            )

            rows = conn.execute(
                "SELECT ts, volume, oi FROM candles WHERE instrument_key=? AND interval=? AND ts>=? AND ts<=?",
                (instrument_key, interval_key, int(range_start_ts), int(end_ts)),
            ).fetchall()
            actual_total = len(rows)
            uniq_ts = {int(r["ts"]) for r in rows if r and r["ts"] is not None}
            duplicate_ts = max(0, actual_total - len(uniq_ts))

            volume_present = 0
            oi_present = 0
            on_grid = 0
            for r in rows:
                try:
                    ts_val = int(r["ts"])
                except Exception:
                    continue
                if ts_val not in expected_set:
                    continue
                on_grid += 1
                try:
                    if float(r["volume"] or 0.0) > 0.0:
                        volume_present += 1
                except Exception:
                    pass
                try:
                    oi = r["oi"]
                    if oi is not None and float(oi) > 0.0:
                        oi_present += 1
                except Exception:
                    pass

            offgrid_bars = max(0, int(actual_total) - int(on_grid))

            volume_pct = (float(volume_present) / float(on_grid) * 100.0) if on_grid > 0 else None
            oi_pct = (float(oi_present) / float(on_grid) * 100.0) if on_grid > 0 else None

            out.append(
                {
                    "instrument_key": instrument_key,
                    "kind": str(ref.kind),
                    "expiry": ref.expiry,
                    "expected_bars": int(expected_bars),
                    "actual_bars": int(actual_bars),
                    "missing_bars": int(missing_bars),
                    "offgrid_bars": int(offgrid_bars),
                    "duplicate_ts": int(duplicate_ts),
                    "volume_present_pct": (None if volume_pct is None else round(float(volume_pct), 2)),
                    "oi_present_pct": (None if oi_pct is None else round(float(oi_pct), 2)),
                }
            )

            try:
                bar_secs = int(interval_seconds(interval_key))
            except Exception:
                bar_secs = 0
            for seg in missing_ranges:
                start_dt = dt.datetime.fromtimestamp(int(seg.start_ts), tz=dt.timezone.utc).astimezone(IST)
                end_dt2 = dt.datetime.fromtimestamp(int(seg.end_ts), tz=dt.timezone.utc).astimezone(IST)
                approx_bars = None
                if bar_secs > 0 and seg.end_ts >= seg.start_ts:
                    approx_bars = int((int(seg.end_ts) - int(seg.start_ts)) // bar_secs) + 1
                missing_rows.append(
                    {
                        "instrument_key": instrument_key,
                        "kind": str(ref.kind),
                        "interval": interval_key,
                        "start_ts": int(seg.start_ts),
                        "end_ts": int(seg.end_ts),
                        "start_dt_ist": start_dt.isoformat(),
                        "end_dt_ist": end_dt2.isoformat(),
                        "approx_missing_bars": approx_bars,
                    }
                )

        return out, missing_rows
    finally:
        try:
            conn.close()
        except Exception:
            pass


def load_sample_window(
    *,
    history_cache_path: str | Path,
    instrument_key: str,
    interval: str,
    center_dt: dt.datetime,
    bars_before: int = 2,
    bars_after: int = 2,
) -> "pd.DataFrame":
    import pandas as pd  # type: ignore

    interval_key = normalize_interval(interval)
    try:
        bar_secs = int(interval_seconds(interval_key))
    except Exception:
        bar_secs = 0
    if bar_secs <= 0:
        bar_secs = 60
    center = center_dt
    if center.tzinfo is None:
        center = center.replace(tzinfo=IST)
    center_utc = center.astimezone(dt.timezone.utc)
    start_ts = int((center_utc - dt.timedelta(seconds=int(max(bars_before, 0)) * bar_secs)).timestamp())
    end_ts = int((center_utc + dt.timedelta(seconds=int(max(bars_after, 0)) * bar_secs)).timestamp())

    cache = HistoryCache(path=Path(history_cache_path), tz=IST)
    return cache.load(instrument_key=str(instrument_key), interval=interval_key, start_ts=int(start_ts), end_ts=int(end_ts))


__all__ = [
    "InstrumentRef",
    "collect_instruments_from_results",
    "compute_data_quality",
    "load_sample_window",
    "parse_option_symbol",
]

