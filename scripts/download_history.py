from __future__ import annotations

import argparse
import datetime as dt
import os
import re
import sys
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Sequence, Tuple

import pandas as pd

from brokerage.upstox_client import CredentialError, INDEX_INSTRUMENT_KEYS, UpstoxSession
from engine.backtest.expiry_resolver import resolve_expiry
from engine.backtest.history_cache import HistoryCache, normalize_interval
from engine.backtest.instrument_master import InstrumentMaster
from engine.backtest.strike_resolver import resolve_atm_strike, resolve_strike_offset, resolve_target_premium
from engine.config import EngineConfig, IST

EXPIRY_MODE_MAP = {
    "weekly_current": "WEEKLY_CURRENT",
    "weekly_next": "WEEKLY_NEXT",
    "monthly": "MONTHLY",
}

STRIKE_MODE_MAP = {
    "atm": "ATM",
    "offset": "ATM_OFFSET",
    "target_premium": "TARGET_PREMIUM",
}

DEFAULT_ENTRY_TIME = dt.time(9, 30)


def _parse_date(value: str) -> dt.date:
    try:
        return dt.date.fromisoformat(str(value))
    except Exception as exc:
        raise argparse.ArgumentTypeError(f"Invalid date: {value!r} (expected YYYY-MM-DD)") from exc


def _parse_csv_list(raw: Optional[str]) -> list[str]:
    if raw is None:
        return []
    text = str(raw).strip()
    if not text:
        return []
    parts = [p.strip() for p in text.split(",")]
    return [p for p in parts if p]


def _parse_int_list(raw: Optional[str]) -> list[int]:
    out: list[int] = []
    for item in _parse_csv_list(raw):
        try:
            out.append(int(item))
        except Exception as exc:
            raise argparse.ArgumentTypeError(f"Invalid int list item: {item!r}") from exc
    return out


def _parse_float_list(raw: Optional[str]) -> list[float]:
    out: list[float] = []
    for item in _parse_csv_list(raw):
        try:
            out.append(float(item))
        except Exception as exc:
            raise argparse.ArgumentTypeError(f"Invalid float list item: {item!r}") from exc
    return out


_SAFE_RE = re.compile(r"[^A-Za-z0-9._-]+")


def _safe_fragment(text: str) -> str:
    return _SAFE_RE.sub("_", str(text or "").strip()).strip("_") or "unknown"


def _utc_epoch_bounds(start_date: dt.date, end_date: dt.date) -> tuple[int, int]:
    start_dt = dt.datetime.combine(start_date, dt.time(0, 0), tzinfo=IST)
    end_dt = dt.datetime.combine(end_date, dt.time(23, 59, 59), tzinfo=IST)
    return int(start_dt.astimezone(dt.timezone.utc).timestamp()), int(end_dt.astimezone(dt.timezone.utc).timestamp())


def _symbol_from_underlying_key(underlying_key: str) -> str:
    key = str(underlying_key or "").strip()
    upper = key.upper()
    if upper in INDEX_INSTRUMENT_KEYS:
        return upper
    for sym, ik in INDEX_INSTRUMENT_KEYS.items():
        if str(ik) == key:
            return str(sym)
    return upper


def _normalize_underlying_key(raw: str) -> tuple[str, str]:
    """
    Returns (underlying_symbol, underlying_instrument_key_or_raw_key).

    For NIFTY/BANKNIFTY aliases, resolves to Upstox instrument_key.
    """

    text = str(raw or "").strip()
    if not text:
        raise ValueError("--underlying_key must be non-empty")
    upper = text.upper()
    if upper in INDEX_INSTRUMENT_KEYS:
        return upper, INDEX_INSTRUMENT_KEYS[upper]
    for sym, ik in INDEX_INSTRUMENT_KEYS.items():
        if str(ik) == text:
            return str(sym), str(ik)
    return upper, text


def _strike_step(cfg: EngineConfig, underlying_symbol: str) -> int:
    steps = getattr(cfg.data, "strike_steps", {}) or {}
    try:
        return max(int(steps.get(str(underlying_symbol).upper(), cfg.data.lot_step)), 1)
    except Exception:
        return max(int(cfg.data.lot_step), 1)


def _fetch_candles_df(session: UpstoxSession, key: str, interval_key: str, start: dt.datetime, end: dt.datetime) -> pd.DataFrame:
    start_dt = start.astimezone(IST)
    end_dt = end.astimezone(IST)
    df = session.get_historical_data(
        key,
        start_dt.date(),
        end_dt.date(),
        interval_key,
        as_dataframe=True,
        tz=IST,
    )
    if isinstance(df, list):
        df = pd.DataFrame(df)
    if df is None or df.empty:
        return pd.DataFrame(columns=["ts", "open", "high", "low", "close", "volume", "oi", "key", "interval"])
    if "ts" not in df.columns:
        df = df.reset_index()
    if "ts" not in df.columns:
        df = df.rename(columns={"index": "ts"})
    df = df.copy()
    df["key"] = str(key)
    df["interval"] = str(interval_key)
    if "volume" not in df.columns:
        df["volume"] = 0.0
    if "oi" not in df.columns:
        df["oi"] = None
    return df[["ts", "open", "high", "low", "close", "volume", "oi", "key", "interval"]]


def _spot_for_day(df_day: pd.DataFrame, *, ref_time: dt.time) -> Optional[float]:
    if df_day is None or df_day.empty:
        return None
    if "ts" not in df_day.columns:
        return None
    ts_series = pd.to_datetime(df_day["ts"], errors="coerce")
    if getattr(ts_series, "dt", None) is None:
        return None
    df_local = df_day.copy()
    df_local["ts"] = ts_series
    try:
        day = df_local["ts"].iloc[0].astimezone(IST).date()
        ref_dt = dt.datetime.combine(day, ref_time, tzinfo=IST)
    except Exception:
        ref_dt = None

    if ref_dt is not None:
        after = df_local[df_local["ts"] >= ref_dt]
        if not after.empty:
            row = after.iloc[0]
        else:
            row = df_local.iloc[0]
    else:
        row = df_local.iloc[0]

    for field in ("open", "close", "high", "low"):
        try:
            val = float(row.get(field))
        except Exception:
            continue
        if val > 0:
            return float(val)
    return None


def _price_at_or_before(df: pd.DataFrame, ts: dt.datetime) -> Optional[float]:
    if df is None or df.empty:
        return None
    if "ts" not in df.columns:
        return None
    ts_series = pd.to_datetime(df["ts"], errors="coerce")
    if getattr(ts_series, "dt", None) is None:
        return None
    df_local = df.copy()
    df_local["ts"] = ts_series
    df_local = df_local.sort_values("ts")
    eligible = df_local[df_local["ts"] <= ts]
    if eligible.empty:
        return None
    row = eligible.iloc[-1]
    for field in ("open", "close"):
        try:
            val = float(row.get(field))
        except Exception:
            continue
        if val > 0:
            return float(val)
    return None


def _export_frame(df: pd.DataFrame, *, out_dir: Path, base_name: str, export_csv: bool, export_parquet: bool) -> None:
    if df is None or df.empty:
        return
    out_dir.mkdir(parents=True, exist_ok=True)
    if export_csv:
        path = out_dir / f"{base_name}.csv"
        df.to_csv(path, index=False)
        print(f"[export] {path}")
    if export_parquet:
        path = out_dir / f"{base_name}.parquet"
        try:
            df.to_parquet(path, index=False)
        except Exception as exc:
            raise RuntimeError(f"Parquet export failed: {exc} (install pyarrow or fastparquet)") from exc
        print(f"[export] {path}")


def _parse_args(argv: Optional[Sequence[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Pre-cache Upstox historical candles into cache/history.sqlite")
    parser.add_argument("--underlying_key", required=True, help="Underlying instrument_key (or alias like NIFTY/BANKNIFTY)")
    parser.add_argument("--interval", required=True, help='Candle interval (e.g. "1minute", "5minute", "15minute")')
    parser.add_argument("--start_date", required=True, type=_parse_date, help="YYYY-MM-DD")
    parser.add_argument("--end_date", required=True, type=_parse_date, help="YYYY-MM-DD")

    parser.add_argument("--options", action="store_true", help="Also pre-cache option candles derived from expiry/strike settings")
    parser.add_argument("--expiry_mode", choices=sorted(EXPIRY_MODE_MAP.keys()), default="weekly_current")
    parser.add_argument("--strike_mode", choices=sorted(STRIKE_MODE_MAP.keys()), default="atm")
    parser.add_argument("--strike_offsets", default="", help='Comma list of strike offsets in points (e.g. "200,400")')
    parser.add_argument("--premium_targets", default="", help='Comma list of target premiums (e.g. "40,60")')

    parser.add_argument("--export_csv", action="store_true", help="Export cached candles to reports/*.csv")
    parser.add_argument("--export_parquet", action="store_true", help="Export cached candles to reports/*.parquet")
    parser.add_argument("--output_dir", default="reports", help='Export directory (default: "reports")')
    parser.add_argument("--cache_path", default=None, help='Override cache SQLite path (default: env HISTORY_CACHE_PATH or "cache/history.sqlite")')
    parser.add_argument("--instrument_master_path", default=None, help='Override instrument master path (default: "cache/nse_master.json.gz")')

    args = parser.parse_args(argv)
    if args.end_date < args.start_date:
        parser.error("--end_date must be on/after --start_date")
    return args


def main(argv: Optional[Sequence[str]] = None) -> int:
    args = _parse_args(argv)
    interval_key = normalize_interval(args.interval)
    start_ts, end_ts = _utc_epoch_bounds(args.start_date, args.end_date)

    underlying_symbol, underlying_key = _normalize_underlying_key(args.underlying_key)
    expiry_mode = EXPIRY_MODE_MAP[str(args.expiry_mode)]
    strike_mode = STRIKE_MODE_MAP[str(args.strike_mode)]
    strike_offsets = [abs(int(x)) for x in _parse_int_list(args.strike_offsets)]
    premium_targets = [float(x) for x in _parse_float_list(args.premium_targets)]

    if args.options and strike_mode == "ATM_OFFSET" and not strike_offsets:
        raise SystemExit("--strike_offsets is required when --strike_mode=offset")
    if args.options and strike_mode == "TARGET_PREMIUM" and not premium_targets:
        raise SystemExit("--premium_targets is required when --strike_mode=target_premium")

    cfg = EngineConfig.load()
    step = _strike_step(cfg, underlying_symbol)

    cache = HistoryCache(path=args.cache_path)
    session = None
    try:
        session = UpstoxSession()
    except CredentialError as exc:
        print(f"[error] {exc}", file=sys.stderr)
        return 2

    def fetch_fn(key: str, interval_key: str, seg_start: dt.datetime, seg_end: dt.datetime) -> pd.DataFrame:
        assert session is not None
        return _fetch_candles_df(session, key, interval_key, seg_start, seg_end)

    print(f"[cache] {cache.path}")
    print(f"[underlying] key={underlying_key} interval={interval_key} range={args.start_date.isoformat()}..{args.end_date.isoformat()}")
    missing = cache.ensure_range(underlying_key, interval_key, start_ts, end_ts, fetch_fn, source="upstox")
    print(f"[underlying] missing_segments={len(missing)}")

    df_under = cache.load(key=underlying_key, interval=interval_key, start_ts=start_ts, end_ts=end_ts)
    if df_under.empty:
        print("[warn] Underlying candles are empty after fetch; cannot resolve option strikes.", file=sys.stderr)
        return 1 if args.options else 0

    # -------------------------------------------------------------- options
    option_keys: Dict[str, Dict[str, object]] = {}
    if args.options:
        inst_master = InstrumentMaster(path=args.instrument_master_path)
        if not inst_master.path.exists():
            print(f"[error] Instrument master not found: {inst_master.path}", file=sys.stderr)
            print("        Expected a gzipped JSON file like cache/nse_master.json.gz", file=sys.stderr)
            return 2
        ref_time = DEFAULT_ENTRY_TIME

        df_under_local = df_under.copy()
        df_under_local["ts"] = pd.to_datetime(df_under_local["ts"], errors="coerce")
        df_under_local = df_under_local.dropna(subset=["ts"]).sort_values("ts")
        df_under_local["trade_date"] = df_under_local["ts"].dt.tz_convert(IST).dt.date
        trade_dates = sorted({d for d in df_under_local["trade_date"].tolist() if isinstance(d, dt.date)})
        if not trade_dates:
            print("[warn] Could not infer trading dates from underlying candles.", file=sys.stderr)
            return 1

        print(f"[options] trade_days={len(trade_dates)} expiry_mode={args.expiry_mode} strike_mode={args.strike_mode} step={step}")

        for trade_date in trade_dates:
            exp_date = resolve_expiry(trade_date, expiry_mode)
            expiry = exp_date.isoformat()

            df_day = df_under_local[df_under_local["trade_date"] == trade_date]
            spot = _spot_for_day(df_day, ref_time=ref_time)
            if spot is None:
                continue
            atm = resolve_atm_strike(spot, step)

            entry_ts = dt.datetime.combine(trade_date, ref_time, tzinfo=IST)

            def _register(opt_key: str, *, expiry_date: dt.date, used_date: dt.date) -> None:
                meta = option_keys.get(opt_key)
                if meta is None:
                    option_keys[opt_key] = {
                        "min_date": used_date,
                        "max_date": used_date,
                        "expiry_date": expiry_date,
                    }
                    return
                min_date = meta.get("min_date")
                max_date = meta.get("max_date")
                if isinstance(min_date, dt.date):
                    meta["min_date"] = min(min_date, used_date)
                else:
                    meta["min_date"] = used_date
                if isinstance(max_date, dt.date):
                    meta["max_date"] = max(max_date, used_date)
                else:
                    meta["max_date"] = used_date

            def _resolve_key(expiry: str, opt_type: str, strike: int) -> Optional[str]:
                try:
                    return inst_master.resolve_option_key(
                        underlying_key=underlying_key,
                        expiry=expiry,
                        opt_type=opt_type,
                        strike=int(strike),
                    )
                except Exception:
                    return None

            def _prefetch_key(key: str, used_date: dt.date) -> None:
                day_start_ts, day_end_ts = _utc_epoch_bounds(used_date, used_date)
                cache.ensure_range(key, interval_key, day_start_ts, day_end_ts, fetch_fn, source="upstox")

            if strike_mode == "ATM":
                for opt_type in ("CE", "PE"):
                    opt_key = _resolve_key(expiry, opt_type, atm)
                    if not opt_key:
                        continue
                    _register(opt_key, expiry_date=exp_date, used_date=trade_date)
            elif strike_mode == "ATM_OFFSET":
                for off in strike_offsets:
                    for opt_type in ("CE", "PE"):
                        strike = resolve_strike_offset(atm, opt_type, off)
                        opt_key = _resolve_key(expiry, opt_type, strike)
                        if not opt_key:
                            continue
                        _register(opt_key, expiry_date=exp_date, used_date=trade_date)
            else:
                for opt_type in ("CE", "PE"):
                    grid_all = inst_master.strikes_for(underlying_key=underlying_key, expiry=expiry, opt_type=opt_type)
                    if not grid_all:
                        continue
                    window_steps = 20
                    lo = int(atm - window_steps * step)
                    hi = int(atm + window_steps * step)
                    grid = [s for s in grid_all if lo <= int(s) <= hi] or list(grid_all)

                    for target in premium_targets:
                        def _price_fn(ts: dt.datetime, exp: dt.date, t: str, strike: int) -> Optional[float]:
                            exp_str = exp.isoformat()
                            opt_key = _resolve_key(exp_str, t, int(strike))
                            if not opt_key:
                                return None
                            # Ensure the candle is present in cache; we only need this day for pricing.
                            _prefetch_key(opt_key, used_date=trade_date)
                            day_start_ts, day_end_ts = _utc_epoch_bounds(trade_date, trade_date)
                            df_opt = cache.load(key=opt_key, interval=interval_key, start_ts=day_start_ts, end_ts=day_end_ts)
                            return _price_at_or_before(df_opt, ts)

                        try:
                            strike = resolve_target_premium(entry_ts, exp_date, opt_type, float(target), grid, _price_fn)
                        except Exception:
                            continue
                        opt_key = _resolve_key(expiry, opt_type, strike)
                        if not opt_key:
                            continue
                        _register(opt_key, expiry_date=exp_date, used_date=trade_date)

        print(f"[options] unique_contracts={len(option_keys)}")
        for idx, (opt_key, meta) in enumerate(sorted(option_keys.items()), start=1):
            min_date = meta.get("min_date")
            max_date = meta.get("max_date")
            expiry_date = meta.get("expiry_date")
            if not isinstance(min_date, dt.date) or not isinstance(max_date, dt.date):
                continue
            if isinstance(expiry_date, dt.date):
                max_fetch_date = min(max_date, expiry_date)
            else:
                max_fetch_date = max_date
            key_start_ts, key_end_ts = _utc_epoch_bounds(min_date, max_fetch_date)
            missing_opt = cache.ensure_range(opt_key, interval_key, key_start_ts, key_end_ts, fetch_fn, source="upstox")
            if idx % 25 == 0 or idx == len(option_keys):
                print(f"[options] {idx}/{len(option_keys)} cached (last_missing_segments={len(missing_opt)})")

    # -------------------------------------------------------------- exports
    out_dir = Path(str(args.output_dir or "reports"))
    base_under = f"history_{_safe_fragment(underlying_key)}_{interval_key}_{args.start_date.isoformat()}_{args.end_date.isoformat()}"
    if args.export_csv or args.export_parquet:
        _export_frame(df_under, out_dir=out_dir, base_name=base_under, export_csv=bool(args.export_csv), export_parquet=bool(args.export_parquet))

    if args.options and (args.export_csv or args.export_parquet) and option_keys:
        frames: List[pd.DataFrame] = []
        for opt_key, meta in option_keys.items():
            try:
                df_opt = cache.load(key=opt_key, interval=interval_key, start_ts=start_ts, end_ts=end_ts)
            except Exception:
                continue
            if df_opt is None or df_opt.empty:
                continue
            frames.append(df_opt)
        if frames:
            df_all = pd.concat(frames, ignore_index=True)
            suffix = f"{args.expiry_mode}_{args.strike_mode}"
            base_opt = f"options_{_safe_fragment(underlying_symbol)}_{interval_key}_{args.start_date.isoformat()}_{args.end_date.isoformat()}_{suffix}"
            _export_frame(df_all, out_dir=out_dir, base_name=base_opt, export_csv=bool(args.export_csv), export_parquet=bool(args.export_parquet))

    return 0


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
