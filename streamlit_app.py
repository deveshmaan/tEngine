from __future__ import annotations

import datetime as dt
import json
import os
import re
import sqlite3
import time
from pathlib import Path
from typing import Any, Dict, List, Optional

import pandas as pd
import requests
import streamlit as st
from prometheus_client.parser import text_string_to_metric_families

from engine import data as engine_data
from engine.config import CONFIG, read_config
from market.instrument_cache import InstrumentCache
from brokerage.upstox_client import INDEX_INSTRUMENT_KEYS, CredentialError, UpstoxSession

IST = dt.timezone(dt.timedelta(hours=5, minutes=30), name="Asia/Kolkata")
DEFAULT_DB = Path(os.getenv("ENGINE_DB_PATH", "engine_state.sqlite"))
PROM_URL = os.getenv("PROM_URL", "http://127.0.0.1:9103/metrics")


def _load_app_config() -> Dict[str, Any]:
    try:
        return read_config()
    except Exception:
        return {}


def _data_config() -> Dict[str, Any]:
    return _load_app_config().get("data", {})


def load_risk_limits() -> Dict[str, float]:
    cfg = _load_app_config()
    return (cfg.get("risk") or {})


def _get_cache(db_path: Path) -> InstrumentCache:
    cache = st.session_state.get("_instrument_cache")
    cache_path = st.session_state.get("_instrument_cache_path")
    if cache is None or cache_path != str(db_path):
        if cache is not None:
            try:
                cache.close()
            except Exception:
                pass
        cache = InstrumentCache(str(db_path))
        st.session_state["_instrument_cache"] = cache
        st.session_state["_instrument_cache_path"] = str(db_path)
    return cache


def _data_index_symbol() -> str:
    data_cfg = _data_config()
    return str(data_cfg.get("index_symbol", "NIFTY")).upper()


def load_weekly_weekday() -> int:
    data_cfg = _data_config()
    try:
        return int(data_cfg.get("weekly_expiry_weekday", 1))
    except (TypeError, ValueError):
        return 1


def load_holidays() -> set[dt.date]:
    data_cfg = _data_config()
    holidays: set[dt.date] = set()
    for raw in data_cfg.get("holidays", []):
        try:
            holidays.add(dt.date.fromisoformat(str(raw)))
        except ValueError:
            continue
    return holidays


def _read_underlying_spot(instrument: str) -> Optional[float]:
    """
    Fetch underlying LTP from metrics; returns None if unavailable.
    """

    target = f'instrument="{instrument.upper()}"'
    try:
        resp = requests.get(PROM_URL, timeout=2.5)
        resp.raise_for_status()
        for line in resp.text.splitlines():
            if not line.startswith("ltp_underlying{"):
                continue
            if target not in line:
                continue
            try:
                return float(line.rsplit("}", 1)[1].strip())
            except Exception:
                continue
    except Exception:
        return None
    return None


def resolve_next_weekly_expiry(now: Optional[dt.datetime] = None) -> str:
    """Legacy helper retained for compatibility; UI now relies on live metrics."""

    now_ist = (now or dt.datetime.now(IST)).astimezone(IST)
    weekday = load_weekly_weekday()
    holidays = load_holidays()
    delta = (weekday - now_ist.weekday()) % 7
    if delta == 0 and now_ist.time() >= dt.time(15, 30):
        delta = 7
    expiry = now_ist.date() + dt.timedelta(days=delta)
    while expiry.weekday() >= 5 or expiry in holidays:
        expiry -= dt.timedelta(days=1)
    return expiry.isoformat()


def _render_bt_summary(results: Dict[str, Any]) -> None:
    exec_meta = results.get("execution") or {}
    cost_meta = results.get("costs") or {}
    errors = results.get("errors") or []

    st.subheader("Backtest Summary")
    st.caption(
        f"Strategy: `{results.get('strategy')}` • Period: `{results.get('period')}` • Interval: `{results.get('interval')}` • "
        f"Underlying: `{results.get('underlying')}` • "
        f"Fill: `{exec_meta.get('fill_model', 'n/a')}` • Latency: `{int(exec_meta.get('latency_ms') or 0)}ms` • "
        f"Partials: `{bool(exec_meta.get('allow_partial_fills'))}` • "
        f"Spread: `{float(cost_meta.get('spread_bps') or 0.0):g}bps` • "
        f"Slippage: `{cost_meta.get('slippage_model', 'none')}` • "
        f"Candles: `{int(results.get('candles') or 0)}` • Run: `{results.get('run_id')}`"
    )
    col1, col2, col3, col4, col5 = st.columns(5)
    col1.metric(
        "Gross P&L",
        f"{float(results.get('gross_pnl') or (float(results.get('realized_pnl') or 0.0) + float(results.get('unrealized_pnl') or 0.0))):.2f}",
    )
    col2.metric("Fees", f"{float(results.get('total_fees') or results.get('fees') or 0.0):.2f}")
    col3.metric("Net P&L", f"{float(results.get('net_pnl') or 0.0):.2f}")
    col4.metric("Trades", int(results.get("trades") or 0))
    col5.metric("Win Rate", f"{float(results.get('win_rate') or 0.0) * 100.0:.1f}%")

    if errors:
        st.warning(f"Notes: {errors[:5]}{' ...' if len(errors) > 5 else ''}")

    with st.expander("Export", expanded=False):
        try:
            st.download_button(
                "Download summary.json",
                json.dumps(dict(results), indent=2, default=str).encode("utf-8"),
                file_name=f"summary_{results.get('run_id')}.json",
                mime="application/json",
                use_container_width=True,
            )
        except Exception:
            pass
        try:
            df_eq = pd.DataFrame(results.get("equity_curve") or [])
            if not df_eq.empty:
                st.download_button(
                    "Download equity.csv",
                    df_eq.to_csv(index=False).encode("utf-8"),
                    file_name=f"equity_{results.get('run_id')}.csv",
                    mime="text/csv",
                    use_container_width=True,
                )
        except Exception:
            pass
        try:
            df_trades = pd.DataFrame(results.get("trade_log") or [])
            if not df_trades.empty:
                st.download_button(
                    "Download trades.csv",
                    df_trades.to_csv(index=False).encode("utf-8"),
                    file_name=f"trades_{results.get('run_id')}.csv",
                    mime="text/csv",
                    use_container_width=True,
                )
        except Exception:
            pass
        try:
            df_orders = pd.DataFrame(results.get("orders") or [])
            if not df_orders.empty:
                st.download_button(
                    "Download orders.csv",
                    df_orders.to_csv(index=False).encode("utf-8"),
                    file_name=f"orders_{results.get('run_id')}.csv",
                    mime="text/csv",
                    use_container_width=True,
                )
        except Exception:
            pass
        try:
            analytics = _compute_bt_analytics(results)
            df_daily = pd.DataFrame(analytics.get("daily") or [])
            if not df_daily.empty:
                st.download_button(
                    "Download daily.csv",
                    df_daily.to_csv(index=False).encode("utf-8"),
                    file_name=f"daily_{results.get('run_id')}.csv",
                    mime="text/csv",
                    use_container_width=True,
                )
            df_monthly = pd.DataFrame(analytics.get("monthly") or [])
            if not df_monthly.empty:
                st.download_button(
                    "Download monthly.csv",
                    df_monthly.to_csv(index=False).encode("utf-8"),
                    file_name=f"monthly_{results.get('run_id')}.csv",
                    mime="text/csv",
                    use_container_width=True,
                )
        except Exception:
            pass


def _render_bt_charts(results: Dict[str, Any]) -> None:
    curve = results.get("equity_curve") or []
    if not curve:
        st.info("No equity curve snapshots recorded.")
        return
    df = pd.DataFrame(curve)
    if df.empty or "ts" not in df or "net" not in df:
        st.info("Equity curve unavailable.")
        return
    df["ts"] = pd.to_datetime(df["ts"])
    df = df.sort_values("ts").set_index("ts")
    df["peak"] = df["net"].cummax()
    df["drawdown"] = df["net"] - df["peak"]
    st.line_chart(df[["net"]])
    st.line_chart(df[["drawdown"]])


def _compute_bt_analytics(results: Dict[str, Any]) -> Dict[str, Any]:
    cached = results.get("analytics")
    if isinstance(cached, dict) and (cached.get("daily") or cached.get("monthly") or cached.get("legs")):
        return cached

    from engine.backtest.analytics import (
        aggregate_strategy_trades,
        compute_daily_metrics,
        compute_leg_breakdown,
        compute_monthly_heatmap,
    )

    trade_log = results.get("trade_log") or []
    strategy_trade_log = results.get("strategy_trade_log") or []
    if not strategy_trade_log and any(isinstance(r, dict) and r.get("strategy_trade_id") for r in trade_log):
        strategy_trade_log = aggregate_strategy_trades([r for r in trade_log if isinstance(r, dict)])

    strategy_level = strategy_trade_log or trade_log
    daily = compute_daily_metrics([r for r in strategy_level if isinstance(r, dict)])
    monthly = compute_monthly_heatmap(daily.get("daily") or [])
    legs = compute_leg_breakdown([r for r in trade_log if isinstance(r, dict)])

    analytics = {
        "daily": daily.get("daily") or [],
        "daily_summary": daily.get("summary") or {},
        "monthly": monthly.get("monthly") or [],
        "legs": legs.get("by_leg") or [],
        "opt_type": legs.get("by_opt_type") or [],
    }
    results["analytics"] = analytics
    return analytics


def _render_bt_daily_breakdown(results: Dict[str, Any]) -> None:
    analytics = _compute_bt_analytics(results)
    daily_rows = analytics.get("daily") or []
    if not daily_rows:
        st.info("No daily metrics available (no completed trades).")
        return

    daily_summary = analytics.get("daily_summary") or {}
    col1, col2, col3, col4 = st.columns(4)
    col1.metric("Days", int(daily_summary.get("days") or 0))
    col2.metric("Win Days", int(daily_summary.get("win_days") or 0))
    col3.metric("Loss Days", int(daily_summary.get("loss_days") or 0))
    col4.metric("Max Consecutive Loss Days", int(daily_summary.get("max_consecutive_losses") or 0))

    df_daily = pd.DataFrame(daily_rows)
    try:
        df_daily["trade_date"] = pd.to_datetime(df_daily["trade_date"])
        df_daily = df_daily.sort_values("trade_date")
    except Exception:
        pass

    st.subheader("Daily P&L")
    try:
        chart_df = df_daily[["trade_date", "daily_net_pnl"]].copy()
        chart_df = chart_df.set_index("trade_date")
        st.bar_chart(chart_df, y="daily_net_pnl")
    except Exception:
        st.dataframe(df_daily, use_container_width=True)

    st.dataframe(df_daily, use_container_width=True)

    st.subheader("Monthly Heatmap")
    df_monthly = pd.DataFrame(analytics.get("monthly") or [])
    if df_monthly.empty:
        st.info("No monthly rollup available.")
    else:
        try:
            month_names = {
                1: "Jan",
                2: "Feb",
                3: "Mar",
                4: "Apr",
                5: "May",
                6: "Jun",
                7: "Jul",
                8: "Aug",
                9: "Sep",
                10: "Oct",
                11: "Nov",
                12: "Dec",
            }
            pnl_pivot = df_monthly.pivot(index="year", columns="month", values="total_net_pnl").fillna(0.0)
            win_pivot = df_monthly.pivot(index="year", columns="month", values="winning_days_pct").fillna(0.0)
            pnl_pivot = pnl_pivot.rename(columns=month_names)
            win_pivot = win_pivot.rename(columns=month_names)
            st.caption("Total Net P&L per month")
            st.dataframe(pnl_pivot, use_container_width=True)
            st.caption("% winning days per month")
            st.dataframe(win_pivot, use_container_width=True)
        except Exception:
            st.dataframe(df_monthly, use_container_width=True)

    st.subheader("Leg Breakdown")
    df_legs = pd.DataFrame(analytics.get("legs") or [])
    if df_legs.empty:
        st.info("No leg-level breakdown available.")
    else:
        try:
            df_legs = df_legs.sort_values("net_pnl", ascending=False)
        except Exception:
            pass
        st.dataframe(df_legs, use_container_width=True)

    df_opt = pd.DataFrame(analytics.get("opt_type") or [])
    if not df_opt.empty:
        st.caption("P&L by option type (CE/PE)")
        try:
            df_opt = df_opt.sort_values("net_pnl", ascending=False)
        except Exception:
            pass
        st.dataframe(df_opt, use_container_width=True)


def _render_bt_trades(results: Dict[str, Any]) -> None:
    trades = results.get("trade_log") or []
    if not trades:
        st.info("No trades recorded.")
        return
    df_trades = pd.DataFrame(trades)

    # Strategy-level grouping (StockMock-style) when strategy_trade_id is present.
    df_strategy = None
    if "strategy_trade_id" in df_trades.columns:
        df_group_src = df_trades.copy()
        df_group_src["strategy_trade_id"] = df_group_src["strategy_trade_id"].astype(str)
        df_group_src = df_group_src[df_group_src["strategy_trade_id"].str.strip() != ""]
        if not df_group_src.empty:
            for col in ("opened_at", "closed_at"):
                if col in df_group_src.columns:
                    try:
                        df_group_src[col] = pd.to_datetime(df_group_src[col], errors="coerce")
                    except Exception:
                        pass

            def _join_unique(values: pd.Series) -> str:
                cleaned = [str(v) for v in values.dropna().tolist() if str(v).strip()]
                return ", ".join(sorted(set(cleaned)))

            df_strategy = (
                df_group_src.groupby("strategy_trade_id", as_index=False)
                .agg(
                    trade_date=("strategy_trade_id", lambda s: str(s.iloc[0])[:10]),
                    opened_at=("opened_at", "min"),
                    closed_at=("closed_at", "max"),
                    gross_pnl=("gross_pnl", "sum"),
                    fees=("fees", "sum"),
                    net_pnl=("net_pnl", "sum"),
                    leg_ids=("leg_id", _join_unique),
                    symbols=("symbol", _join_unique),
                    exit_reasons=("exit_reason", _join_unique),
                )
                .sort_values(["trade_date", "strategy_trade_id"], ascending=True)
            )

    if isinstance(df_strategy, pd.DataFrame) and not df_strategy.empty:
        st.subheader("Strategy Trades")
        strat_ids = df_strategy["strategy_trade_id"].dropna().astype(str).tolist()
        selected_strats = st.multiselect(
            "Strategy trade ids",
            options=strat_ids,
            default=strat_ids,
            key="bt_strategy_trade_ids",
        )
        if selected_strats:
            df_strategy = df_strategy[df_strategy["strategy_trade_id"].isin(selected_strats)]
        st.dataframe(df_strategy, use_container_width=True)
        st.divider()
        st.subheader("Leg Trades")
        if selected_strats and "strategy_trade_id" in df_trades.columns:
            df_trades = df_trades[df_trades["strategy_trade_id"].isin(selected_strats)]

    symbols = sorted(set(str(s) for s in df_trades.get("symbol", []).dropna().tolist()))
    selected = st.multiselect("Symbols", options=symbols, default=symbols, key="bt_tradebook_symbols")
    if selected:
        df_trades = df_trades[df_trades["symbol"].isin(selected)]
    st.dataframe(df_trades, use_container_width=True)


def _render_bt_orders(results: Dict[str, Any]) -> None:
    orders = results.get("orders") or []
    if orders:
        st.dataframe(pd.DataFrame(orders), use_container_width=True)
    else:
        st.info("No orders recorded.")

    executions = results.get("executions") or []
    if executions:
        with st.expander("Executions (fills)", expanded=False):
            st.dataframe(pd.DataFrame(executions), use_container_width=True)


def _render_bt_diagnostics(results: Dict[str, Any]) -> None:
    diagnostics = results.get("diagnostics") or {}
    errors = results.get("errors") or []

    diag_rows = []
    for key in (
        "history_cache_path",
        "first_candle_ts",
        "last_candle_ts",
        "cache_ensure_calls",
        "cache_missing_segments",
        "cache_fetched_rows",
        "cache_expected_bars",
        "cache_missing_bars",
        "option_bars_total",
        "option_oi_coverage_pct",
        "option_volume_coverage_pct",
        "series_loaded",
    ):
        if key in diagnostics:
            diag_rows.append({"key": key, "value": diagnostics.get(key)})
    if diag_rows:
        st.subheader("Diagnostics")
        st.dataframe(pd.DataFrame(diag_rows), use_container_width=True)

    data_warnings = results.get("data_warnings") or []
    if data_warnings:
        st.warning("Data quality warnings")
        st.code("\n".join(str(w) for w in data_warnings[:200]))
    else:
        st.success("No data-quality warnings.")

    if errors:
        st.warning("Engine/strategy notes")
        st.code("\n".join(str(e) for e in errors[:200]))

    # ---------------------------------------------------- Data Quality / Integrity
    try:
        from pathlib import Path
        import os

        from engine.backtest.data_quality import InstrumentRef, collect_instruments_from_results, compute_data_quality, load_sample_window
        from engine.backtest.instrument_master import InstrumentMaster
        from engine.config import EngineConfig, IST
    except Exception:
        return

    st.divider()
    st.subheader("Data Quality / Integrity")

    cache_path = str(diagnostics.get("history_cache_path") or os.getenv("HISTORY_CACHE_PATH", "cache/history.sqlite"))
    if not Path(cache_path).exists():
        st.warning(f"History cache not found at `{cache_path}`")
        return

    def _parse_period(period: object) -> tuple[Optional[dt.date], Optional[dt.date]]:
        text = str(period or "").strip()
        if "to" not in text:
            return None, None
        parts = [p.strip() for p in text.split("to", 1)]
        if len(parts) != 2:
            return None, None
        try:
            return dt.date.fromisoformat(parts[0]), dt.date.fromisoformat(parts[1])
        except Exception:
            return None, None

    start_date, end_date = _parse_period(results.get("period"))
    interval = str(results.get("interval") or "").strip().lower()
    underlying_key = str(results.get("underlying_key") or results.get("underlying") or "").strip()

    entry_time: Optional[dt.time] = None
    exit_time: Optional[dt.time] = None
    spec_json = results.get("spec_json")
    if spec_json:
        try:
            from engine.backtest.strategy_spec import BacktestRunSpec

            run_spec = BacktestRunSpec.from_json(str(spec_json))
            start_date = start_date or run_spec.config.start_date
            end_date = end_date or run_spec.config.end_date
            interval = interval or str(run_spec.config.interval)
            underlying_key = underlying_key or str(run_spec.config.underlying_instrument_key)
            entry_time = run_spec.config.entry_time
            exit_time = run_spec.config.exit_time
        except Exception:
            pass

    if not interval:
        interval = str(st.session_state.get("bt_interval") or "1minute").strip().lower()
    if start_date is None or end_date is None:
        start_date = start_date or st.session_state.get("bt_start_date")
        end_date = end_date or st.session_state.get("bt_end_date")

    if entry_time is None:
        try:
            entry_time = dt.time.fromisoformat(str(st.session_state.get("bt_entry_time") or "09:15"))
        except Exception:
            entry_time = dt.time(9, 15)
    if exit_time is None:
        try:
            exit_time = dt.time.fromisoformat(str(st.session_state.get("bt_exit_time") or "15:20"))
        except Exception:
            exit_time = dt.time(15, 20)

    if start_date is None or end_date is None:
        st.warning("Cannot compute data quality: start/end dates unavailable for this run.")
        return

    try:
        cfg = EngineConfig.load()
        holidays = getattr(cfg.data, "holidays", None)
    except Exception:
        holidays = None

    master = InstrumentMaster()
    instruments = collect_instruments_from_results(results, instrument_master=master, underlying_key=underlying_key)
    if not instruments:
        st.info("No instruments detected for this run.")
        return

    @st.cache_data(show_spinner=False)
    def _dq_compute(
        cache_path: str,
        cache_mtime: float,
        instruments_payload: tuple[tuple[str, str, Optional[str], Optional[str]], ...],
        interval: str,
        start_date: str,
        end_date: str,
        holidays_payload: tuple[str, ...],
    ) -> tuple[pd.DataFrame, pd.DataFrame]:
        inst = [InstrumentRef(instrument_key=k, kind=kind, symbol=sym, expiry=exp) for (k, kind, sym, exp) in instruments_payload]
        hols = [dt.date.fromisoformat(h) for h in holidays_payload] if holidays_payload else None
        rows, missing = compute_data_quality(
            history_cache_path=cache_path,
            instruments=inst,
            interval=interval,
            start_date=dt.date.fromisoformat(start_date),
            end_date=dt.date.fromisoformat(end_date),
            holiday_calendar=hols,
        )
        return pd.DataFrame(rows), pd.DataFrame(missing)

    cache_mtime = 0.0
    try:
        cache_mtime = float(Path(cache_path).stat().st_mtime)
    except Exception:
        cache_mtime = 0.0

    instruments_payload = tuple((i.instrument_key, i.kind, i.symbol, i.expiry) for i in instruments)
    holidays_payload = tuple(sorted({d.isoformat() for d in (holidays or []) if isinstance(d, dt.date)}))
    df_quality, df_missing = _dq_compute(
        cache_path,
        cache_mtime,
        instruments_payload,
        str(interval),
        str(start_date.isoformat()),
        str(end_date.isoformat()),
        holidays_payload,
    )

    if df_quality.empty:
        st.info("No candle data found in cache for this run's instruments.")
        return

    df_quality = df_quality.sort_values(["kind", "instrument_key"]).reset_index(drop=True)
    st.dataframe(df_quality, use_container_width=True)

    gap_rows = df_quality[df_quality["missing_bars"].fillna(0).astype(int) > 0]
    if not gap_rows.empty:
        offenders = ", ".join(gap_rows["instrument_key"].astype(str).head(4).tolist())
        extra = " …" if len(gap_rows) > 4 else ""
        st.warning(f"Detected missing bars for {len(gap_rows)} instrument(s): {offenders}{extra}")
    else:
        st.success("No missing bars detected for the requested range.")

    if not df_missing.empty:
        csv_bytes = df_missing.to_csv(index=False).encode("utf-8")
        st.download_button(
            "Export missing segments (CSV)",
            data=csv_bytes,
            file_name=f"missing_segments_{results.get('run_id')}.csv",
            mime="text/csv",
            use_container_width=True,
        )

    st.markdown("**Sample Candles Around Entry/Exit**")
    keys = df_quality["instrument_key"].astype(str).tolist()
    default_key = underlying_key if underlying_key in keys else (keys[0] if keys else "")
    sel_key = st.selectbox("Instrument", options=keys, index=(keys.index(default_key) if default_key in keys else 0), key="bt_dq_instrument")
    sample_date = st.date_input(
        "Sample date",
        value=start_date,
        min_value=start_date,
        max_value=end_date,
        key="bt_dq_sample_date",
    )
    bars_before = st.number_input("Bars before", min_value=1, max_value=20, value=3, step=1, key="bt_dq_bars_before")
    bars_after = st.number_input("Bars after", min_value=1, max_value=20, value=3, step=1, key="bt_dq_bars_after")

    entry_dt = dt.datetime.combine(sample_date, entry_time, tzinfo=IST)
    exit_dt = dt.datetime.combine(sample_date, exit_time, tzinfo=IST)

    col_a, col_b = st.columns(2)
    with col_a:
        st.caption(f"Entry window @ {entry_time.strftime('%H:%M')}")
        df_entry = load_sample_window(
            history_cache_path=cache_path,
            instrument_key=str(sel_key),
            interval=str(interval),
            center_dt=entry_dt,
            bars_before=int(bars_before),
            bars_after=int(bars_after),
        )
        st.dataframe(df_entry, use_container_width=True)
    with col_b:
        st.caption(f"Exit window @ {exit_time.strftime('%H:%M')}")
        df_exit = load_sample_window(
            history_cache_path=cache_path,
            instrument_key=str(sel_key),
            interval=str(interval),
            center_dt=exit_dt,
            bars_before=int(bars_before),
            bars_after=int(bars_after),
        )
        st.dataframe(df_exit, use_container_width=True)


def _render_backtest_results(results: Dict[str, Any]) -> None:
    tabs = st.tabs(["Summary", "Equity", "Daily Breakdown", "Trades", "Orders", "Diagnostics"])
    with tabs[0]:
        _render_bt_summary(results)
    with tabs[1]:
        _render_bt_charts(results)
    with tabs[2]:
        _render_bt_daily_breakdown(results)
    with tabs[3]:
        _render_bt_trades(results)
    with tabs[4]:
        _render_bt_orders(results)
    with tabs[5]:
        _render_bt_diagnostics(results)


def _render_backtesting_ui() -> None:
    import queue
    import threading
    import uuid
    from datetime import date, timedelta

    from engine.backtesting import BacktestingEngine
    from engine.backtest.strategy_spec import (
        ALLOWED_CANDLE_INTERVALS,
        ALLOWED_EXPIRY_MODES,
        ALLOWED_OPT_TYPES,
        ALLOWED_SIDES,
        ALLOWED_STRIKE_MODES,
        BacktestRunSpec,
        LegSpec,
        StrategySpec,
    )

    store_path = str(DEFAULT_DB)
    st.subheader("Backtesting")

    # -------------------------------------------------------------- run history
    runs = []
    try:
        from persistence.backtest_store import BacktestStore

        with BacktestStore(store_path) as bt_store:
            runs = bt_store.list_runs(limit=250)
    except Exception:
        runs = []

    run_by_id = {r.run_id: r for r in runs}
    run_ids = list(run_by_id.keys())

    def _fmt_run(rid: str) -> str:
        r = run_by_id.get(rid)
        if r is None:
            return str(rid)
        try:
            ts_dt = dt.datetime.fromtimestamp(int(r.created_ts), tz=dt.timezone.utc).astimezone(IST)
            ts_str = ts_dt.strftime("%Y-%m-%d %H:%M")
        except Exception:
            ts_str = str(r.created_ts)
        parts = [ts_str]
        if r.strategy:
            parts.append(str(r.strategy))
        parts.append(str(r.run_id))
        return " • ".join(parts)

    mode = st.radio("Mode", ["New run", "Load past run", "Compare runs"], horizontal=True, key="bt_mode")

    if mode == "Load past run":
        if not run_ids:
            st.info("No saved runs yet.")
            return
        selected_run = st.selectbox("Run", run_ids, format_func=_fmt_run, key="bt_load_run_id")
        if st.button("Load Run", use_container_width=True):
            try:
                from persistence.backtest_store import BacktestStore

                with BacktestStore(store_path) as bt_store:
                    st.session_state["backtest_results"] = bt_store.load_run(str(selected_run))
            except Exception as exc:
                st.error(f"Failed loading run: {exc}")
        results = st.session_state.get("backtest_results")
        if results:
            _render_backtest_results(results)
        return

    if mode == "Compare runs":
        if len(run_ids) < 2:
            st.info("Need at least 2 saved runs to compare.")
            return
        run_a = st.selectbox("Run A", run_ids, format_func=_fmt_run, key="bt_cmp_run_a")
        default_b_idx = 1 if len(run_ids) > 1 else 0
        run_b = st.selectbox("Run B", run_ids, format_func=_fmt_run, index=default_b_idx, key="bt_cmp_run_b")
        try:
            from persistence.backtest_store import BacktestStore

            with BacktestStore(store_path) as bt_store:
                a = bt_store.load_run(str(run_a))
                b = bt_store.load_run(str(run_b))
        except Exception as exc:
            st.error(f"Failed loading runs: {exc}")
            return

        st.subheader("Run Comparison")
        df_a = pd.DataFrame(a.get("equity_curve") or [])
        df_b = pd.DataFrame(b.get("equity_curve") or [])
        try:
            if not df_a.empty:
                df_a["ts"] = pd.to_datetime(df_a["ts"])
                df_a = df_a.sort_values("ts").set_index("ts")[["net"]].rename(columns={"net": str(run_a)})
            if not df_b.empty:
                df_b["ts"] = pd.to_datetime(df_b["ts"])
                df_b = df_b.sort_values("ts").set_index("ts")[["net"]].rename(columns={"net": str(run_b)})
            if not df_a.empty or not df_b.empty:
                st.line_chart(df_a.join(df_b, how="outer"))
        except Exception:
            pass

        rows = []
        for key in ("period", "interval", "gross_pnl", "total_fees", "net_pnl", "trades", "win_rate"):
            rows.append({"metric": key, "run_a": a.get(key), "run_b": b.get(key)})
        st.dataframe(pd.DataFrame(rows), use_container_width=True)

        col1, col2 = st.columns(2)
        with col1:
            st.download_button(
                "Run A summary.json",
                json.dumps(dict(a), indent=2, default=str).encode("utf-8"),
                file_name=f"summary_{run_a}.json",
                mime="application/json",
                use_container_width=True,
            )
            try:
                eq = pd.DataFrame(a.get("equity_curve") or [])
                if not eq.empty:
                    st.download_button(
                        "Run A equity.csv",
                        eq.to_csv(index=False).encode("utf-8"),
                        file_name=f"equity_{run_a}.csv",
                        mime="text/csv",
                        use_container_width=True,
                    )
            except Exception:
                pass
            try:
                trades = pd.DataFrame(a.get("trade_log") or [])
                if not trades.empty:
                    st.download_button(
                        "Run A trades.csv",
                        trades.to_csv(index=False).encode("utf-8"),
                        file_name=f"trades_{run_a}.csv",
                        mime="text/csv",
                        use_container_width=True,
                    )
            except Exception:
                pass
            try:
                orders = pd.DataFrame(a.get("orders") or [])
                if not orders.empty:
                    st.download_button(
                        "Run A orders.csv",
                        orders.to_csv(index=False).encode("utf-8"),
                        file_name=f"orders_{run_a}.csv",
                        mime="text/csv",
                        use_container_width=True,
                    )
            except Exception:
                pass
        with col2:
            st.download_button(
                "Run B summary.json",
                json.dumps(dict(b), indent=2, default=str).encode("utf-8"),
                file_name=f"summary_{run_b}.json",
                mime="application/json",
                use_container_width=True,
            )
            try:
                eq = pd.DataFrame(b.get("equity_curve") or [])
                if not eq.empty:
                    st.download_button(
                        "Run B equity.csv",
                        eq.to_csv(index=False).encode("utf-8"),
                        file_name=f"equity_{run_b}.csv",
                        mime="text/csv",
                        use_container_width=True,
                    )
            except Exception:
                pass
            try:
                trades = pd.DataFrame(b.get("trade_log") or [])
                if not trades.empty:
                    st.download_button(
                        "Run B trades.csv",
                        trades.to_csv(index=False).encode("utf-8"),
                        file_name=f"trades_{run_b}.csv",
                        mime="text/csv",
                        use_container_width=True,
                    )
            except Exception:
                pass
            try:
                orders = pd.DataFrame(b.get("orders") or [])
                if not orders.empty:
                    st.download_button(
                        "Run B orders.csv",
                        orders.to_csv(index=False).encode("utf-8"),
                        file_name=f"orders_{run_b}.csv",
                        mime="text/csv",
                        use_container_width=True,
                    )
            except Exception:
                pass
        return

    # ---------------------------------------------------------- backtest session
    st.session_state.setdefault("bt_config", {})
    st.session_state.setdefault("bt_exec_model", {})
    st.session_state.setdefault("bt_last_run_id", None)
    st.session_state.setdefault("bt_run_status", "idle")  # idle/running/stopped/done/failed
    st.session_state.setdefault("bt_errors", [])
    st.session_state.setdefault("bt_nav", "Builder")
    st.session_state.setdefault("bt_results_run_id", None)

    # ---------------------------------------------------------- builder helpers
    def _symbol_from_underlying_key(key: str) -> str:
        for sym, ik in INDEX_INSTRUMENT_KEYS.items():
            if str(ik) == str(key):
                return str(sym)
        return _data_index_symbol()

    def _leg_field_key(leg_id: str, field: str) -> str:
        return f"bt_leg_{leg_id}_{field}"

    def _clear_leg_keys(leg_id: str) -> None:
        for field in (
            "side",
            "opt_type",
            "qty_lots",
            "expiry_mode",
            "strike_mode",
            "strike_offset_points",
            "target_premium",
            "strike_display",
            "stoploss_type",
            "stoploss_value",
            "profit_target_type",
            "profit_target_value",
            "trailing_enabled",
            "trailing_type",
            "trailing_trigger",
            "trailing_step",
            # Legacy / backward-compat keys (kept for cleanup).
            "stoploss_pct",
            "profit_lock_trigger_pct",
            "profit_lock_lock_to_pct",
            "reentry_enabled",
            "max_reentries",
            "cool_down_minutes",
            "reentry_condition",
            "remove",
        ):
            st.session_state.pop(_leg_field_key(leg_id, field), None)

    def _ensure_legs_state() -> None:
        """
        Canonical leg ordering/state lives in `st.session_state["bt_legs"]`.

        Legacy sessions used `bt_leg_ids` + per-field widget keys; migrate those to
        UUID-backed leg IDs so deletes don't shift widget identity.
        """

        legs = st.session_state.get("bt_legs")
        if isinstance(legs, list):
            cleaned: list[dict[str, object]] = []
            for row in legs:
                if not isinstance(row, dict):
                    continue
                leg_id = str(row.get("leg_id") or "").strip()
                if not leg_id:
                    continue
                cleaned.append(row)
            if cleaned is not legs:
                st.session_state["bt_legs"] = cleaned
            # Always drop legacy fields once the new structure is present.
            st.session_state.pop("bt_leg_ids", None)
            st.session_state.pop("bt_leg_counter", None)
            return

        legacy_ids = list(st.session_state.get("bt_leg_ids") or [])
        if not legacy_ids:
            st.session_state["bt_legs"] = []
            st.session_state.pop("bt_leg_ids", None)
            st.session_state.pop("bt_leg_counter", None)
            return

        def _new_leg_id(existing: set[str]) -> str:
            for _ in range(100):
                candidate = uuid.uuid4().hex[:8]
                if candidate not in existing:
                    return candidate
            raise RuntimeError("Failed to allocate unique leg_id")

        defaults = {
            "side": "SELL",
            "opt_type": "CE",
            "qty_lots": 1,
            "expiry_mode": "WEEKLY_CURRENT",
            "strike_mode": "ATM",
            "strike_offset_points": 0,
            "target_premium": 0.0,
            "strike_display": "ATM",
            "stoploss_type": "(none)",
            "stoploss_value": 25.0,
            "profit_target_type": "(none)",
            "profit_target_value": 50.0,
            "trailing_enabled": False,
            "trailing_type": "POINTS",
            "trailing_trigger": 10.0,
            "trailing_step": 5.0,
            # Legacy / backward-compat keys (kept for migration).
            "stoploss_pct": "",
            "profit_lock_trigger_pct": "",
            "profit_lock_lock_to_pct": "",
            "reentry_enabled": False,
            "max_reentries": 0,
            "cool_down_minutes": 0,
            "reentry_condition": "premium_returns_to_entry_zone",
        }

        existing_ids: set[str] = set()
        migrated: list[dict[str, object]] = []
        for old_leg_id in legacy_ids:
            old_leg_id = str(old_leg_id)
            new_id = _new_leg_id(existing_ids)
            existing_ids.add(new_id)
            leg_state: dict[str, object] = {"leg_id": new_id}
            for field, default in defaults.items():
                old_key = _leg_field_key(old_leg_id, field)
                new_key = _leg_field_key(new_id, field)
                if old_key in st.session_state:
                    st.session_state[new_key] = st.session_state.get(old_key)
                else:
                    st.session_state[new_key] = default
                leg_state[field] = st.session_state.get(new_key)
            migrated.append(leg_state)
            _clear_leg_keys(old_leg_id)

        st.session_state["bt_legs"] = migrated
        st.session_state.pop("bt_leg_ids", None)
        st.session_state.pop("bt_leg_counter", None)

    def _add_leg(*, leg: Optional[LegSpec] = None, leg_id: Optional[str] = None) -> None:
        _ensure_legs_state()
        existing = {str(row.get("leg_id")) for row in (st.session_state.get("bt_legs") or []) if isinstance(row, dict)}
        desired = str(leg_id or "").strip()
        if desired and desired not in existing:
            leg_id = desired
        else:
            for _ in range(100):
                leg_id = uuid.uuid4().hex[:8]
                if leg_id not in existing:
                    break
            else:  # pragma: no cover (extremely unlikely)
                raise RuntimeError("Failed to allocate unique leg_id")

        stoploss_type = "(none)"
        stoploss_value = 25.0
        profit_target_type = "(none)"
        profit_target_value = 50.0
        trailing_enabled = False
        trailing_type = "POINTS"
        trailing_trigger = 10.0
        trailing_step = 5.0
        cool_down_minutes = 0
        reentry_condition = "premium_returns_to_entry_zone"

        if leg is not None:
            stoploss_type = str(getattr(leg, "stoploss_type", None) or "").strip().upper()
            if not stoploss_type and getattr(leg, "stoploss_pct", None) is not None:
                stoploss_type = "PREMIUM_PCT"
            if not stoploss_type:
                stoploss_type = "(none)"
            if stoploss_type == "PREMIUM_PCT":
                sl = getattr(leg, "stoploss_value", None)
                if sl is None:
                    sl = getattr(leg, "stoploss_pct", None)
                try:
                    sl_num = float(sl) if sl is not None else 0.25
                except Exception:
                    sl_num = 0.25
                stoploss_value = sl_num * 100.0 if sl_num <= 1.0 else sl_num
            elif stoploss_type in {"POINTS", "MTM"}:
                try:
                    stoploss_value = float(getattr(leg, "stoploss_value", None) or 10.0)
                except Exception:
                    stoploss_value = 10.0
            else:
                stoploss_type = "(none)"

            profit_target_type = str(getattr(leg, "profit_target_type", None) or "(none)").strip().upper()
            if profit_target_type == "(NONE)":
                profit_target_type = "(none)"
            if profit_target_type == "PREMIUM_PCT":
                try:
                    pt_num = float(getattr(leg, "profit_target_value", None) or 0.5)
                except Exception:
                    pt_num = 0.5
                profit_target_value = pt_num * 100.0 if pt_num <= 1.0 else pt_num
            elif profit_target_type in {"POINTS", "MTM"}:
                try:
                    profit_target_value = float(getattr(leg, "profit_target_value", None) or 50.0)
                except Exception:
                    profit_target_value = 50.0
            else:
                profit_target_type = "(none)"
                profit_target_value = 50.0

            profit_lock = getattr(leg, "profit_lock", None)
            trailing_enabled = bool(getattr(leg, "trailing_enabled", False) or profit_lock is not None)
            if trailing_enabled:
                trailing_type = str(
                    getattr(leg, "trailing_type", None) or ("PREMIUM_PCT" if profit_lock is not None else "POINTS")
                ).strip().upper()
                if trailing_type == "PREMIUM_PCT":
                    if getattr(leg, "trailing_trigger", None) is not None and getattr(leg, "trailing_step", None) is not None:
                        trailing_trigger = float(getattr(leg, "trailing_trigger", 0.1) or 0.1) * 100.0
                        trailing_step = float(getattr(leg, "trailing_step", 0.05) or 0.05) * 100.0
                    elif profit_lock is not None:
                        trigger_pct = float(getattr(profit_lock, "trigger_pct", 0.1) or 0.1)
                        lock_to_pct = float(getattr(profit_lock, "lock_to_pct", 0.0) or 0.0)
                        trailing_trigger = trigger_pct * 100.0
                        diff = max(trigger_pct - lock_to_pct, 0.0)
                        trailing_step = (diff if diff > 0 else max(trigger_pct * 0.25, 0.001)) * 100.0
                else:
                    try:
                        trailing_trigger = float(getattr(leg, "trailing_trigger", None) or 10.0)
                    except Exception:
                        trailing_trigger = 10.0
                    try:
                        trailing_step = float(getattr(leg, "trailing_step", None) or 5.0)
                    except Exception:
                        trailing_step = 5.0

            try:
                cool_down_minutes = int(getattr(leg, "cool_down_minutes", 0) or 0)
            except Exception:
                cool_down_minutes = 0
            reentry_condition = str(getattr(leg, "reentry_condition", None) or "premium_returns_to_entry_zone")

        state = {
            "leg_id": leg_id,
            "side": (leg.side if leg else "SELL"),
            "opt_type": (leg.opt_type if leg else "CE"),
            "qty_lots": int(leg.qty_lots if leg else 1),
            "expiry_mode": (leg.expiry_mode if leg else "WEEKLY_CURRENT"),
            "strike_mode": (leg.strike_mode if leg else "ATM"),
            "strike_offset_points": int(leg.strike_offset_points or 0) if leg else 0,
            "target_premium": float(leg.target_premium or 0.0) if leg else 0.0,
            "strike_display": "ATM",
            "stoploss_type": stoploss_type,
            "stoploss_value": float(stoploss_value),
            "profit_target_type": profit_target_type,
            "profit_target_value": float(profit_target_value),
            "trailing_enabled": bool(trailing_enabled),
            "trailing_type": trailing_type,
            "trailing_trigger": float(trailing_trigger),
            "trailing_step": float(trailing_step),
            "reentry_enabled": bool(leg.reentry_enabled) if leg else False,
            "max_reentries": int(leg.max_reentries or 0) if leg else 0,
            "cool_down_minutes": int(cool_down_minutes),
            "reentry_condition": reentry_condition,
        }

        for field in (
            "side",
            "opt_type",
            "qty_lots",
            "expiry_mode",
            "strike_mode",
            "strike_offset_points",
            "target_premium",
            "strike_display",
            "stoploss_type",
            "stoploss_value",
            "profit_target_type",
            "profit_target_value",
            "trailing_enabled",
            "trailing_type",
            "trailing_trigger",
            "trailing_step",
            "reentry_enabled",
            "max_reentries",
            "cool_down_minutes",
            "reentry_condition",
        ):
            st.session_state[_leg_field_key(leg_id, field)] = state[field]

        st.session_state["bt_legs"] = list(st.session_state.get("bt_legs") or []) + [state]

    def _reset_builder_from_spec(spec: StrategySpec) -> None:
        _ensure_legs_state()
        for row in list(st.session_state.get("bt_legs") or []):
            if not isinstance(row, dict):
                continue
            leg_id = str(row.get("leg_id") or "").strip()
            if leg_id:
                _clear_leg_keys(leg_id)
        for leg_id in list(st.session_state.get("bt_leg_ids") or []):
            _clear_leg_keys(str(leg_id))
        st.session_state["bt_legs"] = []
        st.session_state.pop("bt_leg_ids", None)
        st.session_state.pop("bt_leg_counter", None)

        st.session_state["bt_spec_name"] = spec.name
        st.session_state["bt_underlying_symbol"] = _symbol_from_underlying_key(spec.underlying_instrument_key)
        st.session_state["bt_start_date"] = spec.start_date
        st.session_state["bt_end_date"] = spec.end_date
        st.session_state["bt_interval"] = spec.candle_interval
        st.session_state["bt_entry_time"] = spec.entry_time
        st.session_state["bt_exit_time"] = spec.exit_time
        st.session_state["bt_fill_model"] = spec.fill_model
        st.session_state["bt_allow_partial_fills"] = bool(spec.allow_partial_fills)
        st.session_state["bt_latency_ms"] = int(spec.latency_ms)
        st.session_state["bt_slippage_model"] = spec.slippage_model
        st.session_state["bt_slippage_bps"] = float(spec.slippage_bps)
        st.session_state["bt_slippage_ticks"] = int(spec.slippage_ticks)
        st.session_state["bt_spread_bps"] = float(spec.spread_bps)
        st.session_state["bt_starting_capital"] = float(spec.starting_capital)
        st.session_state["bt_brokerage_profile"] = str(spec.brokerage_profile)

        risk = getattr(spec, "strategy_risk", None)
        st.session_state["bt_strategy_risk_max_daily_loss_mtm"] = float(getattr(risk, "max_daily_loss_mtm", 0.0) or 0.0)
        st.session_state["bt_strategy_risk_max_daily_profit_mtm"] = float(getattr(risk, "max_daily_profit_mtm", 0.0) or 0.0)
        try:
            force_exit = getattr(risk, "force_exit_time", None)
            st.session_state["bt_strategy_risk_force_exit_time"] = (
                f"{force_exit.hour:02d}:{force_exit.minute:02d}" if isinstance(force_exit, dt.time) else ""
            )
        except Exception:
            st.session_state["bt_strategy_risk_force_exit_time"] = ""
        try:
            cutoff = getattr(risk, "disable_entries_after_time", None)
            st.session_state["bt_strategy_risk_disable_entries_after_time"] = (
                f"{cutoff.hour:02d}:{cutoff.minute:02d}" if isinstance(cutoff, dt.time) else ""
            )
        except Exception:
            st.session_state["bt_strategy_risk_disable_entries_after_time"] = ""
        st.session_state["bt_strategy_risk_max_concurrent_positions"] = int(getattr(risk, "max_concurrent_positions", 0) or 0)
        st.session_state["bt_strategy_risk_max_trades_per_day"] = int(getattr(risk, "max_trades_per_day", 0) or 0)

        for leg in spec.legs:
            _add_leg(leg=leg)

    def _reset_builder_from_run_spec(run_spec: BacktestRunSpec) -> None:
        _ensure_legs_state()
        for row in list(st.session_state.get("bt_legs") or []):
            if not isinstance(row, dict):
                continue
            existing_leg_id = str(row.get("leg_id") or "").strip()
            if existing_leg_id:
                _clear_leg_keys(existing_leg_id)
        for legacy_leg_id in list(st.session_state.get("bt_leg_ids") or []):
            _clear_leg_keys(str(legacy_leg_id))
        st.session_state["bt_legs"] = []
        st.session_state.pop("bt_leg_ids", None)
        st.session_state.pop("bt_leg_counter", None)

        st.session_state["bt_spec_name"] = str(run_spec.name or "BacktestRunSpec")
        st.session_state["bt_underlying_symbol"] = _symbol_from_underlying_key(run_spec.config.underlying_instrument_key)
        st.session_state["bt_start_date"] = run_spec.config.start_date
        st.session_state["bt_end_date"] = run_spec.config.end_date
        st.session_state["bt_interval"] = run_spec.config.interval
        st.session_state["bt_entry_time"] = run_spec.config.entry_time
        st.session_state["bt_exit_time"] = run_spec.config.exit_time

        st.session_state["bt_fill_model"] = run_spec.execution_model.fill_model
        st.session_state["bt_allow_partial_fills"] = bool(run_spec.execution_model.allow_partial_fills)
        st.session_state["bt_latency_ms"] = int(run_spec.execution_model.latency_ms)
        st.session_state["bt_slippage_model"] = run_spec.execution_model.slippage_model
        st.session_state["bt_slippage_bps"] = float(run_spec.execution_model.slippage_bps)
        st.session_state["bt_slippage_ticks"] = int(run_spec.execution_model.slippage_ticks)
        st.session_state["bt_spread_bps"] = float(run_spec.execution_model.spread_bps)

        st.session_state["bt_starting_capital"] = float(run_spec.config.starting_capital)
        st.session_state["bt_brokerage_profile"] = str(run_spec.config.brokerage_profile)

        strat_risk = getattr(run_spec.risk, "strategy", None)
        st.session_state["bt_strategy_risk_max_daily_loss_mtm"] = float(getattr(strat_risk, "max_daily_loss_mtm", 0.0) or 0.0)
        st.session_state["bt_strategy_risk_max_daily_profit_mtm"] = float(getattr(strat_risk, "max_daily_profit_mtm", 0.0) or 0.0)
        try:
            force_exit = getattr(strat_risk, "force_exit_time", None)
            st.session_state["bt_strategy_risk_force_exit_time"] = (
                f"{force_exit.hour:02d}:{force_exit.minute:02d}" if isinstance(force_exit, dt.time) else ""
            )
        except Exception:
            st.session_state["bt_strategy_risk_force_exit_time"] = ""
        try:
            cutoff = getattr(strat_risk, "disable_entries_after_time", None)
            st.session_state["bt_strategy_risk_disable_entries_after_time"] = (
                f"{cutoff.hour:02d}:{cutoff.minute:02d}" if isinstance(cutoff, dt.time) else ""
            )
        except Exception:
            st.session_state["bt_strategy_risk_disable_entries_after_time"] = ""
        st.session_state["bt_strategy_risk_max_concurrent_positions"] = int(getattr(strat_risk, "max_concurrent_positions", 0) or 0)
        st.session_state["bt_strategy_risk_max_trades_per_day"] = int(getattr(strat_risk, "max_trades_per_day", 0) or 0)

        per_leg_risk = {r.leg_id: r for r in (run_spec.risk.per_leg or ())}
        for opt_leg in run_spec.legs:
            r = per_leg_risk.get(opt_leg.leg_id)
            leg = LegSpec(
                side=opt_leg.side,
                opt_type=opt_leg.option_type,
                qty_lots=int(opt_leg.qty),
                expiry_mode=opt_leg.expiry_selector.mode,
                strike_mode=opt_leg.strike_selector.mode,
                strike_offset_points=opt_leg.strike_selector.offset_points,
                target_premium=opt_leg.strike_selector.target_premium,
                stoploss_type=(r.stoploss_type if r else None),
                stoploss_value=(r.stoploss_value if r else None),
                profit_target_type=(r.profit_target_type if r else None),
                profit_target_value=(r.profit_target_value if r else None),
                trailing_enabled=bool(r.trailing_enabled) if r else False,
                trailing_type=(r.trailing_type if r else None),
                trailing_trigger=(r.trailing_trigger if r else None),
                trailing_step=(r.trailing_step if r else None),
                reentry_enabled=bool(r.reentry_enabled) if r else False,
                max_reentries=int(r.max_reentries) if r else 0,
                cool_down_minutes=int(r.cool_down_minutes) if r else 0,
                reentry_condition=(r.reentry_condition if r else None),
            )
            _add_leg(leg=leg, leg_id=opt_leg.leg_id)

    def _ensure_builder_defaults() -> None:
        _ensure_legs_state()
        if st.session_state.get("bt_builder_initialized"):
            return
        today = date.today()
        default_end = today - timedelta(days=1)
        default_start = default_end - timedelta(days=7)
        underlying_symbol = _data_index_symbol()
        underlying_key = INDEX_INSTRUMENT_KEYS.get(underlying_symbol, underlying_symbol)
        try:
            from engine.backtest import templates as bt_templates

            spec = bt_templates.ShortStraddle(
                underlying_instrument_key=underlying_key,
                start_date=default_start,
                end_date=default_end,
                candle_interval="5minute",
                qty_lots=1,
            )
        except Exception:
            spec = StrategySpec(
                name="StrategySpec",
                underlying_instrument_key=underlying_key,
                start_date=default_start,
                end_date=default_end,
                candle_interval="5minute",
                entry_time=dt.time(9, 30),
                exit_time=dt.time(15, 20),
                fill_model="next_tick",
                allow_partial_fills=False,
                latency_ms=0,
                slippage_model="none",
                slippage_bps=0.0,
                slippage_ticks=0,
                spread_bps=0.0,
                brokerage_profile="india_options_default",
                starting_capital=100000.0,
                legs=(),
            )
        _reset_builder_from_spec(spec)
        st.session_state["bt_builder_initialized"] = True

    def _build_spec_from_state() -> StrategySpec:
        _ensure_legs_state()
        underlying_symbol = str(st.session_state.get("bt_underlying_symbol") or _data_index_symbol()).upper()
        underlying_key = INDEX_INSTRUMENT_KEYS.get(underlying_symbol, underlying_symbol)
        start_date = st.session_state.get("bt_start_date")
        end_date = st.session_state.get("bt_end_date")
        interval = str(st.session_state.get("bt_interval") or "5minute")
        entry_time = st.session_state.get("bt_entry_time") or dt.time(9, 30)
        exit_time = st.session_state.get("bt_exit_time") or dt.time(15, 20)
        entry_time = entry_time.replace(second=0, microsecond=0)
        exit_time = exit_time.replace(second=0, microsecond=0)

        def _opt_float(raw: object) -> Optional[float]:
            if raw is None:
                return None
            text = str(raw).strip()
            if not text:
                return None
            return float(text)

        def _normalize_pct_ui(raw: object, field_name: str) -> float:
            try:
                num = float(raw)
            except (TypeError, ValueError) as exc:
                raise ValueError(f"{field_name} must be a number") from exc
            if num <= 0:
                raise ValueError(f"{field_name} must be > 0")
            return (num / 100.0) if num > 1.0 else num

        def _risk_type(raw: object) -> Optional[str]:
            text = str(raw or "").strip().upper()
            if not text or text == "(NONE)":
                return None
            if text in {"PCT", "%", "PREMIUM%"}:
                return "PREMIUM_PCT"
            return text

        legs: list[LegSpec] = []
        updated_leg_state: list[dict[str, object]] = []
        for leg_row in list(st.session_state.get("bt_legs") or []):
            if not isinstance(leg_row, dict):
                continue
            leg_id = str(leg_row.get("leg_id") or "").strip()
            if not leg_id:
                continue

            strike_mode = str(st.session_state.get(_leg_field_key(leg_id, "strike_mode")) or leg_row.get("strike_mode") or "ATM").upper()
            strike_offset_points = None
            target_premium = None
            if strike_mode == "ATM_OFFSET":
                strike_offset_points = int(st.session_state.get(_leg_field_key(leg_id, "strike_offset_points")) or 0)
            elif strike_mode == "TARGET_PREMIUM":
                target_premium = float(st.session_state.get(_leg_field_key(leg_id, "target_premium")) or 0.0)

            stoploss_type = _risk_type(st.session_state.get(_leg_field_key(leg_id, "stoploss_type")))
            stoploss_value = None
            if stoploss_type is not None:
                raw_val = st.session_state.get(_leg_field_key(leg_id, "stoploss_value"))
                if stoploss_type == "PREMIUM_PCT":
                    stoploss_value = _normalize_pct_ui(raw_val, "stoploss_value")
                else:
                    stoploss_value = _opt_float(raw_val)
                    if stoploss_value is None or stoploss_value <= 0:
                        raise ValueError("stoploss_value must be > 0")

            if stoploss_type is None:
                legacy_stoploss = _opt_float(st.session_state.get(_leg_field_key(leg_id, "stoploss_pct")))
                if legacy_stoploss is not None:
                    stoploss_type = "PREMIUM_PCT"
                    stoploss_value = _normalize_pct_ui(legacy_stoploss, "stoploss_pct")

            profit_target_type = _risk_type(st.session_state.get(_leg_field_key(leg_id, "profit_target_type")))
            profit_target_value = None
            if profit_target_type is not None:
                raw_val = st.session_state.get(_leg_field_key(leg_id, "profit_target_value"))
                if profit_target_type == "PREMIUM_PCT":
                    profit_target_value = _normalize_pct_ui(raw_val, "profit_target_value")
                else:
                    profit_target_value = _opt_float(raw_val)
                    if profit_target_value is None or profit_target_value <= 0:
                        raise ValueError("profit_target_value must be > 0")

            trailing_enabled = bool(st.session_state.get(_leg_field_key(leg_id, "trailing_enabled")) or False)
            trailing_type = None
            trailing_trigger = None
            trailing_step = None
            if trailing_enabled:
                trailing_type = _risk_type(st.session_state.get(_leg_field_key(leg_id, "trailing_type"))) or "POINTS"
                raw_trigger = st.session_state.get(_leg_field_key(leg_id, "trailing_trigger"))
                raw_step = st.session_state.get(_leg_field_key(leg_id, "trailing_step"))
                if trailing_type == "PREMIUM_PCT":
                    trailing_trigger = _normalize_pct_ui(raw_trigger, "trailing_trigger")
                    trailing_step = _normalize_pct_ui(raw_step, "trailing_step")
                else:
                    trailing_trigger = _opt_float(raw_trigger)
                    trailing_step = _opt_float(raw_step)
                    if trailing_trigger is None or trailing_trigger <= 0 or trailing_step is None or trailing_step <= 0:
                        raise ValueError("trailing_trigger/trailing_step must be > 0")

            if not trailing_enabled:
                pl_trigger = _opt_float(st.session_state.get(_leg_field_key(leg_id, "profit_lock_trigger_pct")))
                pl_lock_to = _opt_float(st.session_state.get(_leg_field_key(leg_id, "profit_lock_lock_to_pct")))
                if pl_trigger is not None and pl_lock_to is not None:
                    trigger = _normalize_pct_ui(pl_trigger, "profit_lock_trigger_pct")
                    lock_to = _normalize_pct_ui(pl_lock_to, "profit_lock_lock_to_pct")
                    trailing_enabled = True
                    trailing_type = "PREMIUM_PCT"
                    trailing_trigger = trigger
                    diff = max(trigger - lock_to, 0.0)
                    trailing_step = diff if diff > 0 else max(trigger * 0.25, 0.001)

            reentry_enabled = bool(st.session_state.get(_leg_field_key(leg_id, "reentry_enabled")) or False)
            max_reentries = int(st.session_state.get(_leg_field_key(leg_id, "max_reentries")) or 0)
            if not reentry_enabled:
                max_reentries = 0
            cool_down_minutes = int(st.session_state.get(_leg_field_key(leg_id, "cool_down_minutes")) or 0)
            reentry_condition = str(st.session_state.get(_leg_field_key(leg_id, "reentry_condition")) or "").strip() or None
            if not reentry_enabled:
                cool_down_minutes = 0
                reentry_condition = None

            leg_side = st.session_state.get(_leg_field_key(leg_id, "side")) or leg_row.get("side") or "SELL"
            leg_opt_type = st.session_state.get(_leg_field_key(leg_id, "opt_type")) or leg_row.get("opt_type") or "CE"
            leg_qty_lots = st.session_state.get(_leg_field_key(leg_id, "qty_lots")) or leg_row.get("qty_lots") or 1
            leg_expiry_mode = st.session_state.get(_leg_field_key(leg_id, "expiry_mode")) or leg_row.get("expiry_mode") or "WEEKLY_CURRENT"

            legs.append(
                LegSpec(
                    side=leg_side,
                    opt_type=leg_opt_type,
                    qty_lots=leg_qty_lots,
                    expiry_mode=leg_expiry_mode,
                    strike_mode=strike_mode,
                    strike_offset_points=strike_offset_points,
                    target_premium=target_premium,
                    stoploss_type=stoploss_type,
                    stoploss_value=stoploss_value,
                    profit_target_type=profit_target_type,
                    profit_target_value=profit_target_value,
                    trailing_enabled=trailing_enabled,
                    trailing_type=trailing_type,
                    trailing_trigger=trailing_trigger,
                    trailing_step=trailing_step,
                    reentry_enabled=reentry_enabled,
                    max_reentries=max_reentries,
                    cool_down_minutes=cool_down_minutes,
                    reentry_condition=reentry_condition,
                )
            )

            expiry_date = None
            try:
                from engine.backtest.expiry_resolver import resolve_expiry

                if isinstance(start_date, dt.date):
                    expiry_date = resolve_expiry(start_date, str(leg_expiry_mode))
            except Exception:
                expiry_date = None

            updated = dict(leg_row)
            updated.update(
                {
                    "leg_id": leg_id,
                    "side": leg_side,
                    "opt_type": leg_opt_type,
                    "qty_lots": int(leg_qty_lots) if str(leg_qty_lots).strip() else int(leg_row.get("qty_lots") or 1),
                    "expiry_mode": str(leg_expiry_mode),
                    "strike_mode": str(strike_mode),
                    "strike_offset_points": int(st.session_state.get(_leg_field_key(leg_id, "strike_offset_points")) or 0),
                    "target_premium": float(st.session_state.get(_leg_field_key(leg_id, "target_premium")) or 0.0),
                    "stoploss_type": str(st.session_state.get(_leg_field_key(leg_id, "stoploss_type")) or "(none)"),
                    "stoploss_value": float(st.session_state.get(_leg_field_key(leg_id, "stoploss_value")) or 0.0),
                    "profit_target_type": str(st.session_state.get(_leg_field_key(leg_id, "profit_target_type")) or "(none)"),
                    "profit_target_value": float(st.session_state.get(_leg_field_key(leg_id, "profit_target_value")) or 0.0),
                    "trailing_enabled": bool(st.session_state.get(_leg_field_key(leg_id, "trailing_enabled")) or False),
                    "trailing_type": str(st.session_state.get(_leg_field_key(leg_id, "trailing_type")) or "POINTS"),
                    "trailing_trigger": float(st.session_state.get(_leg_field_key(leg_id, "trailing_trigger")) or 0.0),
                    "trailing_step": float(st.session_state.get(_leg_field_key(leg_id, "trailing_step")) or 0.0),
                    "reentry_enabled": bool(reentry_enabled),
                    "max_reentries": int(max_reentries),
                    "cool_down_minutes": int(cool_down_minutes),
                    "reentry_condition": str(reentry_condition or ""),
                    "expiry_date": (expiry_date.isoformat() if isinstance(expiry_date, dt.date) else None),
                }
            )
            updated_leg_state.append(updated)

        st.session_state["bt_legs"] = updated_leg_state

        risk_payload: dict[str, object] = {}
        try:
            max_loss = float(st.session_state.get("bt_strategy_risk_max_daily_loss_mtm") or 0.0)
        except Exception:
            max_loss = 0.0
        if max_loss > 0:
            risk_payload["max_daily_loss_mtm"] = max_loss

        try:
            max_profit = float(st.session_state.get("bt_strategy_risk_max_daily_profit_mtm") or 0.0)
        except Exception:
            max_profit = 0.0
        if max_profit > 0:
            risk_payload["max_daily_profit_mtm"] = max_profit

        force_exit_time = str(st.session_state.get("bt_strategy_risk_force_exit_time") or "").strip()
        if force_exit_time:
            risk_payload["force_exit_time"] = force_exit_time
        disable_entries_after_time = str(st.session_state.get("bt_strategy_risk_disable_entries_after_time") or "").strip()
        if disable_entries_after_time:
            risk_payload["disable_entries_after_time"] = disable_entries_after_time

        try:
            max_pos = int(st.session_state.get("bt_strategy_risk_max_concurrent_positions") or 0)
        except Exception:
            max_pos = 0
        if max_pos > 0:
            risk_payload["max_concurrent_positions"] = max_pos

        try:
            max_trades = int(st.session_state.get("bt_strategy_risk_max_trades_per_day") or 0)
        except Exception:
            max_trades = 0
        if max_trades > 0:
            risk_payload["max_trades_per_day"] = max_trades

        return StrategySpec(
            name=st.session_state.get("bt_spec_name") or "StrategySpec",
            underlying_instrument_key=underlying_key,
            start_date=start_date,
            end_date=end_date,
            candle_interval=interval,
            entry_time=entry_time,
            exit_time=exit_time,
            fill_model=st.session_state.get("bt_fill_model") or "next_tick",
            allow_partial_fills=bool(st.session_state.get("bt_allow_partial_fills") or False),
            latency_ms=int(st.session_state.get("bt_latency_ms") or 0),
            slippage_model=st.session_state.get("bt_slippage_model") or "none",
            slippage_bps=float(st.session_state.get("bt_slippage_bps") or 0.0),
            slippage_ticks=int(st.session_state.get("bt_slippage_ticks") or 0),
            spread_bps=float(st.session_state.get("bt_spread_bps") or 0.0),
            brokerage_profile=str(st.session_state.get("bt_brokerage_profile") or "india_options_default"),
            starting_capital=float(st.session_state.get("bt_starting_capital") or 100000.0),
            legs=tuple(legs),
            strategy_risk=(risk_payload or None),
        )

    _ensure_builder_defaults()

    # -------------------------------------------------------------- worker poll
    worker = st.session_state.get("bt_worker")
    if isinstance(worker, dict):
        prog_q = worker.get("progress_q")
        if prog_q is not None:
            try:
                while True:
                    done, total = prog_q.get_nowait()
                    st.session_state["bt_progress_done"] = int(done)
                    st.session_state["bt_progress_total"] = int(total)
            except queue.Empty:
                pass
        result_q = worker.get("result_q")
        if result_q is not None:
            try:
                kind, payload = result_q.get_nowait()
            except queue.Empty:
                kind, payload = None, None
            if kind == "done":
                st.session_state["backtest_results"] = payload
                st.session_state.pop("bt_run_error", None)
                st.session_state["bt_run_status"] = "done"
                try:
                    run_id = payload.get("run_id") if isinstance(payload, dict) else None
                except Exception:
                    run_id = None
                if run_id:
                    st.session_state["bt_last_run_id"] = str(run_id)
                    st.session_state["bt_results_run_id"] = str(run_id)
                # Auto-switch to results view after completion.
                st.session_state["bt_nav"] = "Results"
                st.session_state.pop("bt_worker", None)
            elif kind == "error":
                st.session_state["bt_run_error"] = str(payload)
                st.session_state["bt_run_status"] = "failed"
                # Keep the user on the Builder tab so they can adjust settings.
                if st.session_state.get("bt_nav") == "Results":
                    st.session_state["bt_nav"] = "Builder"
                st.session_state.pop("bt_worker", None)

    # -------------------------------------------------------------- navigation
    worker = st.session_state.get("bt_worker")
    running = bool(isinstance(worker, dict) and worker.get("thread") and worker["thread"].is_alive())
    if running and st.session_state.get("bt_run_status") in {"idle", "done", "failed"}:
        st.session_state["bt_run_status"] = "running"

    nav_options = ["Builder", "Results", "Tradebook", "Charts", "Orders", "Diagnostics"]
    if st.session_state.get("bt_nav") not in nav_options:
        st.session_state["bt_nav"] = "Builder"

    status = str(st.session_state.get("bt_run_status") or "idle")
    last_run = st.session_state.get("bt_last_run_id")
    status_line = f"Status: `{status}`"
    if last_run:
        status_line += f" • Last run: `{last_run}`"
    st.caption(status_line)

    active_tab = st.radio("Backtest", nav_options, horizontal=True, key="bt_nav", label_visibility="collapsed")

    def _select_results() -> Optional[Dict[str, Any]]:
        results_mem = st.session_state.get("backtest_results")
        candidate = st.session_state.get("bt_results_run_id") or st.session_state.get("bt_last_run_id")
        if not candidate and isinstance(results_mem, dict):
            candidate = results_mem.get("run_id")
        if run_ids:
            if candidate not in run_ids:
                candidate = run_ids[0]
            if st.session_state.get("bt_results_run_id") != candidate:
                st.session_state["bt_results_run_id"] = candidate
            st.selectbox("Run", run_ids, format_func=_fmt_run, key="bt_results_run_id")
            candidate = st.session_state.get("bt_results_run_id")
        if not candidate:
            return results_mem if isinstance(results_mem, dict) else None
        if isinstance(results_mem, dict) and str(results_mem.get("run_id")) == str(candidate):
            return results_mem
        try:
            from persistence.backtest_store import BacktestStore

            with BacktestStore(store_path) as bt_store:
                return bt_store.load_run(str(candidate))
        except Exception:
            return results_mem if isinstance(results_mem, dict) else None

    if active_tab == "Builder":
        left, right = st.columns([1.2, 0.8], gap="large")

        # ------------------------------ Strategy Builder (inputs)
        spec_obj: Optional[StrategySpec] = None
        run_spec_obj: Optional[BacktestRunSpec] = None
        spec_error: Optional[str] = None
        underlying_symbol = str(st.session_state.get("bt_underlying_symbol") or _data_index_symbol()).upper()
        underlying_key = INDEX_INSTRUMENT_KEYS.get(underlying_symbol, underlying_symbol)

        with left:
            st.subheader("Strategy Builder")

            underlying_options = sorted(INDEX_INSTRUMENT_KEYS.keys())
            st.selectbox("Underlying", underlying_options, key="bt_underlying_symbol")
            underlying_symbol = str(st.session_state.get("bt_underlying_symbol") or _data_index_symbol()).upper()
            underlying_key = INDEX_INSTRUMENT_KEYS.get(underlying_symbol, underlying_symbol)
            st.caption(f"Instrument key: `{underlying_key}`")

            today = date.today()
            default_end = today - timedelta(days=1)
            default_start = default_end - timedelta(days=7)
            date_range = st.date_input(
                "Date range",
                value=(st.session_state.get("bt_start_date") or default_start, st.session_state.get("bt_end_date") or default_end),
                key="bt_date_range",
            )
            if isinstance(date_range, (list, tuple)) and len(date_range) == 2:
                st.session_state["bt_start_date"], st.session_state["bt_end_date"] = date_range
            else:
                st.session_state["bt_start_date"] = date_range
                st.session_state["bt_end_date"] = date_range

            st.selectbox("Interval", sorted(ALLOWED_CANDLE_INTERVALS), key="bt_interval")

            t1, t2 = st.columns(2)
            t1.time_input("Entry time", key="bt_entry_time")
            t2.time_input("Exit time", key="bt_exit_time")

            st.text_input("Name", key="bt_spec_name")

            try:
                from engine.backtest import templates as bt_templates

                template_defs = bt_templates.list_templates()
            except Exception:
                bt_templates = None  # type: ignore[assignment]
                template_defs = []

            st.subheader("Template")
            if template_defs and bt_templates is not None:
                templates_by_id = {t.template_id: t for t in template_defs}
                template_ids = ["(none)"] + sorted(templates_by_id.keys())

                def _fmt_template(tid: str) -> str:
                    if tid == "(none)":
                        return "(none)"
                    t = templates_by_id.get(tid)
                    if t is None:
                        return str(tid)
                    if t.description:
                        return f"{t.display_name} — {t.description}"
                    return t.display_name

                st.selectbox("Template", template_ids, key="bt_template_id", format_func=_fmt_template)
                selected = st.session_state.get("bt_template_id")
                if selected and selected != "(none)":
                    try:
                        tmpl = templates_by_id.get(str(selected)) or bt_templates.get_template(str(selected))
                        if tmpl.description:
                            st.caption(tmpl.description)
                    except Exception:
                        pass

                apply_clicked = st.button("Apply Template", use_container_width=True, key="bt_template_apply")
                if apply_clicked and selected and selected != "(none)":
                    st.session_state["bt_template_pending"] = str(selected)

                pending = st.session_state.get("bt_template_pending")
                if pending:
                    try:
                        meta = templates_by_id.get(str(pending)) or bt_templates.get_template(str(pending))
                        title = meta.display_name
                    except Exception:
                        title = str(pending)
                    st.warning(
                        f"Apply template `{title}`? This overwrites the current strategy builder (legs, interval, entry/exit times, name)."
                    )
                    c1, c2 = st.columns(2)
                    if c1.button("Confirm apply", use_container_width=True, key="bt_template_confirm"):
                        tmpl_spec = bt_templates.build_template(
                            str(pending),
                            underlying_instrument_key=underlying_key,
                            start_date=st.session_state.get("bt_start_date"),
                            end_date=st.session_state.get("bt_end_date"),
                            fill_model=st.session_state.get("bt_fill_model") or "next_tick",
                            allow_partial_fills=bool(st.session_state.get("bt_allow_partial_fills") or False),
                            latency_ms=int(st.session_state.get("bt_latency_ms") or 0),
                            slippage_model=st.session_state.get("bt_slippage_model") or "none",
                            slippage_bps=float(st.session_state.get("bt_slippage_bps") or 0.0),
                            slippage_ticks=int(st.session_state.get("bt_slippage_ticks") or 0),
                            spread_bps=float(st.session_state.get("bt_spread_bps") or 0.0),
                            brokerage_profile=str(st.session_state.get("bt_brokerage_profile") or "india_options_default"),
                            starting_capital=float(st.session_state.get("bt_starting_capital") or 100000.0),
                        )
                        _reset_builder_from_spec(tmpl_spec)
                        st.session_state.pop("bt_template_pending", None)
                        st.rerun()
                    if c2.button("Cancel", use_container_width=True, key="bt_template_cancel"):
                        st.session_state.pop("bt_template_pending", None)
                        st.rerun()
            else:
                st.info("Templates unavailable.")

            st.subheader("Positions (Legs)")
            add_col, hint_col = st.columns([0.35, 0.65])
            with add_col:
                if st.button("Add leg", use_container_width=True):
                    _add_leg()
                    st.rerun()
            with hint_col:
                st.caption("Configure SL/TP/trailing/re-entry per leg in the expandable risk section.")

            legs_state = list(st.session_state.get("bt_legs") or [])
            if not legs_state:
                st.warning("Add at least one leg.")

            def _ensure_leg_widget_defaults(leg_row: dict[str, object]) -> None:
                leg_id = str(leg_row.get("leg_id") or "").strip()
                if not leg_id:
                    return
                stoploss_type = str(leg_row.get("stoploss_type") or "").strip().upper()
                legacy_stoploss = leg_row.get("stoploss_pct")
                if not stoploss_type or stoploss_type == "(NONE)":
                    stoploss_type = "PREMIUM_PCT" if legacy_stoploss not in (None, "", 0, "0") else "(none)"
                stoploss_value = leg_row.get("stoploss_value")
                if stoploss_type == "PREMIUM_PCT":
                    if stoploss_value is None:
                        stoploss_value = legacy_stoploss if legacy_stoploss not in (None, "") else 25.0
                    try:
                        sl_num = float(stoploss_value)  # may be fraction or percent
                    except Exception:
                        sl_num = 25.0
                    stoploss_value = (sl_num * 100.0) if sl_num <= 1.0 else sl_num
                else:
                    try:
                        stoploss_value = float(stoploss_value) if stoploss_value is not None else 10.0
                    except Exception:
                        stoploss_value = 10.0

                profit_target_type = str(leg_row.get("profit_target_type") or "(none)").strip().upper()
                if profit_target_type == "(NONE)":
                    profit_target_type = "(none)"
                profit_target_value = leg_row.get("profit_target_value")
                if profit_target_type == "PREMIUM_PCT":
                    try:
                        pt_num = float(profit_target_value if profit_target_value is not None else 50.0)
                    except Exception:
                        pt_num = 50.0
                    profit_target_value = (pt_num * 100.0) if pt_num <= 1.0 else pt_num
                else:
                    try:
                        profit_target_value = float(profit_target_value if profit_target_value is not None else 50.0)
                    except Exception:
                        profit_target_value = 50.0

                trailing_enabled = bool(leg_row.get("trailing_enabled") or False)
                trailing_type = str(leg_row.get("trailing_type") or "POINTS").strip().upper() or "POINTS"
                trailing_trigger = leg_row.get("trailing_trigger")
                trailing_step = leg_row.get("trailing_step")
                if trailing_type == "PREMIUM_PCT":
                    try:
                        trg_num = float(trailing_trigger if trailing_trigger is not None else 10.0)
                    except Exception:
                        trg_num = 10.0
                    try:
                        step_num = float(trailing_step if trailing_step is not None else 5.0)
                    except Exception:
                        step_num = 5.0
                    trailing_trigger = (trg_num * 100.0) if trg_num <= 1.0 else trg_num
                    trailing_step = (step_num * 100.0) if step_num <= 1.0 else step_num
                else:
                    try:
                        trailing_trigger = float(trailing_trigger if trailing_trigger is not None else 10.0)
                    except Exception:
                        trailing_trigger = 10.0
                    try:
                        trailing_step = float(trailing_step if trailing_step is not None else 5.0)
                    except Exception:
                        trailing_step = 5.0

                defaults = {
                    "side": str(leg_row.get("side") or "SELL"),
                    "opt_type": str(leg_row.get("opt_type") or "CE"),
                    "qty_lots": int(leg_row.get("qty_lots") or 1),
                    "expiry_mode": str(leg_row.get("expiry_mode") or "WEEKLY_CURRENT"),
                    "strike_mode": str(leg_row.get("strike_mode") or "ATM"),
                    "strike_offset_points": int(leg_row.get("strike_offset_points") or 0),
                    "target_premium": float(leg_row.get("target_premium") or 0.0),
                    "strike_display": str(leg_row.get("strike_display") or "ATM"),
                    "stoploss_type": stoploss_type,
                    "stoploss_value": float(stoploss_value),
                    "profit_target_type": profit_target_type,
                    "profit_target_value": float(profit_target_value),
                    "trailing_enabled": bool(trailing_enabled),
                    "trailing_type": trailing_type,
                    "trailing_trigger": float(trailing_trigger),
                    "trailing_step": float(trailing_step),
                    "reentry_enabled": bool(leg_row.get("reentry_enabled") or False),
                    "max_reentries": int(leg_row.get("max_reentries") or 0),
                    "cool_down_minutes": int(leg_row.get("cool_down_minutes") or 0),
                    "reentry_condition": str(leg_row.get("reentry_condition") or "premium_returns_to_entry_zone"),
                }
                for field, default in defaults.items():
                    k = _leg_field_key(leg_id, field)
                    if k not in st.session_state:
                        st.session_state[k] = default

            for leg_idx, leg_row in enumerate(legs_state, start=1):
                if not isinstance(leg_row, dict):
                    continue
                leg_id = str(leg_row.get("leg_id") or "").strip()
                if not leg_id:
                    continue
                _ensure_leg_widget_defaults(leg_row)
                row = st.columns([1.1, 0.9, 0.8, 1.3, 1.3, 1.6, 0.8])
                row[0].selectbox("Side", sorted(ALLOWED_SIDES), key=_leg_field_key(leg_id, "side"), label_visibility="collapsed")
                row[1].selectbox("CE/PE", sorted(ALLOWED_OPT_TYPES), key=_leg_field_key(leg_id, "opt_type"), label_visibility="collapsed")
                row[2].number_input("Lots", min_value=1, step=1, key=_leg_field_key(leg_id, "qty_lots"), label_visibility="collapsed")
                row[3].selectbox("Expiry", sorted(ALLOWED_EXPIRY_MODES), key=_leg_field_key(leg_id, "expiry_mode"), label_visibility="collapsed")
                row[4].selectbox("Strike", sorted(ALLOWED_STRIKE_MODES), key=_leg_field_key(leg_id, "strike_mode"), label_visibility="collapsed")

                strike_mode = str(st.session_state.get(_leg_field_key(leg_id, "strike_mode")) or "ATM").upper()
                if strike_mode == "ATM_OFFSET":
                    row[5].number_input("Offset (pts)", step=50, key=_leg_field_key(leg_id, "strike_offset_points"), label_visibility="collapsed")
                elif strike_mode == "TARGET_PREMIUM":
                    row[5].number_input("Target premium", min_value=0.0, step=1.0, key=_leg_field_key(leg_id, "target_premium"), label_visibility="collapsed")
                else:
                    row[5].text_input("—", value="ATM", disabled=True, label_visibility="collapsed", key=_leg_field_key(leg_id, "strike_display"))

                if row[6].button("Remove", key=_leg_field_key(leg_id, "remove"), use_container_width=True):
                    st.session_state["bt_legs"] = [
                        x for x in st.session_state.get("bt_legs") or [] if isinstance(x, dict) and str(x.get("leg_id")) != str(leg_id)
                    ]
                    _clear_leg_keys(str(leg_id))
                    st.rerun()

                with st.expander(f"Leg {leg_idx} risk", expanded=False):
                    st.markdown("**Stoploss**")
                    sl1, sl2 = st.columns([0.55, 0.45])
                    stoploss_type = sl1.selectbox(
                        "Stoploss type",
                        ["(none)", "PREMIUM_PCT", "POINTS", "MTM"],
                        key=_leg_field_key(leg_id, "stoploss_type"),
                    )
                    if stoploss_type == "PREMIUM_PCT":
                        sl2.number_input(
                            "Stoploss (% of premium)",
                            min_value=0.01,
                            max_value=1000.0,
                            step=1.0,
                            key=_leg_field_key(leg_id, "stoploss_value"),
                        )
                    elif stoploss_type == "POINTS":
                        sl2.number_input(
                            "Stoploss (premium points)",
                            min_value=0.01,
                            max_value=100000.0,
                            step=1.0,
                            key=_leg_field_key(leg_id, "stoploss_value"),
                        )
                    elif stoploss_type == "MTM":
                        sl2.number_input(
                            "Stoploss (MTM ₹)",
                            min_value=0.01,
                            max_value=100000000.0,
                            step=100.0,
                            key=_leg_field_key(leg_id, "stoploss_value"),
                        )
                    else:
                        sl2.caption("No stoploss configured.")

                    st.markdown("**Profit target**")
                    pt1, pt2 = st.columns([0.55, 0.45])
                    profit_target_type = pt1.selectbox(
                        "Profit target type",
                        ["(none)", "PREMIUM_PCT", "POINTS", "MTM"],
                        key=_leg_field_key(leg_id, "profit_target_type"),
                    )
                    if profit_target_type == "PREMIUM_PCT":
                        pt2.number_input(
                            "Profit target (% of premium)",
                            min_value=0.01,
                            max_value=1000.0,
                            step=1.0,
                            key=_leg_field_key(leg_id, "profit_target_value"),
                        )
                    elif profit_target_type == "POINTS":
                        pt2.number_input(
                            "Profit target (premium points)",
                            min_value=0.01,
                            max_value=100000.0,
                            step=1.0,
                            key=_leg_field_key(leg_id, "profit_target_value"),
                        )
                    elif profit_target_type == "MTM":
                        pt2.number_input(
                            "Profit target (MTM ₹)",
                            min_value=0.01,
                            max_value=100000000.0,
                            step=100.0,
                            key=_leg_field_key(leg_id, "profit_target_value"),
                        )
                    else:
                        pt2.caption("No profit target configured.")

                    st.markdown("**Trailing**")
                    trailing_enabled = st.checkbox("Enable trailing", key=_leg_field_key(leg_id, "trailing_enabled"))
                    if trailing_enabled:
                        tr1, tr2, tr3 = st.columns(3)
                        trailing_type = tr1.selectbox(
                            "Trailing type",
                            ["PREMIUM_PCT", "POINTS", "MTM"],
                            key=_leg_field_key(leg_id, "trailing_type"),
                        )
                        if trailing_type == "PREMIUM_PCT":
                            tr2.number_input(
                                "Trigger (% profit)",
                                min_value=0.01,
                                max_value=1000.0,
                                step=1.0,
                                key=_leg_field_key(leg_id, "trailing_trigger"),
                            )
                            tr3.number_input(
                                "Trail step (% profit)",
                                min_value=0.01,
                                max_value=1000.0,
                                step=1.0,
                                key=_leg_field_key(leg_id, "trailing_step"),
                            )
                        elif trailing_type == "POINTS":
                            tr2.number_input(
                                "Trigger (points)",
                                min_value=0.01,
                                max_value=100000.0,
                                step=1.0,
                                key=_leg_field_key(leg_id, "trailing_trigger"),
                            )
                            tr3.number_input(
                                "Trail step (points)",
                                min_value=0.01,
                                max_value=100000.0,
                                step=1.0,
                                key=_leg_field_key(leg_id, "trailing_step"),
                            )
                        else:
                            tr2.number_input(
                                "Trigger (MTM ₹)",
                                min_value=0.01,
                                max_value=100000000.0,
                                step=100.0,
                                key=_leg_field_key(leg_id, "trailing_trigger"),
                            )
                            tr3.number_input(
                                "Trail step (MTM ₹)",
                                min_value=0.01,
                                max_value=100000000.0,
                                step=100.0,
                                key=_leg_field_key(leg_id, "trailing_step"),
                            )

                    st.markdown("**Re-entry**")
                    reentry_enabled = st.checkbox("Enable re-entry", key=_leg_field_key(leg_id, "reentry_enabled"))
                    rr1, rr2, rr3 = st.columns(3)
                    rr1.number_input(
                        "Max re-entries",
                        min_value=1 if reentry_enabled else 0,
                        step=1,
                        key=_leg_field_key(leg_id, "max_reentries"),
                        disabled=not reentry_enabled,
                    )
                    rr2.number_input(
                        "Cooldown (minutes)",
                        min_value=0,
                        step=1,
                        key=_leg_field_key(leg_id, "cool_down_minutes"),
                        disabled=not reentry_enabled,
                    )
                    rr3.selectbox(
                        "Re-entry condition",
                        [
                            "premium_returns_to_entry_zone",
                            "after_stoploss",
                            "after_profit_target",
                        ],
                        key=_leg_field_key(leg_id, "reentry_condition"),
                        disabled=not reentry_enabled,
                    )

            try:
                spec_obj = _build_spec_from_state()
            except Exception as exc:
                spec_error = str(exc)
                spec_obj = None

            if spec_obj is not None:
                try:
                    leg_ids = [
                        str(row.get("leg_id"))
                        for row in (st.session_state.get("bt_legs") or [])
                        if isinstance(row, dict) and str(row.get("leg_id") or "").strip()
                    ]
                    run_spec_obj = BacktestRunSpec.from_strategy_spec(spec_obj, leg_ids=leg_ids, timezone="Asia/Kolkata")
                except Exception as exc:
                    run_spec_obj = None
                    st.warning(f"Failed to build BacktestRunSpec (falling back to StrategySpec JSON): {exc}")

                with st.expander("Run Spec JSON", expanded=False):
                    payload_json = run_spec_obj.to_json() if run_spec_obj is not None else spec_obj.to_json()
                    st.code(payload_json)
                    st.download_button(
                        "Save Spec JSON",
                        payload_json.encode("utf-8"),
                        file_name=f"{(run_spec_obj.name if run_spec_obj is not None else spec_obj.name)}.json",
                        mime="application/json",
                        use_container_width=True,
                    )
                    uploaded = st.file_uploader("Upload Spec JSON", type=["json"], key="bt_spec_upload")
                    if uploaded is not None and st.button("Load uploaded spec", use_container_width=True, key="bt_load_spec_upload"):
                        try:
                            raw_loaded = uploaded.getvalue().decode("utf-8")
                            try:
                                loaded_run = BacktestRunSpec.from_json(raw_loaded)
                                _reset_builder_from_run_spec(loaded_run)
                            except Exception:
                                loaded = StrategySpec.from_json(raw_loaded)
                                _reset_builder_from_spec(loaded)
                            st.rerun()
                        except Exception as exc:
                            st.error(f"Invalid uploaded spec: {exc}")
                    raw = st.text_area("Paste Spec JSON", value="", height=140, key="bt_spec_paste")
                    if st.button("Load pasted spec", use_container_width=True, key="bt_load_spec_paste"):
                        try:
                            raw_text = str(raw)
                            try:
                                loaded_run = BacktestRunSpec.from_json(raw_text)
                                _reset_builder_from_run_spec(loaded_run)
                            except Exception:
                                loaded = StrategySpec.from_json(raw_text)
                                _reset_builder_from_spec(loaded)
                            st.rerun()
                        except Exception as exc:
                            st.error(f"Invalid spec JSON: {exc}")

        # ------------------------------ Execution Model + Run Controls
        with right:
            st.subheader("Execution Model")
            st.selectbox("Fill model", ["same_tick", "next_tick"], key="bt_fill_model")
            st.number_input("Latency (ms)", min_value=0, max_value=600_000, value=int(st.session_state.get("bt_latency_ms") or 0), step=50, key="bt_latency_ms")
            st.checkbox("Allow partial fills (volume-limited)", value=bool(st.session_state.get("bt_allow_partial_fills") or False), key="bt_allow_partial_fills")

            st.subheader("Costs")
            slippage_model = st.selectbox("Slippage model", ["none", "bps", "ticks"], key="bt_slippage_model")
            if slippage_model == "bps":
                st.number_input(
                    "Slippage (bps)",
                    min_value=0.0,
                    max_value=500.0,
                    value=float(st.session_state.get("bt_slippage_bps") or 0.0),
                    step=0.5,
                    key="bt_slippage_bps",
                )
                st.session_state["bt_slippage_ticks"] = 0
            elif slippage_model == "ticks":
                st.number_input(
                    "Slippage (ticks)",
                    min_value=0,
                    max_value=500,
                    value=int(st.session_state.get("bt_slippage_ticks") or 0),
                    step=1,
                    key="bt_slippage_ticks",
                )
                st.session_state["bt_slippage_bps"] = 0.0
            else:
                st.session_state["bt_slippage_bps"] = 0.0
                st.session_state["bt_slippage_ticks"] = 0
            st.number_input(
                "Spread (bps)",
                min_value=0.0,
                max_value=500.0,
                value=float(st.session_state.get("bt_spread_bps") or 0.0),
                step=0.5,
                key="bt_spread_bps",
            )

            st.subheader("Strategy Risk")
            st.number_input(
                "Max daily loss (MTM ₹)",
                min_value=0.0,
                value=float(st.session_state.get("bt_strategy_risk_max_daily_loss_mtm") or 0.0),
                step=100.0,
                key="bt_strategy_risk_max_daily_loss_mtm",
            )
            st.number_input(
                "Max daily profit (MTM ₹)",
                min_value=0.0,
                value=float(st.session_state.get("bt_strategy_risk_max_daily_profit_mtm") or 0.0),
                step=100.0,
                key="bt_strategy_risk_max_daily_profit_mtm",
            )
            st.text_input(
                "Force exit time (HH:MM, optional)",
                value=str(st.session_state.get("bt_strategy_risk_force_exit_time") or ""),
                key="bt_strategy_risk_force_exit_time",
                placeholder="15:20",
            )
            st.text_input(
                "Disable entries after (HH:MM, optional)",
                value=str(st.session_state.get("bt_strategy_risk_disable_entries_after_time") or ""),
                key="bt_strategy_risk_disable_entries_after_time",
                placeholder="14:30",
            )
            st.number_input(
                "Max concurrent positions",
                min_value=0,
                value=int(st.session_state.get("bt_strategy_risk_max_concurrent_positions") or 0),
                step=1,
                key="bt_strategy_risk_max_concurrent_positions",
            )
            st.number_input(
                "Max trades per day",
                min_value=0,
                value=int(st.session_state.get("bt_strategy_risk_max_trades_per_day") or 0),
                step=1,
                key="bt_strategy_risk_max_trades_per_day",
            )

            st.subheader("Capital")
            st.number_input(
                "Starting capital",
                min_value=1.0,
                value=float(st.session_state.get("bt_starting_capital") or 100000.0),
                step=1000.0,
                key="bt_starting_capital",
            )
            st.selectbox("Brokerage profile", ["india_options_default"], disabled=True, key="bt_brokerage_profile")

            st.subheader("Run Controls")
            worker = st.session_state.get("bt_worker")
            running = bool(isinstance(worker, dict) and worker.get("thread") and worker["thread"].is_alive())
            if running:
                done = int(st.session_state.get("bt_progress_done") or 0)
                total = int(st.session_state.get("bt_progress_total") or 0)
                pct = int((done / total) * 100) if total > 0 else 0
                st.progress(min(max(pct, 0), 100), text=f"Processed {done}/{total} candles")

            run_err = st.session_state.get("bt_run_error")
            if run_err:
                st.error(f"Backtest failed: {run_err}")
            if spec_error:
                st.error(f"Spec invalid: {spec_error}")

            start_disabled = running or run_spec_obj is None
            start_col, stop_col = st.columns(2)
            start_clicked = start_col.button("Start Backtest", use_container_width=True, disabled=start_disabled, key="bt_start")
            stop_clicked = stop_col.button("Stop Backtest", use_container_width=True, disabled=not running, key="bt_stop")

            if start_clicked and run_spec_obj is not None:
                st.session_state.pop("backtest_results", None)
                st.session_state.pop("bt_run_error", None)
                st.session_state["bt_errors"] = []
                st.session_state["bt_run_status"] = "running"
                st.session_state["bt_progress_done"] = 0
                st.session_state["bt_progress_total"] = 0

                progress_q: queue.Queue = queue.Queue(maxsize=50)
                result_q: queue.Queue = queue.Queue(maxsize=2)

                run_spec = run_spec_obj
                exec_cfg = run_spec.to_execution_config()
                engine = BacktestingEngine(
                    store_path=store_path,
                    execution_config=exec_cfg,
                    slippage_model=str(run_spec.execution_model.slippage_model),
                    slippage_bps=float(run_spec.execution_model.slippage_bps),
                    slippage_ticks=int(run_spec.execution_model.slippage_ticks),
                    spread_bps=float(run_spec.execution_model.spread_bps),
                    starting_capital=float(run_spec.config.starting_capital),
                )

                def _on_progress(done: int, total: int) -> None:
                    try:
                        progress_q.put_nowait((int(done), int(total)))
                    except Exception:
                        return

                def _run() -> None:
                    try:
                        results = engine.run_backtest_run_spec(
                            run_spec,
                            progress_callback=_on_progress,
                            underlying_symbol=underlying_symbol,
                        )
                        try:
                            if isinstance(results, dict):
                                results.setdefault("spec_json", run_spec.to_json())
                                results.setdefault("underlying_key", str(results.get("underlying_key") or underlying_key))
                        except Exception:
                            pass
                        try:
                            from persistence.backtest_store import BacktestStore

                            with BacktestStore(store_path) as bt_store:
                                bt_store.save_run(
                                    run_id=str(results.get("run_id") or ""),
                                    results=dict(results),
                                    spec_json=run_spec.to_json(),
                                    start_date=str(run_spec.config.start_date),
                                    end_date=str(run_spec.config.end_date),
                                    interval=str(run_spec.config.interval),
                                    underlying_key=str(results.get("underlying_key") or underlying_key),
                                    strategy=str(results.get("strategy") or "OptionsBacktestRunner"),
                                )
                        except Exception:
                            pass
                        result_q.put(("done", results))
                    except Exception as exc:
                        result_q.put(("error", str(exc)))
                    finally:
                        try:
                            engine.close()
                        except Exception:
                            pass

                thread = threading.Thread(target=_run, name=f"bt-{uuid.uuid4().hex[:6]}", daemon=True)
                st.session_state["bt_worker"] = {
                    "thread": thread,
                    "engine": engine,
                    "progress_q": progress_q,
                    "result_q": result_q,
                }
                thread.start()
                st.rerun()

            if stop_clicked:
                worker = st.session_state.get("bt_worker")
                try:
                    if isinstance(worker, dict):
                        eng = worker.get("engine")
                        if eng is not None:
                            eng.request_stop()
                            st.session_state["bt_run_status"] = "stopped"
                except Exception:
                    pass
                st.info("Stop requested…")

        # Keep unified session objects fresh (for persistence/exports later).
        try:
            st.session_state["bt_config"] = {
                "name": st.session_state.get("bt_spec_name"),
                "underlying_symbol": str(st.session_state.get("bt_underlying_symbol") or _data_index_symbol()).upper(),
                "underlying_key": INDEX_INSTRUMENT_KEYS.get(
                    str(st.session_state.get("bt_underlying_symbol") or _data_index_symbol()).upper(),
                    str(st.session_state.get("bt_underlying_symbol") or _data_index_symbol()).upper(),
                ),
                "start_date": st.session_state.get("bt_start_date"),
                "end_date": st.session_state.get("bt_end_date"),
                "interval": st.session_state.get("bt_interval"),
                "entry_time": st.session_state.get("bt_entry_time"),
                "exit_time": st.session_state.get("bt_exit_time"),
                "strategy_risk": {
                    "max_daily_loss_mtm": float(st.session_state.get("bt_strategy_risk_max_daily_loss_mtm") or 0.0),
                    "max_daily_profit_mtm": float(st.session_state.get("bt_strategy_risk_max_daily_profit_mtm") or 0.0),
                    "force_exit_time": str(st.session_state.get("bt_strategy_risk_force_exit_time") or ""),
                    "disable_entries_after_time": str(st.session_state.get("bt_strategy_risk_disable_entries_after_time") or ""),
                    "max_concurrent_positions": int(st.session_state.get("bt_strategy_risk_max_concurrent_positions") or 0),
                    "max_trades_per_day": int(st.session_state.get("bt_strategy_risk_max_trades_per_day") or 0),
                },
                "legs": list(st.session_state.get("bt_legs") or []),
            }
            st.session_state["bt_exec_model"] = {
                "fill_model": st.session_state.get("bt_fill_model"),
                "latency_ms": int(st.session_state.get("bt_latency_ms") or 0),
                "allow_partial_fills": bool(st.session_state.get("bt_allow_partial_fills") or False),
                "slippage_model": st.session_state.get("bt_slippage_model"),
                "slippage_bps": float(st.session_state.get("bt_slippage_bps") or 0.0),
                "slippage_ticks": int(st.session_state.get("bt_slippage_ticks") or 0),
                "spread_bps": float(st.session_state.get("bt_spread_bps") or 0.0),
                "starting_capital": float(st.session_state.get("bt_starting_capital") or 100000.0),
                "brokerage_profile": str(st.session_state.get("bt_brokerage_profile") or ""),
            }
        except Exception:
            pass

        errors_now: list[str] = []
        if spec_error:
            errors_now.append(str(spec_error))
        if st.session_state.get("bt_run_error"):
            errors_now.append(str(st.session_state.get("bt_run_error")))
        st.session_state["bt_errors"] = errors_now

    else:
        if running:
            done = int(st.session_state.get("bt_progress_done") or 0)
            total = int(st.session_state.get("bt_progress_total") or 0)
            pct = int((done / total) * 100) if total > 0 else 0
            st.progress(min(max(pct, 0), 100), text=f"Processed {done}/{total} candles")

        results = _select_results()
        if results is None:
            if running:
                st.info("Backtest running… results will be available when complete.")
            else:
                st.info("No backtest results yet. Configure a strategy in Builder and click Start Backtest.")
        else:
            if active_tab == "Results":
                _render_bt_summary(results)
            elif active_tab == "Tradebook":
                _render_bt_trades(results)
            elif active_tab == "Charts":
                _render_bt_charts(results)
            elif active_tab == "Orders":
                _render_bt_orders(results)
            elif active_tab == "Diagnostics":
                _render_bt_diagnostics(results)

    # Keep the UI responsive while the background thread runs.
    if running:
        time.sleep(0.5)
        st.rerun()



def _scrape_text(url: str) -> str:
    resp = requests.get(url, timeout=2)
    resp.raise_for_status()
    return resp.text


def _pull_metrics_text() -> Optional[str]:
    try:
        return _scrape_text(PROM_URL)
    except Exception:
        return None


def fetch_metrics(raw_text: Optional[str] = None) -> Dict[str, float]:
    if raw_text is None:
        raw_text = _pull_metrics_text()
    if not raw_text:
        return {}
    parsed: Dict[str, float] = {}
    for fam in text_string_to_metric_families(raw_text):
        for sample in fam.samples:
            name, labels, value = sample[:3]
            key = name
            if labels:
                label_text = ",".join(f'{k}="{v}"' for k, v in labels.items())
                key = f"{name}{{{label_text}}}"
            parsed[key] = value
    return parsed


def fetch_df(conn: sqlite3.Connection, query: str, params: tuple[Any, ...]) -> pd.DataFrame:
    return pd.read_sql_query(query, conn, params=params)


def fetch_latest_snapshot(conn: sqlite3.Connection, run_id: str) -> Optional[Dict[str, Any]]:
    row = conn.execute(
        "SELECT ts, realized, unrealized, fees, net, per_symbol FROM pnl_snapshots WHERE run_id=? ORDER BY ts DESC LIMIT 1",
        (run_id,),
    ).fetchone()
    if not row:
        return None
    entry = dict(row)
    entry["per_symbol"] = json.loads(entry["per_symbol"] or "{}")
    return entry


def fetch_pnl_history(conn: sqlite3.Connection, run_id: str, limit: int = 200) -> pd.DataFrame:
    rows = conn.execute(
        "SELECT ts, net FROM pnl_snapshots WHERE run_id=? ORDER BY ts DESC LIMIT ?",
        (run_id, limit),
    ).fetchall()
    if not rows:
        return pd.DataFrame(columns=["ts", "net"])
    df = pd.DataFrame(rows, columns=["ts", "net"])
    df["ts"] = pd.to_datetime(df["ts"])
    return df.sort_values("ts")


_OPTION_METRICS = {
    "md_subscription",
    "option_ltp",
    "option_bid",
    "option_ask",
    "option_iv",
    "option_oi",
    "option_last_ts_seconds",
    "option_bid_depth_price",
    "option_bid_depth_qty",
    "option_ask_depth_price",
    "option_ask_depth_qty",
}
_METRIC_LINE_RE = re.compile(r'^(?P<name>[a-zA-Z_:][\w:]*)(?:\{(?P<labels>[^}]*)\})?\s+(?P<value>[-+]?\d+(?:\.\d+)?(?:[eE][-+]?\d+)?)$')
_LABEL_RE = re.compile(r'(?P<key>[a-zA-Z_][\w:]*)="(?P<value>(?:\\.|[^"])*)"')


def _parse_prom_labels(raw: str) -> Dict[str, str]:
    labels: Dict[str, str] = {}
    for match in _LABEL_RE.finditer(raw):
        value = match.group("value")
        value = value.replace('\\"', '"').replace("\\\\", "\\")
        labels[match.group("key")] = value
    return labels


def _norm_labels(lbl: dict) -> dict:
    out = dict(lbl)
    # unify instrument key
    if "instrument" not in out and "instrument_key" in out:
        out["instrument"] = out["instrument_key"]
    # unify type
    if "type" not in out and "opt_type" in out:
        out["type"] = out["opt_type"]
    # symbol may be wrong (contract name). If it looks like 'NIFTY-YYYY-MM-DD-...'
    # try to recover underlying from instrument if missing/odd.
    import re

    sym_re = r"^(NIFTY|BANKNIFTY)-\d{4}-\d{2}-\d{2}-\d+(?:[CP]E)?$"
    if "symbol" not in out or re.match(sym_re, out.get("symbol", "")):
        # Try deriving underlying from instrument; fallback to parsing symbol itself.
        m = re.match(r"^(NIFTY|BANKNIFTY)-\d{4}-\d{2}-\d{2}-\d+(?:-)?[CP]E$", out.get("instrument", ""))
        if m:
            out["symbol"] = m.group(1)
        else:
            m = re.match(sym_re, out.get("symbol", ""))
            if m:
                out["symbol"] = m.group(1)
    return out


def _parse_metric(text: str, metric: str):
    import re

    pattern = re.compile(r'^%s\{([^}]*)\}\s+([0-9.eE+-]+)$' % re.escape(metric))
    lblre = re.compile(r'([a-zA-Z_][a-zA-Z0-9_]*)="([^"]*)"')
    out = []
    for line in text.splitlines():
        m = pattern.match(line.strip())
        if not m:
            continue
        labels, val = m.group(1), m.group(2)
        d = {mm.group(1): mm.group(2) for mm in lblre.finditer(labels)}
        out.append((_norm_labels(d), float(val)))
    return out


def _parse_option_metrics(raw_text: Optional[str]) -> Dict[str, Dict[str, Any]]:
    if not raw_text:
        return {}
    rows: Dict[str, Dict[str, Any]] = {}
    for line in raw_text.splitlines():
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        match = _METRIC_LINE_RE.match(line)
        if not match:
            continue
        name = match.group("name")
        if name not in _OPTION_METRICS:
            continue
        labels = _parse_prom_labels(match.group("labels") or "")
        key = labels.get("instrument_key")
        if not key:
            continue
        key = str(key)
        row = rows.setdefault(
            key,
            {
                "instrument_key": key,
                "symbol": "",
                "expiry": "",
                "opt_type": "",
                "strike": "",
                "subscribed": False,
                "ltp": None,
                "bid": None,
                "ask": None,
                "iv": None,
                "oi": None,
                "last_ts": None,
                "bid_depth": {},
                "ask_depth": {},
            },
        )
        if labels.get("symbol"):
            row["symbol"] = labels["symbol"]
        if labels.get("expiry"):
            row["expiry"] = labels["expiry"]
        if labels.get("opt_type"):
            row["opt_type"] = labels["opt_type"]
        if labels.get("strike"):
            row["strike"] = labels["strike"]
        try:
            value = float(match.group("value"))
        except ValueError:
            continue
        if name == "md_subscription":
            row["subscribed"] = value >= 0.5
        elif name == "option_ltp":
            row["ltp"] = value
        elif name == "option_bid":
            row["bid"] = value
        elif name == "option_ask":
            row["ask"] = value
        elif name == "option_iv":
            row["iv"] = value
        elif name == "option_oi":
            row["oi"] = value
        elif name == "option_bid_depth_price":
            level = int(labels.get("level", 0) or 0)
            if level:
                row["bid_depth"].setdefault(level, {})["price"] = value
        elif name == "option_bid_depth_qty":
            level = int(labels.get("level", 0) or 0)
            if level:
                row["bid_depth"].setdefault(level, {})["qty"] = value
        elif name == "option_ask_depth_price":
            level = int(labels.get("level", 0) or 0)
            if level:
                row["ask_depth"].setdefault(level, {})["price"] = value
        elif name == "option_ask_depth_qty":
            level = int(labels.get("level", 0) or 0)
            if level:
                row["ask_depth"].setdefault(level, {})["qty"] = value
        elif name == "option_last_ts_seconds":
            row["last_ts"] = value
    for row in rows.values():
        bids: list[dict[str, Any]] = []
        asks: list[dict[str, Any]] = []
        for lvl in sorted(row.get("bid_depth", {}).keys()):
            entry = row["bid_depth"].get(lvl) or {}
            bids.append({"price": entry.get("price"), "qty": entry.get("qty")})
        for lvl in sorted(row.get("ask_depth", {}).keys()):
            entry = row["ask_depth"].get(lvl) or {}
            asks.append({"price": entry.get("price"), "qty": entry.get("qty")})
        row["depth"] = {"bids": bids, "asks": asks}
        row.pop("bid_depth", None)
        row.pop("ask_depth", None)
    return rows


def _format_depth_levels(levels: List[Dict[str, Any]], depth_cap: int = 5) -> List[str]:
    formatted: List[str] = []
    limit = max(1, int(depth_cap or 1))
    for entry in (levels or [])[:limit]:
        price = entry.get("price")
        qty = entry.get("qty")
        if price is None and qty is None:
            continue
        px_text = f"{float(price):.2f}" if price is not None else ""
        if qty is None:
            formatted.append(px_text)
        else:
            formatted.append(f"{px_text}@{int(qty)}")
    return formatted


def _strike_sort_key(value: Optional[str]) -> float:
    if value is None or value == "":
        return float("inf")
    try:
        return float(value)
    except (TypeError, ValueError):
        return float("inf")


def _format_strike(value: Optional[str]) -> str:
    if value is None or value == "":
        return ""
    try:
        strike = float(value)
    except (TypeError, ValueError):
        return str(value)
    if strike.is_integer():
        return f"{int(strike)}"
    return f"{strike:.2f}".rstrip("0").rstrip(".")


def _build_option_table(rows: list[Dict[str, Any]]) -> pd.DataFrame:
    now = time.time()
    sorted_rows = sorted(rows, key=lambda row: (_strike_sort_key(row.get("strike")), row["instrument_key"]))
    records: list[Dict[str, Any]] = []
    for idx, row in enumerate(sorted_rows, start=1):
        bid = row.get("bid")
        ask = row.get("ask")
        spread = None
        if bid is not None and ask is not None:
            spread = max(ask - bid, 0.0)
        last_ts = row.get("last_ts")
        age = max(0.0, now - last_ts) if last_ts is not None else None
        records.append(
            {
                "Idx": idx,
                "Instrument": row["instrument_key"],
                "Symbol": row.get("symbol") or "",
                "Expiry": row.get("expiry") or "",
                "Strike": _format_strike(row.get("strike")),
                "Type": (row.get("opt_type") or "").upper(),
                "LTP": row.get("ltp"),
                "Bid": bid,
                "Ask": ask,
                "Spread": spread,
                "IV": row.get("iv"),
                "OI": row.get("oi"),
                "Age(s)": age,
            }
        )
    return pd.DataFrame(records)


def render_table(rows: List[Dict[str, Any]], title: str) -> None:
    st.caption(title)
    if not rows:
        st.warning("No contracts to display.")
        return
    if rows and "Instrument" in rows[0]:
        df = pd.DataFrame(rows)
        preferred = ["Instrument", "Symbol", "Expiry", "Strike", "Type", "LTP", "Bid", "Ask", "Spread", "IV", "OI", "Age(s)"]
        order = [col for col in preferred if col in df.columns]
        order += [col for col in df.columns if col not in order]
        st.dataframe(df[order], use_container_width=True)
        return
    st.dataframe(_build_option_table(rows), use_container_width=True)


def _coerce_float(value: object) -> Optional[float]:
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _parse_payload(raw: Any) -> Dict[str, Any]:
    if isinstance(raw, dict):
        return raw
    if isinstance(raw, str):
        try:
            return json.loads(raw)
        except Exception:
            return {}
    return {}


def latest_smoke_result(df: pd.DataFrame) -> Optional[Dict[str, Any]]:
    if df is None or df.empty:
        return None
    for _, row in df.iterrows():
        code = str(row.get("code") or "")
        if not code.startswith("SMOKE_TEST"):
            continue
        if code == "SMOKE_TEST_FILLED":
            continue
        payload = _parse_payload(row.get("payload"))
        payload["code"] = code
        payload["ts"] = row.get("ts")
        payload["reason"] = payload.get("reason") or row.get("reason")
        return payload
    return None


def insert_control_intent(conn: sqlite3.Connection, run_id: str, action: str, payload: Optional[Dict[str, Any]] = None) -> None:
    ts = dt.datetime.utcnow().isoformat()
    conn.execute(
        "INSERT INTO control_intents(run_id, ts, action, payload_json) VALUES (?, ?, ?, ?)",
        (run_id, ts, action, json.dumps(payload) if payload else None),
    )
    conn.commit()


def _active_metric_rows(rows: Dict[str, Dict[str, Any]], symbol: str, expiry: str) -> List[Dict[str, Any]]:
    filtered: List[Dict[str, Any]] = []
    for record in rows.values():
        sym = (record.get("symbol") or "").upper()
        exp = record.get("expiry") or ""
        if sym != symbol or exp != expiry:
            continue
        if int(record.get("subscribed", 0)) != 1:
            continue
        filtered.append(record)
    return filtered


def _chunked(seq: List[str], size: int) -> List[List[str]]:
    span = max(size, 1)
    return [seq[i : i + span] for i in range(0, len(seq), span)]


def fetch_rest_quotes(keys: List[str], batch_size: int = 15) -> Dict[str, Dict[str, Any]]:
    if not keys:
        return {}
    try:
        session = UpstoxSession()
    except Exception as exc:
        return {k: {"instrument_key": k, "error": f"Upstox session unavailable: {exc}"} for k in keys}
    quotes: Dict[str, Dict[str, Any]] = {}
    for batch in _chunked(keys, batch_size):
        try:
            resp = session.get_ltp(batch)
        except Exception as exc:
            msg = str(exc)
            for key in batch:
                quotes[key] = {"instrument_key": key, "error": msg}
            continue
        data = resp
        if isinstance(resp, dict):
            data = resp.get("data") or resp.get("ltp") or resp
        if isinstance(data, dict):
            entries = list(data.values())
        else:
            entries = data or []
        for entry in entries:
            if not isinstance(entry, dict):
                continue
            key = str(entry.get("instrument_key") or entry.get("instrument") or "")
            if not key:
                continue
            depth = entry.get("depth") or {}
            quotes[key] = {
                "instrument_key": key,
                "ltp": entry.get("ltp") or entry.get("last_price") or entry.get("close"),
                "bid": entry.get("bid") or entry.get("best_bid") or depth.get("buy_price") or depth.get("buy_price_1"),
                "ask": entry.get("ask") or entry.get("best_ask") or depth.get("sell_price") or depth.get("sell_price_1"),
                "iv": entry.get("iv"),
                "oi": entry.get("oi") or entry.get("open_interest"),
                "last_ts": entry.get("timestamp") or entry.get("ts"),
            }
    return quotes


def get_live_rows(symbol: str, engine_expiry: str):
    try:
        text = _scrape_text(PROM_URL)
    except Exception:
        return [], None, "metrics_scrape_failed"

    parsed = _parse_option_metrics(text)
    if not parsed:
        return [], None, "no_metrics"
    filtered = [row for row in parsed.values() if (row.get("symbol") or "").upper() == symbol.upper() and row.get("expiry")]
    if not filtered:
        return [], None, "no_metrics_for_symbol"
    expiries = sorted({row["expiry"] for row in filtered if row.get("expiry")})
    metrics_expiry = expiries[0] if expiries else None
    rows = []
    now = time.time()
    for row in filtered:
        if metrics_expiry and row.get("expiry") != metrics_expiry:
            continue
        bid_val = row.get("bid")
        ask_val = row.get("ask")
        spread = (ask_val - bid_val) if (ask_val is not None and bid_val is not None) else None
        last_ts = row.get("last_ts")
        depth = row.get("depth") or {}
        rows.append(
            {
                "Instrument": row["instrument_key"],
                "Symbol": row.get("symbol") or "",
                "Expiry": row.get("expiry") or "",
                "Strike": int(float(row["strike"])) if row.get("strike") else None,
                "Type": row.get("opt_type"),
                "LTP": row.get("ltp"),
                "Bid": bid_val,
                "Ask": ask_val,
                "Spread": spread,
                "IV": row.get("iv"),
                "OI": row.get("oi"),
                "Age(s)": (now - last_ts) if last_ts is not None else None,
                "Depth": depth,
            }
        )
    rows.sort(key=lambda r: (r["Type"] or "", r["Strike"] or 0))
    return rows, metrics_expiry, "metrics_ok"


st.set_page_config(page_title="Trading Engine Console", layout="wide")
st.title("Trading Engine Console")

mode = st.sidebar.radio("Mode", ["Live Trading", "Backtesting"], index=0)
if mode == "Backtesting":
    _render_backtesting_ui()
    st.stop()

db_path = Path(st.sidebar.text_input("State DB", value=str(DEFAULT_DB)))
run_id = st.sidebar.text_input("Run ID", value="dev-run")
auto = st.sidebar.checkbox("Auto refresh", value=True)
interval = st.sidebar.slider("Refresh seconds", min_value=1, max_value=10, value=2)
fallback_refresh = False
if auto:
    st.sidebar.caption("Streaming updates enabled")
    auto_fn = getattr(st, "autorefresh", None)
    if callable(auto_fn):
        auto_fn(interval=int(interval * 1000), key="refresh")
    else:
        fallback_refresh = True
        st.sidebar.caption("Streamlit build lacks autorefresh; using timer fallback.")

cache = _get_cache(db_path)
cfg_data = getattr(CONFIG, "data", None)
default_symbol = cfg_data.index_symbol if cfg_data else _data_index_symbol()
underlying = os.getenv("UNDERLYING", default_symbol).upper()
st.sidebar.markdown(f"**Underlying:** `{underlying}`")
try:
    expiries = engine_data.resolve_expiries_with_fallback(underlying)
except Exception as exc:
    st.error(f"Failed to resolve expiries: {exc}")
    st.stop()
if not expiries:
    st.error("No expiries available via engine resolver.")
    st.stop()
active_expiry = expiries[0]
st.sidebar.markdown(f"**Detected expiry (engine):** `{active_expiry}`")
window = st.sidebar.slider("ATM window (steps)", 1, 5, 2, 1)
opt_type_choice = st.sidebar.selectbox("Option type", ["CE", "PE"], index=0)
spot_fallback_text = st.sidebar.text_input("Spot fallback", value="23000")
spot_metric = _read_underlying_spot(underlying)
spot_guess = spot_metric if spot_metric is not None else (_coerce_float(spot_fallback_text) or 0.0)
try:
    ladder = cache.nearest_strikes(underlying, active_expiry, spot_guess, window=window, opt_type=opt_type_choice)
except Exception as exc:
    st.warning(f"Cache ladder failed: {exc}")
    try:
        ladder = cache.list_contracts_for_expiry(underlying, active_expiry, opt_type=opt_type_choice)
    except Exception:
        ladder = []
use_rest_fallback = st.sidebar.checkbox("Enable REST fallback when metrics missing", value=False)
if spot_metric is not None:
    st.caption(f"Detected spot (metrics): {spot_metric:,.2f}")
else:
    st.caption("Detected spot (metrics): n/a (using fallback)")

metrics_text = _pull_metrics_text()
metrics_data = fetch_metrics(metrics_text)
limits = load_risk_limits()

with sqlite3.connect(db_path) as conn:
    conn.row_factory = sqlite3.Row
    positions = fetch_df(conn, "SELECT symbol, qty, avg_price, opened_at, closed_at FROM positions WHERE run_id=?", (run_id,))
    orders = fetch_df(
        conn,
        "SELECT client_order_id, symbol, side, qty, state, last_update FROM orders WHERE run_id=? ORDER BY last_update DESC LIMIT 100",
        (run_id,),
    )
    incidents = fetch_df(conn, "SELECT ts, code, payload FROM incidents WHERE run_id=? ORDER BY ts DESC LIMIT 50", (run_id,))
    snapshot = fetch_latest_snapshot(conn, run_id)
    pnl_history = fetch_pnl_history(conn, run_id)

run_col1, run_col2, run_col3 = st.columns(3)
heartbeat = metrics_data.get("heartbeat_ts", 0.0)
lag = time.time() - heartbeat if heartbeat else float("nan")
run_col1.metric("Heartbeat", f"{heartbeat:.0f}")
run_col2.metric("Lag (s)", f"{lag:.1f}" if heartbeat else "n/a")
run_col3.metric("Open Orders", int(metrics_data.get("order_queue_depth", 0)))

st.subheader("Risk Dials")
risk_cols = st.columns(3)
realized = snapshot["realized"] if snapshot else 0.0
unrealized = snapshot["unrealized"] if snapshot else 0.0
fees = snapshot["fees"] if snapshot else 0.0
net = realized + unrealized - fees
daily_limit = limits.get("daily_pnl_stop", 0.0)
risk_cols[0].metric("Realized PnL", f"{realized:,.0f}", delta=None)
risk_cols[1].metric("Unrealized PnL", f"{unrealized:,.0f}", delta=None)
risk_cols[2].metric("Net PnL vs Stop", f"{net:,.0f}", delta=f"-{daily_limit:,.0f}" if daily_limit else None)

st.subheader("Open Positions")
st.dataframe(positions, use_container_width=True)

st.subheader("Orders")
st.dataframe(orders, use_container_width=True)

st.subheader("Subscribed Contracts (Live)")
st.caption(f"Data source: {PROM_URL}")
rows, metrics_expiry, mode = get_live_rows(underlying, active_expiry)
depth_cap = getattr(CONFIG.market_data, "depth_levels", 5)

if mode == "metrics_ok":
    st.success("Live metrics OK — showing subscribed contracts.")
    st.caption("Badge: metrics")
    max_age = max((row.get("Age(s)") or 0.0) for row in rows) if rows else 0.0
    st.caption(f"Detected expiry (metrics): `{metrics_expiry or 'n/a'}` • Detected expiry (engine): `{active_expiry}` • Tick age ≤ {max_age:.1f}s")
    render_table([{k: v for k, v in row.items() if k != "Depth"} for row in rows], "Live (metrics)")
    depth_rows = [row for row in rows if row.get("Depth")]
    if depth_rows:
        with st.expander("Depth (Top5)", expanded=False):
            depth_table: List[Dict[str, Any]] = []
            for row in depth_rows:
                depth = row.get("Depth") or {}
                bids = _format_depth_levels(depth.get("bids", []), depth_cap=depth_cap if depth_cap else 5)
                asks = _format_depth_levels(depth.get("asks", []), depth_cap=depth_cap if depth_cap else 5)
                depth_table.append(
                    {
                        "Instrument": row["Instrument"],
                        "Strike": row["Strike"],
                        "Type": row["Type"],
                        "Bid Depth": " | ".join(bids) if bids else "",
                        "Ask Depth": " | ".join(asks) if asks else "",
                    }
                )
            st.dataframe(pd.DataFrame(depth_table), use_container_width=True)
else:
    if mode in ("no_metrics", "metrics_scrape_failed", "no_metrics_for_symbol"):
        st.info("Metrics unavailable; showing cache ladder / REST fallback.")
    else:
        st.warning(f"Unexpected mode: {mode}; falling back to cache.")
    st.caption("Badge: REST fallback")
    fallback_rows = ladder
    if not fallback_rows:
        st.warning("No contracts available from the instrument cache for the detected expiry.")
    else:
        if use_rest_fallback:
            quote_map = fetch_rest_quotes([row["instrument_key"] for row in fallback_rows])
            errors = [info["error"] for info in quote_map.values() if info.get("error")]
            enriched: List[Dict[str, Any]] = []
            for row in fallback_rows:
                merged = dict(row)
                quote = quote_map.get(row["instrument_key"])
                if quote:
                    for field in ("ltp", "bid", "ask", "iv", "oi", "last_ts"):
                        if quote.get(field) is not None:
                            merged[field] = quote[field]
                enriched.append(merged)
            fallback_rows = enriched
            if errors:
                st.warning(f"REST fallback errors: {set(errors)}")
        render_table(fallback_rows, "Cache / REST fallback")

smoke_info = latest_smoke_result(incidents)
st.subheader("Smoke Test")
smoke_cols = st.columns([3, 2])
running_flag = metrics_data.get("smoke_test_running", 0)
status = "Running" if running_flag else "Idle"
reason = None
if smoke_info:
    code = smoke_info.get("code")
    if code == "SMOKE_TEST_OK":
        status = "Completed"
    elif code == "SMOKE_TEST_ABORT":
        status = "Aborted"
        reason = smoke_info.get("reason") or smoke_info.get("payload", {}).get("reason")
    elif code and code.startswith("SMOKE_TEST"):
        status = "Error"
    reason = reason or smoke_info.get("reason")
with smoke_cols[0]:
    pnl_val = smoke_info.get("pnl_realized") if smoke_info else None
    delta_text = f"{pnl_val:,.2f} INR" if pnl_val is not None else None
    st.metric("Status", status, delta=delta_text)
    if smoke_info:
        symbol_hint = smoke_info.get("symbol") or smoke_info.get("instrument_key") or "n/a"
        entry_val = smoke_info.get("entry_price")
        exit_val = smoke_info.get("exit_price")
        entry_notional = smoke_info.get("entry_notional")
        hold_seconds = smoke_info.get("hold_seconds")
        st.caption(f"Instrument: `{symbol_hint}` | When: {smoke_info.get('ts')}")
        st.write(f"Entry: {entry_val if entry_val is not None else 'n/a'} • Exit: {exit_val if exit_val is not None else 'n/a'}")
        st.write(f"Notional: {entry_notional if entry_notional is not None else 'n/a'} • Hold(s): {hold_seconds if hold_seconds is not None else 'n/a'}")
        if reason:
            st.info(f"Last reason: {reason}")
    else:
        st.info("No smoke test entries logged yet.")
with smoke_cols[1]:
    with sqlite3.connect(db_path) as conn:
        conn.row_factory = sqlite3.Row
        if st.button("Run Smoke Test Now (1 lot)"):
            insert_control_intent(conn, run_id, "SMOKE_TEST", {"source": "ui"})
            st.success("Smoke test intent posted")

st.subheader("Incidents")
st.dataframe(incidents, use_container_width=True)

st.subheader("PnL Trend")
if not pnl_history.empty:
    st.line_chart(pnl_history.set_index("ts"))
else:
    st.write("No PnL snapshots yet.")

st.subheader("Controls")
ctrl_cols = st.columns(3)
with sqlite3.connect(db_path) as conn:
    conn.row_factory = sqlite3.Row
    if ctrl_cols[0].button("Kill Switch"):
        insert_control_intent(conn, run_id, "KILL", {"source": "ui"})
        st.success("Kill intent posted")
    if ctrl_cols[1].button("Square-Off"):
        insert_control_intent(conn, run_id, "SQUARE_OFF", {"source": "ui"})
        st.success("Square-off intent posted")
    if ctrl_cols[2].button("Start Strategy Loop"):
        insert_control_intent(conn, run_id, "START_STRATEGY", {"source": "ui"})
        st.success("Strategy start intent posted")

if auto and fallback_refresh:
    rerun_fn = getattr(st, "experimental_rerun", None)
    time.sleep(interval)
    if callable(rerun_fn):
        rerun_fn()
