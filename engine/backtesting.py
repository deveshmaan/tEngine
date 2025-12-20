from __future__ import annotations

import asyncio
import datetime as dt
import math
import sqlite3
import tempfile
import uuid
from bisect import bisect_right
from dataclasses import replace
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable, Dict, Iterable, List, Optional, Tuple

from brokerage.upstox_client import INDEX_INSTRUMENT_KEYS, IST, UpstoxSession
from engine.backtest.costs import apply_slippage, apply_spread
from engine.backtest.execution_config import ExecutionConfig, FILL_MODEL_NEXT_TICK
from engine.backtest.history_cache import HistoryCache, interval_seconds
from engine.backtest.instrument_master import InstrumentMaster
from engine.config import EngineConfig
from engine.data import pick_strike_from_spot, record_tick_seen, resolve_weekly_expiry
from engine.events import EventBus
from engine.exit import ExitEngine
from engine.fees import load_fee_config
from engine.metrics import EngineMetrics
from engine.oms import BrokerOrderAck, BrokerOrderView, OMS, Order, OrderState
from engine.pnl import Execution as PnLExecution
from engine.pnl import PnLCalculator
from engine.risk import RiskManager
from engine.time_machine import now as engine_now
from engine.time_machine import travel
from persistence import SQLiteStore
from strategy_registry import get_strategy_class


class SimBroker:
    """Minimal broker stub that acknowledges orders for OMS."""

    venue = "SIM"
    static_ip_ok = True

    def __init__(self) -> None:
        self._orders: Dict[str, BrokerOrderView] = {}

    def is_streaming_alive(self) -> bool:  # compatibility for strategy gates
        return True

    async def submit_order(self, order: Order) -> BrokerOrderAck:
        broker_order_id = f"SIM-{uuid.uuid4().hex[:12]}"
        self._orders[broker_order_id] = BrokerOrderView(
            broker_order_id=broker_order_id,
            client_order_id=order.client_order_id,
            status="open",
            filled_qty=order.filled_qty,
            avg_price=order.avg_fill_price,
            instrument_key=None,
            side=order.side,
        )
        return BrokerOrderAck(broker_order_id=broker_order_id, status="ok")

    async def replace_order(self, order: Order, *, price: Optional[float], qty: Optional[int]) -> None:
        return

    async def cancel_order(self, order: Order) -> None:
        view = self._orders.get(order.broker_order_id or "")
        if view:
            self._orders[view.broker_order_id] = BrokerOrderView(
                broker_order_id=view.broker_order_id,
                client_order_id=view.client_order_id,
                status="canceled",
                filled_qty=view.filled_qty,
                avg_price=view.avg_price,
                instrument_key=view.instrument_key,
                side=view.side,
            )

    async def fetch_open_orders(self) -> List[BrokerOrderView]:
        return [view for view in self._orders.values() if str(view.status).lower() in {"open", "pending"}]


@dataclass(frozen=True)
class BacktestResult:
    run_id: str
    strategy: str
    period: str
    net_pnl: float
    realized_pnl: float
    unrealized_pnl: float
    fees: float
    trades: int
    wins: int
    win_rate: float
    equity_curve: List[Dict[str, Any]]
    trade_log: List[Dict[str, Any]]
    errors: List[str]

    def as_dict(self) -> Dict[str, Any]:
        return {
            "run_id": self.run_id,
            "strategy": self.strategy,
            "period": self.period,
            "net_pnl": self.net_pnl,
            "realized_pnl": self.realized_pnl,
            "unrealized_pnl": self.unrealized_pnl,
            "fees": self.fees,
            "trades": self.trades,
            "wins": self.wins,
            "win_rate": self.win_rate,
            "equity_curve": self.equity_curve,
            "trade_log": self.trade_log,
            "errors": self.errors,
        }


@dataclass(frozen=True)
class _CandleBar:
    ts: int  # epoch seconds (UTC) for bar open
    open: float
    high: float
    low: float
    close: float
    volume: float
    oi: Optional[float]


@dataclass(frozen=True)
class _CandleSeries:
    key: str
    interval: str
    bar_seconds: int
    ts_list: Tuple[int, ...]
    bars_by_ts: Dict[int, _CandleBar]

class _BacktestInstrumentCache:
    """
    Minimal instrument cache adapter for strategy dependencies.

    The production `InstrumentCache` sets a global runtime cache and can make
    network calls to discover expiries/contracts. Backtesting avoids that by
    exposing only the small surface area strategies use.
    """

    def __init__(
        self,
        *,
        session: UpstoxSession,
        default_meta: tuple[float, int, float, float],
    ) -> None:
        self._session = session
        self._default_meta = default_meta

    def upstox_session(self) -> UpstoxSession:
        return self._session

    def resolve_index_key(self, symbol: str) -> str:
        sym = str(symbol or "").upper()
        if sym in INDEX_INSTRUMENT_KEYS:
            return INDEX_INSTRUMENT_KEYS[sym]
        raise ValueError(f"Unsupported symbol: {symbol!r}")

    def get_meta(self, symbol: str, expiry: str) -> tuple[float, int, float, float]:
        return self._default_meta

    def close(self) -> None:
        return


class BacktestingEngine:
    def __init__(
        self,
        *,
        config: Optional[EngineConfig] = None,
        session: Optional[UpstoxSession] = None,
        store_path: Optional[str | Path] = None,
        run_id: Optional[str] = None,
        disable_md_stale_gate: bool = True,
        history_cache_path: Optional[str | Path] = None,
        instrument_master_path: Optional[str | Path] = None,
        execution_config: Optional[ExecutionConfig] = None,
        slippage_model: str = "none",
        slippage_bps: float = 0.0,
        slippage_ticks: int = 0,
        spread_bps: float = 0.0,
        starting_capital: Optional[float] = None,
    ) -> None:
        self.cfg = config or EngineConfig.load()
        if disable_md_stale_gate:
            try:
                if float(getattr(self.cfg.market_data, "max_tick_age_seconds", 0.0) or 0.0) > 0:
                    self.cfg = replace(
                        self.cfg,
                        market_data=replace(self.cfg.market_data, max_tick_age_seconds=0.0),
                    )
            except Exception:
                pass
        try:
            self.bus = EventBus()
        except RuntimeError as exc:
            # Python 3.9: after `asyncio.run()` there may be no default loop, but some
            # asyncio primitives (used by EventBus) still rely on one at construction time.
            if "no current event loop" not in str(exc).lower():
                raise
            asyncio.set_event_loop(asyncio.new_event_loop())
            self.bus = EventBus()
        metrics_registry = None
        try:  # Streamlit reruns can re-instantiate metrics; isolate via a dedicated registry.
            from prometheus_client import CollectorRegistry  # type: ignore

            metrics_registry = CollectorRegistry()
        except Exception:
            metrics_registry = None
        self.metrics = EngineMetrics(registry=metrics_registry)
        self.session = session or UpstoxSession()

        self._tempdir: Optional[tempfile.TemporaryDirectory[str]] = None
        if store_path is None:
            self._tempdir = tempfile.TemporaryDirectory(prefix="backtest-")
            store_path = Path(self._tempdir.name) / "engine_state.sqlite"
        self.run_id = run_id or f"backtest-{uuid.uuid4().hex[:10]}"
        self.store = SQLiteStore(Path(store_path), run_id=self.run_id)

        default_meta = (self.cfg.data.tick_size, self.cfg.data.lot_step, self.cfg.data.price_band_low, self.cfg.data.price_band_high)
        self.instrument_cache = _BacktestInstrumentCache(session=self.session, default_meta=default_meta)
        capital_base = float(self.cfg.capital_base or 0.0)
        if starting_capital is not None:
            try:
                capital_base = float(starting_capital)
            except (TypeError, ValueError):
                capital_base = float(self.cfg.capital_base or 0.0)
        self.risk = RiskManager(self.cfg.risk, self.store, capital_base=capital_base)
        self.broker = SimBroker()
        self.oms = OMS(
            broker=self.broker,
            store=self.store,
            config=self.cfg.oms,
            bus=self.bus,
            metrics=self.metrics,
            default_meta=default_meta,
            square_off_time=self.cfg.risk.square_off_by,
        )
        self.exit_engine = ExitEngine(
            config=self.cfg.exit,
            risk=self.risk,
            oms=self.oms,
            store=self.store,
            tick_size=self.cfg.data.tick_size,
            metrics=self.metrics,
            iv_exit_threshold=getattr(self.cfg.strategy, "iv_exit_percentile", 0.0),
        )
        self.pnl = PnLCalculator(self.store, load_fee_config())

        # Spec-driven multi-leg strategies (StrategySpec) can be injected by the UI/runner.
        self.strategy_spec: Any = None
        self.strategy_spec_json: Optional[str] = None

        self._stop_requested = False
        self._bt_underlying_symbol: Optional[str] = None
        self._last_spot: Dict[str, float] = {}
        self._last_iv: Dict[str, float] = {}
        self._iv_seeded: Dict[str, bool] = {}
        self._spot_returns: Dict[str, List[float]] = {}

        self._history_cache = HistoryCache(path=history_cache_path)
        self._instrument_master = InstrumentMaster(path=instrument_master_path)
        self.execution = execution_config or ExecutionConfig()
        self.slippage_model = str(slippage_model or "none").strip().lower()
        self.slippage_bps = float(slippage_bps or 0.0)
        self.slippage_ticks = int(slippage_ticks or 0)
        self.spread_bps = float(spread_bps or 0.0)
        self._series_cache: Dict[Tuple[str, str], _CandleSeries] = {}
        self._last_option_price: Dict[str, float] = {}
        self._bt_interval: Optional[str] = None
        self._bt_range_start_ts: int = 0
        self._bt_range_end_ts: int = 0
        self._sim_tick_index: int = 0
        self._order_signal_ts: Dict[str, dt.datetime] = {}
        self._order_signal_tick_idx: Dict[str, int] = {}
        self._liquidity_remaining: Dict[Tuple[str, int], int] = {}
        self._data_warnings: List[str] = []
        self._warned: set[str] = set()
        self._cache_stats: Dict[str, int] = {"ensure_calls": 0, "missing_segments": 0, "fetched_rows": 0}

    def request_stop(self) -> None:
        self._stop_requested = True

    def close(self) -> None:
        try:
            self.store.close()
        finally:
            try:
                self.instrument_cache.close()
            except Exception:
                pass
            if self._tempdir is not None:
                self._tempdir.cleanup()

    # ---------------------------------------------------------------- public API
    def run_backtest(
        self,
        strategy_name: str,
        start_date: dt.date,
        end_date: dt.date,
        *,
        interval: str = "1minute",
        progress_callback: Optional[Callable[[int, int], None]] = None,
        underlying_symbol: Optional[str] = None,
    ) -> Dict[str, Any]:
        try:
            return asyncio.run(
                self.run_backtest_async(
                    strategy_name,
                    start_date,
                    end_date,
                    interval=interval,
                    progress_callback=progress_callback,
                    underlying_symbol=underlying_symbol,
                )
            )
        except RuntimeError:
            loop = asyncio.new_event_loop()
            try:
                return loop.run_until_complete(
                    self.run_backtest_async(
                        strategy_name,
                        start_date,
                        end_date,
                        interval=interval,
                        progress_callback=progress_callback,
                        underlying_symbol=underlying_symbol,
                    )
                )
            finally:
                loop.close()

    async def run_backtest_async(
        self,
        strategy_name: str,
        start_date: dt.date,
        end_date: dt.date,
        *,
        interval: str = "1minute",
        data: Optional[List[Dict[str, Any]]] = None,
        progress_callback: Optional[Callable[[int, int], None]] = None,
        underlying_symbol: Optional[str] = None,
    ) -> Dict[str, Any]:
        underlying = str(underlying_symbol or self.cfg.data.index_symbol or "").strip().upper()
        if not underlying:
            underlying = self.cfg.data.index_symbol.upper()
        self._bt_underlying_symbol = underlying
        underlying_key = INDEX_INSTRUMENT_KEYS.get(underlying, underlying)
        cfg_for_strategy = self.cfg
        if underlying_symbol is not None and underlying != self.cfg.data.index_symbol.upper():
            try:
                cfg_for_strategy = replace(self.cfg, data=replace(self.cfg.data, index_symbol=underlying))
            except Exception:
                cfg_for_strategy = self.cfg
        self._bt_interval = str(interval or "1minute")
        range_start = dt.datetime.combine(start_date, dt.time(0, 0), tzinfo=IST)
        range_end = dt.datetime.combine(end_date, dt.time(23, 59, 59), tzinfo=IST)
        self._bt_range_start_ts = int(range_start.astimezone(dt.timezone.utc).timestamp())
        self._bt_range_end_ts = int(range_end.astimezone(dt.timezone.utc).timestamp())
        self._data_warnings = []
        self._warned = set()
        self._last_option_price = {}
        self._series_cache = {}
        self._cache_stats = {"ensure_calls": 0, "missing_segments": 0, "fetched_rows": 0}
        self._sim_tick_index = 0
        self._order_signal_ts = {}
        self._order_signal_tick_idx = {}
        self._liquidity_remaining = {}
        # Reset monotonic store clock so historical backtests can record real timestamps.
        try:
            anchor = dt.datetime.fromtimestamp(self._bt_range_start_ts, tz=dt.timezone.utc) - dt.timedelta(seconds=1)
            setattr(self.store, "_latest_ts", anchor)
        except Exception:
            pass

        if data is not None:
            candles = data
            try:
                import pandas as pd  # type: ignore

                df = pd.DataFrame(list(candles or []))
                if not df.empty:
                    self._series_cache[(str(underlying_key), str(self._bt_interval))] = _df_to_series(
                        key=underlying_key, interval=str(self._bt_interval), df=df
                    )
            except Exception:
                pass
        else:
            def _fetch(key: str, interval_key: str, seg_start: dt.datetime, seg_end: dt.datetime):
                df = self._fetch_candles_df(key, interval_key, seg_start, seg_end)
                try:
                    self._cache_stats["fetched_rows"] += int(len(df) if df is not None else 0)
                except Exception:
                    pass
                return df

            self._cache_stats["ensure_calls"] += 1
            missing = self._history_cache.ensure_range(
                underlying_key,
                self._bt_interval,
                self._bt_range_start_ts,
                self._bt_range_end_ts,
                _fetch,
                source="upstox",
            )
            self._cache_stats["missing_segments"] += len(missing or [])
            df = self._history_cache.load(
                key=underlying_key,
                interval=self._bt_interval,
                start_ts=self._bt_range_start_ts,
                end_ts=self._bt_range_end_ts,
            )
            try:
                self._series_cache[(str(underlying_key), str(self._bt_interval))] = _df_to_series(
                    key=underlying_key, interval=self._bt_interval, df=df
                )
            except Exception:
                pass
            candles = _df_to_rows(df)

        first_ts: Optional[str] = None
        last_ts: Optional[str] = None
        try:
            for row in candles:
                ts = row.get("ts") if isinstance(row, dict) else None
                if not isinstance(ts, dt.datetime):
                    continue
                if first_ts is None:
                    first_ts = ts.astimezone(IST).isoformat()
                last_ts = ts.astimezone(IST).isoformat()
        except Exception:
            first_ts = None
            last_ts = None

        total_candles = len(candles)
        if not candles:
            out = BacktestResult(
                run_id=self.run_id,
                strategy=strategy_name,
                period=f"{start_date.isoformat()} to {end_date.isoformat()}",
                net_pnl=0.0,
                realized_pnl=0.0,
                unrealized_pnl=0.0,
                fees=0.0,
                trades=0,
                wins=0,
                win_rate=0.0,
                equity_curve=[],
                trade_log=[],
                errors=["no_data"],
            ).as_dict()
            out["gross_pnl"] = 0.0
            out["total_fees"] = 0.0
            out["interval"] = interval
            out["underlying"] = underlying
            out["underlying_key"] = underlying_key
            out["candles"] = total_candles
            out["ticks"] = 0
            out["data_warnings"] = list(self._data_warnings or [])
            out["diagnostics"] = {
                "history_cache_path": str(self._history_cache.path),
                "first_candle_ts": first_ts,
                "last_candle_ts": last_ts,
                "cache_ensure_calls": int(self._cache_stats.get("ensure_calls", 0)),
                "cache_missing_segments": int(self._cache_stats.get("missing_segments", 0)),
                "cache_fetched_rows": int(self._cache_stats.get("fetched_rows", 0)),
                "series_loaded": int(len(self._series_cache)),
            }
            out["execution"] = {
                "fill_model": self.execution.fill_model,
                "latency_ms": int(self.execution.latency_ms),
                "allow_partial_fills": bool(self.execution.allow_partial_fills),
            }
            out["costs"] = {
                "slippage_model": self.slippage_model,
                "slippage_bps": float(self.slippage_bps),
                "slippage_ticks": int(self.slippage_ticks),
                "spread_bps": float(self.spread_bps),
            }
            out["orders"] = []
            out["executions"] = []
            return out

        strategy_cls = get_strategy_class(strategy_name)
        strategy = strategy_cls(
            cfg_for_strategy,
            self.risk,
            self.oms,
            self.bus,
            self.exit_engine,
            self.instrument_cache,  # duck-typed adapter
            self.metrics,
            subscription_expiry_provider=self._subscription_expiry_for,
        )
        await strategy.init(self)

        fills_q = await self.bus.subscribe("orders/fill", maxsize=5000)

        trade_log: List[Dict[str, Any]] = []
        last_realized_by_key: Dict[Tuple[str, Optional[str], Optional[float], Optional[str]], float] = {}
        last_fees_by_key: Dict[Tuple[str, Optional[str], Optional[float], Optional[str]], float] = {}
        equity_curve: List[Dict[str, Any]] = []
        errors: List[str] = []

        current_day: Optional[dt.date] = None
        prev_ts: Optional[dt.datetime] = None
        ticks_processed = 0

        for candle_idx, row in enumerate(candles, start=1):
            if self._stop_requested:
                break
            if progress_callback and (candle_idx == 1 or candle_idx == total_candles or candle_idx % 25 == 0):
                try:
                    progress_callback(candle_idx, total_candles)
                except Exception:
                    pass
            ts = row.get("ts")
            if not isinstance(ts, dt.datetime):
                continue
            ts = ts.astimezone(IST)
            if current_day is None:
                current_day = ts.date()
            if ts.date() != current_day:
                if prev_ts is not None:
                    with travel(prev_ts):
                        await self._square_off_all(prev_ts, underlying, reason="DAY_END")
                        await self._fill_pending_orders(prev_ts, underlying)
                        await self._force_next_tick_fill(prev_ts, underlying)
                        await self._drain_fills(
                            strategy,
                            fills_q,
                            trade_log=trade_log,
                            last_realized_by_key=last_realized_by_key,
                            last_fees_by_key=last_fees_by_key,
                            equity_curve=equity_curve,
                        )
                self._reset_daily_state()
                current_day = ts.date()

            open_px = _coerce_float(row.get("open"))
            high_px = _coerce_float(row.get("high"))
            low_px = _coerce_float(row.get("low"))
            close_px = _coerce_float(row.get("close"))
            if open_px is None and close_px is None and high_px is None and low_px is None:
                continue
            open_px = open_px if open_px is not None else (close_px if close_px is not None else high_px if high_px is not None else low_px)
            close_px = close_px if close_px is not None else open_px
            high_px = high_px if high_px is not None else max(open_px, close_px)
            low_px = low_px if low_px is not None else min(open_px, close_px)
            high_px = max(high_px, open_px, close_px)
            low_px = min(low_px, open_px, close_px)

            candle_volume = _coerce_float(row.get("volume")) or 0.0
            if candle_volume <= 0:
                candle_volume = self._synthetic_volume(underlying, ts, open_px, close_px)

            # Emit multiple ticks per candle so strategies that build their own minute bars
            # (AdvancedBuyStrategy, ScalpingBuyStrategy) have non-degenerate bar
            # open/close values and can react to intra-minute extremes.
            if close_px >= open_px:
                prices = (open_px, low_px, high_px, close_px)
            else:
                prices = (open_px, high_px, low_px, close_px)
            try:
                bar_secs = interval_seconds(self._bt_interval or interval)
            except Exception:
                bar_secs = 60
            offsets = (
                0,
                max(1, int(bar_secs // 4)),
                max(2, int(bar_secs // 2)),
                max(3, int((3 * bar_secs) // 4)),
            )
            tick_plan: List[Tuple[dt.datetime, float, float]] = []
            for idx, px in enumerate(prices):
                tick_plan.append((ts + dt.timedelta(seconds=offsets[idx]), float(px), float(candle_volume) if idx == 0 else 0.0))

            for tick_ts, tick_price, tick_volume in tick_plan:
                if self._stop_requested:
                    break
                ticks_processed += 1
                self._sim_tick_index = ticks_processed
                with travel(tick_ts):
                    self._record_spot(underlying, tick_price)
                    expiry = self._subscription_expiry_for(underlying)
                    step = self._strike_step(underlying)
                    atm = pick_strike_from_spot(tick_price, step=step)

                    self._inject_chain(strategy, underlying, tick_ts, expiry, tick_price, step, atm, volume=tick_volume)

                    # Send synthetic option ticks first so strategies see a fresh premium.
                    opt_events = self._build_option_events(underlying, tick_ts, expiry, tick_price, step, atm)
                    for event in opt_events:
                        try:
                            await strategy.on_tick(event)
                        except Exception as exc:
                            errors.append(f"strategy_tick_error:{exc}")

                    # Underlying tick (drives entry logic for most strategies).
                    underlying_event = {
                        "ts": tick_ts.isoformat(),
                        "type": "tick",
                        "payload": {
                            "instrument_key": INDEX_INSTRUMENT_KEYS.get(underlying, underlying),
                            "underlying": underlying,
                            "symbol": underlying,
                            "ltp": tick_price,
                            "volume": tick_volume,
                        },
                    }
                    record_tick_seen(
                        instrument_key=str(underlying_event["payload"].get("instrument_key") or underlying),
                        underlying=underlying,
                        ts_seconds=tick_ts.timestamp(),
                    )
                    try:
                        await strategy.on_tick(underlying_event)
                    except Exception as exc:
                        errors.append(f"strategy_tick_error:{exc}")

                    # Drive exits for any open positions (covers strategies that don't call ExitEngine).
                    await self._drive_exit_engine(tick_ts, underlying)

                    # Fill all pending orders and process resulting executions.
                    await self._fill_pending_orders(tick_ts, underlying)
                    await self._drain_fills(
                        strategy,
                        fills_q,
                        trade_log=trade_log,
                        last_realized_by_key=last_realized_by_key,
                        last_fees_by_key=last_fees_by_key,
                        equity_curve=equity_curve,
                    )
                prev_ts = tick_ts

        # Ensure we close any remaining positions at the end.
        if prev_ts is not None:
            with travel(prev_ts):
                await self._square_off_all(prev_ts, underlying, reason="END")
                await self._fill_pending_orders(prev_ts, underlying)
                await self._force_next_tick_fill(prev_ts, underlying)
                await self._drain_fills(
                    strategy,
                    fills_q,
                    trade_log=trade_log,
                    last_realized_by_key=last_realized_by_key,
                    last_fees_by_key=last_fees_by_key,
                    equity_curve=equity_curve,
                )

        realized, unrealized, fees = self.pnl.totals()
        net = realized + unrealized - fees
        wins = sum(1 for t in trade_log if (t.get("net_pnl") or 0.0) > 0)
        trades = len(trade_log)
        win_rate = (wins / trades) if trades else 0.0
        result = BacktestResult(
            run_id=self.run_id,
            strategy=strategy_name,
            period=f"{start_date.isoformat()} to {end_date.isoformat()}",
            net_pnl=float(net),
            realized_pnl=float(realized),
            unrealized_pnl=float(unrealized),
            fees=float(fees),
            trades=trades,
            wins=wins,
            win_rate=float(win_rate),
            equity_curve=equity_curve,
            trade_log=trade_log,
            errors=errors,
        )
        out = result.as_dict()
        out["gross_pnl"] = float(realized + unrealized)
        out["total_fees"] = float(fees)
        out["interval"] = interval
        out["underlying"] = underlying
        out["underlying_key"] = underlying_key
        out["candles"] = total_candles
        out["ticks"] = ticks_processed
        out["data_warnings"] = list(self._data_warnings or [])
        out["diagnostics"] = {
            "history_cache_path": str(self._history_cache.path),
            "first_candle_ts": first_ts,
            "last_candle_ts": last_ts,
            "cache_ensure_calls": int(self._cache_stats.get("ensure_calls", 0)),
            "cache_missing_segments": int(self._cache_stats.get("missing_segments", 0)),
            "cache_fetched_rows": int(self._cache_stats.get("fetched_rows", 0)),
            "series_loaded": int(len(self._series_cache)),
        }
        out["execution"] = {
            "fill_model": self.execution.fill_model,
            "latency_ms": int(self.execution.latency_ms),
            "allow_partial_fills": bool(self.execution.allow_partial_fills),
        }
        out["costs"] = {
            "slippage_model": self.slippage_model,
            "slippage_bps": float(self.slippage_bps),
            "slippage_ticks": int(self.slippage_ticks),
            "spread_bps": float(self.spread_bps),
        }
        out["orders"] = self._load_orders_table()
        out["executions"] = self._load_executions_table()
        if progress_callback:
            try:
                progress_callback(total_candles, total_candles)
            except Exception:
                pass
        return out

    # ----------------------------------------------------------------- internals
    def _subscription_expiry_for(self, symbol: str) -> str:
        now_ist = engine_now(IST)
        expiry = resolve_weekly_expiry(
            symbol.upper(),
            now_ist,
            holidays=self.cfg.data.holidays,
            weekly_weekday=self.cfg.data.weekly_expiry_weekday,
        )
        return expiry.isoformat()

    def _strike_step(self, symbol: str) -> int:
        steps = getattr(self.cfg.data, "strike_steps", {}) or {}
        try:
            return max(int(steps.get(symbol.upper(), self.cfg.data.lot_step)), 1)
        except Exception:
            return max(int(self.cfg.data.lot_step), 1)

    def _record_spot(self, symbol: str, spot: float) -> None:
        sym = symbol.upper()
        prev = self._last_spot.get(sym)
        self._last_spot[sym] = float(spot)
        if prev is None or prev <= 0:
            return
        ret = (float(spot) / float(prev)) - 1.0
        self._spot_returns.setdefault(sym, []).append(ret)
        if len(self._spot_returns[sym]) > 240:
            self._spot_returns[sym] = self._spot_returns[sym][-240:]

    def _synthetic_iv(self, underlying: str) -> float:
        sym = underlying.upper()
        returns = self._spot_returns.get(sym, [])
        if len(returns) < 10:
            return self._last_iv.get(sym, 0.2) or 0.2
        window = returns[-60:] if len(returns) >= 60 else returns
        mean = sum(window) / len(window)
        var = sum((x - mean) ** 2 for x in window) / max(len(window), 1)
        stdev = math.sqrt(max(var, 0.0))
        iv = 0.15 + min(max(stdev * 25.0, 0.0), 0.35)
        self._last_iv[sym] = float(iv)
        return float(iv)

    def _synthetic_volume(self, underlying: str, ts: dt.datetime, open_px: float, close_px: float) -> float:
        # Some index candles do not provide volume; synthesize a deterministic proxy that
        # still allows volume-based strategies to function.
        move = abs(float(close_px) - float(open_px))
        base = 1000.0
        scaled = base + (move * 1000.0)
        osc = 1.0 + 0.5 * math.sin(float(ts.timestamp()) / 1800.0)
        return max(base, scaled * max(osc, 0.1))

    def _warn_once(self, msg: str) -> None:
        text = str(msg or "").strip()
        if not text:
            return
        if text in self._warned:
            return
        self._warned.add(text)
        self._data_warnings.append(text)

    def _fetch_candles_df(self, key: str, interval_key: str, start: dt.datetime, end: dt.datetime):
        import pandas as pd  # type: ignore

        start_dt = start.astimezone(IST)
        end_dt = end.astimezone(IST)
        df = self.session.get_historical_data(
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

    def _load_series(self, key: str, interval_key: str, *, is_option: bool) -> _CandleSeries:
        cache_key = (str(key), str(interval_key))
        existing = self._series_cache.get(cache_key)
        if existing is not None:
            return existing

        def _fetch(k: str, i: str, seg_start: dt.datetime, seg_end: dt.datetime):
            df = self._fetch_candles_df(k, i, seg_start, seg_end)
            try:
                self._cache_stats["fetched_rows"] += int(len(df) if df is not None else 0)
            except Exception:
                pass
            return df

        self._cache_stats["ensure_calls"] += 1
        missing = self._history_cache.ensure_range(
            key,
            interval_key,
            self._bt_range_start_ts,
            self._bt_range_end_ts,
            _fetch,
            source="upstox",
        )
        self._cache_stats["missing_segments"] += len(missing or [])
        df = self._history_cache.load(
            key=key,
            interval=interval_key,
            start_ts=self._bt_range_start_ts,
            end_ts=self._bt_range_end_ts,
        )
        series = _df_to_series(key=key, interval=interval_key, df=df)
        self._series_cache[cache_key] = series

        # ------------------------ data-quality warnings (best-effort, non-fatal)
        try:
            bar_secs = interval_seconds(interval_key)
        except Exception:
            bar_secs = 0
        if bar_secs > 0 and len(series.ts_list) >= 2:
            gaps = 0
            for prev_ts, curr_ts in zip(series.ts_list, series.ts_list[1:]):
                delta = int(curr_ts) - int(prev_ts)
                if delta <= 2 * bar_secs:
                    continue
                prev_dt = dt.datetime.fromtimestamp(int(prev_ts), tz=dt.timezone.utc).astimezone(IST)
                curr_dt = dt.datetime.fromtimestamp(int(curr_ts), tz=dt.timezone.utc).astimezone(IST)
                if prev_dt.date() != curr_dt.date():
                    continue
                gaps += 1
                if gaps <= 3:
                    missing = max(int(round(delta / bar_secs)) - 1, 0)
                    self._warn_once(
                        f"gap>{2}bars: key={key} interval={interval_key} missing~{missing} between {prev_dt.time()} and {curr_dt.time()} on {curr_dt.date().isoformat()}"
                    )
            if gaps > 3:
                self._warn_once(f"gap>{2}bars: key={key} interval={interval_key} additional_gaps={gaps - 3}")

        if is_option:
            try:
                oi = df.get("oi")
                if oi is None or getattr(oi, "empty", True):
                    self._warn_once(f"oi_missing: key={key} interval={interval_key} (no OI column)")
                else:
                    oi_numeric = oi.fillna(0.0).astype(float)
                    if (oi_numeric == 0.0).all():
                        self._warn_once(f"oi_missing: key={key} interval={interval_key} (all 0/NULL)")
            except Exception:
                pass

        return series

    def _bar_for_ts(self, series: _CandleSeries, ts: dt.datetime) -> Optional[_CandleBar]:
        ts_epoch = int(ts.astimezone(dt.timezone.utc).timestamp())
        idx = bisect_right(series.ts_list, ts_epoch) - 1
        if idx < 0:
            return None
        return series.bars_by_ts.get(series.ts_list[idx])

    def _ltp_from_bar(self, bar: _CandleBar, ts: dt.datetime) -> tuple[float, float, Optional[float]]:
        ts_epoch = int(ts.astimezone(dt.timezone.utc).timestamp())
        elapsed = max(0, ts_epoch - int(bar.ts))
        try:
            bar_secs = interval_seconds(self._bt_interval or "1minute")
        except Exception:
            bar_secs = 60
        offsets = (0, max(1, bar_secs // 4), max(2, bar_secs // 2), max(3, (3 * bar_secs) // 4))
        offset_idx = bisect_right(offsets, elapsed) - 1
        offset_idx = min(max(int(offset_idx), 0), 3)
        if float(bar.close) >= float(bar.open):
            prices = (float(bar.open), float(bar.low), float(bar.high), float(bar.close))
        else:
            prices = (float(bar.open), float(bar.high), float(bar.low), float(bar.close))
        ltp = prices[offset_idx]
        vol = float(bar.volume) if offset_idx == 0 else 0.0
        return float(ltp), vol, (None if bar.oi is None else float(bar.oi))

    def _resolve_option_instrument_key(self, *, underlying: str, expiry: str, strike: int, opt_type: str) -> str:
        underlying_key = INDEX_INSTRUMENT_KEYS.get(str(underlying).upper(), str(underlying))
        return self._instrument_master.resolve_option_key(
            underlying_key=underlying_key,
            expiry=str(expiry),
            opt_type=str(opt_type).upper(),
            strike=int(strike),
        )

    def _build_option_events(self, underlying: str, ts: dt.datetime, expiry: str, spot: float, step: int, atm: int) -> List[Dict[str, Any]]:
        events: List[Dict[str, Any]] = []
        iv_val = self._synthetic_iv(underlying)
        for opt_type, strikes in (
            ("CE", (atm, atm + step)),
            ("PE", (atm, atm - step)),
        ):
            for strike in strikes:
                symbol = f"{underlying}-{expiry}-{int(strike)}{opt_type}"
                try:
                    instrument_key = self._resolve_option_instrument_key(underlying=underlying, expiry=expiry, strike=int(strike), opt_type=opt_type)
                except Exception as exc:
                    self._warn_once(f"option_key_missing: {symbol} ({exc})")
                    continue
                try:
                    series = self._load_series(instrument_key, self._bt_interval or "1minute", is_option=True)
                except Exception as exc:
                    self._warn_once(f"option_candles_load_failed: key={instrument_key} ({exc})")
                    continue
                bar = self._bar_for_ts(series, ts)
                if bar is None:
                    continue
                ltp, vol, oi = self._ltp_from_bar(bar, ts)
                tick = max(float(self.cfg.data.tick_size or 0.0), 0.0) or 0.05
                bid = max(ltp - tick, tick)
                ask = ltp + tick
                payload = {
                    "instrument_key": instrument_key,
                    "underlying": underlying,
                    "symbol": symbol,
                    "expiry": expiry,
                    "strike": int(strike),
                    "opt_type": opt_type,
                    "ltp": ltp,
                    "bid": bid,
                    "ask": ask,
                    "iv": iv_val,
                    "oi": oi,
                    "volume": vol,
                    "ts": ts.isoformat(),
                }
                self._last_option_price[symbol] = float(ltp)
                record_tick_seen(instrument_key=instrument_key, underlying=underlying, ts_seconds=ts.timestamp())
                events.append({"ts": ts.isoformat(), "type": "tick", "payload": payload})
        return events

    def _inject_chain(
        self,
        strategy: Any,
        underlying: str,
        ts: dt.datetime,
        expiry: str,
        spot: float,
        step: int,
        atm: int,
        *,
        volume: float,
    ) -> None:
        # AdvancedBuyStrategy: expects {"CE": {strike: {...}}, "PE": {...}, "__pcr__": float}
        if hasattr(strategy, "_chain_cache") and hasattr(strategy, "_chain_ts"):
            chain_cache = getattr(strategy, "_chain_cache")
            chain_ts = getattr(strategy, "_chain_ts")
            if isinstance(chain_cache, dict) and isinstance(chain_ts, dict):
                iv_val = self._synthetic_iv(underlying)
                tick = max(float(self.cfg.data.tick_size or 0.0), 0.0) or 0.05
                # Prefer explicit capability detection instead of shape-based heuristics (empty caches at startup).
                if hasattr(strategy, "_pcr_from_cache"):
                    ce_strikes = (atm, atm + step)
                    pe_strikes = (atm, atm - step)
                    chain: Dict[str, Any] = {"CE": {}, "PE": {}, "__pcr__": 1.0}
                    for strike in ce_strikes:
                        symbol = f"{underlying}-{expiry}-{int(strike)}CE"
                        try:
                            instrument_key = self._resolve_option_instrument_key(underlying=underlying, expiry=expiry, strike=int(strike), opt_type="CE")
                            series = self._load_series(instrument_key, self._bt_interval or "1minute", is_option=True)
                            bar = self._bar_for_ts(series, ts)
                            if bar is None:
                                continue
                            ltp, vol, oi = self._ltp_from_bar(bar, ts)
                        except Exception:
                            continue
                        chain["CE"][int(strike)] = {
                            "instrument_key": instrument_key,
                            "oi": oi,
                            "volume": float(vol or 0.0),
                            "ltp": float(ltp),
                            "iv": iv_val,
                            "gamma": 0.0,
                            "expiry": expiry,
                        }
                    for strike in pe_strikes:
                        symbol = f"{underlying}-{expiry}-{int(strike)}PE"
                        try:
                            instrument_key = self._resolve_option_instrument_key(underlying=underlying, expiry=expiry, strike=int(strike), opt_type="PE")
                            series = self._load_series(instrument_key, self._bt_interval or "1minute", is_option=True)
                            bar = self._bar_for_ts(series, ts)
                            if bar is None:
                                continue
                            ltp, vol, oi = self._ltp_from_bar(bar, ts)
                        except Exception:
                            continue
                        chain["PE"][int(strike)] = {
                            "instrument_key": instrument_key,
                            "oi": oi,
                            "volume": float(vol or 0.0),
                            "ltp": float(ltp),
                            "iv": iv_val,
                            "gamma": 0.0,
                            "expiry": expiry,
                        }
                    try:
                        call_oi = sum(float(v.get("oi") or 0.0) for v in chain["CE"].values())
                        put_oi = sum(float(v.get("oi") or 0.0) for v in chain["PE"].values())
                        if call_oi > 0:
                            chain["__pcr__"] = put_oi / call_oi
                    except Exception:
                        pass
                    chain_cache[underlying] = chain
                    chain_ts[underlying] = ts.timestamp()
                else:
                    # ScalpingBuyStrategy: expects {strike: {...}, "__pcr__": float}
                    chain: Dict[Any, Any] = {"__pcr__": 1.0}
                    for strike in (atm, atm + step):
                        symbol = f"{underlying}-{expiry}-{int(strike)}CE"
                        try:
                            instrument_key = self._resolve_option_instrument_key(underlying=underlying, expiry=expiry, strike=int(strike), opt_type="CE")
                            series = self._load_series(instrument_key, self._bt_interval or "1minute", is_option=True)
                            bar = self._bar_for_ts(series, ts)
                            if bar is None:
                                continue
                            ltp, vol, oi = self._ltp_from_bar(bar, ts)
                        except Exception:
                            continue
                        chain[int(strike)] = {
                            "instrument_key": instrument_key,
                            "oi": oi,
                            "volume": float(vol or 0.0),
                            "ltp": float(ltp),
                            "bid": max(float(ltp) - tick, tick),
                            "ask": float(ltp) + tick,
                            "expiry": expiry,
                        }
                    chain_cache[underlying] = chain
                    chain_ts[underlying] = ts.timestamp()

    async def _fill_pending_orders(self, ts: dt.datetime, underlying: str) -> None:
        # Fill any ACK'd orders at the latest available (cached) price.
        now = ts.astimezone(IST)
        for order in list(getattr(self.oms, "_orders", {}).values()):
            if not isinstance(order, Order):
                continue
            if order.state not in {OrderState.ACKNOWLEDGED, OrderState.SUBMITTED, OrderState.PARTIALLY_FILLED}:
                continue
            oid = str(getattr(order, "client_order_id", "") or "")
            if oid and oid not in self._order_signal_ts:
                self._order_signal_ts[oid] = now
                self._order_signal_tick_idx[oid] = int(self._sim_tick_index)
            remaining = max(int(order.qty) - int(order.filled_qty), 0)
            if remaining <= 0:
                continue

            if oid:
                signal_ts = self._order_signal_ts.get(oid, now)
                signal_idx = int(self._order_signal_tick_idx.get(oid, int(self._sim_tick_index)))
                if self.execution.fill_model == FILL_MODEL_NEXT_TICK and int(self._sim_tick_index) <= signal_idx:
                    continue
                if self.execution.latency_ms > 0:
                    if now < (signal_ts + dt.timedelta(milliseconds=int(self.execution.latency_ms))):
                        continue

            fill_price = self._fill_price(order.symbol, ts, underlying)
            if fill_price is None:
                continue

            fill_qty = self._fill_qty_for_order(order.symbol, ts, underlying, remaining, allow_partial=self.execution.allow_partial_fills)
            if fill_qty <= 0:
                continue
            raw_price = float(fill_price)
            effective_price = apply_spread(raw_price, order.side, self.spread_bps)
            effective_price = apply_slippage(
                effective_price,
                order.side,
                self.slippage_model,
                self.slippage_bps,
                self.slippage_ticks,
                float(self.cfg.data.tick_size or 0.0),
            )
            await self.oms.record_fill(
                order.client_order_id,
                qty=fill_qty,
                price=effective_price,
                raw_price=raw_price,
                broker_order_id=order.broker_order_id,
            )

    def _fill_price(self, symbol: str, ts: dt.datetime, underlying: str) -> Optional[float]:
        sym = str(symbol or "")
        if sym.upper() in {"NIFTY", "BANKNIFTY"}:
            return float(self._last_spot.get(sym.upper(), 0.0) or 0.0)
        if sym in self._last_option_price:
            return float(self._last_option_price[sym])
        try:
            u, expiry, strike, opt = _parse_option_symbol(sym)
        except ValueError:
            return float(self._last_spot.get(underlying.upper(), 0.0) or 0.0)
        try:
            instrument_key = self._resolve_option_instrument_key(underlying=u, expiry=expiry, strike=int(strike), opt_type=opt)
            series = self._load_series(instrument_key, self._bt_interval or "1minute", is_option=True)
            bar = self._bar_for_ts(series, ts)
            if bar is None:
                self._warn_once(f"fill_price_missing: {sym} ts={ts.astimezone(IST).isoformat()}")
                return None
            ltp, _, _ = self._ltp_from_bar(bar, ts)
            return float(ltp)
        except Exception as exc:
            self._warn_once(f"fill_price_failed: {sym} ({exc})")
            return None

    def _fill_qty_for_order(self, symbol: str, ts: dt.datetime, underlying: str, remaining: int, *, allow_partial: bool) -> int:
        """
        Best-effort liquidity model based on candle volume.

        - Volume is scoped per (instrument_key, candle_ts) and consumed as fills occur.
        - If candle volume is missing/0, treat as unlimited to avoid deadlocks on sparse feeds.
        """

        sym = str(symbol or "").strip()
        if not sym or remaining <= 0:
            return 0

        # Resolve instrument_key for volume lookup.
        instrument_key: Optional[str] = None
        is_option = False
        if sym.upper() in {"NIFTY", "BANKNIFTY"}:
            instrument_key = INDEX_INSTRUMENT_KEYS.get(sym.upper(), sym)
        else:
            try:
                u, expiry, strike, opt = _parse_option_symbol(sym)
                instrument_key = self._resolve_option_instrument_key(underlying=u, expiry=expiry, strike=int(strike), opt_type=opt)
                is_option = True
            except Exception:
                instrument_key = INDEX_INSTRUMENT_KEYS.get(underlying.upper(), underlying)

        if not instrument_key:
            return remaining

        try:
            series = self._load_series(instrument_key, self._bt_interval or "1minute", is_option=is_option)
            bar = self._bar_for_ts(series, ts)
        except Exception:
            bar = None

        if bar is None:
            return remaining

        bar_ts = int(bar.ts)
        key = (str(instrument_key), bar_ts)
        avail = self._liquidity_remaining.get(key)
        if avail is None:
            vol_int = int(float(bar.volume) or 0.0)
            avail = vol_int if vol_int > 0 else -1
            self._liquidity_remaining[key] = avail

        if avail < 0:
            return remaining

        if allow_partial:
            fill_qty = min(int(remaining), int(avail))
        else:
            fill_qty = int(remaining) if int(avail) >= int(remaining) else 0

        # Keep fills aligned to configured lot size to avoid breaking downstream assumptions
        # (ExitEngine/RiskManager/OMS validations expect lot-multiple positions).
        try:
            lot_size = max(int(getattr(self.cfg.data, "lot_step", 1) or 1), 1)
        except Exception:
            lot_size = 1
        if lot_size > 1 and fill_qty > 0:
            fill_qty = (int(fill_qty) // lot_size) * lot_size

        if fill_qty > 0:
            self._liquidity_remaining[key] = max(int(avail) - int(fill_qty), 0)
        return int(fill_qty)

    def _has_pending_orders(self) -> bool:
        for order in list(getattr(self.oms, "_orders", {}).values()):
            if not isinstance(order, Order):
                continue
            if order.state not in {OrderState.ACKNOWLEDGED, OrderState.SUBMITTED, OrderState.PARTIALLY_FILLED}:
                continue
            remaining = max(int(order.qty) - int(order.filled_qty), 0)
            if remaining > 0:
                return True
        return False

    async def _force_next_tick_fill(self, ts: dt.datetime, underlying: str) -> None:
        """
        Ensure `fill_model="next_tick"` orders can fill at the end of a day/run.

        The main loop ends on the last historical tick, but `next_tick` needs a subsequent
        tick. We synthesize a single "bump" tick after the configured latency.
        """

        if not self._has_pending_orders():
            return
        if self.execution.fill_model != FILL_MODEL_NEXT_TICK and int(self.execution.latency_ms or 0) <= 0:
            return
        bump_ms = max(int(self.execution.latency_ms or 0), 1)
        bump_ts = ts + dt.timedelta(milliseconds=bump_ms)
        self._sim_tick_index += 1
        with travel(bump_ts):
            await self._fill_pending_orders(bump_ts, underlying)

    async def _drain_fills(
        self,
        strategy: Any,
        queue: asyncio.Queue,
        *,
        trade_log: List[Dict[str, Any]],
        last_realized_by_key: Dict[Tuple[str, Optional[str], Optional[float], Optional[str]], float],
        last_fees_by_key: Dict[Tuple[str, Optional[str], Optional[float], Optional[str]], float],
        equity_curve: List[Dict[str, Any]],
    ) -> None:
        while True:
            try:
                event = queue.get_nowait()
            except asyncio.QueueEmpty:
                break
            try:
                exec_obj = _execution_from_fill_event(event)
            except Exception:
                continue
            self.pnl.on_execution(exec_obj)
            try:
                self.exit_engine.on_fill(
                    symbol=exec_obj.symbol,
                    side=exec_obj.side,
                    qty=exec_obj.qty,
                    price=exec_obj.price,
                    ts=exec_obj.ts,
                )
            except Exception:
                pass
            try:
                lot_guess = getattr(self.cfg.data, "lot_step", 1)
                self.risk.on_fill(symbol=exec_obj.symbol, side=exec_obj.side, qty=exec_obj.qty, price=exec_obj.price, lot_size=lot_guess)
            except Exception:
                pass
            try:
                await strategy.on_fill(event)
            except Exception:
                pass

            # Record a snapshot at each execution for charting and for per-trade deltas.
            self.pnl.snapshot(exec_obj.ts)
            realized, unrealized, fees = self.pnl.totals()
            equity_curve.append({"ts": exec_obj.ts, "net": realized + unrealized - fees})

            key = (exec_obj.symbol, exec_obj.expiry, exec_obj.strike, exec_obj.opt_type)
            state = getattr(self.pnl, "_positions", {}).get(key)
            if state is None:
                continue
            if state.net_qty != 0 or state.closed_at is None:
                continue
            prev_realized = float(last_realized_by_key.get(key, 0.0))
            prev_fees = float(last_fees_by_key.get(key, 0.0))
            trade_realized = float(state.realized) - prev_realized
            trade_fees = float(state.fees) - prev_fees
            last_realized_by_key[key] = float(state.realized)
            last_fees_by_key[key] = float(state.fees)
            trade_log.append(
                {
                    "symbol": exec_obj.symbol,
                    "opened_at": state.opened_at,
                    "closed_at": state.closed_at,
                    "realized_pnl": trade_realized,
                    "gross_pnl": trade_realized,
                    "fees": trade_fees,
                    "net_pnl": trade_realized - trade_fees,
                }
            )

    def _load_orders_table(self) -> List[Dict[str, Any]]:
        try:
            conn = sqlite3.connect(str(self.store.path))
            conn.row_factory = sqlite3.Row
        except Exception:
            return []
        try:
            rows = conn.execute(
                """
                WITH order_fees AS (
                    SELECT
                        e.order_id AS order_id,
                        COALESCE(SUM(c.amount), 0.0) AS fees
                    FROM executions e
                    LEFT JOIN cost_ledger c
                        ON c.run_id=e.run_id AND c.exec_id=e.exec_id
                    WHERE e.run_id=?
                    GROUP BY e.order_id
                )
                SELECT
                    o.client_order_id AS order_id,
                    o.strategy,
                    o.symbol,
                    o.side,
                    o.qty,
                    COALESCE(SUM(e.qty), 0) AS fill_qty,
                    CASE WHEN COALESCE(SUM(e.qty), 0) > 0 THEN SUM(e.qty * COALESCE(e.raw_price, e.price)) / SUM(e.qty) END AS avg_raw_price,
                    CASE WHEN COALESCE(SUM(e.qty), 0) > 0 THEN SUM(e.qty * COALESCE(e.effective_price, e.price)) / SUM(e.qty) END AS avg_effective_price,
                    COALESCE(MAX(of.fees), 0.0) AS fees,
                    o.price AS limit_price,
                    o.state,
                    o.last_update,
                    o.broker_order_id
                FROM orders o
                LEFT JOIN executions e
                    ON e.run_id=o.run_id AND e.order_id=o.client_order_id
                LEFT JOIN order_fees of
                    ON of.order_id=o.client_order_id
                WHERE o.run_id=?
                GROUP BY o.client_order_id
                ORDER BY o.last_update ASC
                """,
                (self.run_id, self.run_id),
            ).fetchall()
            return [dict(r) for r in rows]
        except Exception:
            return []
        finally:
            try:
                conn.close()
            except Exception:
                pass

    def _load_executions_table(self) -> List[Dict[str, Any]]:
        try:
            conn = sqlite3.connect(str(self.store.path))
            conn.row_factory = sqlite3.Row
        except Exception:
            return []
        try:
            rows = conn.execute(
                """
                SELECT
                    e.exec_id,
                    e.order_id,
                    e.symbol,
                    e.side,
                    e.qty,
                    COALESCE(e.raw_price, e.price) AS raw_price,
                    COALESCE(e.effective_price, e.price) AS effective_price,
                    e.price,
                    e.ts,
                    e.venue,
                    COALESCE(SUM(c.amount), 0.0) AS fees
                FROM executions e
                LEFT JOIN cost_ledger c
                    ON c.run_id=e.run_id AND c.exec_id=e.exec_id
                WHERE e.run_id=?
                GROUP BY e.id
                ORDER BY e.ts ASC
                """,
                (self.run_id,),
            ).fetchall()
            return [dict(r) for r in rows]
        except Exception:
            return []
        finally:
            try:
                conn.close()
            except Exception:
                pass

    async def _drive_exit_engine(self, ts: dt.datetime, underlying: str) -> None:
        # Evaluate exits for every open position.
        for pos in self.store.list_open_positions():
            symbol = str(pos.get("symbol") or "")
            if not symbol:
                continue
            ltp = self._fill_price(symbol, ts, underlying)
            try:
                await self.exit_engine.on_tick(symbol, ltp, ts)
            except Exception:
                continue

    async def _square_off_all(self, ts: dt.datetime, underlying: str, *, reason: str) -> None:
        positions = self.store.list_open_positions()
        if not positions:
            return
        for row in positions:
            symbol = str(row.get("symbol") or "")
            qty = int(row.get("qty") or 0)
            if not symbol or qty == 0:
                continue
            side = "SELL" if qty > 0 else "BUY"
            close_qty = abs(qty)
            price = self._fill_price(symbol, ts, underlying)
            try:
                await self.oms.submit(
                    strategy=f"squareoff:{reason.lower()}",
                    symbol=symbol,
                    side=side,
                    qty=close_qty,
                    order_type="MARKET",
                    limit_price=price,
                    ts=ts,
                )
            except Exception:
                continue
        try:
            self.store.clear_exit_plans()
        except Exception:
            pass

    def _reset_daily_state(self) -> None:
        # Reset only daily counters and the session kill switch for multi-day backtests.
        for attr, value in (
            ("_order_timestamps", None),
            ("_scalping_timestamps", None),
        ):
            if hasattr(self.risk, attr):
                try:
                    getattr(self.risk, attr).clear()
                except Exception:
                    pass
        for attr, value in (
            ("_trades_executed_today", 0),
            ("_consecutive_losses", 0),
            ("_slippage_sum_pct", 0.0),
            ("_slippage_count", 0),
            ("_slippage_bad", 0),
            ("_kill_switch", False),
            ("_halt_reason", None),
        ):
            if hasattr(self.risk, attr):
                try:
                    setattr(self.risk, attr, value)
                except Exception:
                    pass
        try:
            self.store.clear_exit_plans()
        except Exception:
            pass
        try:
            setattr(self.exit_engine, "_risk_halt_executed", False)
        except Exception:
            pass


def _df_to_rows(df) -> List[Dict[str, Any]]:
    if df is None or getattr(df, "empty", True):
        return []
    records = df.to_dict("records")
    out: List[Dict[str, Any]] = []
    for row in records:
        ts = row.get("ts")
        if hasattr(ts, "to_pydatetime"):
            ts = ts.to_pydatetime()
        out.append(
            {
                "ts": ts,
                "open": row.get("open"),
                "high": row.get("high"),
                "low": row.get("low"),
                "close": row.get("close"),
                "volume": row.get("volume"),
                "oi": row.get("oi"),
            }
        )
    return out


def _df_to_series(*, key: str, interval: str, df) -> _CandleSeries:
    if df is None or getattr(df, "empty", True):
        return _CandleSeries(
            key=str(key),
            interval=str(interval),
            bar_seconds=0,
            ts_list=tuple(),
            bars_by_ts={},
        )

    bars_by_ts: Dict[int, _CandleBar] = {}
    ts_list: List[int] = []
    for row in df.to_dict("records"):
        ts_val = row.get("ts")
        if ts_val is None:
            continue
        if hasattr(ts_val, "to_pydatetime"):
            ts_val = ts_val.to_pydatetime()
        if not isinstance(ts_val, dt.datetime):
            continue
        if ts_val.tzinfo is None:
            ts_val = ts_val.replace(tzinfo=IST)
        ts_epoch = int(ts_val.astimezone(dt.timezone.utc).timestamp())
        try:
            bar = _CandleBar(
                ts=ts_epoch,
                open=float(row.get("open") or 0.0),
                high=float(row.get("high") or 0.0),
                low=float(row.get("low") or 0.0),
                close=float(row.get("close") or 0.0),
                volume=float(row.get("volume") or 0.0),
                oi=(None if row.get("oi") is None else float(row.get("oi"))),
            )
        except Exception:
            continue
        if ts_epoch not in bars_by_ts:
            ts_list.append(ts_epoch)
        bars_by_ts[ts_epoch] = bar

    ts_list.sort()
    try:
        bar_seconds = interval_seconds(interval)
    except Exception:
        bar_seconds = 0
    return _CandleSeries(
        key=str(key),
        interval=str(interval),
        bar_seconds=int(bar_seconds),
        ts_list=tuple(ts_list),
        bars_by_ts=bars_by_ts,
    )


def _coerce_float(value: object) -> Optional[float]:
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _parse_option_symbol(symbol: str) -> tuple[str, str, float, str]:
    parts = symbol.split("-")
    if len(parts) < 3:
        raise ValueError("unsupported_symbol")
    expiry = "-".join(parts[1:-1])
    tail = parts[-1]
    opt_type = tail[-2:].upper() if tail[-2:].upper() in {"CE", "PE"} else "CE"
    strike_part = tail[:-2] if opt_type in {"CE", "PE"} else tail
    try:
        dt.date.fromisoformat(expiry)
    except ValueError as exc:
        raise ValueError("bad_expiry") from exc
    try:
        strike = float(strike_part)
    except ValueError as exc:
        raise ValueError("bad_strike") from exc
    return parts[0].upper(), expiry, strike, opt_type


def _execution_from_fill_event(event: Dict[str, Any]) -> PnLExecution:
    symbol = str(event["symbol"])
    expiry: Optional[str]
    strike: Optional[float]
    opt_type: Optional[str]
    try:
        _, expiry, strike, opt_type = _parse_option_symbol(symbol)
    except Exception:
        expiry, strike, opt_type = None, None, None
    ts_val = event.get("ts")
    ts = dt.datetime.fromisoformat(ts_val) if isinstance(ts_val, str) else engine_now(IST)
    if ts.tzinfo is None:
        ts = ts.replace(tzinfo=IST)
    return PnLExecution(
        exec_id=str(event.get("exec_id") or event["order_id"]),
        order_id=str(event["order_id"]),
        symbol=symbol,
        side=str(event["side"]),
        qty=int(event["qty"]),
        price=float(event["price"]),
        ts=ts.astimezone(IST),
        expiry=expiry,
        strike=strike,
        opt_type=opt_type,
    )

__all__ = ["BacktestingEngine", "BacktestResult", "SimBroker"]
