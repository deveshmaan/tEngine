from __future__ import annotations

import asyncio
import datetime as dt
import math
import tempfile
import uuid
from dataclasses import replace
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable, Dict, Iterable, List, Optional, Tuple

from brokerage.upstox_client import INDEX_INSTRUMENT_KEYS, IST, UpstoxSession
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
        self.risk = RiskManager(self.cfg.risk, self.store, capital_base=self.cfg.capital_base)
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

        self._stop_requested = False
        self._last_spot: Dict[str, float] = {}
        self._last_iv: Dict[str, float] = {}
        self._iv_seeded: Dict[str, bool] = {}
        self._spot_returns: Dict[str, List[float]] = {}

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
    ) -> Dict[str, Any]:
        try:
            return asyncio.run(
                self.run_backtest_async(
                    strategy_name,
                    start_date,
                    end_date,
                    interval=interval,
                    progress_callback=progress_callback,
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
    ) -> Dict[str, Any]:
        underlying = self.cfg.data.index_symbol.upper()
        candles = data if data is not None else self.session.get_historical_data(underlying, start_date, end_date, interval, as_dataframe=False)
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
            out["interval"] = interval
            out["underlying"] = underlying
            out["candles"] = total_candles
            out["ticks"] = 0
            return out

        strategy_cls = get_strategy_class(strategy_name)
        strategy = strategy_cls(
            self.cfg,
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
            offsets = (0, 15, 30, 45)
            tick_plan: List[Tuple[dt.datetime, float, float]] = []
            for idx, px in enumerate(prices):
                tick_plan.append((ts + dt.timedelta(seconds=offsets[idx]), float(px), float(candle_volume) if idx == 0 else 0.0))

            for tick_ts, tick_price, tick_volume in tick_plan:
                if self._stop_requested:
                    break
                ticks_processed += 1
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
        out["interval"] = interval
        out["underlying"] = underlying
        out["candles"] = total_candles
        out["ticks"] = ticks_processed
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

    def _option_price(self, *, underlying: str, spot: float, expiry: str, strike: int, opt_type: str, ts: dt.datetime) -> float:
        tick = max(float(self.cfg.data.tick_size or 0.0), 0.0) or 0.05
        try:
            exp = dt.date.fromisoformat(expiry)
        except Exception:
            exp = ts.date()
        days = max((exp - ts.date()).days, 0)
        tau = max(days + 0.5, 0.5) / 7.0  # ~weeks
        time_value = max(float(spot) * 0.004 * math.sqrt(tau), tick)
        if opt_type.upper() == "PE":
            intrinsic = max(float(strike) - float(spot), 0.0)
            premium = time_value + 0.5 * intrinsic
        else:
            intrinsic = max(float(spot) - float(strike), 0.0)
            premium = time_value + 0.5 * intrinsic
        if premium < tick:
            premium = tick
        # snap to tick size
        return round(round(premium / tick) * tick, ndigits=8)

    def _build_option_events(self, underlying: str, ts: dt.datetime, expiry: str, spot: float, step: int, atm: int) -> List[Dict[str, Any]]:
        events: List[Dict[str, Any]] = []
        iv_val = self._synthetic_iv(underlying)
        for opt_type, strikes in (
            ("CE", (atm, atm + step)),
            ("PE", (atm, atm - step)),
        ):
            for strike in strikes:
                symbol = f"{underlying}-{expiry}-{int(strike)}{opt_type}"
                ltp = self._option_price(underlying=underlying, spot=spot, expiry=expiry, strike=int(strike), opt_type=opt_type, ts=ts)
                bid = max(ltp - self.cfg.data.tick_size, self.cfg.data.tick_size)
                ask = ltp + self.cfg.data.tick_size
                payload = {
                    "instrument_key": f"SIM|{symbol}",
                    "underlying": underlying,
                    "symbol": symbol,
                    "expiry": expiry,
                    "strike": int(strike),
                    "opt_type": opt_type,
                    "ltp": ltp,
                    "bid": bid,
                    "ask": ask,
                    "iv": iv_val,
                    "oi": 1000.0,
                    "volume": 1000.0,
                    "ts": ts.isoformat(),
                }
                record_tick_seen(instrument_key=symbol, underlying=underlying, ts_seconds=ts.timestamp())
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
                # Prefer explicit capability detection instead of shape-based heuristics (empty caches at startup).
                if hasattr(strategy, "_pcr_from_cache"):
                    ce_strikes = (atm, atm + step)
                    pe_strikes = (atm, atm - step)
                    chain: Dict[str, Any] = {"CE": {}, "PE": {}, "__pcr__": 1.0}
                    for strike in ce_strikes:
                        symbol = f"{underlying}-{expiry}-{int(strike)}CE"
                        chain["CE"][int(strike)] = {
                            "instrument_key": f"SIM|{symbol}",
                            "oi": 1000.0,
                            "volume": float(volume or 1000.0),
                            "ltp": self._option_price(underlying=underlying, spot=spot, expiry=expiry, strike=int(strike), opt_type="CE", ts=ts),
                            "iv": iv_val,
                            "gamma": 0.0,
                            "expiry": expiry,
                        }
                    for strike in pe_strikes:
                        symbol = f"{underlying}-{expiry}-{int(strike)}PE"
                        chain["PE"][int(strike)] = {
                            "instrument_key": f"SIM|{symbol}",
                            "oi": 1000.0,
                            "volume": float(volume or 1000.0),
                            "ltp": self._option_price(underlying=underlying, spot=spot, expiry=expiry, strike=int(strike), opt_type="PE", ts=ts),
                            "iv": iv_val,
                            "gamma": 0.0,
                            "expiry": expiry,
                        }
                    chain_cache[underlying] = chain
                    chain_ts[underlying] = ts.timestamp()
                else:
                    # ScalpingBuyStrategy: expects {strike: {...}, "__pcr__": float}
                    chain: Dict[Any, Any] = {"__pcr__": 1.0}
                    for strike in (atm, atm + step):
                        symbol = f"{underlying}-{expiry}-{int(strike)}CE"
                        ltp = self._option_price(underlying=underlying, spot=spot, expiry=expiry, strike=int(strike), opt_type="CE", ts=ts)
                        chain[int(strike)] = {
                            "instrument_key": f"SIM|{symbol}",
                            "oi": 1000.0,
                            "volume": float(volume or 1000.0),
                            "ltp": ltp,
                            "bid": max(ltp - self.cfg.data.tick_size, self.cfg.data.tick_size),
                            "ask": ltp + self.cfg.data.tick_size,
                            "expiry": expiry,
                        }
                    chain_cache[underlying] = chain
                    chain_ts[underlying] = ts.timestamp()

    async def _fill_pending_orders(self, ts: dt.datetime, underlying: str) -> None:
        # Fill any ACK'd orders at the latest synthetic price.
        for order in list(getattr(self.oms, "_orders", {}).values()):
            if not isinstance(order, Order):
                continue
            if order.state not in {OrderState.ACKNOWLEDGED, OrderState.SUBMITTED}:
                continue
            remaining = max(int(order.qty) - int(order.filled_qty), 0)
            if remaining <= 0:
                continue
            fill_price = self._fill_price(order.symbol, ts, underlying)
            await self.oms.record_fill(order.client_order_id, qty=remaining, price=fill_price, broker_order_id=order.broker_order_id)

    def _fill_price(self, symbol: str, ts: dt.datetime, underlying: str) -> float:
        sym = str(symbol or "")
        if sym.upper() in {"NIFTY", "BANKNIFTY"}:
            return float(self._last_spot.get(sym.upper(), 0.0) or 0.0)
        try:
            u, expiry, strike, opt = _parse_option_symbol(sym)
        except ValueError:
            return float(self._last_spot.get(underlying.upper(), 0.0) or 0.0)
        spot = float(self._last_spot.get(u.upper(), 0.0) or self._last_spot.get(underlying.upper(), 0.0) or 0.0)
        return self._option_price(underlying=u, spot=spot, expiry=expiry, strike=int(strike), opt_type=opt, ts=ts)

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
                    "fees": trade_fees,
                    "net_pnl": trade_realized - trade_fees,
                }
            )

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
            if not symbol or qty <= 0:
                continue
            price = self._fill_price(symbol, ts, underlying)
            try:
                await self.oms.submit(
                    strategy=f"squareoff:{reason.lower()}",
                    symbol=symbol,
                    side="SELL",
                    qty=qty,
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
