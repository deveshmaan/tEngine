from __future__ import annotations

import asyncio
import datetime as dt
import logging
import os
import random
import threading
import time
from typing import Any, Awaitable, Callable, Iterable, List, Mapping, Optional, Protocol, Sequence, Union

import upstox_client
from upstox_client.rest import ApiException
try:
    # SDKs ship MarketDataStreamerV3 under slightly different module layouts.
    from upstox_client.feeder.market_data_streamer_v3 import MarketDataStreamerV3
except Exception:  # pragma: no cover
    try:
        from upstox_client.feeder import MarketDataStreamerV3  # type: ignore
    except Exception:  # pragma: no cover
        MarketDataStreamerV3 = None  # type: ignore

try:
    # Protobuf generated from Upstox MarketDataFeed v3 .proto
    from engine._proto import MarketDataFeed_pb2 as pb  # type: ignore
except Exception:  # pragma: no cover
    try:
        import MarketDataFeed_pb2 as pb  # type: ignore
    except Exception:  # pragma: no cover
        pb = None  # type: ignore

# Best-effort metrics helpers (never crash the stream)
try:
    from engine.metrics import (
        record_md_frame,
        record_md_decode_error,
        publish_spread,
        publish_depth10,
        set_underlying_last_ts,
    )
except Exception:  # pragma: no cover

    def record_md_frame(nbytes: int): ...

    def record_md_decode_error(msg: str): ...

    def publish_spread(instrument_key: str, bid: float, ask: float, ltp: float): ...

    def publish_depth10(instrument_key: str, bid_qty10: float, ask_qty10: float): ...

    def set_underlying_last_ts(instrument: str, ts_seconds: float): ...

from brokerage.upstox_client import INDEX_INSTRUMENT_KEYS, InvalidDateError, PlacedOrder, UpstoxConfig, UpstoxSession, normalize_place_order_response
from engine.config import BrokerConfig, CONFIG, IST
from engine.data import record_tick_seen
from engine.data import normalize_date, pick_subscription_expiry, preopen_expiry_smoke, resolve_expiries_with_fallback
from engine.events import EventBus
from engine.logging_utils import get_logger
from engine.oms import BrokerOrderAck, BrokerOrderView, Order
from engine.instruments import InstrumentResolver
from engine.metrics import (
    EngineMetrics,
    clear_subscription_info,
    incr_ws_reconnects,
    publish_depth10,
    publish_option_depth,
    publish_option_quote,
    publish_spread,
    publish_subscription_info,
    publish_underlying,
    record_md_decode_error,
    record_md_frame,
    inc_market_data_stale_drop,
    set_market_data_stale,
    set_last_tick_ts,
    set_md_subscription,
    set_md_subscription_gauge,
    set_subscription_expiry,
    set_session_state,
    update_option_quote,
)
from utils.ip_check import assert_static_ip
from engine.alerts import notify_incident
from engine.risk import RiskManager
from market.instrument_cache import InstrumentCache, labels_for_key


class BrokerError(Exception):
    def __init__(self, *, code: str, message: str, status: Optional[int] = None, context: Optional[dict[str, Any]] = None):
        super().__init__(message)
        self.code = code
        self.status = status
        self.context = context or {}


class MarketStreamClient(Protocol):
    async def connect(self) -> None: ...

    async def close(self) -> None: ...

    async def subscribe(self, instruments: Sequence[str], mode: Optional[str] = None) -> None: ...

    async def heartbeat(self) -> None: ...


class TokenBucket:
    def __init__(self, rate_per_sec: float, burst: int):
        self._rate = max(rate_per_sec, 0.1)
        self._capacity = max(1, burst)
        self._tokens = float(self._capacity)
        self._updated = time.monotonic()
        self._lock = asyncio.Lock()

    async def acquire(self) -> float:
        while True:
            async with self._lock:
                now = time.monotonic()
                elapsed = now - self._updated
                self._updated = now
                self._tokens = min(self._capacity, self._tokens + elapsed * self._rate)
                if self._tokens >= 1.0:
                    self._tokens -= 1.0
                    return self._tokens
                deficit = 1.0 - self._tokens
                wait_for = deficit / self._rate if self._rate > 0 else 0.5
            await asyncio.sleep(max(wait_for, 0.0))


LOG = logging.getLogger("engine.market.full_d30")

FULL_D30_LIMIT_PER_USER = 50  # Upstox Plus documented limit
INTRADAY_PRODUCT = "I"  # Upstox intraday/MIS product; this engine is intraday-only.


class FullD30Streamer:
    """Upstox V3 WS streamer in FULL_D30 mode with protobuf decode + metrics emission."""

    def __init__(self, access_token: str, instrument_keys: List[str], loop: Optional[asyncio.AbstractEventLoop] = None):
        self._access_token = access_token
        self._instrument_keys = list(dict.fromkeys(instrument_keys))  # de-dupe, stable order
        self._loop = loop or asyncio.get_event_loop()
        self._stream: Optional[MarketDataStreamerV3] = None
        self._connected = asyncio.Event()
        self._stop = asyncio.Event()

    async def start(self) -> None:
        if MarketDataStreamerV3 is None:
            raise RuntimeError("MarketDataStreamerV3 unavailable in installed upstox_client package")
        if pb is None:
            raise RuntimeError("MarketDataFeed_pb2 missing; generate protobuf bindings for the v3 feed")
        if not self._instrument_keys:
            LOG.warning("No instruments provided to FULL_D30 streamer; nothing to subscribe.")
            return

        if len(self._instrument_keys) > FULL_D30_LIMIT_PER_USER:
            LOG.warning(
                "Truncating FULL_D30 subscription set from %d to %d (Upstox per-user cap).",
                len(self._instrument_keys),
                FULL_D30_LIMIT_PER_USER,
            )
            self._instrument_keys = self._instrument_keys[:FULL_D30_LIMIT_PER_USER]

        cfg = upstox_client.Configuration()
        cfg.access_token = self._access_token

        self._stream = MarketDataStreamerV3(configuration=cfg)
        self._connected.clear()

        self._stream.on_open(self._on_open)
        self._stream.on_data(self._on_data)  # raw protobuf bytes
        self._stream.on_error(self._on_error)
        self._stream.on_close(self._on_close)

        await self._loop.run_in_executor(None, self._stream.connect)
        await asyncio.wait_for(self._connected.wait(), timeout=10.0)

    async def stop(self) -> None:
        self._stop.set()
        if self._stream:
            try:
                await self._loop.run_in_executor(None, self._stream.disconnect)
            except Exception:
                pass

    # ---- Callbacks ----
    def _on_open(self, *args, **kwargs):
        LOG.info("V3 WebSocket connected; subscribing %d instruments in full_d30", len(self._instrument_keys))
        try:
            self._stream.subscribe(mode="full_d30", instrument_keys=self._instrument_keys)  # type: ignore[attr-defined]
        except Exception as e:
            LOG.exception("Subscribe failed: %s", e)
            raise
        set_md_subscription_gauge(len(self._instrument_keys))
        self._connected.set()

    def _on_close(self, *args, **kwargs):
        LOG.warning("V3 WebSocket closed.")
        if not self._stop.is_set():
            asyncio.run_coroutine_threadsafe(self._reconnect(), self._loop)

    def _on_error(self, err, *args, **kwargs):
        LOG.error("V3 WebSocket error: %s", err)

    def _on_data(self, payload: bytes):
        """Decode protobuf MarketDataFeed and publish metrics."""
        try:
            record_md_frame(len(payload or b""))
        except Exception:
            pass
        if pb is None:
            try:
                record_md_decode_error("pb_missing")
            except Exception:
                pass
            return
        try:
            feed = pb.Feed()  # recommended message name in v3 proto
            feed.ParseFromString(payload)
        except Exception as e:
            try:
                record_md_decode_error(str(e))
            except Exception:
                pass
            return

        ts = time.time()

        for item in getattr(feed, "feeds", []):
            key = getattr(item, "instrumentKey", "")  # e.g., "NSE_FO|12345" or "NSE_INDEX|Nifty 50"
            try:
                q = getattr(item, "marketFF", None) or getattr(item, "marketFFQuote", None) or item
                ltp = float(getattr(q, "ltp", 0.0))
                book = getattr(q, "marketDepth", None) or getattr(q, "marketLevel", None)
                bids_seq = getattr(book, "bids", None) or getattr(book, "bidAskQuote", None) or []
                asks_seq = getattr(book, "asks", None) or []
                bid1 = float(getattr(bids_seq[0], "price", getattr(bids_seq[0], "bidP", 0.0))) if bids_seq else float("nan")
                ask1 = float(getattr(asks_seq[0], "price", getattr(asks_seq[0], "askP", 0.0))) if asks_seq else float("nan")
                iv = float(getattr(q, "iv", float("nan"))) if hasattr(q, "iv") else None
                oi = int(getattr(q, "oi", 0)) if hasattr(q, "oi") else None
            except Exception as e:  # pragma: no cover - defensive parse
                record_md_decode_error(f"field_parse:{e}")
                continue
            bid_q10 = 0.0
            ask_q10 = 0.0
            try:
                if book:
                    for i in range(min(10, len(bids_seq))):
                        try:
                            bid_q10 += float(getattr(bids_seq[i], "quantity", getattr(bids_seq[i], "bidQ", 0.0)))
                        except Exception:
                            continue
                    for i in range(min(10, len(asks_seq))):
                        try:
                            ask_q10 += float(getattr(asks_seq[i], "quantity", getattr(asks_seq[i], "askQ", 0.0)))
                        except Exception:
                            continue
            except Exception:
                bid_q10 = bid_q10
                ask_q10 = ask_q10

            sym, expiry, strike, opt = labels_for_key(key)  # returns (symbol, 'YYYY-MM-DD' or '', int, 'CE'/'PE'/None)
            if opt is None:
                publish_underlying(sym or key, ltp, ts)
                try:
                    if sym:
                        set_underlying_last_ts(sym, ts)
                except Exception:
                    pass
            else:
                publish_option_quote(key, sym, expiry, opt, strike, ltp, bid1, ask1, iv, oi, ts)
                publish_spread(key, bid1, ask1, ltp)
                publish_depth10(key, bid_q10, ask_q10)
                publish_spread(key, bid1, ask1, ltp)
                # Sum top-10 quantities if present for imbalance gauges
                bid_qtys = []
                ask_qtys = []
                for entry in list(bids_seq)[:10]:
                    qty = getattr(entry, "qty", None) or getattr(entry, "quantity", None) or getattr(entry, "bidQ", None)
                    try:
                        bid_qtys.append(float(qty))
                    except Exception:
                        continue
                for entry in list(asks_seq)[:10]:
                    qty = getattr(entry, "qty", None) or getattr(entry, "quantity", None) or getattr(entry, "askQ", None)
                    try:
                        ask_qtys.append(float(qty))
                    except Exception:
                        continue
                if bid_qtys or ask_qtys:
                    publish_depth10(key, sum(bid_qtys), sum(ask_qtys))

    async def _reconnect(self):
        incr_ws_reconnects()
        backoff = [1, 2, 5, 10]
        for d in backoff:
            if self._stop.is_set():
                return
            try:
                LOG.info("Reconnecting V3 WS…")
                await self.start()
                return
            except Exception as e:
                LOG.warning("Reconnect failed (%s); retrying in %ss", e, d)
                await asyncio.sleep(d)
        LOG.error("Exhausted reconnect attempts for V3 WS")


class _AsyncMarketDataStream(MarketStreamClient):
    """Async wrapper around Upstox MarketDataStreamerV3 with an internal queue."""

    def __init__(self, access_token: str, mode: str = "full_d30") -> None:
        cfg = upstox_client.Configuration(sandbox=False)
        cfg.access_token = access_token
        self.ws = upstox_client.MarketDataStreamerV3(upstox_client.ApiClient(cfg), mode=mode)
        self._mode = mode or "full_d30"
        self._loop = asyncio.get_event_loop()
        self._open_event: asyncio.Event = asyncio.Event()
        self._messages: asyncio.Queue[dict[str, Any]] = asyncio.Queue(maxsize=2048)
        self._last_event = time.monotonic()
        self._thread: Optional[threading.Thread] = None
        self._closed = False
        self.ws.on("open", self._on_open)
        self.ws.on("message", self._on_message)
        self.ws.on("error", self._on_error)
        self.ws.on("close", self._on_close)
        self.ws.auto_reconnect(True, 1, 10)

    # ------------------------------------------------------------------ callbacks
    def _on_open(self, *_: Any) -> None:
        self._last_event = time.monotonic()
        self._loop.call_soon_threadsafe(self._open_event.set)

    def _on_message(self, message: dict[str, Any]) -> None:
        self._last_event = time.monotonic()
        self._loop.call_soon_threadsafe(self._enqueue, message)

    def _on_error(self, *_: Any) -> None:
        self._last_event = time.monotonic()

    def _on_close(self, *_: Any) -> None:
        self._last_event = time.monotonic()

    def _enqueue(self, message: dict[str, Any]) -> None:
        try:
            self._messages.put_nowait(message)
        except asyncio.QueueFull:
            try:
                self._messages.get_nowait()
            except Exception:
                pass
            try:
                self._messages.put_nowait(message)
            except Exception:
                pass

    # ---------------------------------------------------------------------- api
    async def connect(self) -> None:
        if self._thread and self._thread.is_alive():
            return
        self._open_event.clear()
        self._closed = False
        self._thread = threading.Thread(target=self.ws.connect, daemon=True)
        self._thread.start()
        try:
            await asyncio.wait_for(self._open_event.wait(), timeout=10.0)
        except asyncio.TimeoutError as exc:
            raise RuntimeError("Market data websocket open timed out") from exc

    async def close(self) -> None:
        self._closed = True
        try:
            await asyncio.to_thread(self.ws.disconnect)
        except Exception:
            return

    async def subscribe(self, instruments: Sequence[str], mode: Optional[str] = None) -> None:
        tokens = [str(tok) for tok in instruments if tok]
        if not tokens:
            return
        await asyncio.to_thread(self.ws.subscribe, tokens, mode or self._mode)

    async def heartbeat(self) -> float:
        if self._closed:
            raise RuntimeError("stream closed")
        return self._last_event

    async def recv(self, timeout: float = 1.0) -> Optional[Mapping[str, Any]]:
        try:
            return await asyncio.wait_for(self._messages.get(), timeout=timeout)
        except asyncio.TimeoutError:
            return None


class _TickCoalescer:
    """Rate-limits Prometheus updates by coalescing recent ticks."""

    def __init__(self, *, depth_levels: int = 5) -> None:
        self._depth_levels = max(int(depth_levels or 1), 1)
        self._underlyings: dict[str, tuple[float, float]] = {}
        self._options: dict[str, dict[str, Any]] = {}
        self._last_option_quotes: dict[str, dict[str, Any]] = {}
        self._task: Optional[asyncio.Task] = None
        self._stop_event = asyncio.Event()

    def start(self) -> None:
        if self._task:
            return
        self._stop_event.clear()
        self._task = asyncio.create_task(self._run(), name="tick-coalescer")

    async def stop(self) -> None:
        if not self._task:
            return
        self._stop_event.set()
        self._task.cancel()
        try:
            await self._task
        except asyncio.CancelledError:
            pass
        self._task = None

    def update_underlying(self, symbol: str, ltp: float, ts: float) -> None:
        self._underlyings[str(symbol or "").upper()] = (float(ltp), float(ts))

    def update_option(self, instrument_key: str, payload: Mapping[str, Any]) -> None:
        snapshot = dict(payload)
        key = str(instrument_key or "")
        self._options[key] = snapshot
        self._last_option_quotes[key] = snapshot

    def get_option(self, instrument_key: str) -> Optional[dict[str, Any]]:
        key = str(instrument_key or "")
        if not key:
            return None
        payload = self._last_option_quotes.get(key)
        if not payload:
            return None
        return dict(payload)

    def snapshot_options(self) -> dict[str, dict[str, Any]]:
        return {k: dict(v) for k, v in self._last_option_quotes.items()}

    async def _run(self) -> None:
        try:
            while not self._stop_event.is_set():
                await asyncio.sleep(0.15)
                self._flush()
        except asyncio.CancelledError:
            pass
        finally:
            self._flush()

    def _flush(self) -> None:
        if self._underlyings:
            for sym, (ltp, ts) in list(self._underlyings.items()):
                publish_underlying(sym, ltp, ts)
            self._underlyings.clear()
        if self._options:
            for key, data in list(self._options.items()):
                publish_option_quote(
                    key,
                    data.get("symbol", ""),
                    data.get("expiry", ""),
                    data.get("opt", ""),
                    data.get("strike", ""),
                    ltp=data.get("ltp"),
                    bid=data.get("bid"),
                    ask=data.get("ask"),
                    iv=data.get("iv"),
                    oi=data.get("oi"),
                    ts=data.get("ts"),
                )
                depth = data.get("depth") or {}
                bids = depth.get("bids")
                asks = depth.get("asks")
                if bids or asks:
                    publish_option_depth(
                        key,
                        data.get("symbol", ""),
                        data.get("expiry", ""),
                        data.get("opt", ""),
                        data.get("strike", ""),
                        bids=bids,
                        asks=asks,
                        levels=self._depth_levels,
                    )
            self._options.clear()


class UpstoxBroker:
    """REST + streaming broker wrapper with watchdogs and rate-limits."""

    _SESSION_STATE_MAP = {
        "PREOPEN": 0,
        "LIVE": 1,
        "AFTER_CUTOFF": 2,
        "POST_CLOSE": 3,
    }

    def __init__(
        self,
        *,
        config: BrokerConfig,
        session_factory: Optional[Callable[[Optional[str]], UpstoxSession]] = None,
        stream_client: Optional[MarketStreamClient] = None,
        token_refresh_cb: Optional[Callable[[], Union[str, Awaitable[str]]]] = None,
        ws_failure_callback: Optional[Callable[[int], Union[None, Awaitable[None]]]] = None,
        instrument_resolver: Optional[InstrumentResolver] = None,
        instrument_cache: Optional[InstrumentCache] = None,
        metrics: Optional[EngineMetrics] = None,
        auth_halt_callback: Optional[Callable[[str], Union[None, Awaitable[None]]]] = None,
        reconcile_callback: Optional[Callable[[], Awaitable[None]]] = None,
        risk_manager: Optional[RiskManager] = None,
        bus: Optional[EventBus] = None,
        allowed_ips: Optional[Sequence[str]] = None,
    ):
        self._cfg = config
        try:
            assert_static_ip(allowed_ips)
        except RuntimeError:
            raise
        except Exception as exc:
            raise RuntimeError(f"Static IP validation failed: {exc}") from exc
        self._static_ip_ok = True
        self._session_factory = session_factory or self._default_session_factory
        self._session = self._session_factory(None)
        self._stream = stream_client
        self.ws: Optional[Any] = None
        try:
            self._stream_mode = str(CONFIG.market_data.get("stream_mode", "full_d30")).strip().lower() or "full_d30"
        except Exception:
            self._stream_mode = "full_d30"
        self._token_refresh_cb = token_refresh_cb
        self._subscriptions: set[str] = set()
        self._subscription_meta: dict[str, dict[str, str]] = {}
        self._subscription_last_seen: dict[str, float] = {}
        self._watchdog: Optional[asyncio.Task] = None
        self._stop = asyncio.Event()
        self._logger = get_logger("UpstoxBroker")
        self._ws_failure_cb = ws_failure_callback
        self._resolver = instrument_resolver
        self.instrument_cache = instrument_cache or InstrumentCache.runtime_cache()
        self._metrics = metrics
        self._auth_halt_cb = auth_halt_callback
        self._reconcile_cb = reconcile_callback
        self._risk = risk_manager
        self._square_off_cb: Optional[Callable[[str], Awaitable[None]]] = None
        self._shutdown_cb: Optional[Callable[[str], Awaitable[None]]] = None
        self._session_guard_decision: Optional[dict[str, Any]] = None
        self._rate_limits = self._build_rate_limits()
        self._pending_requests = {name: 0 for name in self._rate_limits}
        self._halt_new_orders = False
        self._halt_reason: Optional[str] = None
        self._queue_lock = asyncio.Lock()
        self._last_ws_heartbeat = time.monotonic()
        self._preopen_checked = False
        self._last_spot: dict[str, float] = {}
        self._underlying_tokens: set[str] = set()
        self._starting = False
        self._stream_reader: Optional[asyncio.Task] = None
        self._bus = bus
        try:
            depth_levels = int(CONFIG.market_data.get("depth_levels", 5))
        except Exception:
            depth_levels = 5
        self._tick_coalescer = _TickCoalescer(depth_levels=depth_levels)
        if self._metrics:
            self._metrics.set_risk_halt_state(0)
        try:
            self._max_tick_age = float(getattr(CONFIG.market_data, "max_tick_age_seconds", 0.0))
        except Exception:
            self._max_tick_age = 0.0
        self._last_ws_recv = time.time()

    def cached_option_quote(self, instrument_key: str) -> Optional[dict[str, Any]]:
        if not instrument_key:
            return None
        snapshot = self._tick_coalescer.get_option(instrument_key)
        return dict(snapshot) if snapshot else None

    def cached_option_quotes(self) -> dict[str, dict[str, Any]]:
        return self._tick_coalescer.snapshot_options()

    def cached_spot(self, symbol: str) -> Optional[float]:
        sym = str(symbol or "").upper()
        if not sym:
            return None
        return self._last_spot.get(sym)

    async def get_spot(self, symbol: str) -> float:
        cache = self.instrument_cache or InstrumentCache.runtime_cache()
        symbol_up = symbol.upper()
        if cache is None:
            if symbol_up in self._last_spot:
                return self._last_spot[symbol_up]
            raise RuntimeError("Instrument cache unavailable for spot lookup")
        if self.instrument_cache is None:
            self.instrument_cache = cache
        try:
            index_key = cache.resolve_index_key(symbol_up)
        except Exception as exc:
            if symbol_up in self._last_spot:
                return self._last_spot[symbol_up]
            raise RuntimeError(f"Unable to resolve index key for {symbol_up}") from exc
        try:
            resp = await asyncio.to_thread(self._session.get_ltp, index_key)
        except Exception as exc:
            fallback = self._last_spot.get(symbol_up)
            if fallback is not None:
                return fallback
            raise RuntimeError(f"Failed to fetch spot for {symbol_up}: {exc}") from exc
        data: Any = resp
        if isinstance(resp, Mapping):
            data = resp.get("data") or resp.get("ltp") or resp
        entries: list[Any]
        if isinstance(data, Mapping):
            entries = list(data.values())
        elif isinstance(data, list):
            entries = data
        else:
            entries = [data] if data not in (None, "") else []
        price: Optional[float] = None
        for entry in entries:
            if not isinstance(entry, Mapping):
                continue
            raw = entry.get("ltp") or entry.get("last_price") or entry.get("close") or entry.get("price")
            try:
                price = float(raw)
            except (TypeError, ValueError):
                continue
            else:
                break
        if price is None:
            fallback = self._last_spot.get(symbol_up)
            if fallback is not None:
                return fallback
            raise RuntimeError(f"Unable to extract spot price for {symbol_up}")
        self._last_spot[symbol_up] = price
        return price

    async def fetch_ltp(self, instrument_key: str) -> Optional[float]:
        key = str(instrument_key or "").strip()
        if not key:
            return None
        try:
            resp = await self._rest_call(
                "quote",
                "ltp",
                lambda session: session.get_ltp(key),
                context={"instrument_key": key},
            )
        except Exception:
            return None
        data: Any = resp
        if isinstance(resp, Mapping):
            data = resp.get("data") or resp.get("ltp") or resp
        entry: Any = None
        if isinstance(data, Mapping):
            entry = data.get(key) if key in data else next(iter(data.values()), None)
        elif isinstance(data, list):
            entry = data[0] if data else None
        else:
            entry = data
        if isinstance(entry, Mapping):
            raw = entry.get("ltp") or entry.get("last_price") or entry.get("close") or entry.get("price")
        else:
            raw = entry
        try:
            val = float(raw)
        except (TypeError, ValueError):
            return None
        return val if val > 0 else None

    async def _subscribe_current_weekly(self, symbol: str) -> None:
        """
        Subscribes +/- window_steps strikes around ATM for the earliest weekly expiry,
        and emits md_subscription rows. Respects option_type (CE|PE|BOTH).
        """

        cache = self.instrument_cache or InstrumentCache.runtime_cache()
        idx_symbol = symbol.upper()
        if cache is None:
            self._logger.warning("Instrument cache unavailable; skipping auto-subscribe for %s", idx_symbol)
            return
        if self.instrument_cache is None:
            self.instrument_cache = cache
        expiries = resolve_expiries_with_fallback(idx_symbol)
        if not expiries:
            self._logger.warning("No expiries available for %s", idx_symbol)
            return
        pref = CONFIG.data.get("subscription_expiry_preference", "current")
        try:
            expiry = pick_subscription_expiry(idx_symbol, pref)
        except Exception:
            expiry = expiries[1] if pref == "next" and len(expiries) > 1 else expiries[0]
        self.record_subscription_expiry(idx_symbol, expiry, pref)
        try:
            step_map = getattr(CONFIG.data, "strike_steps", {}) or {}
            step = int(step_map.get(idx_symbol, 50))
        except Exception:
            step = 50
        step = max(step, 1)
        try:
            spot = await self.get_spot(idx_symbol)
        except Exception as exc:
            self._logger.warning("Unable to fetch spot for %s: %s", idx_symbol, exc)
            cached = self._last_spot.get(idx_symbol)
            if cached is None:
                return
            spot = cached
        atm = round(float(spot) / step) * step
        window = max(int(CONFIG.market_data.get("window_steps", 2)), 0)
        strikes = [atm + i * step for i in range(-window, window + 1)]
        opt_mode = str(CONFIG.market_data.get("option_type", "CE")).upper()
        opt_list = ["CE", "PE"] if opt_mode == "BOTH" else [opt_mode]
        tokens: list[Any] = list(self._subscriptions)
        for opt in opt_list:
            for ikey, st in cache.contract_keys_for(idx_symbol, expiry, opt, strikes):
                tokens.append(
                    {
                        "instrument_key": ikey,
                        "symbol": idx_symbol,
                        "expiry": expiry,
                        "opt_type": opt,
                        "strike": st,
                    }
                )
        if len(tokens) == len(self._subscriptions):
            self._logger.warning("No option contracts resolved for %s %s (%s)", idx_symbol, expiry, opt_mode)
            return
        await self.subscribe_marketdata(tokens)

    async def start(self) -> None:
        if self._starting:
            return
        self._starting = True
        await self._run_preopen_probe()
        try:
            if not await self._apply_session_guard():
                return
            self._ensure_stream_client()
            self._tick_coalescer.start()
            if self._stream and not self._watchdog:
                await self._stream.connect()
                self._last_ws_heartbeat = time.monotonic()
                await self._subscribe_underlying(CONFIG.data.get("index_symbol", "NIFTY"))
                await self._subscribe_current_weekly(CONFIG.data.get("index_symbol", "NIFTY"))
                self._ensure_stream_reader()
                self._watchdog = asyncio.create_task(self._stream_watchdog(), name="upstox-stream-watchdog")
            else:
                await self._subscribe_underlying(CONFIG.data.get("index_symbol", "NIFTY"))
                await self._subscribe_current_weekly(CONFIG.data.get("index_symbol", "NIFTY"))
        finally:
            self._starting = False

    async def stop(self) -> None:
        self._stop.set()
        if self._stream_reader:
            self._stream_reader.cancel()
            try:
                await self._stream_reader
            except asyncio.CancelledError:
                pass
            self._stream_reader = None
        await self._tick_coalescer.stop()
        if self._subscriptions:
            self._emit_subscription_metrics(list(self._subscriptions), active=False)
            self._subscriptions.clear()
        self._underlying_tokens.clear()
        if self._watchdog:
            self._watchdog.cancel()
            try:
                await self._watchdog
            except asyncio.CancelledError:
                pass
            self._watchdog = None
        if self._stream:
            await self._stream.close()

    def static_ip_ok(self) -> bool:
        return bool(self._static_ip_ok)

    def is_streaming_alive(self, max_lag_seconds: float = 5.0) -> bool:
        if self._stop.is_set():
            return False
        if not self._stream:
            return False
        try:
            allowed_lag = max(float(max_lag_seconds), float(self._cfg.ws_heartbeat_interval))
        except Exception:
            allowed_lag = max_lag_seconds
        return (time.monotonic() - self._last_ws_heartbeat) <= max(allowed_lag, 0.0)

    async def submit_order(self, order: Order) -> BrokerOrderAck:
        if self._halt_new_orders and order.side.upper() == "BUY":
            raise BrokerError(code="auth_halt", message="Broker halted due to authentication state")
        instrument_symbol = await self._resolve_instrument_token(order)
        payload = upstox_client.PlaceOrderV3Request(
            instrument_token=instrument_symbol,
            transaction_type=order.side.upper(),
            order_type="LIMIT" if order.order_type == "IOC_LIMIT" else order.order_type,
            product=INTRADAY_PRODUCT,
            validity="IOC" if order.order_type == "IOC_LIMIT" else "DAY",
            quantity=int(order.qty),
            disclosed_quantity=0,
            trigger_price=0.0,
            price=order.limit_price or 0.0,
            is_amo=False,
            slice=False,
            tag=f"{order.strategy}:{order.client_order_id}",
        )
        context = {
            "symbol": instrument_symbol,
            "side": order.side,
            "qty": order.qty,
            "client_order_id": order.client_order_id,
        }
        def _place(session):
            algo_id = getattr(session.config, "algo_id", None)
            if algo_id:
                return session.order_api_v3.place_order(payload, algo_id=algo_id)
            return session.order_api_v3.place_order(payload)
        resp = await self._rest_call(
            "place",
            "submit_order",
            _place,
            context=context,
        )
        placed = normalize_place_order_response(resp)
        if not placed.success:
            raise BrokerError(code="submit_failed", message=placed.message or "place_order_failed")
        broker_id = str(placed.order_id or "")
        status = placed.status or "submitted"
        return BrokerOrderAck(broker_order_id=broker_id, status=status)

    async def replace_order(self, order: Order, *, price: Optional[float], qty: Optional[int]) -> None:
        instrument_symbol = await self._resolve_instrument_token(order)
        body = upstox_client.ModifyOrderV3Request(
            instrument_token=instrument_symbol,
            order_id=order.broker_order_id,
            transaction_type=order.side.upper(),
            order_type=order.order_type,
            product=INTRADAY_PRODUCT,
            validity="DAY",
            quantity=int(qty or order.qty),
            disclosed_quantity=0,
            trigger_price=0.0,
            price=price if price is not None else order.limit_price or 0.0,
        )
        context = {
            "broker_order_id": order.broker_order_id,
            "client_order_id": order.client_order_id,
            "qty": qty or order.qty,
        }
        await self._rest_call("modify", "replace_order", lambda session: session.order_api_v3.modify_order(body), context=context)

    async def cancel_order(self, order: Order) -> None:
        if not order.broker_order_id:
            return
        context = {"broker_order_id": order.broker_order_id, "client_order_id": order.client_order_id}
        await self._rest_call("cancel", "cancel_order", lambda session: session.order_api_v3.cancel_order(order.broker_order_id), context=context)

    async def fetch_open_orders(self) -> List[BrokerOrderView]:
        try:
            resp = await self._rest_call("history", "order_book", lambda session: session.order_api_v3.get_order_book())
        except BrokerError:
            return []
        data = resp.get("data") or []
        views: List[BrokerOrderView] = []
        for entry in data:
            status = str(entry.get("status") or "").lower()
            views.append(
                BrokerOrderView(
                    broker_order_id=str(entry.get("order_id")),
                    client_order_id=str(entry.get("client_order_id") or "") or None,
                    status=status,
                    filled_qty=int(entry.get("filled_quantity") or 0),
                    avg_price=float(entry.get("average_price") or 0.0),
                    instrument_key=str(entry.get("instrument_token") or entry.get("instrument_key") or "") or None,
                    side=str(entry.get("transaction_type") or entry.get("side") or ""),
                )
            )
        return views

    async def subscribe_marketdata(self, instruments: Iterable[Any]) -> None:
        normalized, overrides = self._normalize_subscription_inputs(instruments)
        ordered_tokens: list[str] = []
        new_set: set[str] = set()
        for token in normalized:
            clean = str(token or "").strip()
            if not clean or clean in new_set:
                continue
            ordered_tokens.append(clean)
            new_set.add(clean)
        must_subscribe = self._must_subscribe_keys()
        candidate_set = new_set | must_subscribe
        if self._stream_mode == "full_d30" and len(candidate_set) > FULL_D30_LIMIT_PER_USER:
            candidate_set = self._apply_subscription_cap(candidate_set, must_subscribe)
        ordered_kept: list[str] = []
        seen: set[str] = set()
        for token in ordered_tokens:
            if token in candidate_set and token not in seen:
                ordered_kept.append(token)
                seen.add(token)
        for token in must_subscribe:
            if token in candidate_set and token not in seen:
                ordered_kept.append(token)
                seen.add(token)
        for token in candidate_set:
            if token not in seen:
                ordered_kept.append(token)
                seen.add(token)
        removed = self._subscriptions - candidate_set
        self._subscriptions = candidate_set
        self._apply_subscription_metadata(candidate_set, overrides)
        self._emit_subscription_metrics(removed, active=False)
        self._emit_subscription_metrics(candidate_set, active=True)
        if not self._stream:
            return
        await self._subscribe_batches(ordered_kept, mode=self._stream_mode)
        if not self._watchdog and not self._starting:
            await self.start()
        self._last_ws_heartbeat = time.monotonic()

    async def ensure_position_subscription(self, symbol: str) -> None:
        key = self._resolve_instrument_key(symbol)
        if not key:
            return
        if key in self._subscriptions:
            return
        await self.subscribe_marketdata(list(self._subscriptions | {key}))

    # ------------------------------------------------------------------ helpers
    def _build_rate_limits(self) -> dict[str, TokenBucket]:
        limits = self._cfg.rate_limits
        return {
            "place": TokenBucket(limits.place.rate_per_sec, limits.place.burst),
            "modify": TokenBucket(limits.modify.rate_per_sec, limits.modify.burst),
            "cancel": TokenBucket(limits.cancel.rate_per_sec, limits.cancel.burst),
            "history": TokenBucket(limits.history.rate_per_sec, limits.history.burst),
        }

    def _normalize_subscription_inputs(self, instruments: Iterable[Any]) -> tuple[list[str], dict[str, dict[str, str]]]:
        normalized: list[str] = []
        overrides: dict[str, dict[str, str]] = {}
        for entry in instruments:
            token, meta = self._coerce_subscription_entry(entry)
            if not token:
                continue
            normalized.append(token)
            if meta:
                overrides[token] = meta
        return normalized, overrides

    def _must_subscribe_keys(self) -> set[str]:
        required = set()
        required.update(self._index_subscription_keys())
        required.update(self._position_subscription_keys())
        return required

    def _index_subscription_keys(self) -> set[str]:
        keys: set[str] = set()
        for symbol in ("NIFTY", "BANKNIFTY"):
            key = None
            if self.instrument_cache:
                try:
                    key = self.instrument_cache.resolve_index_key(symbol)
                except Exception:
                    key = None
            key = key or INDEX_INSTRUMENT_KEYS.get(symbol)
            if key:
                keys.add(str(key))
        return keys

    def _position_subscription_keys(self) -> set[str]:
        keys: set[str] = set()
        if not self._risk:
            return keys
        try:
            positions = self._risk.store.list_open_positions()
        except Exception:
            positions = []
        for pos in positions:
            symbol = str(pos.get("symbol") or "")
            key = self._resolve_instrument_key(symbol)
            if key:
                keys.add(key)
        return keys

    def _resolve_instrument_key(self, symbol: str) -> Optional[str]:
        if not symbol:
            return None
        if "|" in symbol:
            return symbol
        if self._resolver:
            try:
                meta = self._resolver.metadata_for(symbol)
            except Exception:
                meta = None
            if meta and getattr(meta, "instrument_key", None):
                return str(meta.instrument_key)
        parsed = self._parse_symbol(symbol)
        if parsed and self.instrument_cache:
            underlying, expiry, strike, opt_type = parsed
            try:
                return self.instrument_cache.lookup(underlying, expiry, strike, opt_type)
            except Exception:
                return None
        return None

    @staticmethod
    def _parse_symbol(symbol: str) -> Optional[tuple[str, str, float, str]]:
        parts = symbol.split("-")
        if len(parts) < 3:
            return None
        expiry = "-".join(parts[1:-1])
        tail = parts[-1]
        opt_type = tail[-2:].upper() if tail[-2:].upper() in {"CE", "PE"} else "CE"
        strike_part = tail[:-2] if opt_type in {"CE", "PE"} else tail
        try:
            strike = float(strike_part)
        except (TypeError, ValueError):
            return None
        return parts[0].upper(), expiry, strike, opt_type

    def _apply_subscription_cap(self, tokens: set[str], protected: set[str]) -> set[str]:
        if len(tokens) <= FULL_D30_LIMIT_PER_USER:
            return tokens
        if len(protected) >= FULL_D30_LIMIT_PER_USER:
            self._logger.warning("subscription_cap_exceeded", count=len(tokens), protected=len(protected))
            return set(list(protected)[:FULL_D30_LIMIT_PER_USER])
        keep_slots = FULL_D30_LIMIT_PER_USER - len(protected)
        candidates = [token for token in tokens if token not in protected]
        now = time.time()

        def _score(token: str) -> tuple[float, float]:
            meta = self._subscription_meta.get(token)
            if not meta:
                meta = self._subscription_metadata_for(token)
                self._subscription_meta[token] = meta
            symbol = str(meta.get("symbol") or "")
            strike_val = meta.get("strike")
            distance = float("inf")
            try:
                spot = self._last_spot.get(symbol.upper())
                if spot is not None and strike_val not in (None, ""):
                    distance = abs(float(strike_val) - float(spot))
            except (TypeError, ValueError):
                distance = float("inf")
            last_seen = self._subscription_last_seen.get(token, 0.0)
            age = now - last_seen if last_seen else float("inf")
            return (distance, age)

        candidates.sort(key=_score)
        kept = set(candidates[:keep_slots])
        return protected | kept

    def _record_subscription_seen(self, instrument_key: str, ts_seconds: Optional[float] = None) -> None:
        self._subscription_last_seen[instrument_key] = float(ts_seconds or time.time())

    async def _subscribe_batches(self, tokens: Sequence[str], *, mode: Optional[str] = None) -> None:
        if not self._stream or not tokens:
            return
        batch_size = 50
        stream_mode = mode or self._stream_mode
        for idx in range(0, len(tokens), batch_size):
            batch = list(tokens[idx : idx + batch_size])
            try:
                await self._stream.subscribe(batch, stream_mode)
            except TypeError:
                await self._stream.subscribe(batch)

    def _coerce_subscription_entry(self, entry: Any) -> tuple[str, dict[str, str]]:
        token = ""
        meta: dict[str, str] = {}
        if isinstance(entry, str):
            token = entry
        elif isinstance(entry, Mapping):
            raw_token = entry.get("instrument_key") or entry.get("instrument") or entry.get("symbol") or entry.get("key") or entry.get("token")
            token = str(raw_token or "")
            meta = {
                "symbol": str(entry.get("symbol") or entry.get("name") or ""),
                "expiry": str(entry.get("expiry") or ""),
                "opt_type": str(entry.get("opt_type") or entry.get("option_type") or ""),
                "strike": str(entry.get("strike") or ""),
            }
        else:
            raw_token = getattr(entry, "instrument_key", None) or getattr(entry, "instrument", None) or getattr(entry, "symbol", None)
            token = str(raw_token or "")
            meta = {
                "symbol": str(getattr(entry, "symbol", "") or getattr(entry, "name", "")),
                "expiry": str(getattr(entry, "expiry", "")),
                "opt_type": str(getattr(entry, "opt_type", "") or getattr(entry, "option_type", "")),
                "strike": str(getattr(entry, "strike", "")),
            }
        sanitized = {k: v for k, v in meta.items() if v not in ("", None)}
        return token, sanitized

    def _apply_subscription_metadata(self, tokens: set[str], overrides: dict[str, dict[str, str]]) -> None:
        for token in tokens:
            hints = overrides.get(token)
            if token not in self._subscription_meta:
                self._subscription_meta[token] = self._subscription_metadata_for(token, hints)
            elif hints:
                current = self._subscription_meta[token]
                for key, value in hints.items():
                    if value:
                        current[key] = value

    def _subscription_metadata_for(self, instrument_key: str, hints: Optional[dict[str, str]] = None) -> dict[str, str]:
        details = {"symbol": "", "expiry": "", "opt_type": "", "strike": ""}
        if hints:
            for key in details:
                if hints.get(key):
                    details[key] = str(hints[key])
        cache = self.instrument_cache or InstrumentCache.runtime_cache()
        if cache:
            cache_symbol, cache_expiry, cache_strike, cache_opt = cache.labels_for_instrument(instrument_key)
            details["symbol"] = details["symbol"] or cache_symbol
            details["expiry"] = details["expiry"] or cache_expiry
            details["opt_type"] = details["opt_type"] or cache_opt
            if cache_strike:
                details["strike"] = details["strike"] or str(cache_strike)
        if self._resolver:
            cache_meta = self._resolver.metadata_for_key(instrument_key)
            if cache_meta:
                details["symbol"] = details["symbol"] or (cache_meta.symbol or "")
                details["expiry"] = details["expiry"] or (cache_meta.expiry_date or "")
                if cache_meta.option_type:
                    details["opt_type"] = details["opt_type"] or cache_meta.option_type
                if cache_meta.strike is not None:
                    strike_val: Any = cache_meta.strike
                    try:
                        strike_float = float(strike_val)
                        if strike_float.is_integer():
                            strike_val = int(strike_float)
                        else:
                            strike_val = strike_float
                    except (TypeError, ValueError):
                        strike_val = cache_meta.strike
                    details["strike"] = details["strike"] or str(strike_val)
        return {k: str(v or "") for k, v in details.items()}

    def _emit_subscription_metrics(self, tokens: Iterable[str], *, active: bool) -> None:
        if not self._metrics:
            return
        for token in tokens:
            clean = str(token or "").strip()
            if not clean:
                continue
            details = self._subscription_meta.get(clean)
            if not details and active:
                details = self._subscription_metadata_for(clean)
                self._subscription_meta[clean] = details
            elif not details:
                details = self._subscription_metadata_for(clean)
            symbol = details.get("symbol", "") or ""
            expiry = details.get("expiry", "") or ""
            opt = details.get("opt_type", "") or ""
            strike_text = str(details.get("strike", "") or "")
            set_md_subscription(clean, symbol, expiry, opt, strike_text, active)
            if active:
                publish_subscription_info(clean, symbol=symbol, expiry=expiry, strike=strike_text, opt=opt, tag="live")
            else:
                clear_subscription_info(clean, symbol, expiry, strike_text, opt, tag="live")
                self._subscription_meta.pop(clean, None)

    def record_subscription_expiry(self, symbol: str, expiry: str, mode: str) -> None:
        """Expose subscription expiry selection to metrics."""

        if not self._metrics:
            return
        try:
            set_subscription_expiry(symbol, expiry, mode)
        except Exception:
            pass

    def _ensure_stream_client(self) -> None:
        if self._stream is not None:
            return
        token = getattr(getattr(self._session, "config", None), "access_token", None)
        if not token:
            raise RuntimeError("Access token unavailable for market data stream")
        client = _AsyncMarketDataStream(str(token), mode=self._stream_mode)
        self._stream = client
        self.ws = client.ws

    def _ensure_stream_reader(self) -> None:
        if self._stream_reader or not self._stream:
            return
        self._stream_reader = asyncio.create_task(self._drain_market_stream(), name="upstox-market-stream")

    async def _drain_market_stream(self) -> None:
        while not self._stop.is_set():
            if self._metrics:
                try:
                    self._metrics.ws_lag_ms.set(max(0.0, (time.time() - self._last_ws_recv) * 1000.0))
                except Exception:
                    pass
            if not self._stream:
                await asyncio.sleep(0.5)
                continue
            recv_fn = getattr(self._stream, "recv", None)
            if not callable(recv_fn):
                await asyncio.sleep(1.0)
                continue
            try:
                frame = await recv_fn(timeout=1.0)
            except Exception:
                await asyncio.sleep(0.25)
                continue
            if not frame:
                continue
            self._last_ws_recv = time.time()
            try:
                record_md_frame(len(str(frame)))
            except Exception:
                pass
            try:
                self._handle_feed_response(frame)
            except Exception as exc:
                self._logger.exception("Failed to decode market data frame: %s", exc)

    def _handle_feed_response(self, frame: Mapping[str, Any]) -> None:
        feeds = frame.get("feeds") if isinstance(frame, Mapping) else None
        if not isinstance(feeds, Mapping):
            return
        frame_ts = self._extract_ts_seconds(frame.get("currentTs")) or None
        for key, payload in feeds.items():
            self._handle_feed_entry(str(key), payload or {}, frame_ts)

    def _option_labels_for(self, instrument_key: str, payload: Mapping[str, Any]) -> tuple[bool, str, str, int, str]:
        cache_symbol = cache_expiry = cache_opt = ""
        cache_strike = 0
        cache = self.instrument_cache or InstrumentCache.runtime_cache()
        is_option = False
        if cache and self.instrument_cache is None:
            self.instrument_cache = cache
        if cache:
            try:
                is_option = cache.is_option_key(instrument_key)
                if is_option:
                    labels = cache.labels_for_key(instrument_key) or cache.labels_for_instrument(instrument_key)
                    if labels:
                        cache_symbol, cache_expiry, cache_strike, cache_opt = labels
            except Exception:
                is_option = False
        meta_hint = {
            "symbol": str(payload.get("symbol") or payload.get("name") or ""),
            "expiry": str(payload.get("expiry") or ""),
            "opt_type": str(payload.get("opt_type") or payload.get("option_type") or ""),
            "strike": payload.get("strike") or cache_strike,
        }
        cached = self._subscription_meta.get(instrument_key)
        hints = {k: str(v) for k, v in meta_hint.items() if v not in ("", None, 0)}
        if not cached:
            cached = self._subscription_metadata_for(instrument_key, hints)
            self._subscription_meta[instrument_key] = cached
        else:
            for name, value in hints.items():
                if value and not cached.get(name):
                    cached[name] = value
        symbol = meta_hint.get("symbol") or cached.get("symbol") or cache_symbol
        expiry = meta_hint.get("expiry") or cached.get("expiry") or cache_expiry
        opt_type = (meta_hint.get("opt_type") or cached.get("opt_type") or cache_opt).upper()
        strike_val = meta_hint.get("strike") or cached.get("strike") or cache_strike
        try:
            strike_int = int(float(strike_val))
        except (TypeError, ValueError):
            strike_int = cache_strike
        if opt_type:
            is_option = True
        return is_option, symbol, expiry, strike_int, opt_type

    def _handle_feed_entry(self, instrument_key: str, payload: Mapping[str, Any], frame_ts: Optional[float]) -> None:
        ltpc = payload.get("ltpc") if isinstance(payload, Mapping) else None
        if not isinstance(ltpc, Mapping):
            ltpc = {}
        full_feed = payload.get("fullFeed") if isinstance(payload, Mapping) else None
        full_feed = full_feed or {}
        if not isinstance(full_feed, Mapping):
            full_feed = {}
        market_ff = full_feed.get("marketFF") or full_feed.get("marketFf") or full_feed.get("market_ff") or {}
        index_ff = full_feed.get("indexFF") or full_feed.get("indexFf") or full_feed.get("index_ff") or {}
        is_option, symbol, expiry, strike, opt_type = self._option_labels_for(instrument_key, payload)
        if not is_option and market_ff:
            is_option = True
        if is_option:
            ts_seconds = (
                self._extract_ts_seconds((market_ff.get("ltpc") or {}).get("ltt"))
                or self._extract_ts_seconds(ltpc.get("ltt"))
                or frame_ts
                or time.time()
            )
            ltp = self._coerce_float((market_ff.get("ltpc") or {}).get("ltp") or ltpc.get("ltp"))
            depth_entries = (market_ff.get("marketLevel") or {}).get("bidAskQuote") or []
            bids: list[dict[str, Any]] = []
            asks: list[dict[str, Any]] = []
            if isinstance(depth_entries, list):
                for entry in depth_entries:
                    if not isinstance(entry, Mapping):
                        continue
                    bid_p = self._coerce_float(entry.get("bidP"))
                    bid_q = self._coerce_float(entry.get("bidQ"))
                    ask_p = self._coerce_float(entry.get("askP"))
                    ask_q = self._coerce_float(entry.get("askQ"))
                    if bid_p is not None or bid_q is not None:
                        bids.append({"price": bid_p, "qty": bid_q})
                    if ask_p is not None or ask_q is not None:
                        asks.append({"price": ask_p, "qty": ask_q})
            if not bids and not asks:
                first_depth = (full_feed.get("firstLevelWithGreeks") or {}).get("firstDepth") or {}
                if isinstance(first_depth, Mapping):
                    bid_p = self._coerce_float(first_depth.get("bidP"))
                    bid_q = self._coerce_float(first_depth.get("bidQ"))
                    ask_p = self._coerce_float(first_depth.get("askP"))
                    ask_q = self._coerce_float(first_depth.get("askQ"))
                    if bid_p is not None or bid_q is not None:
                        bids.append({"price": bid_p, "qty": bid_q})
                    if ask_p is not None or ask_q is not None:
                        asks.append({"price": ask_p, "qty": ask_q})
            best_bid = bids[0]["price"] if bids else None
            best_ask = asks[0]["price"] if asks else None
            oi = self._coerce_float(market_ff.get("oi") or payload.get("oi"))
            iv = self._coerce_float(market_ff.get("iv") or payload.get("iv"))
            record_tick_seen(instrument_key=instrument_key, underlying=symbol or self._subscription_meta.get(instrument_key, {}).get("symbol"), ts_seconds=ts_seconds)
            if symbol and expiry and strike and opt_type:
                try:
                    option_symbol = f"{symbol}-{expiry}-{int(strike)}{opt_type}"
                    record_tick_seen(instrument_key=option_symbol, ts_seconds=ts_seconds)
                except Exception:
                    pass
            self._record_subscription_seen(instrument_key, ts_seconds)
            stale = False
            if self._max_tick_age > 0:
                stale = (time.time() - float(ts_seconds or time.time())) > self._max_tick_age
                try:
                    set_market_data_stale(instrument_key, 1 if stale else 0)
                except Exception:
                    pass
                if stale:
                    try:
                        inc_market_data_stale_drop(instrument_key)
                    except Exception:
                        pass
            else:
                try:
                    set_market_data_stale(instrument_key, 0)
                except Exception:
                    pass
            set_last_tick_ts(instrument_key, ts_seconds)
            publish_spread(instrument_key, best_bid or 0.0, best_ask or 0.0, ltp or 0.0)
            if bids or asks:
                bid_qty10 = sum(self._coerce_float(entry.get("qty")) or 0.0 for entry in bids[:10] if isinstance(entry, Mapping))
                ask_qty10 = sum(self._coerce_float(entry.get("qty")) or 0.0 for entry in asks[:10] if isinstance(entry, Mapping))
                if bid_qty10 or ask_qty10:
                    publish_depth10(instrument_key, bid_qty10, ask_qty10)
            if stale:
                return
            self._tick_coalescer.update_option(
                instrument_key,
                {
                    "symbol": symbol or self._subscription_meta.get(instrument_key, {}).get("symbol", ""),
                    "expiry": expiry,
                    "opt": opt_type,
                    "strike": strike,
                    "ltp": ltp,
                    "bid": best_bid,
                    "ask": best_ask,
                    "iv": iv,
                    "oi": oi,
                    "ts": ts_seconds or time.time(),
                    "depth": {"bids": bids, "asks": asks},
                },
            )
            if self._bus:
                try:
                    ts_dt = dt.datetime.fromtimestamp(ts_seconds or time.time(), tz=IST)
                except Exception:
                    ts_dt = dt.datetime.now(IST)
                option_symbol = ""
                if symbol and expiry and strike:
                    option_symbol = f"{symbol}-{expiry}-{strike}{opt_type or ''}"
                payload_evt = {
                    "instrument_key": instrument_key,
                    "underlying": symbol or "",
                    "symbol": option_symbol or symbol or instrument_key,
                    "ltp": ltp,
                    "bid": best_bid,
                    "ask": best_ask,
                    "expiry": expiry,
                    "strike": strike,
                    "opt_type": opt_type,
                    "iv": iv,
                    "oi": oi,
                    "depth": {"bids": bids, "asks": asks},
                    "ts": ts_dt.isoformat(),
                }
                asyncio.create_task(self._bus.publish("market/events", {"ts": ts_dt.isoformat(), "type": "tick", "payload": payload_evt}))
            return
        ts_seconds = (
            self._extract_ts_seconds((index_ff.get("ltpc") or {}).get("ltt"))
            or self._extract_ts_seconds(ltpc.get("ltt"))
            or frame_ts
            or time.time()
        )
        ltp = self._coerce_float((index_ff.get("ltpc") or {}).get("ltp") or (market_ff.get("ltpc") or {}).get("ltp") or ltpc.get("ltp"))
        if ltp is None:
            return
        sym = self._underlying_symbol_for(instrument_key, payload)
        self._last_spot[sym.upper()] = ltp
        record_tick_seen(instrument_key=instrument_key, underlying=sym, ts_seconds=ts_seconds)
        self._record_subscription_seen(instrument_key, ts_seconds)
        stale = False
        if self._max_tick_age > 0:
            stale = (time.time() - float(ts_seconds or time.time())) > self._max_tick_age
            try:
                set_market_data_stale(instrument_key, 1 if stale else 0)
            except Exception:
                pass
            if stale:
                try:
                    inc_market_data_stale_drop(instrument_key)
                except Exception:
                    pass
        else:
            try:
                set_market_data_stale(instrument_key, 0)
            except Exception:
                pass
        self._tick_coalescer.update_underlying(sym, ltp, ts_seconds)
        try:
            set_underlying_last_ts(sym, ts_seconds)
        except Exception:
            pass
        if stale:
            return
        if self._bus:
            try:
                ts_dt = dt.datetime.fromtimestamp(ts_seconds or time.time(), tz=IST)
            except Exception:
                ts_dt = dt.datetime.now(IST)
            payload_evt = {
                "instrument_key": instrument_key,
                "underlying": sym,
                "symbol": sym,
                "ltp": ltp,
                "bid": None,
                "ask": None,
                "ts": ts_dt.isoformat(),
            }
            asyncio.create_task(self._bus.publish("market/events", {"ts": ts_dt.isoformat(), "type": "tick", "payload": payload_evt}))

    def _resolve_underlying_key(self, symbol: str) -> Optional[str]:
        cache = self.instrument_cache or InstrumentCache.runtime_cache()
        if cache is None:
            return None
        try:
            return cache.resolve_index_key(symbol)
        except Exception:
            return None

    async def _resolve_instrument_token(self, order: Order) -> str:
        instrument_symbol = order.symbol
        if self._resolver:
            try:
                instrument_symbol = await self._resolver.resolve_symbol(order.symbol)
            except Exception:
                instrument_symbol = order.symbol
        return instrument_symbol

    async def _subscribe_underlying(self, symbol: str) -> None:
        key = self._resolve_underlying_key(symbol)
        if not key:
            return
        if not self._stream:
            self._ensure_stream_client()
        if not self._stream:
            return
        await self._subscribe_batches([key], mode=self._stream_mode)
        self._underlying_tokens.add(key)

    def _underlying_symbol_for(self, instrument_key: str, payload: Mapping[str, Any]) -> str:
        sym = str(payload.get("symbol") or payload.get("underlying") or payload.get("name") or "").upper()
        if sym in {"NIFTY", "BANKNIFTY"}:
            return sym
        cache = self.instrument_cache or InstrumentCache.runtime_cache()
        if cache:
            try:
                for candidate in ("NIFTY", "BANKNIFTY"):
                    try_key = cache.resolve_index_key(candidate)
                    if try_key == instrument_key:
                        return candidate
            except Exception:
                pass
        if "Nifty Bank" in instrument_key:
            return "BANKNIFTY"
        if "Nifty" in instrument_key:
            return "NIFTY"
        return sym or instrument_key

    def record_market_tick(self, instrument_key: str, payload: dict[str, Any]) -> None:
        key = str(instrument_key or "").strip()
        if not key:
            return

        def _pluck(*names: str) -> Any:
            if isinstance(payload, Mapping):
                for name in names:
                    if name in payload:
                        return payload.get(name)
            for name in names:
                value = getattr(payload, name, None)
                if value is not None:
                    return value
            return None
        cache_symbol = cache_expiry = cache_opt = ""
        cache_strike = 0
        cache = self.instrument_cache or InstrumentCache.runtime_cache()
        if cache and self.instrument_cache is None:
            self.instrument_cache = cache
        is_option = False
        if cache:
            try:
                is_option = cache.is_option_key(key)
                if is_option:
                    labels = cache.labels_for_key(key) or cache.labels_for_instrument(key)
                    if labels:
                        cache_symbol, cache_expiry, cache_strike, cache_opt = labels
            except Exception:
                is_option = False

        if is_option:
            meta_hint = {
                "symbol": _pluck("symbol", "name") or cache_symbol,
                "expiry": _pluck("expiry") or cache_expiry,
                "opt_type": (_pluck("opt_type", "option_type") or cache_opt or "").upper(),
                "strike": _pluck("strike") or cache_strike,
            }
            cached = self._subscription_meta.get(key)
            if not cached:
                hints = {k: str(v) for k, v in meta_hint.items() if v not in (None, "", 0)}
                cached = self._subscription_metadata_for(key, hints)
                self._subscription_meta[key] = cached
            else:
                for name, value in meta_hint.items():
                    if value not in (None, "", 0) and not cached.get(name):
                        cached[name] = str(value)
            symbol = meta_hint.get("symbol") or cached.get("symbol") or key
            expiry = meta_hint.get("expiry") or cached.get("expiry") or ""
            opt_type = (meta_hint.get("opt_type") or cached.get("opt_type") or "").upper()
            strike_val: Any = meta_hint.get("strike") or cached.get("strike") or cache_strike or ""
            try:
                strike_val = int(float(strike_val))
            except (TypeError, ValueError):
                strike_val = strike_val or ""
            ltp = self._coerce_float(_pluck("last_price", "ltp", "price"))
            bid = None
            for field in ("best_bid", "best_bid_price", "bid", "bidP", "bid_price", "buy_price"):
                bid = self._coerce_float(_pluck(field))
                if bid is not None:
                    break
            depth = _pluck("depth") or {}
            if bid is None and isinstance(depth, Mapping):
                bid = self._coerce_float(depth.get("buy_price") or depth.get("buy_price_1"))
            ask = None
            for field in ("best_ask", "best_ask_price", "ask", "askP", "ask_price", "sell_price"):
                ask = self._coerce_float(_pluck(field))
                if ask is not None:
                    break
            if ask is None and isinstance(depth, Mapping):
                ask = self._coerce_float(depth.get("sell_price") or depth.get("sell_price_1"))
            greeks = _pluck("optionGreeks", "greeks") or {}
            if not isinstance(greeks, Mapping):
                greeks = {}
            iv = self._coerce_float(_pluck("implied_volatility", "iv") or greeks.get("iv"))
            oi = self._coerce_float(_pluck("oi", "open_interest"))
            ts_seconds = (
                self._extract_ts_seconds(_pluck("exchange_ts"))
                or self._extract_ts_seconds(_pluck("timestamp"))
                or self._extract_ts_seconds(_pluck("ts"))
                or time.time()
            )
            record_tick_seen(instrument_key=key, underlying=symbol, ts_seconds=ts_seconds)
            try:
                if symbol and expiry and strike_val not in ("", None) and opt_type:
                    option_symbol = f"{symbol}-{expiry}-{int(float(strike_val))}{opt_type}"
                    record_tick_seen(instrument_key=option_symbol, ts_seconds=ts_seconds)
            except Exception:
                pass
            self._record_subscription_seen(key, ts_seconds)
            try:
                set_market_data_stale(key, 0)
            except Exception:
                pass
            set_last_tick_ts(key, ts_seconds)
            publish_option_quote(
                key,
                str(symbol),
                str(expiry),
                str(opt_type),
                strike_val,
                ltp=ltp,
                bid=bid,
                ask=ask,
                iv=iv,
                oi=oi,
                ts=ts_seconds,
            )
            return

        # Underlying tick
        spot = self._coerce_float(_pluck("ltp", "last_price", "price", "close"))
        if spot is None:
            return
        ts_seconds = (
            self._extract_ts_seconds(_pluck("exchange_ts"))
            or self._extract_ts_seconds(_pluck("timestamp"))
            or self._extract_ts_seconds(_pluck("ts"))
            or time.time()
        )
        sym = self._underlying_symbol_for(key, payload)
        record_tick_seen(instrument_key=key, underlying=sym, ts_seconds=ts_seconds)
        self._record_subscription_seen(key, ts_seconds)
        try:
            set_market_data_stale(key, 0)
        except Exception:
            pass
        publish_underlying(sym, spot, ts_seconds)

    @staticmethod
    def _coerce_float(value: Any) -> Optional[float]:
        if value is None:
            return None
        try:
            return float(value)
        except (TypeError, ValueError):
            return None

    @staticmethod
    def _extract_ts_seconds(value: Any) -> Optional[float]:
        if value is None:
            return None
        if isinstance(value, (int, float)):
            return float(value)
        if isinstance(value, str):
            text = value.strip()
            if not text:
                return None
            try:
                return float(text)
            except ValueError:
                try:
                    parsed = dt.datetime.fromisoformat(text)
                except ValueError:
                    return None
                return parsed.timestamp()
        return None

    async def _rest_call(self, endpoint: str, action: str, fn: Callable[[UpstoxSession], Any], *, context: Optional[dict[str, Any]] = None) -> Any:
        retries = 3
        backoff = 0.5
        last_exc: Optional[Exception] = None
        sanitized_context = self._sanitize_context(context)
        for attempt in range(1, retries + 1):
            tokens_left = await self._acquire_token(endpoint)
            if self._metrics:
                self._metrics.set_ratelimit_tokens(endpoint, tokens_left)
            try:
                result = await self._invoke_upstox(action, fn, context=sanitized_context)
                return result
            except InvalidDateError as exc:
                raise
            except asyncio.TimeoutError as exc:
                last_exc = exc
                if attempt < retries:
                    await self._record_retry(endpoint)
                    await asyncio.sleep(backoff + random.uniform(0, backoff))
                    backoff *= 2
                    continue
                break
            except ApiException as exc:
                last_exc = exc
                if exc.status == 401:
                    await self._handle_auth_failure(sanitized_context)
                    raise BrokerError(code="auth", message=f"{action} returned 401", status=401, context=sanitized_context) from exc
                if self._is_insufficient_funds_error(exc):
                    raise BrokerError(code="insufficient_funds", message=str(exc), status=exc.status, context=sanitized_context) from exc
                if self._should_retry(exc.status) and attempt < retries:
                    await self._record_retry(endpoint)
                    await asyncio.sleep(backoff + random.uniform(0, backoff))
                    backoff *= 2
                    continue
                raise BrokerError(code="api_error", message=str(exc), status=exc.status) from exc
            except Exception as exc:
                last_exc = exc
                if attempt < retries:
                    await self._record_retry(endpoint)
                    await asyncio.sleep(backoff + random.uniform(0, backoff))
                    backoff *= 2
                    continue
                break
        raise BrokerError(code="unknown", message=str(last_exc)) from last_exc

    async def _invoke_upstox(self, action: str, fn: Callable[[UpstoxSession], Any], *, context: Optional[dict[str, Any]] = None) -> Any:
        try:
            return await asyncio.wait_for(asyncio.to_thread(fn, self._session), timeout=self._cfg.rest_timeout)
        except ApiException as exc:
            if self._is_invalid_date_error(exc):
                self._handle_invalid_date(action, exc, context)
                raise InvalidDateError("UDAPI1088 Invalid date") from exc
            raise

    @staticmethod
    def _is_invalid_date_error(exc: ApiException) -> bool:
        body = getattr(exc, "body", "") or ""
        if isinstance(body, bytes):
            body_text = body.decode("utf-8", errors="ignore")
        else:
            body_text = str(body)
        text = body_text or str(exc)
        return "UDAPI1088" in text or "Invalid date" in text

    @staticmethod
    def _is_insufficient_funds_error(exc: ApiException) -> bool:
        body = getattr(exc, "body", "") or ""
        if isinstance(body, bytes):
            text = body.decode("utf-8", errors="ignore").lower()
        else:
            text = str(body).lower()
        combined = f"{text} {str(exc).lower()}"
        tokens = ("insufficient", "add rs", "add funds", "add money", "not enough balance", "insufficient margin")
        return any(tok in combined for tok in tokens)

    def _handle_invalid_date(self, action: str, exc: ApiException, context: Optional[dict[str, Any]]) -> None:
        if self._metrics:
            self._metrics.inc_api_error("UDAPI1088")
            self._metrics.inc_orders_rejected("invalid_date")
        body = self._compact_context(action, context)
        notify_incident("ERROR", "UDAPI1088 Invalid date", body, tags=["invalid_date"])

    async def _acquire_token(self, endpoint: str) -> float:
        bucket = self._rate_limits.get(endpoint)
        if bucket is None:
            return 0.0
        async with self._queue_lock:
            self._pending_requests[endpoint] += 1
            self._update_queue_metric(endpoint)
        try:
            tokens_left = await bucket.acquire()
        finally:
            async with self._queue_lock:
                self._pending_requests[endpoint] = max(0, self._pending_requests[endpoint] - 1)
                self._update_queue_metric(endpoint)
        return tokens_left

    def _update_queue_metric(self, endpoint: str) -> None:
        if not self._metrics:
            return
        depth = self._pending_requests.get(endpoint, 0)
        self._metrics.set_broker_queue_depth(endpoint, depth)

    def _sanitize_context(self, context: Optional[dict[str, Any]]) -> dict[str, Any]:
        if not context:
            return {}
        sanitized: dict[str, Any] = {}
        for key, value in context.items():
            if value is None:
                continue
            sanitized[key] = value
        for key, value in list(sanitized.items()):
            lowered = key.lower()
            if any(token in lowered for token in ("date", "expiry")):
                try:
                    sanitized[key] = normalize_date(value)
                except ValueError:
                    sanitized[key] = str(value)
        return sanitized

    def _compact_context(self, action: str, context: Optional[dict[str, Any]]) -> str:
        parts = [f"action={action}"]
        if context:
            for key, value in context.items():
                parts.append(f"{key}={value}")
        return " ".join(parts)

    async def _record_retry(self, endpoint: str) -> None:
        if self._metrics:
            self._metrics.rest_retries_total.labels(endpoint=endpoint).inc()

    @staticmethod
    def _should_retry(status: Optional[int]) -> bool:
        if status is None:
            return True
        if status in {408, 425, 429}:
            return True
        return 500 <= status < 600

    async def _refresh_token(self) -> None:
        if not self._token_refresh_cb:
            return
        token = self._token_refresh_cb()
        if asyncio.iscoroutine(token):
            token = await token
        self._session = self._session_factory(str(token))

    async def _stream_watchdog(self) -> None:
        backoffs = self._cfg.ws_backoff_seconds or (1, 2, 5, 10)
        failures = 0
        while not self._stop.is_set():
            if not self._stream:
                await asyncio.sleep(self._cfg.ws_heartbeat_interval)
                continue
            try:
                await asyncio.wait_for(self._stream.heartbeat(), timeout=self._cfg.ws_heartbeat_interval)  # type: ignore[arg-type]
                self._last_ws_heartbeat = time.monotonic()
                await asyncio.sleep(self._cfg.ws_heartbeat_interval)
                failures = 0
            except Exception:
                failures += 1
                idx = min(failures - 1, len(backoffs) - 1)
                delay = backoffs[idx]
                await asyncio.sleep(delay + random.uniform(0, delay))
                if self._ws_failure_cb and failures >= len(backoffs):
                    await self._notify_ws_failure(failures)
                try:
                    await self._stream.close()  # type: ignore[union-attr]
                except Exception:
                    pass
                await self._stream.connect()  # type: ignore[union-attr]
                self._ensure_stream_reader()
                await self._subscribe_underlying(CONFIG.data.get("index_symbol", "NIFTY"))
                if self._subscriptions:
                    await self._subscribe_batches(list(self._subscriptions), mode=self._stream_mode)
                await self._subscribe_current_weekly(CONFIG.data.get("index_symbol", "NIFTY"))
                if self._metrics:
                    self._metrics.ws_reconnects_total.inc()
                if self._reconcile_cb:
                    await self._reconcile_cb()

    async def _run_preopen_probe(self) -> None:
        if self._preopen_checked:
            return
        self._logger.info("Pre-open expiry probe starting")
        try:
            await asyncio.to_thread(preopen_expiry_smoke)
        except Exception as exc:  # pragma: no cover - network dependent
            self._logger.exception("Pre-open expiry probe FAILED: %s", exc)
            if self._metrics:
                self._metrics.set_risk_halt_state(1)
            if self._auth_halt_cb:
                result = self._auth_halt_cb("EXPIRY_DISCOVERY_FAILED")
                if asyncio.iscoroutine(result):
                    await result
        else:
            self._logger.info("Pre-open expiry probe OK")
        finally:
            self._preopen_checked = True

    async def _apply_session_guard(self) -> bool:
        if not self._risk:
            self._session_guard_decision = {"action": "continue", "reason": "LIVE", "state": 1, "positions": 0}
            set_session_state(1)
            return True
        now = self._risk.session_guard_now()
        positions_count = self._risk.open_position_count()
        decision = self._risk.evaluate_boot_state(now, positions_count > 0)
        state_value = self._SESSION_STATE_MAP.get(decision.get("reason", "").upper(), 4)
        set_session_state(state_value)
        payload = dict(decision)
        payload["state"] = state_value
        payload["positions"] = positions_count
        self._session_guard_decision = payload
        action = decision.get("action")
        reason = decision.get("reason", "UNKNOWN")
        if action == "continue":
            if self._metrics:
                self._metrics.set_risk_halt_state(0)
            return True
        if self._metrics:
            self._metrics.set_risk_halt_state(1)
        if action == "halt":
            self._risk.halt_new_entries(reason)
            if self._auth_halt_cb:
                result = self._auth_halt_cb(reason)
                if asyncio.iscoroutine(result):
                    await result
            self._logger.warning("SessionGuard: HALT (%s)", reason)
            return False
        if action == "squareoff_then_halt":
            if self._square_off_cb:
                await self._square_off_cb("SESSION_END")
            else:
                self._logger.warning("SessionGuard: square-off callback not bound; skipping exits")
            self._risk.halt_new_entries("POST_CLOSE")
            if self._auth_halt_cb:
                result = self._auth_halt_cb("POST_CLOSE")
                if asyncio.iscoroutine(result):
                    await result
            self._logger.warning("SessionGuard: squared-off and HALTED (post-close)")
            return False
        if action == "shutdown":
            self._logger.warning("SessionGuard: shutting down post-close (per config)")
            if self._shutdown_cb:
                await self._shutdown_cb("POST_CLOSE_SHUTDOWN")
            return False
        return True

    @staticmethod
    def _default_session_factory(token: Optional[str]) -> UpstoxSession:
        algo_id = os.getenv("UPSTOX_ALGO_ID")
        cfg = UpstoxConfig(access_token=token, sandbox=False, algo_id=algo_id) if token else None  # type: ignore[arg-type]
        return UpstoxSession(cfg)

    async def _notify_ws_failure(self, failures: int) -> None:
        if not self._ws_failure_cb:
            return
        cb = self._ws_failure_cb
        result = cb(failures)
        if asyncio.iscoroutine(result):
            await result

    async def _handle_auth_failure(self, context: Optional[dict[str, Any]]) -> None:
        if self._metrics:
            self._metrics.http_401_total.inc()
        if self._halt_new_orders:
            return
        body = self._compact_context("401", context)
        await self._halt_and_notify("AUTH_401", body, level="ERROR")

    async def _halt_and_notify(self, reason: str, detail: str, level: str = "ERROR") -> None:
        self._halt_new_orders = True
        self._halt_reason = reason
        if self._metrics:
            self._metrics.set_risk_halt_state(1)
        self._logger.log_event(40, "broker_halt", reason=reason, detail=detail)
        notify_incident(level.upper(), reason, detail)
        cb = self._auth_halt_cb
        if cb:
            result = cb(reason)
            if asyncio.iscoroutine(result):
                await result

    def resume_trading(self) -> None:
        self._halt_new_orders = False
        self._halt_reason = None
        if self._metrics:
            self._metrics.set_risk_halt_state(0)

    def halt_reason(self) -> Optional[str]:
        return self._halt_reason

    def bind_reconcile_callback(self, callback: Callable[[], Awaitable[None]]) -> None:
        self._reconcile_cb = callback

    def bind_square_off_callback(self, callback: Callable[[str], Awaitable[None]]) -> None:
        self._square_off_cb = callback

    def bind_shutdown_callback(self, callback: Callable[[str], Awaitable[None]]) -> None:
        self._shutdown_cb = callback

    @property
    def session_guard_decision(self) -> Optional[dict[str, Any]]:
        return self._session_guard_decision


__all__ = ["BrokerError", "INTRADAY_PRODUCT", "MarketStreamClient", "TokenBucket", "FullD30Streamer", "UpstoxBroker"]
