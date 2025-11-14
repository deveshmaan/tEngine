
"""
buy_engine_ui.py — Intraday BUY options engine (NIFTY/BANKNIFTY) with metrics.
Python 3.12.11
"""
from __future__ import annotations

import os
import time
import math
import json
import queue
import signal
import socket
import threading
import dataclasses
from dataclasses import dataclass
from typing import Optional, Dict, Any, List, Tuple

import numpy as np
import pandas as pd

from prometheus_client import CollectorRegistry, Counter, Gauge, Histogram, Summary, start_http_server

# --- Upstox SDK ---
from upstox_api.api import Upstox, Session, LiveFeedType, TransactionType, OrderType, ProductType, DurationType


@dataclass
class EngineConfig:
    api_key: str
    access_token: str
    symbol_root: str = "NIFTY"         # or "BANKNIFTY"
    exchange: str = "NSE_FO"
    product: ProductType = ProductType.Intraday
    order_type: OrderType = OrderType.Market
    max_slippage_pct: float = 0.0025   # 0.25% guardrail
    ivp_window: int = 252              # trading days
    ivp_min: float = 0.30              # require IV percentile >= 30% to engage
    qty_lots: int = 1                  # lots per signal
    lot_size: int = 50                 # adjust per instrument (NIFTY=50 as of 2024)
    metrics_port: int = 9103           # Prometheus exporter port


class PromMetrics:
    def __init__(self, port: int):
        self.registry = CollectorRegistry()
        self.signals = Counter("buy_signals_total", "Signals emitted", registry=self.registry)
        self.orders = Counter("orders_submitted_total", "Orders submitted", ["symbol"], registry=self.registry)
        self.fills = Counter("orders_filled_total", "Orders filled", ["symbol"], registry=self.registry)
        self.rejects = Counter("orders_rejected_total", "Orders rejected", ["symbol"], registry=self.registry)
        self.pnl = Gauge("pnl_net_rupees", "Unrealized + realized PnL (₹)", registry=self.registry)
        self.margin_avail = Gauge("margin_available_rupees", "Available margin (₹)", registry=self.registry)
        self.last_latency_ms = Gauge("last_execution_latency_ms", "Last order latency (ms)", registry=self.registry)
        self.ltp = Gauge("ltp_underlying", "Underlying last traded price", ["symbol"], registry=self.registry)
        self.iv = Gauge("option_iv", "Option implied volatility", ["symbol"], registry=self.registry)
        self.ivp = Gauge("option_iv_percentile", "IV percentile [0..1]", ["symbol"], registry=self.registry)
        start_http_server(port, registry=self.registry)


class BuyEngine:
    """
    Minimal intraday BUY-only flow with IV-percentile gating and pre-trade margin/brokerage checks.
    """
    def __init__(self, cfg: EngineConfig):
        self.cfg = cfg
        self.metrics = PromMetrics(cfg.metrics_port)
        self.u = Upstox(cfg.api_key, cfg.access_token)
        self.u.get_profile()  # sanity
        self.u.get_master_contract(cfg.exchange)
        self._iv_hist: Dict[str, List[float]] = {}  # rolling IV store by option symbol
        self._open_positions: Dict[str, Dict[str, Any]] = {}
        self._pnl_net = 0.0

    # ---------- Helpers ----------
    def _now(self) -> float: return time.time()

    def _get_balance_available(self) -> float:
        bal = self.u.get_balance()   # Upstox returns dict with equity/commodity
        try:
            avail = float(bal['equity']['available_margin'])
        except Exception:
            # fallback to any number present
            avail = float(bal.get('available_margin', 0.0)) if isinstance(bal, dict) else 0.0
        self.metrics.margin_avail.set(avail)
        return avail

    def _fetch_ltp(self, instrument) -> float:
        # Prefer live LTP feed; fallback to quote snapshot
        try:
            q = self.u.get_live_feed(instrument, LiveFeedType.LTP)
            return float(q['ltp']) if isinstance(q, dict) and 'ltp' in q else float(q)
        except Exception:
            q = self.u.get_quote(instrument)
            return float(q.get('last_price') or q.get('last_traded_price') or 0.0)

    def _estimate_brokerage_cost(self, notional_rupees: float) -> float:
        """
        Conservative cost model for F&O BUY:
        - Upstox brokerage: capped at ₹20/order (most F&O)
        - Exchange + SEBI + Stamp + GST approximated at ~0.06% notional (varies; verify in production)
        This is an **estimate**; Upstox may expose an official charges API; if available, wire it here.
        """
        return 20.0 + 0.0006 * float(notional_rupees)

    def _required_cash_for_buy(self, price: float, lots: int) -> float:
        return float(price) * self.cfg.lot_size * lots + self._estimate_brokerage_cost(float(price) * self.cfg.lot_size * lots)

    def _iv_percentile(self, symbol: str, new_iv: Optional[float]) -> float:
        arr = self._iv_hist.setdefault(symbol, [])
        if new_iv is not None and new_iv > 0:
            arr.append(float(new_iv))
            if len(arr) > self.cfg.ivp_window:
                del arr[0]
        if not arr:
            return 0.0
        # Percentile rank of last value within the window
        x = arr[-1]
        rank = sum(1 for v in arr if v <= x)
        return rank / len(arr)

    # ---------- Strategy stubs (BUY-only) ----------
    def _pick_weekly_option(self, underlying_symbol: str) -> Tuple[Any, str]:
        """
        Select ATM CE or PE based on simple directional signal (placeholder: buy CE on momentum up).
        """
        # Example: buy ATM CE when underlying above 20EMA (this is simplified; replace with your signal)
        idx_inst = self.u.get_instrument_by_symbol(self.cfg.exchange, f"{underlying_symbol} 50") if False else None  # keep for reference
        # For clarity and safety in this sample, we leave selection to user by passing a fully-qualified symbol.
        raise NotImplementedError("Provide a resolved option instrument via place_buy()")

    # ---------- Public API ----------
    def place_buy(self, option_symbol: str, lots: Optional[int] = None, price_limit: Optional[float] = None) -> Dict[str, Any]:
        lots = int(lots or self.cfg.qty_lots)
        instrument = self.u.get_instrument_by_symbol(self.cfg.exchange, option_symbol)
        ltp = self._fetch_ltp(instrument)
        self.metrics.ltp.labels(symbol=option_symbol).set(ltp)

        # Try to pull IV from quote if available
        iv_val = None
        try:
            q = self.u.get_quote(instrument)
            iv_val = float(q.get("implied_volatility")) if "implied_volatility" in q and q.get("implied_volatility") else None
        except Exception:
            pass
        if iv_val is not None:
            self.metrics.iv.labels(symbol=option_symbol).set(iv_val)
        ivp = self._iv_percentile(option_symbol, iv_val)
        self.metrics.ivp.labels(symbol=option_symbol).set(ivp)

        # IV percentile gate
        if ivp < self.cfg.ivp_min:
            self.metrics.rejects.labels(symbol=option_symbol).inc()
            return {"status": "rejected", "reason": f"IV percentile too low ({ivp:.2%} < {self.cfg.ivp_min:.0%})"}

        # Pre-trade margin / cash gating
        notional = ltp * self.cfg.lot_size * lots
        need_cash = self._required_cash_for_buy(ltp, lots)
        avail = self._get_balance_available()
        if avail < need_cash:
            self.metrics.rejects.labels(symbol=option_symbol).inc()
            return {"status": "rejected", "reason": f"Insufficient funds. Need ₹{need_cash:,.0f}, available ₹{avail:,.0f}"}

        # Place market buy (default) or limit buy
        start = self._now()
        if price_limit:
            order = self.u.place_order(
                TransactionType.Buy, instrument, lots * self.cfg.lot_size,
                OrderType.Limit, self.cfg.product,
                price_limit, None, None, DurationType.DAY
            )
        else:
            order = self.u.place_order(
                TransactionType.Buy, instrument, lots * self.cfg.lot_size,
                OrderType.Market, self.cfg.product,
                0.0, None, None, DurationType.DAY
            )
        latency_ms = (self._now() - start) * 1000.0
        self.metrics.last_latency_ms.set(latency_ms)
        self.metrics.orders.labels(symbol=option_symbol).inc()

        # Very simple book-keeping
        oid = order.get("order_id") if isinstance(order, dict) else str(order)
        self._open_positions[oid] = {
            "symbol": option_symbol, "lots": lots, "avg_price": ltp, "timestamp": time.time()
        }
        return {"status": "submitted", "order": order, "latency_ms": latency_ms}

    def update_pnl(self):
        total = 0.0
        for oid, pos in list(self._open_positions.items()):
            instrument = self.u.get_instrument_by_symbol(self.cfg.exchange, pos["symbol"])
            ltp = self._fetch_ltp(instrument)
            qty = pos["lots"] * self.cfg.lot_size
            total += (ltp - pos["avg_price"]) * qty
        self._pnl_net = total
        self.metrics.pnl.set(total)
        return total

