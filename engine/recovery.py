from __future__ import annotations

import asyncio
from dataclasses import dataclass
from typing import Any, Dict, Iterable, List, Mapping, Optional, Set

from brokerage.upstox_client import UpstoxSession, load_upstox_credentials
from engine.logging_utils import get_logger
from engine.metrics import set_startup_unmanaged_positions
from engine.risk import RiskManager
from engine.oms import OMS, Order
from persistence import SQLiteStore
from market.instrument_cache import InstrumentCache

LOG = get_logger("Recovery")


@dataclass
class RecoveryManager:
    store: SQLiteStore
    oms: OMS

    async def reconcile(self) -> None:
        rows = self.store.load_active_orders()
        if not rows:
            return
        orders: List[Order] = [Order.from_snapshot(row) for row in rows]
        self.oms.restore_orders(orders)
        await self.oms.reconcile_from_broker()


def _extract_entries(payload: object) -> List[Dict[str, Any]]:
    data: object = payload
    if isinstance(payload, Mapping):
        data = payload.get("data") or payload.get("positions") or payload.get("holdings") or payload
    if isinstance(data, Mapping):
        maybe_list = data.get("data") or data.get("positions") or data.get("holdings")
        data = maybe_list if isinstance(maybe_list, list) else []
    entries: List[Dict[str, Any]] = []
    if not isinstance(data, list):
        return entries
    for entry in data:
        if not isinstance(entry, Mapping):
            continue
        qty_raw = entry.get("net_quantity")
        for key in ("net_qty", "quantity", "qty", "available_qty", "quantity_available"):
            if qty_raw is None:
                qty_raw = entry.get(key)
        try:
            qty = int(qty_raw)
        except (TypeError, ValueError):
            continue
        if qty == 0:
            continue
        ident = entry.get("instrument_key") or entry.get("instrument_token") or entry.get("trading_symbol") or entry.get("symbol")
        if not ident:
            continue
        symbol = str(ident).strip()
        if not symbol:
            continue
        product = entry.get("product")
        entries.append({"id": symbol, "qty": qty, "product": product, "raw": dict(entry)})
    return entries


def _merge_entries(*payloads: object) -> List[Dict[str, Any]]:
    merged: Dict[str, Dict[str, Any]] = {}
    for payload in payloads:
        for entry in _extract_entries(payload):
            key = entry["id"].upper()
            existing = merged.get(key)
            if existing:
                existing["qty"] = int(existing.get("qty", 0)) + int(entry.get("qty", 0))
                continue
            merged[key] = entry
    return list(merged.values())


def _detect_underlying(symbol: str, underlyings: Set[str]) -> Optional[str]:
    text = symbol.upper()
    for root in sorted(underlyings, key=len, reverse=True):
        if root in text:
            return root
    return None


def _local_signatures(store: SQLiteStore, underlyings: Set[str], instrument_cache: Optional[InstrumentCache]) -> tuple[Set[str], Set[str]]:
    symbols: Set[str] = set()
    underlying_hits: Set[str] = set()
    for pos in store.list_open_positions():
        symbol = str(pos.get("symbol") or "").upper()
        if symbol:
            symbols.add(symbol)
            root = _detect_underlying(symbol, underlyings)
            if root:
                underlying_hits.add(root)
        expiry = pos.get("expiry")
        strike = pos.get("strike")
        opt_type = pos.get("opt_type") or pos.get("opt")
        base = symbol.split("-")[0] if symbol else None
        if instrument_cache and base and expiry and strike is not None and opt_type:
            try:
                inst_key = instrument_cache.lookup(base.upper(), str(expiry), float(strike), str(opt_type))
            except Exception:
                inst_key = None
            if inst_key:
                symbols.add(str(inst_key).upper())
    return symbols, underlying_hits


def _find_unmanaged(
    entries: List[Dict[str, Any]],
    underlyings: Set[str],
    local_signatures: Set[str],
) -> List[Dict[str, Any]]:
    unmanaged: List[Dict[str, Any]] = []
    seen: Set[str] = set()
    for entry in entries:
        ident = str(entry.get("id") or "").strip()
        if not ident:
            continue
        norm = ident.upper()
        if norm in seen:
            continue
        seen.add(norm)
        underlying = _detect_underlying(norm, underlyings)
        if underlying is None:
            continue
        if norm in local_signatures:
            continue
        unmanaged.append(
            {
                "id": ident,
                "qty": entry.get("qty"),
                "underlying": underlying,
                "product": entry.get("product"),
            }
        )
    return unmanaged


async def enforce_intraday_clean_start(
    store: SQLiteStore,
    *,
    instrument_cache: Optional[InstrumentCache],
    underlyings: Iterable[str],
    strict_mode: bool = True,
    risk: Optional[RiskManager] = None,
) -> List[Dict[str, Any]]:
    """
    Ensure there are no unmanaged overnight positions before intraday trading begins.

    Returns:
        List of unmanaged positions detected (may be empty).
    """

    roots = {u.upper() for u in underlyings if u}
    if not roots:
        set_startup_unmanaged_positions(0)
        return []
    try:
        session = UpstoxSession(load_upstox_credentials())
    except Exception as exc:
        LOG.log_event(40, "startup_credentials_missing", message=str(exc))
        set_startup_unmanaged_positions(0)
        if strict_mode:
            raise
        return []
    try:
        positions_payload = await asyncio.to_thread(session.get_positions)
        holdings_payload = await asyncio.to_thread(session.get_holdings)
    except Exception as exc:
        LOG.log_event(40, "startup_position_probe_failed", error=str(exc))
        set_startup_unmanaged_positions(0)
        if strict_mode:
            raise
        return []

    local_signatures, _ = _local_signatures(store, roots, instrument_cache)
    remote_entries = _merge_entries(positions_payload, holdings_payload)
    unmanaged = _find_unmanaged(remote_entries, roots, local_signatures)
    set_startup_unmanaged_positions(len(unmanaged))

    if unmanaged:
        symbols = [entry["id"] for entry in unmanaged]
        level = 40 if strict_mode else 30
        LOG.log_event(level, "startup_unmanaged_positions", count=len(unmanaged), symbols=symbols, strict=strict_mode)
        if strict_mode:
            raise RuntimeError(f"Intraday-only engine found unmanaged positions: {', '.join(symbols[:5])}")
        if risk:
            risk.halt_new_entries("UNMANAGED_POSITIONS")
            LOG.log_event(30, "recovery_only_mode", reason="UNMANAGED_POSITIONS", symbols=symbols)
    else:
        LOG.log_event(20, "startup_positions_clean", underlyings=",".join(sorted(roots)))
    return unmanaged


__all__ = ["RecoveryManager", "enforce_intraday_clean_start"]
