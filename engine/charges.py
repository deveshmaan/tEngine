from __future__ import annotations

import logging
import os
import threading
import time
from dataclasses import dataclass
from typing import Dict, List, Optional, Tuple

import upstox_client
from upstox_client import ApiClient
from upstox_client.rest import ApiException

from engine.fees import FeeRow

LOG = logging.getLogger(__name__)


@dataclass(frozen=True)
class ChargesBreakdown:
    rows: List[FeeRow]
    total: float
    raw: Dict[str, object]


class UpstoxChargesClient:
    """Lightweight wrapper around Upstox ChargeApi with caching + rate limiting."""

    def __init__(
        self,
        access_token: str,
        *,
        sandbox: bool = False,
        api_version: Optional[str] = None,
        cache_ttl_seconds: float = 15.0,
        min_interval_seconds: float = 0.5,
        per_instrument_min_interval: float = 2.0,
    ) -> None:
        cfg = upstox_client.Configuration(sandbox=sandbox)
        cfg.access_token = access_token
        self._api_client = ApiClient(cfg)
        self._api = upstox_client.ChargeApi(self._api_client)
        self._api_version = api_version or os.getenv("UPSTOX_API_VERSION", "2.0")
        self._cache_ttl = max(float(cache_ttl_seconds or 0.0), 0.0)
        self._min_interval = max(float(min_interval_seconds or 0.0), 0.0)
        self._per_instrument_interval = max(float(per_instrument_min_interval or 0.0), 0.0)
        self._cache: Dict[Tuple[str, int, str, str, float], Tuple[float, ChargesBreakdown]] = {}
        self._last_call = 0.0
        self._last_call_by_instr: Dict[str, float] = {}
        self._lock = threading.Lock()

    def get_brokerage_breakdown(
        self,
        instrument_token: str,
        quantity: int,
        product: str,
        transaction_type: str,
        price: float,
    ) -> Optional[ChargesBreakdown]:
        if not instrument_token or quantity <= 0 or price <= 0:
            return None
        key = (
            str(instrument_token),
            int(quantity),
            str(product),
            str(transaction_type).upper(),
            float(price),
        )
        now = time.time()
        with self._lock:
            cached = self._cache.get(key)
            if cached and (now - cached[0]) <= self._cache_ttl:
                return cached[1]
            last_global = self._last_call
            last_instr = self._last_call_by_instr.get(key[0], 0.0)
            if (self._min_interval and (now - last_global) < self._min_interval) or (
                self._per_instrument_interval and (now - last_instr) < self._per_instrument_interval
            ):
                return None
            self._last_call = now
            self._last_call_by_instr[key[0]] = now
        try:
            resp = self._api.get_brokerage(
                key[0],
                key[1],
                key[2],
                key[3],
                key[4],
                self._api_version,
            )
        except ApiException as exc:  # pragma: no cover - network dependent
            LOG.warning("charges_api_error status=%s", getattr(exc, "status", None))
            return None
        except Exception as exc:  # pragma: no cover - defensive
            LOG.warning("charges_api_error %s", exc)
            return None
        payload = _normalize_payload(resp)
        rows, total = _parse_charges(payload)
        breakdown = ChargesBreakdown(rows=rows, total=total, raw=payload)
        with self._lock:
            self._cache[key] = (now, breakdown)
        return breakdown


def _normalize_payload(resp: object) -> Dict[str, object]:
    payload: object
    if isinstance(resp, dict):
        payload = resp
    else:
        to_dict = getattr(resp, "to_dict", None)
        payload = to_dict() if callable(to_dict) else {}
    if isinstance(payload, dict) and isinstance(payload.get("data"), dict):
        return dict(payload["data"])
    return dict(payload) if isinstance(payload, dict) else {}


def _parse_charges(payload: Dict[str, object]) -> Tuple[List[FeeRow], float]:
    rows: List[FeeRow] = []
    total = 0.0
    charges = payload.get("charges")
    if isinstance(charges, dict):
        for key, value in charges.items():
            if str(key).lower() == "total":
                continue
            try:
                amount = float(value)
            except (TypeError, ValueError):
                continue
            if amount:
                rows.append(FeeRow(category=str(key), amount=abs(amount), note="charge_api"))
        try:
            total = float(charges.get("total") or 0.0)
        except (TypeError, ValueError):
            total = 0.0
    if payload.get("total_charges") is not None:
        try:
            total = float(payload.get("total_charges") or 0.0)
        except (TypeError, ValueError):
            pass
    if total <= 0:
        total = sum(row.amount for row in rows)
    return rows, abs(total)


__all__ = ["ChargesBreakdown", "UpstoxChargesClient"]
