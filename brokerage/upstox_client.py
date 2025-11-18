from __future__ import annotations

import datetime as dt
import json
import logging
import os
import random
import time
from dataclasses import dataclass
from typing import Callable, Iterable, List, Optional, TypeVar

import upstox_client
from upstox_client import ApiClient
from upstox_client.rest import ApiException

IST = dt.timezone(dt.timedelta(hours=5, minutes=30), name="Asia/Kolkata")

INDEX_INSTRUMENT_KEYS = {
    "NIFTY": "NSE_INDEX|Nifty 50",
    "BANKNIFTY": "NSE_INDEX|Nifty Bank",
}


@dataclass(frozen=True)
class UpstoxConfig:
    access_token: str
    sandbox: bool = False
    algo_name: str = "intraday_buy_engine"


T = TypeVar("T")


def _require_token() -> str:
    token = os.getenv("UPSTOX_ACCESS_TOKEN", "").strip()
    if not token:
        raise RuntimeError("UPSTOX_ACCESS_TOKEN not set; export it before running the engine.")
    return token


def with_retry(fn: Callable[[], T], *, retries: int = 3, base_delay: float = 0.4) -> T:
    last_exc: Optional[Exception] = None
    for attempt in range(1, retries + 1):
        try:
            return fn()
        except ApiException as exc:  # pragma: no cover - network dependent
            last_exc = exc
            sleep_for = base_delay * attempt + random.random() * 0.1
            logging.warning("Upstox API error %s (attempt %s/%s)", exc, attempt, retries)
            time.sleep(sleep_for)
    assert last_exc is not None
    raise last_exc


class UpstoxSession:
    """Thin, typed wrapper around the official Upstox SDK."""

    def __init__(self, cfg: Optional[UpstoxConfig] = None):
        if cfg is None:
            cfg = UpstoxConfig(access_token=_require_token(), sandbox=os.getenv("UPSTOX_SANDBOX", "false").lower() in {"1", "true", "yes"}, algo_name=os.getenv("UPSTOX_ALGO_NAME", "intraday_buy_engine"))
        configuration = upstox_client.Configuration(sandbox=cfg.sandbox)
        configuration.access_token = cfg.access_token
        self._api_client = ApiClient(configuration)
        self._cfg = cfg
        self.options_api = upstox_client.OptionsApi(self._api_client)
        self.order_api_v3 = upstox_client.OrderApiV3(self._api_client)
        self.mq_v3 = upstox_client.MarketQuoteV3Api(self._api_client)

    @property
    def config(self) -> UpstoxConfig:
        return self._cfg

    def get_option_chain(self, instrument_key: str, expiry_date: str) -> dict:
        """Return raw dict from GET /v2/option/chain."""

        def _call() -> dict:
            resp = self.options_api.get_put_call_option_chain(
                instrument_key=instrument_key,
                expiry_date=expiry_date,
            )
            return resp.to_dict() if hasattr(resp, "to_dict") else resp

        return with_retry(_call)

    def get_option_contracts(self, instrument_key: str, expiry_date: Optional[str] = None) -> dict:
        """Return contracts metadata for an underlying (option chain metadata)."""

        def _call() -> dict:
            resp = self.options_api.get_option_contracts(
                instrument_key=instrument_key,
                expiry_date=expiry_date,
            )
            return resp.to_dict() if hasattr(resp, "to_dict") else resp

        return with_retry(_call)

    def get_ltp(self, instrument_keys: List[str]) -> dict:
        """Call the MarketQuote V3 LTP endpoint for up to ~20 keys."""

        def _call() -> dict:
            resp = self.mq_v3.get_ltp(instrument_keys=instrument_keys)
            return resp.to_dict() if hasattr(resp, "to_dict") else resp

        return with_retry(_call)

    def place_market_buy_stub(self, instrument_key: str, qty: int, tag: Optional[str] = None) -> dict:
        """Build a market BUY request. Only executes when sandbox mode is enabled."""
        body = upstox_client.PlaceOrderV3Request(
            instrument_token=instrument_key,
            transaction_type="BUY",
            order_type="MARKET",
            product="I",
            validity="DAY",
            quantity=int(qty),
            disclosed_quantity=0,
            trigger_price=0.0,
            is_amo=False,
            slice=False,
            tag=tag or self._cfg.algo_name,
        )
        if self._api_client.configuration.sandbox:
            logging.info("[SANDBOX BUY] %s qty=%s", instrument_key, qty)
            resp = self.order_api_v3.place_order(body, algo_name=self._cfg.algo_name)
            return resp.to_dict() if hasattr(resp, "to_dict") else resp
        logging.info("[DRY-RUN BUY] %s qty=%s", instrument_key, qty)
        return {"status": "dry-run", "request": body.to_dict()}


__all__ = [
    "IST",
    "INDEX_INSTRUMENT_KEYS",
    "UpstoxConfig",
    "UpstoxSession",
]
