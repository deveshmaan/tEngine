from __future__ import annotations

import datetime as dt
import logging
import os
import random
import re
import time
from dataclasses import dataclass
from typing import Callable, Iterable, List, Optional, Sequence, TypeVar

import upstox_client
from upstox_client import ApiClient
from upstox_client.rest import ApiException

LOG = logging.getLogger(__name__)
LOG.info("upstox_client.py loaded from %s", __file__)

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


def _safe_repr(obj: object, maxlen: int = 256) -> str:
    s = repr(obj)
    return s if len(s) <= maxlen else s[:maxlen] + "...(trunc)"


def _kwargs_discovery(instrument_key: str) -> dict[str, str]:
    return {"instrument_key": instrument_key}


def _kwargs_filtered(instrument_key: str, expiry: str) -> dict[str, str]:
    if not _DATE_RE.fullmatch(expiry):
        raise ValueError("bad expiry format: %r" % expiry)
    return {"instrument_key": instrument_key, "expiry_date": expiry}


def _require_token() -> str:
    token = os.getenv("UPSTOX_ACCESS_TOKEN", "eyJ0eXAiOiJKV1QiLCJrZXlfaWQiOiJza192MS4wIiwiYWxnIjoiSFMyNTYifQ.eyJzdWIiOiIzR0NMM1kiLCJqdGkiOiI2OTFlOWQxNjdlOWMzYTVhNDM5YjdhMzMiLCJpc011bHRpQ2xpZW50IjpmYWxzZSwiaXNQbHVzUGxhbiI6dHJ1ZSwiaWF0IjoxNzYzNjEzOTc0LCJpc3MiOiJ1ZGFwaS1nYXRld2F5LXNlcnZpY2UiLCJleHAiOjE3NjM2NzYwMDB9.CTEf01BEM-zIaLiaF9kxLjV8LM3X5U9J6cwaGccsqSo").strip()
    if not token:
        raise RuntimeError("UPSTOX_ACCESS_TOKEN not set; export it before running the engine.")
    return token


def with_retry(fn: Callable[[], T], *, retries: int = 2, base_delay: float = 0.2) -> T:
    last_exc: Optional[Exception] = None
    for attempt in range(1, retries + 1):
        try:
            return fn()
        except InvalidDateError:
            raise
        except ApiException as exc:  # pragma: no cover - network dependent
            last_exc = exc
            sleep_for = base_delay + (attempt - 1) * base_delay
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
        if os.getenv("UPSTOX_SDK_DEBUG", "").lower() in {"1", "true", "yes"}:
            self.options_api.api_client.configuration.debug = True

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

        from engine.data import normalize_date

        debug = os.getenv("UPSTOX_SDK_DEBUG", "").lower() in {"1", "true", "yes"}
        kwargs: dict[str, str]
        expiry_text = "" if expiry_date is None else str(expiry_date).strip()
        if not expiry_text:
            kwargs = _kwargs_discovery(instrument_key)
            if debug:
                LOG.debug("OptionContracts(discovery) kwargs=%s", _safe_repr(kwargs))
            assert "expiry_date" not in kwargs
        else:
            norm = normalize_date(expiry_text)
            kwargs = _kwargs_filtered(instrument_key, norm)
            if debug:
                LOG.debug("OptionContracts(filtered) kwargs=%s", _safe_repr(kwargs))

        def _call() -> dict:
            try:
                resp = self.options_api.get_option_contracts(**kwargs)
            except ApiException as exc:
                body = getattr(exc, "body", "") or ""
                text = body.decode("utf-8", errors="ignore") if isinstance(body, bytes) else str(body)
                if "UDAPI1088" in text or "Invalid date" in text:
                    from engine.alerts import notify_incident
                    from engine.metrics import inc_api_error, inc_orders_rejected

                    inc_api_error("UDAPI1088")
                    inc_orders_rejected("invalid_date")
                    notify_incident(
                        level="ERROR",
                        title="UDAPI1088 Invalid date",
                        body=f"instrument_key={instrument_key} kwargs={kwargs!r}",
                        tags=["invalid_date"],
                    )
                    raise InvalidDateError("UDAPI1088 Invalid date") from exc
                raise
            return resp.to_dict() if hasattr(resp, "to_dict") else resp

        return with_retry(_call)

    def get_ltp(self, instrument_keys: Sequence[str] | str) -> dict:
        """Call the MarketQuote V3 LTP endpoint for up to ~20 keys."""

        keys: List[str] = []
        if isinstance(instrument_keys, str):
            keys = [instrument_keys]
        else:
            keys = [str(k) for k in instrument_keys if k]
        if not keys:
            raise ValueError("instrument_keys must be non-empty")
        primary_payload: object = keys[0] if len(keys) == 1 else keys

        def _call() -> dict:
            try:
                resp = self.mq_v3.get_ltp(instrument_key=primary_payload)
            except TypeError:
                # accommodate SDKs that expect plural param name
                resp = self.mq_v3.get_ltp(instrument_keys=primary_payload)
            except ApiException as exc:
                if len(keys) == 1 and primary_payload != keys[0]:
                    # retry with a singular string payload
                    return self.mq_v3.get_ltp(instrument_key=keys[0])
                raise
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
    "InvalidDateError",
    "UpstoxConfig",
    "UpstoxSession",
]
class InvalidDateError(Exception):
    """Raised when the broker rejects an expiry parameter."""


_DATE_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$")
