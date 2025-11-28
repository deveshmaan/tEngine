from __future__ import annotations

import datetime as dt
import logging
import os
import random
import re
import time
from dataclasses import dataclass
from typing import Any, Callable, Iterable, List, Optional, Sequence, TypeVar

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
    api_key: Optional[str] = None
    api_secret: Optional[str] = None


@dataclass(frozen=True)
class PlacedOrder:
    """Normalized view of PlaceOrderV3Response for backward compatibility."""

    success: bool
    order_id: Optional[str]
    status: Optional[str] = None
    message: Optional[str] = None
    raw: Any = None


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


class CredentialError(RuntimeError):
    """Raised when broker credentials are unavailable or incomplete."""


def _read_env(name: str) -> Optional[str]:
    value = os.getenv(name)
    if value is None:
        return None
    text = value.strip()
    return text or None


def load_upstox_credentials(secrets: Optional[object] = None) -> UpstoxConfig:
    """
    Load Upstox credentials strictly from environment variables.

    Required:
        - UPSTOX_ACCESS_TOKEN
    Optional:
        - UPSTOX_API_KEY
        - UPSTOX_API_SECRET
    """

    # token = getattr(secrets, "upstox_access_token", None) or _read_env("UPSTOX_ACCESS_TOKEN")
    token = 'eyJ0eXAiOiJKV1QiLCJrZXlfaWQiOiJza192MS4wIiwiYWxnIjoiSFMyNTYifQ.eyJzdWIiOiIzR0NMM1kiLCJqdGkiOiI2OTI5NDhlZmY0NmQ1ZjdjYTNkY2QwNGQiLCJpc011bHRpQ2xpZW50IjpmYWxzZSwiaXNQbHVzUGxhbiI6dHJ1ZSwiaWF0IjoxNzY0MzEzMzI3LCJpc3MiOiJ1ZGFwaS1nYXRld2F5LXNlcnZpY2UiLCJleHAiOjE3NjQzNjcyMDB9.v6OgGq9GB_jUf9udvhMs3uZ3kF1FAnmQtWczzK55FyM'
    if not token:
        raise CredentialError("UPSTOX_ACCESS_TOKEN not set; export it before running the engine.")
    sandbox = str(os.getenv("UPSTOX_SANDBOX", "false")).lower() in {"1", "true", "yes"}
    algo_name = os.getenv("UPSTOX_ALGO_NAME", "intraday_buy_engine")
    return UpstoxConfig(
        access_token=token,
        sandbox=sandbox,
        algo_name=algo_name,
        api_key=getattr(secrets, "upstox_api_key", None) or _read_env("UPSTOX_API_KEY"),
        api_secret=getattr(secrets, "upstox_api_secret", None) or _read_env("UPSTOX_API_SECRET"),
    )


def _api_version() -> str:
    """Return Upstox API version header (defaults to 2.0)."""

    return os.getenv("UPSTOX_API_VERSION", "2.0")


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


def normalize_place_order_response(resp: Any) -> PlacedOrder:
    """Coerce Upstox PlaceOrderV3Response/model/dict into a simple PlacedOrder."""

    if resp is None:
        return PlacedOrder(False, None, message="empty_response", raw=resp)

    def _as_dict(obj: Any) -> dict:
        if isinstance(obj, dict):
            return obj
        to_dict = getattr(obj, "to_dict", None)
        if callable(to_dict):
            try:
                return to_dict()
            except Exception:
                pass
        if hasattr(obj, "__dict__"):
            try:
                return dict(obj.__dict__)
            except Exception:
                return {}
        return {}

    try:
        payload = _as_dict(resp)
        data = payload.get("data") if isinstance(payload, dict) else None
        status = (payload.get("status") if isinstance(payload, dict) else None) or getattr(resp, "status", None)
        message = payload.get("message") if isinstance(payload, dict) else None
        order_id = None
        if isinstance(data, dict):
            order_id = data.get("order_id") or data.get("orderId")
        if order_id is None:
            order_id = payload.get("order_id") if isinstance(payload, dict) else None
        if order_id is None:
            order_id = getattr(resp, "order_id", None) or getattr(resp, "orderId", None)
        success = bool(order_id) or str(status or "").lower() in {"success", "ok", "submitted", "complete", "completed"}
        order_id_str = str(order_id) if order_id is not None else None
        status_text = str(status) if status is not None else None
        return PlacedOrder(success, order_id_str, status=status_text, message=message, raw=payload or resp)
    except Exception as exc:
        return PlacedOrder(False, None, message=f"normalize_error:{exc}", raw=resp)


class UpstoxSession:
    """Thin, typed wrapper around the official Upstox SDK."""

    def __init__(self, cfg: Optional[UpstoxConfig] = None):
        if cfg is None:
            cfg = load_upstox_credentials()
        configuration = upstox_client.Configuration(sandbox=cfg.sandbox)
        configuration.access_token = cfg.access_token
        self._api_client = ApiClient(configuration)
        self._cfg = cfg
        self.options_api = upstox_client.OptionsApi(self._api_client)
        self.order_api_v3 = upstox_client.OrderApiV3(self._api_client)
        self.mq_v3 = upstox_client.MarketQuoteV3Api(self._api_client)
        try:
            self.portfolio_api = upstox_client.PortfolioApi(self._api_client)
        except Exception:
            self.portfolio_api = None  # type: ignore[assignment]
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

    def get_positions(self) -> dict:
        """Return open positions from PortfolioApi."""

        if self.portfolio_api is None:
            raise RuntimeError("PortfolioApi unavailable; upgrade upstox_client package.")

        def _call() -> dict:
            resp = self.portfolio_api.get_positions(_api_version())
            return resp.to_dict() if hasattr(resp, "to_dict") else resp

        return with_retry(_call)

    def get_holdings(self) -> dict:
        """Return holdings from PortfolioApi."""

        if self.portfolio_api is None:
            raise RuntimeError("PortfolioApi unavailable; upgrade upstox_client package.")

        def _call() -> dict:
            resp = self.portfolio_api.get_holdings(_api_version())
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
    "CredentialError",
    "InvalidDateError",
    "PlacedOrder",
    "UpstoxConfig",
    "UpstoxSession",
    "load_upstox_credentials",
    "normalize_place_order_response",
]
class InvalidDateError(Exception):
    """Raised when the broker rejects an expiry parameter."""


_DATE_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$")
