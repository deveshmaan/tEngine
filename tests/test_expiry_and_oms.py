import datetime as dt
from typing import Optional
from types import SimpleNamespace

import pytest

from engine.config import OMSConfig, OMSSubmitConfig
from engine import data as data_mod
from engine.data import assert_valid_expiry, normalize_date
from engine.oms import OMS, OrderValidationError, validate_order
from market.instrument_cache import InstrumentCache
from persistence import SQLiteStore


class FakeCache:
    def __init__(self):
        self.expiry_map = {
            "NIFTY": ["2024-08-08", "2024-08-15", "2024-08-22"],
            "BANKNIFTY": ["2024-08-07", "2024-08-14"],
        }

    def list_expiries(self, symbol: str, kind: Optional[str] = None):  # noqa: D401 - test double
        return list(self.expiry_map.get(symbol.upper(), []))

    def refresh_option_chain(self, symbol: str, expiry: str, session=None):
        return 0

    def refresh_expiry(self, symbol: str, expiry: str, session=None):
        return 0

    def get_contract(self, symbol: str, expiry: str, strike: float, opt_type: str):
        return {
            "lot_size": 50,
            "tick_size": 0.05,
            "band_low": 10.0,
            "band_high": 500.0,
        }

    def get_meta(self, symbol: str, expiry: str):
        return (0.05, 50, 10.0, 500.0)


@pytest.fixture(autouse=True)
def runtime_cache():
    cache = FakeCache()
    InstrumentCache._runtime = cache  # type: ignore[attr-defined]
    yield cache
    InstrumentCache._runtime = None  # type: ignore[attr-defined]


class DummyMetric:
    def __init__(self):
        self.counts: dict[tuple[tuple[str, str], ...], float] = {}

    def labels(self, **labels):  # type: ignore[override]
        key = tuple(sorted(labels.items()))

        class _Handle:
            def __init__(self, parent, key):
                self._parent = parent
                self._key = key

            def inc(self, amount: float = 1.0):
                self._parent.counts[self._key] = self._parent.counts.get(self._key, 0.0) + amount

            def observe(self, *_args, **_kwargs):
                return None

            def set(self, *_args, **_kwargs):
                return None

        return _Handle(self, key)

    def inc(self, amount: float = 1.0) -> None:
        key: tuple[tuple[str, str], ...] = tuple()
        self.counts[key] = self.counts.get(key, 0.0) + amount

    def set(self, *_args, **_kwargs) -> None:
        return None


class DummyGauge:
    def set(self, *_args, **_kwargs):
        return None


class DummyHistogram:
    def labels(self, **_labels):  # type: ignore[override]
        class _Handle:
            def observe(self, *_args, **_kwargs):
                return None

        return _Handle()


def _oms_metrics():
    return SimpleNamespace(
        order_queue_depth=DummyGauge(),
        orders_submitted_total=DummyMetric(),
        orders_filled_total=DummyMetric(),
        orders_rejected_total=DummyMetric(),
        order_latency_ms_bucketed=DummyHistogram(),
    )


class DummyBroker:
    def __init__(self):
        self.submissions = []

    async def submit_order(self, order):
        self.submissions.append(order)
        return SimpleNamespace(broker_order_id="TEST", status="submitted")

    async def replace_order(self, order, *, price=None, qty=None):
        return None

    async def cancel_order(self, order):
        return None

    async def fetch_open_orders(self):
        return []


def _oms(tmp_path):
    store = SQLiteStore(tmp_path / "expiry.sqlite", run_id="test")
    submit_cfg = OMSSubmitConfig(default="ioc", max_spread_ticks=2, depth_threshold=50)
    cfg = OMSConfig(resubmit_backoff=0.1, reconciliation_interval=0.5, max_inflight_orders=10, submit=submit_cfg)
    default_meta = (0.05, 50, 10.0, 500.0)
    return OMS(
        broker=DummyBroker(),
        store=store,
        config=cfg,
        metrics=_oms_metrics(),
        default_meta=default_meta,
        square_off_time=dt.time(15, 20),
    )


def test_assert_valid_expiry_falls_back(monkeypatch):
    monkeypatch.setattr(data_mod, "engine_now", lambda tz: dt.datetime(2024, 8, 10, tzinfo=tz))
    chosen = assert_valid_expiry("NIFTY", "2024-08-01")
    assert chosen == "2024-08-15"


def test_normalize_date_strictness():
    assert normalize_date("2024-07-01") == "2024-07-01"
    assert normalize_date(dt.datetime(2024, 7, 1)) == "2024-07-01"
    with pytest.raises(ValueError):
        normalize_date("2024/07/01")
    with pytest.raises(ValueError):
        normalize_date("2024-07-01T00:00:00")


def test_validate_order_rounding_and_bands():
    qty, price = validate_order(
        "NIFTY",
        "2024-08-15",
        22000,
        "CE",
        "BUY",
        50,
        123.123,
        lot_size=50,
        tick=0.05,
        band_low=10.0,
        band_high=500.0,
    )
    assert qty == 50
    assert price == pytest.approx(123.1)
    with pytest.raises(OrderValidationError):
        validate_order(
            "NIFTY",
            "2024-08-15",
            22000,
            "CE",
            "BUY",
            25,
            100.0,
            lot_size=50,
            tick=0.05,
            band_low=10.0,
            band_high=500.0,
        )
    with pytest.raises(OrderValidationError):
        validate_order(
            "NIFTY",
            "2024-08-15",
            22000,
            "CE",
            "BUY",
            50,
            5.0,
            lot_size=50,
            tick=0.05,
            band_low=10.0,
            band_high=500.0,
        )


@pytest.mark.asyncio
async def test_local_validation_reject_emits_metric(tmp_path):
    oms = _oms(tmp_path)
    metrics = oms._metrics  # type: ignore[attr-defined]
    with pytest.raises(OrderValidationError):
        await oms.submit(
            strategy="demo",
            symbol="NIFTY-2024-08-15-22000CE",
            side="BUY",
            qty=25,
            order_type="LIMIT",
            limit_price=120.0,
        )
    key = tuple(sorted({"reason": "validation"}.items()))
    assert metrics.orders_rejected_total.counts[key] == 1


@pytest.mark.asyncio
async def test_market_depth_triggers_ioc(tmp_path):
    oms = _oms(tmp_path)
    broker: DummyBroker = oms._broker  # type: ignore[attr-defined]
    symbol = "NIFTY-2024-08-15-22000CE"
    oms.update_market_depth(symbol, bid=100.0, ask=100.1, bid_qty=200, ask_qty=200)
    order = await oms.submit(
        strategy="demo",
        symbol=symbol,
        side="BUY",
        qty=50,
        order_type="LIMIT",
        limit_price=100.2,
    )
    assert order.order_type == "IOC_LIMIT"
    assert broker.submissions
    assert broker.submissions[0].order_type == "IOC_LIMIT"
    assert order.limit_price == pytest.approx(100.1)
