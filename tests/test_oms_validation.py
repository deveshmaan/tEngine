import asyncio
import datetime as dt
from types import SimpleNamespace

import pytest

from engine.config import IST, OMSConfig, OMSSubmitConfig
from engine.oms import OMS, OrderValidationError
from market.instrument_cache import InstrumentCache
from persistence import SQLiteStore


class DummyBroker:
    def __init__(self):
        self.submissions = []

    async def submit_order(self, order):
        self.submissions.append(order)
        return type("Ack", (), {"broker_order_id": f"BRK-{len(self.submissions)}", "status": "submitted"})()

    async def replace_order(self, order, *, price=None, qty=None):
        return None

    async def cancel_order(self, order):
        return None

    async def fetch_open_orders(self):
        return []


class FakeCache:
    def __init__(self):
        self.contracts = {}

    def add_contract(self, symbol, expiry, strike, opt_type, *, lot, tick, low, high):
        key = (symbol.upper(), expiry, float(strike), opt_type.upper())
        self.contracts[key] = {
            "lot_size": lot,
            "tick_size": tick,
            "band_low": low,
            "band_high": high,
        }

    def get_contract(self, symbol, expiry, strike, opt_type):
        return self.contracts.get((symbol.upper(), expiry, float(strike), opt_type.upper()))

    def refresh_expiry(self, symbol, expiry):
        return 0

    def get_meta(self, symbol, expiry):
        return (0.05, 50, 10.0, 500.0)

    def list_expiries(self, symbol, kind=None):
        return []


class NullMetric:
    def inc(self, *_args, **_kwargs):
        return None

    def set(self, *_args, **_kwargs):
        return None

    def observe(self, *_args, **_kwargs):
        return None

    def labels(self, **_kwargs):  # type: ignore[override]
        return self


@pytest.fixture(autouse=True)
def runtime_cache(monkeypatch):
    cache = FakeCache()
    cache.add_contract("NIFTY", "2024-07-11", 22650, "CE", lot=50, tick=0.05, low=10.0, high=500.0)
    cache.add_contract("NIFTY", "2024-07-18", 22650, "CE", lot=50, tick=0.05, low=10.0, high=500.0)
    InstrumentCache._runtime = cache  # type: ignore[attr-defined]
    yield cache
    InstrumentCache._runtime = None  # type: ignore[attr-defined]


@pytest.fixture
def app_config(tmp_path, monkeypatch):
    cfg = tmp_path / "app.yml"
    cfg.write_text(
        """
        data:
          tick_size: 0.05
          lot_step: 50
          price_band_low: 10.0
          price_band_high: 500.0
        oms:
          submit:
            default: "ioc"
            max_spread_ticks: 2
            depth_threshold: 50
        """,
        encoding="utf-8",
    )
    monkeypatch.setenv("APP_CONFIG_PATH", str(cfg))
    from engine import data as data_mod

    data_mod._reset_app_config_cache()
    yield cfg
    data_mod._reset_app_config_cache()
    monkeypatch.delenv("APP_CONFIG_PATH", raising=False)


@pytest.fixture
def oms(tmp_path, app_config):
    store = SQLiteStore(tmp_path / "state.sqlite", run_id="test-run")
    broker = DummyBroker()
    metric = NullMetric()
    metrics = SimpleNamespace(
        order_queue_depth=metric,
        orders_submitted_total=metric,
        orders_filled_total=metric,
        orders_rejected_total=metric,
        order_latency_ms_bucketed=metric,
    )
    submit_cfg = OMSSubmitConfig(default="ioc", max_spread_ticks=2, depth_threshold=50)
    cfg = OMSConfig(resubmit_backoff=0.1, reconciliation_interval=1.0, max_inflight_orders=10, submit=submit_cfg)
    default_meta = (0.05, 50, 10.0, 500.0)
    square_off_time = dt.time(15, 20)
    return (
        OMS(
            broker=broker,
            store=store,
            config=cfg,
            metrics=metrics,
            default_meta=default_meta,
            square_off_time=square_off_time,
        ),
        broker,
    )


@pytest.mark.asyncio
async def test_rejects_invalid_lot(oms):
    engine, broker = oms
    with pytest.raises(OrderValidationError) as exc:
        await engine.submit(
            strategy="test",
            symbol="NIFTY-2024-07-11-22650CE",
            side="BUY",
            qty=25,
            order_type="LIMIT",
            limit_price=120.0,
        )
    assert exc.value.code == "lot_multiple"
    assert broker.submissions == []


@pytest.mark.asyncio
async def test_price_band_validation(oms):
    engine, _ = oms
    with pytest.raises(OrderValidationError) as exc:
        await engine.submit(
            strategy="test",
            symbol="NIFTY-2024-07-11-22650CE",
            side="BUY",
            qty=50,
            order_type="LIMIT",
            limit_price=5.0,
        )
    assert exc.value.code == "price_band_low"


@pytest.mark.asyncio
async def test_tick_rounding(oms):
    engine, broker = oms
    order = await engine.submit(
        strategy="test",
        symbol="NIFTY-2024-07-11-22650CE",
        side="BUY",
        qty=50,
        order_type="LIMIT",
        limit_price=123.123,
    )
    assert order.limit_price == pytest.approx(123.10)
    assert broker.submissions[0].limit_price == pytest.approx(123.10)


@pytest.mark.asyncio
async def test_ioc_market_style(oms):
    engine, broker = oms
    engine.update_market_depth("NIFTY-2024-07-11-22650CE", bid=100.0, ask=100.1, bid_qty=200, ask_qty=200)
    order = await engine.submit(
        strategy="test",
        symbol="NIFTY-2024-07-11-22650CE",
        side="BUY",
        qty=50,
        order_type="MARKET",
        limit_price=0.0,
    )
    assert order.order_type == "IOC_LIMIT"
    assert order.limit_price == pytest.approx(100.1)
    assert broker.submissions[0].order_type == "IOC_LIMIT"
