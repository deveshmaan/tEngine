import asyncio
import datetime as dt
import json
from pathlib import Path

import pytest

from engine.config import EngineConfig, IST, OMSConfig, RiskLimits, ExitConfig
from engine.events import EventBus
from engine.exit import ExitEngine
from engine.metrics import EngineMetrics
from engine.multi_strategy_executor import MultiStrategyExecutor
from engine.oms import BrokerOrderAck, BrokerOrderView, OMS
from engine.replay import ReplayConfig, configure_runtime, replay
from engine.risk import RiskManager
from persistence import SQLiteStore


class DummyInstrumentCache:
    def get_meta(self, *_args, **_kwargs):
        return None


class CollectingBroker:
    def __init__(self):
        self.submitted = []
        self.views = {}

    async def submit_order(self, order):
        self.submitted.append(order)
        broker_id = f"BRK-{order.client_order_id}"
        self.views[order.client_order_id] = BrokerOrderView(
            broker_order_id=broker_id,
            client_order_id=order.client_order_id,
            status="open",
            filled_qty=0,
            avg_price=0.0,
        )
        return BrokerOrderAck(broker_order_id=broker_id, status="accepted")

    async def replace_order(self, order, *, price=None, qty=None):
        return None

    async def cancel_order(self, order):
        self.views.pop(order.client_order_id, None)

    async def fetch_open_orders(self):
        return list(self.views.values())


def _risk_limits() -> RiskLimits:
    return RiskLimits(
        daily_pnl_stop=-10_000,
        per_symbol_loss_stop=5_000,
        max_open_lots=10,
        notional_premium_cap=200_000,
        max_order_rate=50,
        no_new_entries_after=dt.time(15, 0),
        square_off_by=dt.time(15, 20),
    )


def _oms_cfg() -> OMSConfig:
    return OMSConfig(resubmit_backoff=0.1, reconciliation_interval=0.5, max_inflight_orders=20)


def _write_events(path: Path, events):
    with path.open("w", encoding="utf-8") as handle:
        for event in events:
            handle.write(json.dumps(event) + "\n")


async def _fill_consumer(bus: EventBus, risk: RiskManager, exit_engine: ExitEngine, stop: asyncio.Event, *, lot_size: int):
    q = await bus.subscribe("orders/fill", maxsize=100)
    while not stop.is_set():
        try:
            fill = await asyncio.wait_for(q.get(), timeout=0.5)
        except asyncio.TimeoutError:
            continue
        ts = dt.datetime.fromisoformat(fill["ts"])
        exit_engine.on_fill(symbol=fill["symbol"], side=fill["side"], qty=int(fill["qty"]), price=float(fill["price"]), ts=ts)
        risk.on_fill(symbol=fill["symbol"], side=fill["side"], qty=int(fill["qty"]), price=float(fill["price"]), lot_size=lot_size)


@pytest.mark.asyncio
async def test_multistrategy_replay_generates_order_and_exit(tmp_path: Path) -> None:
    cfg = EngineConfig.load()
    expiry = "2024-01-01"
    app_cfg = {
        "multi_strategy": {
            "enabled": True,
            "max_global_positions": 1,
            "strategies": [
                {
                    "id": "orb",
                    "import": "strategy.strategies.orb:OpeningRangeBreakoutStrategy",
                    "enabled": True,
                    "params": {"range_minutes": 1, "breakout_margin_points": 0, "min_signal_interval_s": 999},
                    "risk": {"max_positions": 1, "cooldown_seconds": 0, "risk_percent_per_trade": 0.12, "max_premium_per_trade": 50_000},
                }
            ],
        }
    }
    store = SQLiteStore(tmp_path / "engine.sqlite", run_id="ms")
    bus = EventBus()
    configure_runtime(bus=bus, store=store)
    metrics = EngineMetrics(_fresh_registry())
    broker = CollectingBroker()
    default_meta = (0.05, int(cfg.data.lot_step), float(cfg.data.price_band_low), float(cfg.data.price_band_high))
    oms = OMS(broker=broker, store=store, config=_oms_cfg(), bus=bus, metrics=metrics, default_meta=default_meta, square_off_time=_risk_limits().square_off_by)
    risk = RiskManager(_risk_limits(), store)
    exit_engine = ExitEngine(config=cfg.exit, risk=risk, oms=oms, store=store, tick_size=float(cfg.data.tick_size), metrics=metrics)
    executor = MultiStrategyExecutor(
        cfg,
        risk,
        oms,
        bus,
        exit_engine,
        DummyInstrumentCache(),  # type: ignore[arg-type]
        metrics,
        subscription_expiry_provider=lambda _u: expiry,
        app_config=app_cfg,
    )
    await executor.init(app=None)

    stop = asyncio.Event()
    consumer_stop = asyncio.Event()
    consumer_task = asyncio.create_task(_fill_consumer(bus, risk, exit_engine, consumer_stop, lot_size=int(cfg.data.lot_step)))
    exec_task = asyncio.create_task(executor.run(stop))

    events = [
        {"ts": "2024-01-01T09:15:00+05:30", "type": "tick", "payload": {"symbol": "NIFTY", "underlying": "NIFTY", "ltp": 100.0}},
        {
            "ts": "2024-01-01T09:15:10+05:30",
            "type": "tick",
            "payload": {
                "instrument_key": "OPTCE",
                "underlying": "NIFTY",
                "symbol": f"NIFTY-{expiry}-103CE",
                "ltp": 10.0,
                "bid": 9.95,
                "ask": 10.05,
                "expiry": expiry,
                "strike": 103,
                "opt_type": "CE",
                "iv": 0.2,
                "oi": 1000.0,
            },
        },
        {"ts": "2024-01-01T09:15:30+05:30", "type": "tick", "payload": {"symbol": "NIFTY", "underlying": "NIFTY", "ltp": 101.0}},
        {"ts": "2024-01-01T09:16:00+05:30", "type": "tick", "payload": {"symbol": "NIFTY", "underlying": "NIFTY", "ltp": 103.0}},
    ]
    feed_path = tmp_path / "feed.jsonl"
    _write_events(feed_path, events)
    await replay(ReplayConfig(run_id=None, input_path=feed_path, speed=0.0, seed=42))

    await asyncio.sleep(0.2)
    assert broker.submitted
    entry = next((o for o in broker.submitted if o.side.upper() == "BUY"), None)
    assert entry is not None
    assert entry.strategy == "orb"

    await oms.record_fill(entry.client_order_id, qty=int(entry.qty), price=float(entry.limit_price or 10.05))
    await asyncio.sleep(0.2)

    await exit_engine.handle_risk_halt("TEST_SQUARE_OFF")
    await asyncio.sleep(0.2)
    exit_orders = [o for o in broker.submitted if o.side.upper() == "SELL"]
    assert exit_orders
    assert any(o.symbol == entry.symbol for o in exit_orders)

    consumer_stop.set()
    stop.set()
    await exec_task
    await consumer_task


def _fresh_registry():
    try:
        from prometheus_client import CollectorRegistry  # type: ignore

        return CollectorRegistry()
    except Exception:
        return None
