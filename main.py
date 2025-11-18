from __future__ import annotations

import asyncio
import datetime as dt
import random
import signal
from typing import Optional

from engine.broker import UpstoxBroker
from engine.config import EngineConfig, IST
from engine.data import pick_strike_from_spot, resolve_weekly_expiry
from engine.logging_utils import configure_logging, get_logger
from engine.oms import OMS
from engine.risk import OrderBudget, RiskManager
from persistence import SQLiteStore

try:  # pragma: no cover - optional acceleration
    import uvloop

    uvloop.install()
except Exception:  # pragma: no cover
    pass


class IntradayBuyStrategy:
    """Toy BUY-side strategy that demonstrates the risk→data→oms pipeline."""

    def __init__(self, config: EngineConfig, risk: RiskManager, oms: OMS):
        self.cfg = config
        self.risk = risk
        self.oms = oms
        self.logger = get_logger("IntradayStrategy")
        self._last_spot = 0.0

    async def run(self, stop_event: asyncio.Event) -> None:
        while not stop_event.is_set():
            await asyncio.sleep(1.0)
            ts = dt.datetime.now(IST)
            mock_spot = self._mock_spot()
            await self._maybe_trade(mock_spot, ts)

    async def _maybe_trade(self, spot: float, ts: dt.datetime) -> None:
        expiry = resolve_weekly_expiry(self.cfg.data.index_symbol, ts, self.cfg.data.holidays)
        strike = pick_strike_from_spot(
            spot,
            self.cfg.data.lot_step,
            self.cfg.data.tick_size,
            price_band=(self.cfg.data.price_band_low, self.cfg.data.price_band_high),
        )
        symbol = f"{self.cfg.data.index_symbol}-{expiry.isoformat()}-{int(strike)}"
        budget = OrderBudget(symbol=symbol, qty=self.cfg.data.lot_step, price=spot, lot_size=self.cfg.data.lot_step, side="BUY")
        if not self.risk.budget_ok_for(budget):
            return
        await self.oms.submit(
            strategy=self.cfg.strategy_tag,
            symbol=symbol,
            side="BUY",
            qty=budget.qty,
            order_type="MARKET",
            limit_price=budget.price,
            ts=ts,
        )
        self.logger.log_event(20, "submitted", symbol=symbol, price=budget.price)

    def _mock_spot(self) -> float:
        if self._last_spot <= 0:
            self._last_spot = 23000.0
        drift = random.uniform(-5, 5)
        self._last_spot = max(1.0, self._last_spot + drift)
        return self._last_spot


class EngineApp:
    def __init__(self, config: EngineConfig):
        self.cfg = config
        self.logger = get_logger("EngineApp")
        self.store = SQLiteStore(config.persistence_path, run_id=config.run_id)
        self.risk = RiskManager(config.risk, self.store)
        self.broker = UpstoxBroker(config=config.broker)
        self.oms = OMS(broker=self.broker, store=self.store, config=config.oms)
        self.strategy = IntradayBuyStrategy(config, self.risk, self.oms)
        self._stop = asyncio.Event()

    async def run(self) -> None:
        configure_logging("INFO")
        self._install_signal_handlers()
        await self.broker.start()
        square_task = asyncio.create_task(self._square_off_watchdog(), name="square-off")
        strat_task = asyncio.create_task(self.strategy.run(self._stop), name="strategy-loop")
        await self._stop.wait()
        strat_task.cancel()
        square_task.cancel()
        await self._cleanup()

    async def trigger_shutdown(self, reason: str) -> None:
        if not self._stop.is_set():
            self.logger.log_event(20, "shutdown", reason=reason)
            self._stop.set()

    async def _cleanup(self) -> None:
        await self.oms.cancel_all(reason="shutdown")
        await self.broker.stop()
        self.logger.log_event(20, "engine_stopped")

    def _install_signal_handlers(self) -> None:
        loop = asyncio.get_running_loop()
        for sig in (signal.SIGINT, signal.SIGTERM):
            loop.add_signal_handler(sig, lambda sig=sig: asyncio.create_task(self.trigger_shutdown(f"signal:{sig.name}")))

    async def _square_off_watchdog(self) -> None:
        while not self._stop.is_set():
            now = dt.datetime.now(IST)
            deadline = dt.datetime.combine(now.date(), self.cfg.risk.square_off_by, tzinfo=IST)
            if now >= deadline:
                self.risk.trigger_kill("SQUARE_OFF_DEADLINE")
                await self.oms.cancel_all(reason="square_off")
                await self.trigger_shutdown("square_off")
                return
            await asyncio.sleep((deadline - now).total_seconds())


async def main() -> None:
    cfg = EngineConfig.load()
    app = EngineApp(cfg)
    await app.run()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:  # pragma: no cover
        pass
