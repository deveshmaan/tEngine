from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass
from typing import List

from engine.oms import OMS, Order
from persistence import SQLiteStore


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


__all__ = ["RecoveryManager"]
