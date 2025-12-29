from __future__ import annotations

import math
from dataclasses import dataclass


def risk_fraction(value: float) -> float:
    """Normalize risk input as fraction of capital (accepts 0.12 or 12.0)."""

    v = float(value or 0.0)
    if v <= 0:
        return 0.0
    return v if v <= 1.0 else (v / 100.0)


@dataclass(frozen=True)
class SizingInputs:
    capital_base: float
    risk_fraction: float
    premium: float
    lot_size: int
    max_premium_per_trade: float = 0.0
    portfolio_cap: float = 0.0
    portfolio_open_premium: float = 0.0


def compute_qty(inputs: SizingInputs) -> int:
    capital = float(inputs.capital_base or 0.0)
    rf = float(inputs.risk_fraction or 0.0)
    premium = float(inputs.premium or 0.0)
    lot = max(int(inputs.lot_size or 1), 1)
    if capital <= 0 or rf <= 0 or premium <= 0:
        return 0

    per_lot_cost = premium * lot
    if per_lot_cost <= 0:
        return 0

    budget_rupees = capital * rf
    lots = int(math.floor(budget_rupees / per_lot_cost))
    qty = max(lots, 0) * lot

    if inputs.max_premium_per_trade and inputs.max_premium_per_trade > 0:
        cap_lots = int(math.floor(float(inputs.max_premium_per_trade) / per_lot_cost))
        qty = min(qty, max(cap_lots, 0) * lot)

    if inputs.portfolio_cap and inputs.portfolio_cap > 0:
        remaining = max(float(inputs.portfolio_cap) - float(inputs.portfolio_open_premium or 0.0), 0.0)
        rem_lots = int(math.floor(remaining / per_lot_cost))
        qty = min(qty, max(rem_lots, 0) * lot)

    return max(int(qty), 0)


__all__ = ["SizingInputs", "compute_qty", "risk_fraction"]

