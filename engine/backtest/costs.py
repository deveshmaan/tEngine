from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, Iterable, Mapping, Optional

from engine.data import align_to_tick
from engine.fees import FeeConfig, fees_for_execution, load_fee_config


def apply_spread(fill_price: float, side: str, spread_bps: float) -> float:
    """
    Worsen the given price by `spread_bps`.

    Notes:
    - `fill_price` is treated as a mid/mark proxy; the function applies a one-sided
      worsening in the trade direction.
    """

    price = float(fill_price)
    if not (price > 0):
        return price
    bps = float(spread_bps or 0.0)
    if bps <= 0:
        return price
    side_u = str(side or "").strip().upper()
    mult = 1.0 + (bps / 10_000.0) if side_u == "BUY" else 1.0 - (bps / 10_000.0)
    return float(price * mult)


def apply_slippage(
    fill_price: float,
    side: str,
    model: str,
    bps: float,
    ticks: int,
    tick_size: float,
) -> float:
    """
    Apply slippage to the price in the trade direction.

    Models:
    - none: no slippage
    - bps: +/- (price * bps / 10000)
    - ticks: +/- (ticks * tick_size)

    The returned price is aligned to `tick_size`.
    """

    base = float(fill_price)
    if not (base > 0):
        return base
    tick = float(tick_size or 0.0)
    tick = tick if tick > 0 else 0.0

    model_key = str(model or "none").strip().lower()
    side_u = str(side or "").strip().upper()
    sign = 1.0 if side_u == "BUY" else -1.0

    delta = 0.0
    if model_key == "none":
        delta = 0.0
    elif model_key == "bps":
        delta = base * (float(bps or 0.0) / 10_000.0)
    elif model_key == "ticks":
        delta = float(int(ticks or 0)) * float(tick or 0.0)
    else:
        raise ValueError(f"Unsupported slippage model: {model!r}")

    out = base + (sign * delta)
    if tick > 0:
        out = align_to_tick(out, tick)
    return float(out)


@dataclass(frozen=True)
class _ExecutionAdapter:
    exec_id: str
    order_id: str
    symbol: str
    side: str
    qty: int
    price: float


def india_options_charges(
    fills: Iterable[Mapping[str, Any]],
    *,
    fee_config: Optional[FeeConfig] = None,
) -> Dict[str, float]:
    """
    Aggregate India options charges for a list of fills (executions).

    Input rows should have: `order_id` (or `client_order_id`), `side`, `qty`, `price`.
    Brokerage is treated as "per order" (charged once per unique order_id).
    """

    cfg = fee_config or load_fee_config()
    totals: Dict[str, float] = {
        "brokerage": 0.0,
        "exch_txn": 0.0,
        "sebi": 0.0,
        "gst": 0.0,
        "stamp": 0.0,
        "stt": 0.0,
        "other": 0.0,
        "total": 0.0,
    }
    seen_orders: set[str] = set()

    for idx, row in enumerate(fills):
        order_id = str(row.get("order_id") or row.get("client_order_id") or row.get("exec_id") or f"order-{idx}")
        side = str(row.get("side") or "").upper()
        qty = int(row.get("qty") or 0)
        price = float(row.get("price") or 0.0)
        exec_id = str(row.get("exec_id") or f"{order_id}:{idx}")
        symbol = str(row.get("symbol") or "")

        include_brokerage = order_id not in seen_orders
        exec_like = _ExecutionAdapter(exec_id=exec_id, order_id=order_id, symbol=symbol, side=side, qty=qty, price=price)
        fee_rows = fees_for_execution(exec_like, cfg, include_brokerage=include_brokerage)
        if include_brokerage:
            seen_orders.add(order_id)

        for fee_row in fee_rows:
            cat = str(fee_row.category or "")
            amt = float(fee_row.amount or 0.0)
            if cat == "brokerage":
                totals["brokerage"] += amt
            elif cat == "exchange_txn":
                totals["exch_txn"] += amt
            elif cat == "sebi_charges":
                totals["sebi"] += amt
            elif cat == "gst":
                totals["gst"] += amt
            elif cat == "stamp_duty":
                totals["stamp"] += amt
            elif cat == "stt":
                totals["stt"] += amt
            else:
                totals["other"] += amt

    totals["total"] = float(sum(v for k, v in totals.items() if k != "total"))
    return totals


__all__ = ["apply_slippage", "apply_spread", "india_options_charges"]

