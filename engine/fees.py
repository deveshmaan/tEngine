from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Dict, List, Protocol

import yaml


class ExecutionLike(Protocol):
    exec_id: str
    order_id: str
    symbol: str
    side: str
    qty: int
    price: float


@dataclass(frozen=True)
class FeeRow:
    category: str
    amount: float
    currency: str = "INR"
    note: str | None = None


@dataclass(frozen=True)
class FeeConfig:
    brokerage_per_order: float = 0.0
    exchange_txn_rate_per_turnover: float = 0.0
    sebi_rate_per_turnover: float = 0.0
    gst_rate_on_services: float = 0.0
    stamp_duty_buy_rate: float = 0.0
    stt_options_sell_rate: float = 0.0
    other_fees: Dict[str, float] = field(default_factory=dict)


def fees_for_execution(exec: ExecutionLike, cfg: FeeConfig, *, include_brokerage: bool = True) -> List[FeeRow]:
    turnover = abs(exec.qty) * exec.price
    rows: List[FeeRow] = []
    if include_brokerage and cfg.brokerage_per_order:
        rows.append(FeeRow("brokerage", cfg.brokerage_per_order))
    exchange_fee = turnover * cfg.exchange_txn_rate_per_turnover
    if exchange_fee:
        rows.append(FeeRow("exchange_txn", exchange_fee))
    sebi_fee = turnover * cfg.sebi_rate_per_turnover
    if sebi_fee:
        rows.append(FeeRow("sebi_charges", sebi_fee))
    service_tax_base = sum(r.amount for r in rows if r.category in {"brokerage", "exchange_txn"})
    gst_fee = service_tax_base * cfg.gst_rate_on_services
    if gst_fee:
        rows.append(FeeRow("gst", gst_fee))
    if exec.side.upper() == "BUY":
        stamp = turnover * cfg.stamp_duty_buy_rate
        if stamp:
            rows.append(FeeRow("stamp_duty", stamp))
    if exec.side.upper() == "SELL":
        stt = turnover * cfg.stt_options_sell_rate
        if stt:
            rows.append(FeeRow("stt", stt))
    for name, amount in (cfg.other_fees or {}).items():
        if amount:
            rows.append(FeeRow(name, amount))
    return rows


def load_fee_config(path: str | Path | None = None) -> FeeConfig:
    target = Path(path or "config/fees.yml")
    if not target.exists():
        return FeeConfig()
    with target.open("r", encoding="utf-8") as handle:
        raw = yaml.safe_load(handle) or {}
    other = raw.get("other_fees") or {}
    return FeeConfig(
        brokerage_per_order=float(raw.get("brokerage_per_order", 0.0)),
        exchange_txn_rate_per_turnover=float(raw.get("exchange_txn_rate_per_turnover", 0.0)),
        sebi_rate_per_turnover=float(raw.get("sebi_rate_per_turnover", 0.0)),
        gst_rate_on_services=float(raw.get("gst_rate_on_services", 0.0)),
        stamp_duty_buy_rate=float(raw.get("stamp_duty_buy_rate", 0.0)),
        stt_options_sell_rate=float(raw.get("stt_options_sell_rate", 0.0)),
        other_fees={str(k): float(v) for k, v in other.items()},
    )


__all__ = ["FeeConfig", "FeeRow", "fees_for_execution", "load_fee_config"]
