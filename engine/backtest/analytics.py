from __future__ import annotations

import datetime as dt
from typing import Any, Dict, Iterable, List, Optional, Tuple

from engine.config import IST


def _coerce_ts(value: Any) -> Optional[dt.datetime]:
    if value is None:
        return None
    if isinstance(value, dt.datetime):
        return value.astimezone(IST) if value.tzinfo is not None else value.replace(tzinfo=IST)
    text = str(value).strip()
    if not text:
        return None
    try:
        ts = dt.datetime.fromisoformat(text)
    except Exception:
        return None
    if ts.tzinfo is None:
        ts = ts.replace(tzinfo=IST)
    return ts.astimezone(IST)


def _trade_date_from_row(row: Dict[str, Any]) -> Optional[str]:
    raw = row.get("trade_date")
    if raw:
        text = str(raw).strip()
        try:
            dt.date.fromisoformat(text)
            return text
        except Exception:
            pass
    closed = _coerce_ts(row.get("closed_at"))
    if closed is not None:
        return closed.date().isoformat()
    opened = _coerce_ts(row.get("opened_at"))
    if opened is not None:
        return opened.date().isoformat()
    stid = str(row.get("strategy_trade_id") or "").strip()
    if len(stid) >= 10:
        maybe_date = stid[:10]
        try:
            dt.date.fromisoformat(maybe_date)
            return maybe_date
        except Exception:
            return None
    return None


def _parse_opt_type(symbol: str) -> Optional[str]:
    sym = str(symbol or "").strip().upper()
    if sym.endswith("CE"):
        return "CE"
    if sym.endswith("PE"):
        return "PE"
    return None


def aggregate_strategy_trades(trades: Iterable[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    Group per-leg trade rows into StockMock-style strategy trades.

    Each leg trade row may contain:
      - strategy_trade_id
      - leg_id
      - leg_role
      - exit_reason
    """

    groups: Dict[str, Dict[str, Any]] = {}
    for row in trades:
        stid = str(row.get("strategy_trade_id") or "").strip()
        if not stid:
            continue
        opened_at = _coerce_ts(row.get("opened_at"))
        closed_at = _coerce_ts(row.get("closed_at"))
        gross = row.get("gross_pnl")
        if gross is None:
            gross = row.get("realized_pnl") or 0.0
        fees = row.get("fees") or 0.0
        net = row.get("net_pnl")
        if net is None:
            net = float(gross or 0.0) - float(fees or 0.0)
        leg_id = str(row.get("leg_id") or "").strip()
        leg_role = str(row.get("leg_role") or "").strip().lower()
        symbol = str(row.get("symbol") or "").strip()
        exit_reason = str(row.get("exit_reason") or "").strip()

        g = groups.get(stid)
        if g is None:
            g = {
                "strategy_trade_id": stid,
                "trade_date": _trade_date_from_row({"strategy_trade_id": stid}),
                "opened_at": opened_at,
                "closed_at": closed_at,
                "gross_pnl": 0.0,
                "fees": 0.0,
                "net_pnl": 0.0,
                "legs": 0,
                "_leg_ids": set(),
                "_leg_roles": set(),
                "_symbols": set(),
                "_exit_reasons": set(),
            }
            groups[stid] = g

        if opened_at is not None:
            if g.get("opened_at") is None or (isinstance(g.get("opened_at"), dt.datetime) and opened_at < g["opened_at"]):
                g["opened_at"] = opened_at
        if closed_at is not None:
            if g.get("closed_at") is None or (isinstance(g.get("closed_at"), dt.datetime) and closed_at > g["closed_at"]):
                g["closed_at"] = closed_at

        g["gross_pnl"] = float(g.get("gross_pnl") or 0.0) + float(gross or 0.0)
        g["fees"] = float(g.get("fees") or 0.0) + float(fees or 0.0)
        g["net_pnl"] = float(g.get("net_pnl") or 0.0) + float(net or 0.0)
        g["legs"] = int(g.get("legs") or 0) + 1
        if leg_id:
            g["_leg_ids"].add(leg_id)
        if leg_role:
            g["_leg_roles"].add(leg_role)
        if symbol:
            g["_symbols"].add(symbol)
        if exit_reason:
            g["_exit_reasons"].add(exit_reason)

    out: List[Dict[str, Any]] = []
    for stid, g in groups.items():
        leg_ids = sorted(list(g.pop("_leg_ids", set())))
        leg_roles = sorted(list(g.pop("_leg_roles", set())))
        symbols = sorted(list(g.pop("_symbols", set())))
        reasons = sorted(list(g.pop("_exit_reasons", set())))
        g["leg_ids"] = leg_ids
        if leg_roles:
            g["leg_roles"] = leg_roles
        g["symbols"] = symbols
        if reasons:
            g["exit_reason"] = reasons[0] if len(reasons) == 1 else "MIXED"
            g["exit_reasons"] = reasons
        out.append(g)

    def _sort_key(item: Dict[str, Any]) -> Tuple[str, dt.datetime, str]:
        opened = _coerce_ts(item.get("opened_at")) or dt.datetime.min.replace(tzinfo=dt.timezone.utc)
        return (str(item.get("trade_date") or ""), opened, str(item.get("strategy_trade_id") or ""))

    out.sort(key=_sort_key)
    return out


def compute_daily_metrics(trades: Iterable[Dict[str, Any]]) -> Dict[str, Any]:
    """
    Compute daily P&L metrics from a list of trade rows.

    The input should be *strategy-level* trades to avoid double counting multi-leg strategies.
    """

    by_day: Dict[str, Dict[str, Any]] = {}
    for row in trades:
        trade_date = _trade_date_from_row(row)
        if not trade_date:
            continue
        g = by_day.get(trade_date)
        if g is None:
            g = {
                "trade_date": trade_date,
                "daily_gross_pnl": 0.0,
                "daily_fees": 0.0,
                "daily_net_pnl": 0.0,
                "trades": 0,
            }
            by_day[trade_date] = g
        gross = row.get("gross_pnl")
        if gross is None:
            gross = row.get("realized_pnl") or 0.0
        fees = row.get("fees") or 0.0
        net = row.get("net_pnl")
        if net is None:
            net = float(gross or 0.0) - float(fees or 0.0)
        g["daily_gross_pnl"] = float(g["daily_gross_pnl"]) + float(gross or 0.0)
        g["daily_fees"] = float(g["daily_fees"]) + float(fees or 0.0)
        g["daily_net_pnl"] = float(g["daily_net_pnl"]) + float(net or 0.0)
        g["trades"] = int(g["trades"]) + 1

    rows = [by_day[k] for k in sorted(by_day.keys())]
    cumulative = 0.0
    win_days = 0
    loss_days = 0
    flat_days = 0
    max_consecutive_losses = 0
    cur_loss_streak = 0

    for r in rows:
        daily = float(r.get("daily_net_pnl") or 0.0)
        cumulative += daily
        r["cumulative_pnl"] = float(cumulative)
        if daily > 0:
            win_days += 1
            cur_loss_streak = 0
        elif daily < 0:
            loss_days += 1
            cur_loss_streak += 1
            max_consecutive_losses = max(max_consecutive_losses, cur_loss_streak)
        else:
            flat_days += 1
            cur_loss_streak = 0

    return {
        "daily": rows,
        "summary": {
            "days": len(rows),
            "win_days": int(win_days),
            "loss_days": int(loss_days),
            "flat_days": int(flat_days),
            "max_consecutive_losses": int(max_consecutive_losses),
        },
    }


def compute_monthly_heatmap(daily_rows: Iterable[Dict[str, Any]]) -> Dict[str, Any]:
    """
    Roll up daily rows into a month-level summary.
    """

    by_month: Dict[Tuple[int, int], Dict[str, Any]] = {}
    for row in daily_rows:
        trade_date = str(row.get("trade_date") or "").strip()
        if not trade_date:
            continue
        try:
            d = dt.date.fromisoformat(trade_date)
        except Exception:
            continue
        key = (int(d.year), int(d.month))
        g = by_month.get(key)
        if g is None:
            g = {
                "year": int(d.year),
                "month": int(d.month),
                "month_label": f"{d.year:04d}-{d.month:02d}",
                "total_gross_pnl": 0.0,
                "total_fees": 0.0,
                "total_net_pnl": 0.0,
                "days": 0,
                "winning_days": 0,
            }
            by_month[key] = g

        gross = float(row.get("daily_gross_pnl") or 0.0)
        fees = float(row.get("daily_fees") or 0.0)
        net = float(row.get("daily_net_pnl") or 0.0)

        g["total_gross_pnl"] = float(g["total_gross_pnl"]) + gross
        g["total_fees"] = float(g["total_fees"]) + fees
        g["total_net_pnl"] = float(g["total_net_pnl"]) + net
        g["days"] = int(g["days"]) + 1
        if net > 0:
            g["winning_days"] = int(g["winning_days"]) + 1

    rows = [by_month[k] for k in sorted(by_month.keys())]
    for r in rows:
        days = int(r.get("days") or 0)
        wins = int(r.get("winning_days") or 0)
        r["winning_days_pct"] = (float(wins) / float(days) * 100.0) if days > 0 else 0.0

    return {"monthly": rows}


def compute_leg_breakdown(trades: Iterable[Dict[str, Any]]) -> Dict[str, Any]:
    """
    Compute leg-wise P&L breakdown from per-leg trade rows.
    """

    by_leg: Dict[Tuple[str, str, str], Dict[str, Any]] = {}
    by_opt_type: Dict[str, Dict[str, Any]] = {}

    for row in trades:
        symbol = str(row.get("symbol") or "").strip()
        if not symbol:
            continue
        leg_id = str(row.get("leg_id") or "").strip() or symbol
        leg_role = str(row.get("leg_role") or "").strip().lower()
        opt_type = _parse_opt_type(symbol) or ""

        gross = row.get("gross_pnl")
        if gross is None:
            gross = row.get("realized_pnl") or 0.0
        fees = row.get("fees") or 0.0
        net = row.get("net_pnl")
        if net is None:
            net = float(gross or 0.0) - float(fees or 0.0)

        key = (leg_id, leg_role, opt_type)
        g = by_leg.get(key)
        if g is None:
            g = {
                "leg_id": leg_id,
                "leg_role": leg_role or None,
                "opt_type": opt_type or None,
                "trades": 0,
                "gross_pnl": 0.0,
                "fees": 0.0,
                "net_pnl": 0.0,
            }
            by_leg[key] = g
        g["trades"] = int(g["trades"]) + 1
        g["gross_pnl"] = float(g["gross_pnl"]) + float(gross or 0.0)
        g["fees"] = float(g["fees"]) + float(fees or 0.0)
        g["net_pnl"] = float(g["net_pnl"]) + float(net or 0.0)

        if opt_type:
            o = by_opt_type.get(opt_type)
            if o is None:
                o = {"opt_type": opt_type, "trades": 0, "gross_pnl": 0.0, "fees": 0.0, "net_pnl": 0.0}
                by_opt_type[opt_type] = o
            o["trades"] = int(o["trades"]) + 1
            o["gross_pnl"] = float(o["gross_pnl"]) + float(gross or 0.0)
            o["fees"] = float(o["fees"]) + float(fees or 0.0)
            o["net_pnl"] = float(o["net_pnl"]) + float(net or 0.0)

    legs = [by_leg[k] for k in sorted(by_leg.keys(), key=lambda x: (x[0], x[1], x[2]))]
    opt_types = [by_opt_type[k] for k in sorted(by_opt_type.keys())]
    return {"by_leg": legs, "by_opt_type": opt_types}


__all__ = [
    "aggregate_strategy_trades",
    "compute_daily_metrics",
    "compute_leg_breakdown",
    "compute_monthly_heatmap",
]

