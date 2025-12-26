from __future__ import annotations

from engine.backtest.analytics import aggregate_strategy_trades, compute_daily_metrics, compute_leg_breakdown, compute_monthly_heatmap


def test_compute_daily_metrics_counts_and_streak() -> None:
    trades = [
        {"trade_date": "2024-01-01", "gross_pnl": 100.0, "fees": 10.0, "net_pnl": 90.0},
        {"trade_date": "2024-01-02", "gross_pnl": -50.0, "fees": 5.0, "net_pnl": -55.0},
        {"trade_date": "2024-01-03", "gross_pnl": -1.0, "fees": 0.0, "net_pnl": -1.0},
        {"trade_date": "2024-01-04", "gross_pnl": 0.0, "fees": 0.0, "net_pnl": 0.0},
        {"trade_date": "2024-01-05", "gross_pnl": 10.0, "fees": 0.0, "net_pnl": 10.0},
    ]
    out = compute_daily_metrics(trades)
    daily = out["daily"]
    summary = out["summary"]

    assert [row["trade_date"] for row in daily] == ["2024-01-01", "2024-01-02", "2024-01-03", "2024-01-04", "2024-01-05"]
    assert [row["daily_net_pnl"] for row in daily] == [90.0, -55.0, -1.0, 0.0, 10.0]
    assert daily[-1]["cumulative_pnl"] == 44.0

    assert summary["days"] == 5
    assert summary["win_days"] == 2
    assert summary["loss_days"] == 2
    assert summary["flat_days"] == 1
    assert summary["max_consecutive_losses"] == 2


def test_compute_monthly_heatmap_rollup() -> None:
    daily_rows = [
        {"trade_date": "2024-01-01", "daily_net_pnl": 10.0, "daily_gross_pnl": 12.0, "daily_fees": 2.0},
        {"trade_date": "2024-01-02", "daily_net_pnl": -5.0, "daily_gross_pnl": -4.0, "daily_fees": 1.0},
        {"trade_date": "2024-02-01", "daily_net_pnl": 20.0, "daily_gross_pnl": 21.0, "daily_fees": 1.0},
    ]
    out = compute_monthly_heatmap(daily_rows)
    monthly = out["monthly"]
    assert [row["month_label"] for row in monthly] == ["2024-01", "2024-02"]
    jan = monthly[0]
    assert jan["total_net_pnl"] == 5.0
    assert jan["winning_days"] == 1
    assert jan["days"] == 2
    assert jan["winning_days_pct"] == 50.0


def test_leg_breakdown_groups_by_leg_and_opt_type() -> None:
    trades = [
        {"symbol": "NIFTY-2024-01-02-20000CE", "leg_id": "leg_ce", "gross_pnl": 10.0, "fees": 1.0, "net_pnl": 9.0},
        {"symbol": "NIFTY-2024-01-02-20000CE", "leg_id": "leg_ce", "gross_pnl": -5.0, "fees": 1.0, "net_pnl": -6.0},
        {"symbol": "NIFTY-2024-01-02-20000PE", "leg_id": "leg_pe", "gross_pnl": 20.0, "fees": 2.0, "net_pnl": 18.0},
    ]
    out = compute_leg_breakdown(trades)
    by_leg = out["by_leg"]
    assert len(by_leg) == 2
    ce = next(r for r in by_leg if r["leg_id"] == "leg_ce")
    assert ce["opt_type"] == "CE"
    assert ce["trades"] == 2
    assert ce["net_pnl"] == 3.0

    by_opt = out["by_opt_type"]
    assert {r["opt_type"] for r in by_opt} == {"CE", "PE"}
    ce_total = next(r for r in by_opt if r["opt_type"] == "CE")
    assert ce_total["net_pnl"] == 3.0


def test_aggregate_strategy_trades_sums_legs() -> None:
    trade_log = [
        {"symbol": "NIFTY-2024-01-02-20000CE", "strategy_trade_id": "2024-01-01-001", "leg_id": "leg_ce", "gross_pnl": 10.0, "fees": 1.0, "net_pnl": 9.0},
        {"symbol": "NIFTY-2024-01-02-20000PE", "strategy_trade_id": "2024-01-01-001", "leg_id": "leg_pe", "gross_pnl": 20.0, "fees": 2.0, "net_pnl": 18.0},
    ]
    grouped = aggregate_strategy_trades(trade_log)
    assert len(grouped) == 1
    g = grouped[0]
    assert g["strategy_trade_id"] == "2024-01-01-001"
    assert g["legs"] == 2
    assert g["gross_pnl"] == 30.0
    assert g["fees"] == 3.0
    assert g["net_pnl"] == 27.0

