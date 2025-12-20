from __future__ import annotations

import datetime as dt

from persistence import SQLiteStore
from persistence.backtest_store import BacktestStore


def test_backtest_store_roundtrip(tmp_path) -> None:
    db_path = tmp_path / "engine_state.sqlite"
    run_id = "backtest-test-1"

    ist = dt.timezone(dt.timedelta(hours=5, minutes=30))
    exec_ts = dt.datetime(2024, 1, 1, 9, 15, tzinfo=ist)

    # Seed engine tables that the run loader reads (executions + cost ledger).
    store = SQLiteStore(db_path, run_id=run_id)
    try:
        store.record_execution(
            "o1",
            exec_id="e1",
            symbol="OPT",
            side="BUY",
            qty=50,
            price=100.1,
            raw_price=100.0,
            effective_price=100.1,
            ts=exec_ts,
            venue="SIM",
        )
        store.record_cost("e1", category="brokerage", amount=2.0, ts=exec_ts)
    finally:
        store.close()

    results = {
        "run_id": run_id,
        "strategy": "DummyStrategy",
        "period": "2024-01-01 to 2024-01-01",
        "interval": "1minute",
        "underlying": "NSE_INDEX|Nifty 50",
        "execution": {"fill_model": "same_tick", "latency_ms": 0, "allow_partial_fills": False},
        "costs": {"slippage_model": "ticks", "slippage_ticks": 2, "slippage_bps": 0.0, "spread_bps": 0.0},
        "gross_pnl": 10.0,
        "total_fees": 2.0,
        "net_pnl": 8.0,
        "realized_pnl": 10.0,
        "unrealized_pnl": 0.0,
        "trades": 1,
        "wins": 1,
        "win_rate": 1.0,
        "candles": 1,
        "ticks": 4,
        "equity_curve": [
            {"ts": exec_ts.isoformat(), "net": 0.0},
            {"ts": (exec_ts + dt.timedelta(minutes=1)).isoformat(), "net": 8.0},
        ],
        "trade_log": [
            {
                "trade_id": "t1",
                "symbol": "OPT",
                "opened_at": exec_ts.isoformat(),
                "closed_at": (exec_ts + dt.timedelta(minutes=1)).isoformat(),
                "gross_pnl": 10.0,
                "fees": 2.0,
                "net_pnl": 8.0,
            }
        ],
        "orders": [
            {
                "order_id": "o1",
                "strategy": "dummy",
                "symbol": "OPT",
                "side": "BUY",
                "qty": 50,
                "fill_qty": 50,
                "avg_raw_price": 100.0,
                "avg_effective_price": 100.1,
                "state": "FILLED",
                "last_update": exec_ts.isoformat(),
            }
        ],
        "errors": [],
    }

    with BacktestStore(db_path) as bt:
        bt.save_run(run_id=run_id, results=dict(results), spec_json=None)

    with BacktestStore(db_path) as bt:
        runs = bt.list_runs(limit=10)
        assert any(r.run_id == run_id for r in runs)
        loaded = bt.load_run(run_id)

    assert loaded["run_id"] == run_id
    assert loaded["net_pnl"] == 8.0
    assert loaded["gross_pnl"] == 10.0
    assert loaded["total_fees"] == 2.0
    assert loaded["orders"] and loaded["orders"][0]["order_id"] == "o1"
    assert loaded["executions"] and loaded["executions"][0]["exec_id"] == "e1"

