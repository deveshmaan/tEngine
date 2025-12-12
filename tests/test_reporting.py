import unittest

import pandas as pd

from backtesting.execution import ExecutionConfig, run_backtest
from backtesting.reporting import compute_drawdown, compute_equity_metrics, compute_trade_metrics, summarize_backtest


class TestReporting(unittest.TestCase):
    def test_drawdown_computation_known_series(self) -> None:
        equity = pd.Series([100.0, 110.0, 105.0, 120.0, 90.0], index=pd.RangeIndex(0, 5))
        dd = compute_drawdown(equity)

        expected_peak = [100.0, 110.0, 110.0, 120.0, 120.0]
        expected_dd = [0.0, 0.0, 105.0 / 110.0 - 1.0, 0.0, 90.0 / 120.0 - 1.0]

        self.assertEqual(dd["peak"].tolist(), expected_peak)
        for got, exp in zip(dd["drawdown"].tolist(), expected_dd):
            self.assertAlmostEqual(float(got), float(exp), places=10)

        self.assertAlmostEqual(float(dd["drawdown"].min()), -0.25, places=10)

    def test_sharpe_zero_volatility(self) -> None:
        equity = pd.Series([100.0, 100.0, 100.0], index=pd.RangeIndex(0, 3))
        metrics = compute_equity_metrics(equity, periods_per_year=252, risk_free_rate=0.0)
        self.assertAlmostEqual(float(metrics["volatility"]), 0.0, places=10)
        self.assertAlmostEqual(float(metrics["sharpe"]), 0.0, places=10)

    def test_empty_trades_handled(self) -> None:
        trades = pd.DataFrame(columns=["net_pnl"])
        metrics = compute_trade_metrics(trades)
        self.assertEqual(float(metrics["num_trades"]), 0.0)
        self.assertEqual(float(metrics["win_rate"]), 0.0)

    def test_summarize_backtest_keys(self) -> None:
        prices = pd.DataFrame(
            {"open": [10.0, 11.0], "close": [10.0, 11.0]},
            index=pd.to_datetime(["2020-01-01", "2020-01-02"]),
        )
        signals = pd.Series([0, 0], index=prices.index)
        result = run_backtest(prices, signals, ExecutionConfig(initial_cash=100.0, fill_method="close"))

        summary = summarize_backtest(result, periods_per_year=252, risk_free_rate=0.0)
        for key in [
            "total_return",
            "max_drawdown",
            "volatility",
            "sharpe",
            "num_trades",
            "win_rate",
            "avg_win",
            "avg_loss",
            "profit_factor",
        ]:
            self.assertIn(key, summary)


if __name__ == "__main__":
    unittest.main()

