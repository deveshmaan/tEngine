import unittest

import pandas as pd

from backtesting.execution import ExecutionConfig, run_backtest


class TestExecution(unittest.TestCase):
    def test_simple_close_fill_known_pnl(self) -> None:
        prices = pd.DataFrame(
            {
                "open": [10.0, 11.0, 12.0],
                "close": [10.0, 11.0, 12.0],
            },
            index=pd.to_datetime(["2020-01-01", "2020-01-02", "2020-01-03"]),
        )
        signals = pd.Series([0, 1, 0], index=prices.index)
        cfg = ExecutionConfig(initial_cash=100.0, fill_method="close", fee_per_trade=0.0)

        result = run_backtest(prices, signals, cfg)

        self.assertEqual(len(result.trades_df), 1)
        trade = result.trades_df.iloc[0]
        self.assertEqual(trade["side"], "long")
        self.assertAlmostEqual(float(trade["gross_pnl"]), 1.0, places=10)
        self.assertAlmostEqual(float(trade["net_pnl"]), 1.0, places=10)

        # Final equity should be initial + pnl.
        final_equity = float(result.equity_curve_df["equity"].iloc[-1])
        self.assertAlmostEqual(final_equity, 101.0, places=10)

    def test_flip_long_to_short_close_fill(self) -> None:
        prices = pd.DataFrame(
            {
                "open": [10.0, 12.0, 11.0],
                "close": [10.0, 12.0, 11.0],
            },
            index=pd.to_datetime(["2020-01-01", "2020-01-02", "2020-01-03"]),
        )
        signals = pd.Series([1, -1, 0], index=prices.index)
        cfg = ExecutionConfig(initial_cash=100.0, fill_method="close", fee_per_trade=0.0, allow_short=True)

        result = run_backtest(prices, signals, cfg)

        # Two completed trades: long then short.
        self.assertEqual(len(result.trades_df), 2)
        self.assertEqual(result.trades_df.iloc[0]["side"], "long")
        self.assertEqual(result.trades_df.iloc[1]["side"], "short")

        long_trade = result.trades_df.iloc[0]
        short_trade = result.trades_df.iloc[1]
        self.assertAlmostEqual(float(long_trade["net_pnl"]), 2.0, places=10)  # 10 -> 12
        self.assertAlmostEqual(float(short_trade["net_pnl"]), 1.0, places=10)  # 12 -> 11 (short)

        final_equity = float(result.equity_curve_df["equity"].iloc[-1])
        self.assertAlmostEqual(final_equity, 103.0, places=10)

    def test_flip_short_to_long_close_fill(self) -> None:
        prices = pd.DataFrame(
            {
                "open": [10.0, 8.0, 9.0],
                "close": [10.0, 8.0, 9.0],
            },
            index=pd.to_datetime(["2020-01-01", "2020-01-02", "2020-01-03"]),
        )
        signals = pd.Series([-1, 1, 0], index=prices.index)
        cfg = ExecutionConfig(initial_cash=100.0, fill_method="close", fee_per_trade=0.0, allow_short=True)

        result = run_backtest(prices, signals, cfg)

        self.assertEqual(len(result.trades_df), 2)
        self.assertEqual(result.trades_df.iloc[0]["side"], "short")
        self.assertEqual(result.trades_df.iloc[1]["side"], "long")

        short_trade = result.trades_df.iloc[0]
        long_trade = result.trades_df.iloc[1]
        self.assertAlmostEqual(float(short_trade["net_pnl"]), 2.0, places=10)  # 10 -> 8 (short)
        self.assertAlmostEqual(float(long_trade["net_pnl"]), 1.0, places=10)  # 8 -> 9

        final_equity = float(result.equity_curve_df["equity"].iloc[-1])
        self.assertAlmostEqual(final_equity, 103.0, places=10)

    def test_next_open_last_bar_signal_skipped(self) -> None:
        # Signal changes to flat on the last bar, but next_open cannot execute it.
        prices = pd.DataFrame(
            {
                "open": [100.0, 101.0, 102.0],
                "close": [100.0, 101.0, 102.0],
            },
            index=pd.to_datetime(["2020-01-01", "2020-01-02", "2020-01-03"]),
        )
        signals = pd.Series([1, 1, 0], index=prices.index)
        cfg = ExecutionConfig(initial_cash=1000.0, fill_method="next_open", fee_per_trade=0.0)

        result = run_backtest(prices, signals, cfg)

        # The long entry happens on 2020-01-02 open (101), but the close on the last bar is skipped.
        self.assertEqual(len(result.trades_df), 0)
        self.assertEqual(int(result.equity_curve_df["position"].iloc[-1]), 1)

    def test_fees_and_slippage_reduce_pnl(self) -> None:
        prices = pd.DataFrame(
            {
                "open": [100.0, 110.0],
                "close": [100.0, 110.0],
            },
            index=pd.to_datetime(["2020-01-01", "2020-01-02"]),
        )
        signals = pd.Series([1, 0], index=prices.index)
        cfg = ExecutionConfig(
            initial_cash=1000.0,
            fill_method="close",
            slippage_bps=100.0,  # 1%
            fee_per_trade=1.0,
        )

        result = run_backtest(prices, signals, cfg)

        self.assertEqual(len(result.trades_df), 1)
        trade = result.trades_df.iloc[0]
        self.assertAlmostEqual(float(trade["fees"]), 2.0, places=10)
        # Expected net PnL:
        # entry buy @ 100*(1+0.01)=101
        # exit sell @ 110*(1-0.01)=108.9
        # gross = 7.9, fees = 2.0 -> net = 5.9
        self.assertAlmostEqual(float(trade["gross_pnl"]), 7.9, places=10)
        self.assertAlmostEqual(float(trade["net_pnl"]), 5.9, places=10)

        final_equity = float(result.equity_curve_df["equity"].iloc[-1])
        self.assertAlmostEqual(final_equity, 1005.9, places=10)


if __name__ == "__main__":
    unittest.main()

