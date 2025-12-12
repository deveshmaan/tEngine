import unittest

import pandas as pd

from backtesting.signals import ALLOWED_SIGNALS, generate_signals


class _ClassStrategy:
    def generate_signals(self, data: pd.DataFrame) -> pd.Series:
        # A simple class-based strategy: long if close is increasing, short if decreasing.
        close = data["close"]
        sig = pd.Series(0, index=data.index, dtype="int64")
        sig[close.diff() > 0] = 1
        sig[close.diff() < 0] = -1
        return sig


class TestSignals(unittest.TestCase):
    def setUp(self) -> None:
        self.data = pd.DataFrame(
            {
                "close": [100.0, 101.0, 100.5, 102.0],
            },
            index=pd.to_datetime(
                ["2020-01-01 09:15:00", "2020-01-01 09:16:00", "2020-01-01 09:17:00", "2020-01-01 09:18:00"]
            ),
        )

    def test_callable_strategy_returns_list(self) -> None:
        def strategy(df: pd.DataFrame):
            # Return a list-like output with mixed types.
            return ["hold", "buy", "sell", None]

        out = generate_signals(self.data, strategy, price_col="close", fill_value=0)
        self.assertTrue(out.index.equals(self.data.index))
        self.assertEqual(out.dtype.kind, "i")
        self.assertEqual(out.tolist(), [0, 1, -1, 0])

    def test_callable_strategy_returns_series(self) -> None:
        def strategy(df: pd.DataFrame) -> pd.Series:
            # Return a Series but with a default RangeIndex (positional alignment).
            return pd.Series([0, 1, 0, -1])

        out = generate_signals(self.data, strategy, price_col="close")
        self.assertTrue(out.index.equals(self.data.index))
        self.assertEqual(out.tolist(), [0, 1, 0, -1])

    def test_class_strategy_generate_signals(self) -> None:
        strat = _ClassStrategy()
        out = generate_signals(self.data, strat, price_col="close", fill_value=0)
        self.assertTrue(out.index.equals(self.data.index))
        self.assertTrue(set(out.unique().tolist()).issubset(set(ALLOWED_SIGNALS)))

    def test_bad_signals_raise(self) -> None:
        def strategy(df: pd.DataFrame):
            # 2 is invalid; must raise ValueError after processing.
            return [0, 2, 0, 0]

        with self.assertRaises(ValueError):
            generate_signals(self.data, strategy, price_col="close")

    def test_index_alignment_error_for_non_rangeindex(self) -> None:
        def strategy(df: pd.DataFrame) -> pd.Series:
            # Same length but mismatched index that is not a default RangeIndex.
            idx = pd.to_datetime(["2020-01-01", "2020-01-02", "2020-01-03", "2020-01-04"])
            return pd.Series([0, 1, 0, -1], index=idx)

        with self.assertRaises(ValueError):
            generate_signals(self.data, strategy, price_col="close")


if __name__ == "__main__":
    unittest.main()

