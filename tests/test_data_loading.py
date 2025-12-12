import tempfile
import unittest
from pathlib import Path

import pandas as pd

from backtesting.data_loading import OhlcvSchemaError, load_ohlcv


class TestDataLoading(unittest.TestCase):
    def test_csv_loading_to_normalized_frame(self) -> None:
        # A minimal OHLCV CSV with mixed-case column names.
        raw = pd.DataFrame(
            {
                "timestamp": ["2020-01-01 09:15:00", "2020-01-01 09:16:00"],
                "Open": [100.0, 101.0],
                "High": [102.0, 103.0],
                "Low": [99.5, 100.5],
                "Close": [101.5, 102.5],
                "Volume": [10, 20],
                # Extra columns must be preserved (not dropped).
                "symbol": ["TEST", "TEST"],
            }
        )

        with tempfile.TemporaryDirectory() as tmpdir:
            path = Path(tmpdir) / "ohlcv.csv"
            raw.to_csv(path, index=False)

            out = load_ohlcv(path, timestamp_col="timestamp")

        self.assertIsInstance(out.index, pd.DatetimeIndex)
        self.assertEqual(list(out.columns)[:5], ["open", "high", "low", "close", "volume"])
        self.assertIn("symbol", out.columns)
        self.assertEqual(out.loc[pd.Timestamp("2020-01-01 09:15:00"), "close"], 101.5)

    def test_dataframe_input_with_datetimeindex(self) -> None:
        idx = pd.to_datetime(["2020-01-01 09:15:00", "2020-01-01 09:16:00"])
        raw = pd.DataFrame(
            {
                "open": [1.0, 2.0],
                "high": [1.5, 2.5],
                "low": [0.5, 1.5],
                "close": [1.25, 2.25],
                "volume": [100, 200],
            },
            index=idx,
        )

        out = load_ohlcv(raw, sort=True)
        self.assertIsInstance(out.index, pd.DatetimeIndex)
        self.assertEqual(out.index.name, "timestamp")
        self.assertEqual(out.iloc[0]["open"], 1.0)

    def test_missing_required_columns_raises(self) -> None:
        idx = pd.to_datetime(["2020-01-01 09:15:00"])
        raw = pd.DataFrame(
            {
                "open": [1.0],
                "high": [1.0],
                "low": [1.0],
                "close": [1.0],
                # volume is missing
            },
            index=idx,
        )

        with self.assertRaises(OhlcvSchemaError):
            load_ohlcv(raw)

    def test_duplicate_timestamps_keep_last(self) -> None:
        # Intentionally unsorted with duplicates for "2020-01-01 09:15:00".
        raw = pd.DataFrame(
            {
                "timestamp": ["2020-01-01 09:16:00", "2020-01-01 09:15:00", "2020-01-01 09:15:00"],
                "open": [1.0, 10.0, 10.0],
                "high": [1.0, 10.0, 10.0],
                "low": [1.0, 10.0, 10.0],
                "close": [1.0, 11.0, 99.0],  # last duplicate should win
                "volume": [1, 1, 1],
            }
        )

        out = load_ohlcv(raw, timestamp_col="timestamp", sort=True, drop_duplicates=True)
        self.assertEqual(len(out), 2)
        self.assertTrue(out.index.is_monotonic_increasing)
        self.assertEqual(out.loc[pd.Timestamp("2020-01-01 09:15:00"), "close"], 99.0)


if __name__ == "__main__":
    unittest.main()

