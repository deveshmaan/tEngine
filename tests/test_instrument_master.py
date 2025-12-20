from __future__ import annotations

import datetime as dt
import gzip
import json
from pathlib import Path

import pytest

from engine.backtest.instrument_master import InstrumentMaster


IST = dt.timezone(dt.timedelta(hours=5, minutes=30), name="Asia/Kolkata")


def _expiry_ms_for(date_str: str) -> int:
    d = dt.date.fromisoformat(date_str)
    # match the repo master convention (typically 23:59:59 IST)
    expiry_dt = dt.datetime.combine(d, dt.time(23, 59, 59), tzinfo=IST)
    return int(expiry_dt.astimezone(dt.timezone.utc).timestamp() * 1000)


def test_instrument_master_resolves_option_key(tmp_path: Path) -> None:
    gz_path = tmp_path / "master.json.gz"
    expiry = "2024-01-04"
    payload = [
        {
            "instrument_key": "NSE_FO|111",
            "instrument_type": "CE",
            "underlying_key": "NSE_INDEX|Nifty 50",
            "strike_price": 19500.0,
            "expiry": _expiry_ms_for(expiry),
        },
        {
            "instrument_key": "NSE_FO|222",
            "instrument_type": "PE",
            "underlying_key": "NSE_INDEX|Nifty 50",
            "strike_price": 19500.0,
            "expiry": _expiry_ms_for(expiry),
        },
    ]
    with gzip.open(gz_path, "wt", encoding="utf-8") as handle:
        json.dump(payload, handle)

    master = InstrumentMaster(path=gz_path)
    assert master.resolve_option_key(underlying_key="NSE_INDEX|Nifty 50", expiry=expiry, opt_type="CE", strike=19500) == "NSE_FO|111"
    assert master.resolve_option_key(underlying_key="NSE_INDEX|Nifty 50", expiry=expiry, opt_type="PE", strike=19500) == "NSE_FO|222"
    assert master.strikes_for(underlying_key="NSE_INDEX|Nifty 50", expiry=expiry, opt_type="CE") == [19500]

    with pytest.raises(KeyError):
        master.resolve_option_key(underlying_key="NSE_INDEX|Nifty 50", expiry=expiry, opt_type="CE", strike=19600)

