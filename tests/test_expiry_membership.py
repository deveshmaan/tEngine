from __future__ import annotations

import datetime as dt

import pytest

from engine import data as data_mod
from market.instrument_cache import InstrumentCache


class DummyCache:
    def __init__(self, expiries):
        self._expiries = expiries

    def list_expiries(self, symbol: str, kind: str | None = None):
        return self._expiries.get(symbol.upper(), [])

    def refresh_option_chain(self, symbol: str, expiry: str):
        return 0


@pytest.fixture(autouse=True)
def runtime_cache(monkeypatch):
    cache = DummyCache({"NIFTY": ["2025-11-25", "2025-12-02"]})
    InstrumentCache._runtime = cache  # type: ignore[attr-defined]
    yield
    InstrumentCache._runtime = None  # type: ignore[attr-defined]


def test_assert_valid_expiry_returns_member(monkeypatch):
    assert data_mod.assert_valid_expiry("NIFTY", "2025-11-25") == "2025-11-25"


def test_assert_valid_expiry_falls_back_future(monkeypatch):
    monkeypatch.setattr(
        data_mod,
        "engine_now",
        lambda tz=None: dt.datetime(2025, 11, 20, tzinfo=dt.timezone.utc),
    )
    assert data_mod.assert_valid_expiry("NIFTY", "2025-11-10") == "2025-11-25"
