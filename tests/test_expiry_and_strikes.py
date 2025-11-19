import datetime as dt

import pytest

from engine.config import IST
from engine import data as data_mod
from engine.data import pick_strike_from_spot, resolve_next_expiry
from market.instrument_cache import InstrumentCache


class FakeSession:
    def __init__(self, expiries):
        self._expiries = expiries

    def get_option_contracts(self, instrument_key, expiry_date=None):
        return {"data": {"expiries": self._expiries[instrument_key]}}

    def get_option_chain(self, instrument_key, expiry_date):
        strike = 22650 if "Nifty" in instrument_key else 48800
        return {
            "data": {
                "call": {
                    "data": [
                        {
                            "instrument_key": f"{instrument_key}-{expiry_date}-CE",
                            "strike": strike,
                            "lot_size": 50,
                            "tick_size": 0.05,
                            "price_band_lower": 10.0,
                            "price_band_upper": 5000.0,
                        }
                    ]
                },
                "put": {
                    "data": [
                        {
                            "instrument_key": f"{instrument_key}-{expiry_date}-PE",
                            "strike": strike,
                            "lot_size": 50,
                            "tick_size": 0.05,
                            "price_band_lower": 10.0,
                            "price_band_upper": 5000.0,
                        }
                    ]
                },
            }
        }


@pytest.fixture
def app_config_file(tmp_path, monkeypatch):
    cfg_path = tmp_path / "app.yml"
    cfg_path.write_text(
        """
        data:
          holidays:
            - "2024-07-04"
        """,
        encoding="utf-8",
    )
    monkeypatch.setenv("APP_CONFIG_PATH", str(cfg_path))
    data_mod._reset_app_config_cache()
    yield cfg_path
    data_mod._reset_app_config_cache()
    monkeypatch.delenv("APP_CONFIG_PATH", raising=False)


@pytest.fixture
def instrument_cache(tmp_path, app_config_file):
    expiries = {
        "NSE_INDEX|Nifty 50": ["2024-07-04", "2024-07-11", "2024-07-25"],
        "NSE_INDEX|Nifty Bank": ["2024-07-25"],
    }
    cache_path = tmp_path / "cache.sqlite"
    cache = InstrumentCache(str(cache_path), session_factory=lambda: FakeSession(expiries))
    yield cache
    cache.close()
    InstrumentCache._runtime = None  # type: ignore[attr-defined]


def test_weekly_expiry_skips_holiday(instrument_cache):
    now = dt.datetime(2024, 7, 1, 9, 30, tzinfo=IST)
    expiry = resolve_next_expiry("NIFTY", now, kind="weekly")
    assert expiry == "2024-07-11"


def test_weekly_fallback_to_monthly(instrument_cache):
    now = dt.datetime(2024, 7, 1, 9, 30, tzinfo=IST)
    expiry = resolve_next_expiry("BANKNIFTY", now, kind="weekly")
    assert expiry == "2024-07-25"


def test_pick_strike_rounding():
    assert pick_strike_from_spot(spot=22654, step=50) == 22650
    assert pick_strike_from_spot(spot=48765, step=100) == 48800
