import datetime as dt

import pytest
from upstox_client.rest import ApiException

from engine.config import IST
from engine import data as data_mod
from engine.data import pick_strike_from_spot, resolve_next_expiry
from market.instrument_cache import InstrumentCache


class FakeSession:
    def __init__(self, expiries):
        self._expiries = expiries

    def get_option_contracts(self, instrument_key, expiry_date=None):
        entries = []
        for idx, expiry in enumerate(self._expiries[instrument_key]):
            strike = 20000 + idx * 50
            entries.append(
                {
                    "instrument_key": f"{instrument_key}-{idx}-CE",
                    "expiry": expiry,
                    "instrument_type": "CE",
                    "strike_price": strike,
                    "lot_size": 50,
                    "tick_size": 0.05,
                }
            )
            entries.append(
                {
                    "instrument_key": f"{instrument_key}-{idx}-PE",
                    "expiry": expiry,
                    "instrument_type": "PE",
                    "strike_price": strike,
                    "lot_size": 50,
                    "tick_size": 0.05,
                }
            )
        if expiry_date:
            entries = [entry for entry in entries if entry["expiry"] == expiry_date]
        return {"status": "success", "data": entries}

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


class InvalidDateSession(FakeSession):
    def __init__(self, expiries, invalid_expiry):
        super().__init__(expiries)
        self._invalid_expiry = invalid_expiry
        self.contract_calls: list[str | None] = []

    def get_option_contracts(self, instrument_key, expiry_date=None):
        self.contract_calls.append(expiry_date)
        if expiry_date == self._invalid_expiry:
            # Drop the invalid expiry to mirror broker behaviour.
            self._expiries[instrument_key] = [exp for exp in self._expiries[instrument_key] if exp != expiry_date]
            exc = ApiException(status=400, reason="Bad Request")
            exc.body = '{"errors":[{"errorCode":"UDAPI1088","message":"Invalid date"}]}'
            raise exc
        return super().get_option_contracts(instrument_key, expiry_date)


class ChainFallbackSession(FakeSession):
    def __init__(self, expiries):
        super().__init__(expiries)
        self.calls: list[str] = []

    def get_option_contracts(self, instrument_key, expiry_date=None):
        if expiry_date is None:
            return super().get_option_contracts(instrument_key, expiry_date)
        # Return an empty payload to force the cache to try the option chain endpoint.
        return {"status": "success", "data": []}

    def get_option_chain(self, instrument_key, expiry_date):
        self.calls.append(expiry_date)
        if len(self.calls) == 1:
            self._expiries[instrument_key] = [exp for exp in self._expiries[instrument_key] if exp != expiry_date]
            exc = ApiException(status=400, reason="Bad Request")
            exc.body = '{"errors":[{"errorCode":"UDAPI1088","message":"Invalid date"}]}'
            raise exc
        return super().get_option_chain(instrument_key, expiry_date)


class InvalidContractsSession(FakeSession):
    def get_option_contracts(self, instrument_key, expiry_date=None):
        exc = ApiException(status=400, reason="Bad Request")
        exc.body = '{"errors":[{"errorCode":"UDAPI1088","message":"Invalid date"}]}'
        raise exc


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
    cache = InstrumentCache(
        str(cache_path),
        session_factory=lambda: FakeSession(expiries),
        enable_remote_expiry_probe=True,
    )
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


def test_refresh_expiry_recovers_after_invalid_date(tmp_path, monkeypatch):
    expiries = {"NSE_INDEX|Nifty 50": ["2024-07-04", "2024-07-11"]}
    session = InvalidDateSession(expiries, "2024-07-04")
    cache_path = tmp_path / "invalid.sqlite"
    cache = InstrumentCache(
        str(cache_path),
        session_factory=lambda: session,
        enable_remote_expiry_probe=True,
    )
    monkeypatch.setattr(
        data_mod,
        "engine_now",
        lambda tz=None: dt.datetime(2024, 7, 3, tzinfo=IST),
    )
    inserted = cache.refresh_expiry("NIFTY", "2024-07-04")
    expiries_after = cache.list_expiries("NIFTY")
    cache.close()
    InstrumentCache._runtime = None  # type: ignore[attr-defined]
    assert inserted == 2
    assert "2024-07-04" in session.contract_calls
    assert "2024-07-11" in session.contract_calls
    assert expiries_after == ["2024-07-11"]


def test_option_chain_fallback_handles_invalid_date(tmp_path, monkeypatch):
    expiries = {"NSE_INDEX|Nifty 50": ["2024-07-04", "2024-07-11"]}
    session = ChainFallbackSession(expiries)
    cache_path = tmp_path / "chain.sqlite"
    cache = InstrumentCache(
        str(cache_path),
        session_factory=lambda: session,
        enable_remote_expiry_probe=True,
    )
    monkeypatch.setattr(
        data_mod,
        "engine_now",
        lambda tz=None: dt.datetime(2024, 7, 3, tzinfo=IST),
    )
    inserted = cache.refresh_expiry("NIFTY", "2024-07-04")
    cache.close()
    InstrumentCache._runtime = None  # type: ignore[attr-defined]
    assert inserted == 2
    assert session.calls == ["2024-07-04", "2024-07-11"]


def test_refresh_expiries_fallback_to_guess_on_invalid_contracts(tmp_path, monkeypatch, app_config_file):
    cache_path = tmp_path / "contract-fallback.sqlite"
    guesses = ["2024-07-04", "2024-07-11", "2024-07-18"]
    monkeypatch.setattr(
        InstrumentCache,
        "_guess_future_expiries",
        lambda self, weeks=6, months=3: guesses,
    )
    session = InvalidContractsSession({"NSE_INDEX|Nifty 50": [], "NSE_INDEX|Nifty Bank": []})
    cache = InstrumentCache(
        str(cache_path),
        session_factory=lambda: session,
        enable_remote_expiry_probe=True,
    )
    expiries = cache.list_expiries("NIFTY")
    cache.close()
    InstrumentCache._runtime = None  # type: ignore[attr-defined]
    assert expiries == guesses
