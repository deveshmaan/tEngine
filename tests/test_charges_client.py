import pytest
import upstox_client

from engine.charges import UpstoxChargesClient


class DummyChargeApi:
    def __init__(self) -> None:
        self.calls = 0

    def get_brokerage(self, instrument_token, quantity, product, transaction_type, price, api_version):
        self.calls += 1
        return {"data": {"charges": {"brokerage": 10.0, "gst": 1.8, "total": 11.8}}}


def test_charges_client_parses_breakdown(monkeypatch):
    api = DummyChargeApi()
    monkeypatch.setattr(upstox_client, "ChargeApi", lambda _client: api)
    client = UpstoxChargesClient(access_token="token", cache_ttl_seconds=60.0)

    breakdown = client.get_brokerage_breakdown("TOKEN", 1, "I", "BUY", 100.0)
    assert breakdown is not None
    assert breakdown.total == pytest.approx(11.8)
    categories = {row.category for row in breakdown.rows}
    assert "brokerage" in categories
    assert "gst" in categories

    breakdown_again = client.get_brokerage_breakdown("TOKEN", 1, "I", "BUY", 100.0)
    assert breakdown_again is not None
    assert api.calls == 1
