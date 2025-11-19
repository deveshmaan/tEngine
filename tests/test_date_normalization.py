import datetime as dt

import pytest

from engine.data import normalize_date


def test_normalize_accepts_date_inputs() -> None:
    assert normalize_date("2025-11-25") == "2025-11-25"
    assert normalize_date(dt.date(2025, 11, 25)) == "2025-11-25"
    assert normalize_date(dt.datetime(2025, 11, 25, 9, 15)) == "2025-11-25"


@pytest.mark.parametrize(
    "value",
    [
        "2025-11-25T09:15:00",
        "25-11-2025",
        "2025/11/25",
        "2025-1-5",
    ],
)
def test_normalize_rejects_invalid_formats(value: str) -> None:
    with pytest.raises(ValueError):
        normalize_date(value)
