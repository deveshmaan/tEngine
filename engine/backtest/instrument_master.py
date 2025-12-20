from __future__ import annotations

import datetime as dt
import gzip
import json
from dataclasses import dataclass
from functools import lru_cache
from pathlib import Path
from typing import Dict, List, Optional, Tuple

from engine.config import IST


@dataclass(frozen=True)
class OptionContractKey:
    underlying_key: str
    expiry: str  # YYYY-MM-DD (IST)
    opt_type: str  # CE/PE
    strike: int


def _expiry_ms_to_date_str(expiry_ms: object) -> Optional[str]:
    if expiry_ms is None:
        return None
    try:
        ms = int(float(expiry_ms))
    except (TypeError, ValueError):
        return None
    if ms <= 0:
        return None
    ts = dt.datetime.fromtimestamp(ms / 1000.0, tz=dt.timezone.utc).astimezone(IST)
    return ts.date().isoformat()


def _coerce_strike(strike_raw: object) -> Optional[int]:
    if strike_raw is None:
        return None
    try:
        value = float(strike_raw)
    except (TypeError, ValueError):
        return None
    if not (value == value):  # NaN
        return None
    return int(round(value))


class InstrumentMaster:
    """
    Offline resolver backed by `cache/nse_master.json.gz`.

    The file is expected to be a JSON array of dicts with (at least) fields:
      - instrument_key
      - instrument_type ('CE'/'PE')
      - underlying_key
      - strike_price
      - expiry (epoch ms)
    """

    def __init__(self, path: Optional[str | Path] = None) -> None:
        self._path = Path(path) if path is not None else Path("cache/nse_master.json.gz")
        self._index: Dict[OptionContractKey, str] = {}
        self._strike_grid: Dict[Tuple[str, str, str], List[int]] = {}
        self._loaded = False

    @property
    def path(self) -> Path:
        return self._path

    def resolve_option_key(self, *, underlying_key: str, expiry: str, opt_type: str, strike: int) -> str:
        self._ensure_loaded()
        key = OptionContractKey(
            underlying_key=str(underlying_key),
            expiry=str(expiry),
            opt_type=str(opt_type).upper(),
            strike=int(strike),
        )
        instrument_key = self._index.get(key)
        if not instrument_key:
            raise KeyError(f"Option contract not found in instrument master: {key}")
        return instrument_key

    def strikes_for(self, *, underlying_key: str, expiry: str, opt_type: str) -> List[int]:
        self._ensure_loaded()
        grid_key = (str(underlying_key), str(expiry), str(opt_type).upper())
        return list(self._strike_grid.get(grid_key, []))

    def _ensure_loaded(self) -> None:
        if self._loaded:
            return
        self._loaded = True
        self._index, self._strike_grid = _load_master_index(self._path)


@lru_cache(maxsize=4)
def _load_master_index(path: Path) -> Tuple[Dict[OptionContractKey, str], Dict[Tuple[str, str, str], List[int]]]:
    with gzip.open(path, "rt", encoding="utf-8") as handle:
        payload = json.load(handle) or []
    if not isinstance(payload, list):
        raise ValueError(f"Instrument master at {path} must be a JSON array")
    index: Dict[OptionContractKey, str] = {}
    strike_grid: Dict[Tuple[str, str, str], set[int]] = {}
    for row in payload:
        if not isinstance(row, dict):
            continue
        opt_type = str(row.get("instrument_type") or "").upper()
        if opt_type not in {"CE", "PE"}:
            continue
        instrument_key = str(row.get("instrument_key") or "").strip()
        underlying_key = str(row.get("underlying_key") or "").strip()
        if not instrument_key or not underlying_key:
            continue
        expiry = _expiry_ms_to_date_str(row.get("expiry"))
        if not expiry:
            continue
        strike = _coerce_strike(row.get("strike_price"))
        if strike is None:
            continue

        k = OptionContractKey(underlying_key=underlying_key, expiry=expiry, opt_type=opt_type, strike=strike)
        index[k] = instrument_key
        strike_grid.setdefault((underlying_key, expiry, opt_type), set()).add(strike)

    grid_out: Dict[Tuple[str, str, str], List[int]] = {k: sorted(v) for k, v in strike_grid.items()}
    return index, grid_out


__all__ = [
    "InstrumentMaster",
    "OptionContractKey",
]
