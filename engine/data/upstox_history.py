from __future__ import annotations

import datetime as dt
import os
import random
import time
from dataclasses import dataclass
from typing import Any, Iterable, List, Optional, Sequence, Tuple

from engine.config import IST

try:  # pragma: no cover - depends on optional SDK wiring in tests
    from upstox_client.rest import ApiException
except Exception:  # pragma: no cover
    ApiException = Exception  # type: ignore[assignment]


_RETRYABLE_STATUS = {429, 500, 502, 503, 504}


def _as_date(value: dt.date | dt.datetime | str) -> dt.date:
    if isinstance(value, dt.datetime):
        return value.date()
    if isinstance(value, dt.date):
        return value
    text = str(value).strip()
    if not text:
        raise ValueError("date must be non-empty")
    try:
        return dt.date.fromisoformat(text[:10])
    except Exception as exc:
        raise ValueError(f"Invalid date: {value!r}") from exc


def _to_date_str(value: dt.date | dt.datetime | str) -> str:
    return _as_date(value).isoformat()


def _parse_ts(raw: object, *, tz: dt.tzinfo) -> dt.datetime:
    if raw is None:
        raise ValueError("Missing candle timestamp")
    if isinstance(raw, (int, float)):
        ts_num = float(raw)
        if ts_num > 1_000_000_000_000:
            ts_num /= 1000.0
        return dt.datetime.fromtimestamp(ts_num, tz=dt.timezone.utc).astimezone(tz)
    text = str(raw).strip()
    if not text:
        raise ValueError("Missing candle timestamp")
    try:
        ts = dt.datetime.fromisoformat(text)
    except ValueError:
        try:
            ts_num = float(text)
        except ValueError as exc:
            raise ValueError(f"Invalid candle timestamp: {raw!r}") from exc
        if ts_num > 1_000_000_000_000:
            ts_num /= 1000.0
        ts = dt.datetime.fromtimestamp(ts_num, tz=dt.timezone.utc)
    if ts.tzinfo is None:
        ts = ts.replace(tzinfo=tz)
    return ts.astimezone(tz)


def parse_interval_spec(interval: str) -> Tuple[str, int]:
    """
    Parse a human interval like "1minute"/"5minute"/"1day" into Upstox V3 params.

    Returns: (unit, interval)
      unit ∈ {"minutes","days","weeks","months"}
      interval ∈ positive int
    """

    text = str(interval or "").strip().lower()
    if not text:
        return "minutes", 1
    if text in {"day", "1day", "d", "1d"}:
        return "days", 1
    if text in {"week", "1week", "w", "1w"}:
        return "weeks", 1
    if text in {"month", "1month", "mo", "1mo"}:
        return "months", 1
    import re

    match = re.fullmatch(r"(\d+)\s*(minute|min|m|day|d|week|w|month|mo)s?", text)
    if not match:
        raise ValueError(f"Unsupported interval: {interval!r}")
    count = int(match.group(1))
    unit_raw = match.group(2)
    unit = unit_raw
    if unit in {"min", "m", "minute"}:
        unit = "minutes"
    elif unit in {"d", "day"}:
        unit = "days"
    elif unit in {"w", "week"}:
        unit = "weeks"
    elif unit in {"mo", "month"}:
        unit = "months"
    return unit, max(count, 1)


@dataclass(frozen=True)
class UpstoxHistoryClient:
    """
    Wrapper for Upstox historical candle API (V3 via official SDK).

    - Retries with backoff on 429/5xx
    - Normalizes output into a list of dicts with:
        ts (datetime), open, high, low, close, volume, oi
    """

    history_api_v3: Any
    tz: dt.tzinfo = IST
    retries: int = 6
    base_delay_s: float = 0.6
    max_delay_s: float = 8.0
    max_span_days_minutes: int = 30

    @classmethod
    def from_env(cls, history_api_v3: Any, *, tz: dt.tzinfo = IST) -> "UpstoxHistoryClient":
        try:
            max_span = int(os.getenv("UPSTOX_HISTORY_MAX_SPAN_DAYS_MINUTES", "30"))
        except Exception:
            max_span = 30
        return cls(history_api_v3=history_api_v3, tz=tz, max_span_days_minutes=max_span)

    def fetch_candles_v3(
        self,
        instrument_key: str,
        unit: str,
        interval: int,
        from_date: dt.date | dt.datetime | str,
        to_date: dt.date | dt.datetime | str,
    ) -> List[dict[str, object]]:
        instrument_key = str(instrument_key or "").strip()
        if not instrument_key:
            raise ValueError("instrument_key must be non-empty")
        unit_text = str(unit or "").strip().lower()
        if unit_text.endswith("s"):
            unit_text = unit_text[:-1]
        if unit_text not in {"minute", "day", "week", "month"}:
            raise ValueError(f"Unsupported unit: {unit!r}")
        unit_param = unit_text + "s"
        try:
            interval_n = int(interval)
        except (TypeError, ValueError) as exc:
            raise ValueError("interval must be an int") from exc
        if interval_n <= 0:
            raise ValueError("interval must be >= 1")

        from_str = _to_date_str(from_date)
        to_str = _to_date_str(to_date)

        def _do_call() -> Any:
            return self.history_api_v3.get_historical_candle_data1(
                instrument_key=instrument_key,
                unit=unit_param,
                interval=interval_n,
                to_date=to_str,
                from_date=from_str,
            )

        payload = self._call_with_retry(_do_call)
        if hasattr(payload, "to_dict"):
            payload = payload.to_dict()
        rows = self._parse_payload(payload)
        return self._validate_rows(rows)

    def fetch_candles(
        self,
        instrument_key: str,
        interval: str,
        from_date: dt.date | dt.datetime | str,
        to_date: dt.date | dt.datetime | str,
    ) -> List[dict[str, object]]:
        unit, interval_n = parse_interval_spec(interval)
        start_d = _as_date(from_date)
        end_d = _as_date(to_date)
        if end_d < start_d:
            raise ValueError("Invalid date range: end_date < start_date")

        rows: List[dict[str, object]] = []
        if unit == "minutes":
            max_span_days = int(self.max_span_days_minutes or 30)
            chunk_start = start_d
            while chunk_start <= end_d:
                chunk_end = min(end_d, chunk_start + dt.timedelta(days=max_span_days))
                rows.extend(self.fetch_candles_v3(instrument_key, unit, interval_n, chunk_start, chunk_end))
                chunk_start = chunk_end + dt.timedelta(days=1)
        else:
            rows = self.fetch_candles_v3(instrument_key, unit, interval_n, start_d, end_d)

        rows.sort(key=lambda r: r["ts"])  # type: ignore[index]
        return rows

    def _call_with_retry(self, fn) -> Any:  # type: ignore[no-untyped-def]
        last_exc: Optional[Exception] = None
        for attempt in range(1, int(self.retries) + 1):
            try:
                return fn()
            except ApiException as exc:  # pragma: no cover - network dependent
                status = int(getattr(exc, "status", 0) or 0)
                last_exc = exc
                if status and status not in _RETRYABLE_STATUS:
                    raise
            except Exception as exc:  # pragma: no cover - network dependent
                last_exc = exc
            # Retry with exponential backoff + jitter.
            delay = min(float(self.max_delay_s), float(self.base_delay_s) * (2 ** max(attempt - 1, 0)))
            delay = max(0.05, delay)
            delay = delay * (0.75 + (random.random() * 0.5))
            time.sleep(delay)
        assert last_exc is not None
        raise last_exc

    def _parse_payload(self, payload: Any) -> List[dict[str, object]]:
        data = payload.get("data") if isinstance(payload, dict) else None
        candles = (data or {}).get("candles") if isinstance(data, dict) else None
        if candles is None and isinstance(payload, dict):
            candles = payload.get("candles")
        out: List[dict[str, object]] = []
        for candle in candles or []:
            if not isinstance(candle, (list, tuple)) or len(candle) < 5:
                continue
            try:
                ts = _parse_ts(candle[0], tz=self.tz)
                open_ = float(candle[1])
                high = float(candle[2])
                low = float(candle[3])
                close = float(candle[4])
            except Exception:
                continue
            volume = 0.0
            if len(candle) >= 6:
                try:
                    volume = float(candle[5] or 0.0)
                except Exception:
                    volume = 0.0
            oi: float | None = None
            if len(candle) >= 7:
                try:
                    oi = float(candle[6]) if candle[6] is not None else None
                except Exception:
                    oi = None
            out.append({"ts": ts, "open": open_, "high": high, "low": low, "close": close, "volume": volume, "oi": oi})
        return out

    def _validate_rows(self, rows: Sequence[dict[str, object]]) -> List[dict[str, object]]:
        if not rows:
            return []
        out = sorted(rows, key=lambda r: r.get("ts"))  # type: ignore[arg-type]
        # Deduplicate by timestamp while preserving last.
        dedup: dict[dt.datetime, dict[str, object]] = {}
        for row in out:
            ts = row.get("ts")
            if isinstance(ts, dt.datetime):
                dedup[ts] = row
        out = [dedup[k] for k in sorted(dedup)]
        # Monotonic timestamps (strictly non-decreasing after sort).
        prev: Optional[dt.datetime] = None
        for row in out:
            ts = row.get("ts")
            if not isinstance(ts, dt.datetime):
                raise ValueError("UpstoxHistoryClient received row without datetime ts")
            if prev is not None and ts < prev:
                raise ValueError("UpstoxHistoryClient received non-monotonic timestamps")
            prev = ts
        return list(out)


__all__ = ["UpstoxHistoryClient", "parse_interval_spec"]

