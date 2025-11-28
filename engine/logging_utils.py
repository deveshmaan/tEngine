from __future__ import annotations

import datetime as dt
import json
import logging
import time
from typing import Any, Dict, Optional, Tuple


class StructuredLogger(logging.LoggerAdapter):
    """Thin adapter that enforces structured JSON logging."""

    def log_event(self, level: int, event: str, **fields: Any) -> None:
        stamp = dt.datetime.now(dt.timezone.utc).isoformat()
        payload: Dict[str, Any] = {"event": event, "ts": stamp}
        payload.update(fields)
        message = json.dumps(payload, default=str, separators=(",", ":"))
        self.logger.log(level, message)


def configure_logging(level: str = "INFO") -> None:
    logging.basicConfig(level=getattr(logging, level.upper(), logging.INFO), format="%(message)s")


def get_logger(name: str, level: Optional[str] = None) -> StructuredLogger:
    if level:
        configure_logging(level)
    base = logging.getLogger(name)
    return StructuredLogger(base, {})


class RateLimitedLogger:
    """Helper to rate-limit noisy log messages per key."""

    def __init__(self, logger: StructuredLogger, min_interval_seconds: float = 1.0):
        self._logger = logger
        self._interval = max(min_interval_seconds, 0.0)
        self._last: Dict[Tuple[str, str], float] = {}

    def log_event(self, level: int, event: str, key: str, **fields: Any) -> None:
        now = time.time()
        marker = (event, key)
        last_ts = self._last.get(marker, 0.0)
        if self._interval > 0 and (now - last_ts) < self._interval:
            return
        self._last[marker] = now
        self._logger.log_event(level, event, **fields)


__all__ = ["StructuredLogger", "configure_logging", "get_logger"]
