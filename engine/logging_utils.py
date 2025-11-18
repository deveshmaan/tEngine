from __future__ import annotations

import datetime as dt
import json
import logging
from typing import Any, Dict, Optional


class StructuredLogger(logging.LoggerAdapter):
    """Thin adapter that enforces structured JSON logging."""

    def log_event(self, level: int, event: str, **fields: Any) -> None:
        payload: Dict[str, Any] = {"event": event, "ts": dt.datetime.utcnow().isoformat()}
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


__all__ = ["StructuredLogger", "configure_logging", "get_logger"]
