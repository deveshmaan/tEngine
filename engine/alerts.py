from __future__ import annotations

import json
import os
import time
import urllib.error
import urllib.request
from typing import Callable, Dict, Iterable, Optional


class AlertService:
    def __init__(self, throttle_seconds: float = 30.0, transport: Optional[Callable[[str, bytes, Dict[str, str]], None]] = None):
        self.throttle_seconds = max(throttle_seconds, 0.0)
        self._last_sent = 0.0
        self._transport = transport or self._http_post
        self.slack_url = os.getenv("SLACK_WEBHOOK_URL")
        self.telegram_token = os.getenv("TELEGRAM_BOT_TOKEN")
        self.telegram_chat = os.getenv("TELEGRAM_CHAT_ID")

    def notify(self, level: str, title: str, body: str, tags: Optional[Iterable[str]] = None) -> bool:
        now = time.monotonic()
        if now - self._last_sent < self.throttle_seconds:
            return False
        payload = {"level": level, "title": title, "body": body, "tags": list(tags or [])}
        sent = False
        if self.slack_url:
            sent |= self._send_slack(payload)
        if self.telegram_token and self.telegram_chat:
            sent |= self._send_telegram(payload)
        if sent:
            self._last_sent = now
        return sent

    def _send_slack(self, payload: Dict[str, str]) -> bool:
        data = {
            "text": f"*{payload['title']}* ({payload['level']})\n{payload['body']}",
            "attachments": [{"text": ", ".join(payload["tags"])}] if payload["tags"] else [],
        }
        return self._post_json(self.slack_url, data)

    def _send_telegram(self, payload: Dict[str, str]) -> bool:
        url = f"https://api.telegram.org/bot{self.telegram_token}/sendMessage"
        tags = ", ".join(payload["tags"])
        text = f"{payload['title']} [{payload['level']}]\n{payload['body']}"
        if tags:
            text += f"\nTags: {tags}"
        data = {"chat_id": self.telegram_chat, "text": text}
        return self._post_json(url, data)

    def _post_json(self, url: Optional[str], data: Dict[str, object]) -> bool:
        if not url:
            return False
        try:
            body = json.dumps(data).encode("utf-8")
            headers = {"Content-Type": "application/json"}
            self._transport(url, body, headers)
            return True
        except Exception:
            return False

    @staticmethod
    def _http_post(url: str, body: bytes, headers: Dict[str, str]) -> None:
        req = urllib.request.Request(url, data=body, headers=headers, method="POST")
        with urllib.request.urlopen(req, timeout=5):
            pass


_DEFAULT_SERVICE: Optional[AlertService] = None


def configure_alerts(throttle_seconds: float = 30.0) -> None:
    global _DEFAULT_SERVICE
    _DEFAULT_SERVICE = AlertService(throttle_seconds=throttle_seconds)


def notify_incident(level: str, title: str, body: str, tags: Optional[Iterable[str]] = None) -> bool:
    if _DEFAULT_SERVICE is None:
        configure_alerts()
    assert _DEFAULT_SERVICE is not None
    return _DEFAULT_SERVICE.notify(level, title, body, tags)


__all__ = ["AlertService", "configure_alerts", "notify_incident"]
