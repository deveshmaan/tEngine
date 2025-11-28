from __future__ import annotations

import os
from typing import Iterable, Optional, Sequence

import requests


def _coerce_ips(source: Optional[Iterable[str] | str]) -> list[str]:
    if source is None:
        return []
    if isinstance(source, str):
        items = source.replace(";", ",").split(",")
        return [item.strip() for item in items if item.strip()]
    return [str(item).strip() for item in source if str(item).strip()]


def assert_static_ip(allowed_ips: Optional[Sequence[str]] = None) -> Optional[str]:
    """
    Ensure the current public IP is within the allowed allowlist.

    If no allowlist is provided (or ALLOWED_IPS env is empty), the check is skipped.
    Returns the detected IP on success.
    """

    env_ips = os.getenv("ALLOWED_IPS")
    allowed = _coerce_ips(env_ips if env_ips is not None else allowed_ips)
    if not allowed:
        return None
    try:
        resp = requests.get("https://api.ipify.org", timeout=5)
        resp.raise_for_status()
        current_ip = (resp.text or "").strip()
    except Exception as exc:  # pragma: no cover - network dependent
        raise RuntimeError("Unable to determine public IP for static IP enforcement.") from exc
    if not current_ip:
        raise RuntimeError("Empty public IP response during static IP enforcement.")
    if current_ip not in allowed:
        raise RuntimeError(f"Public IP {current_ip} not in allowed IPs: {', '.join(allowed)}")
    return current_ip


__all__ = ["assert_static_ip"]
