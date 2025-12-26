from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Iterable, Optional

import yaml


@dataclass(frozen=True)
class TemplateDefinition:
    template_id: str
    display_name: str
    description: str
    default_underlying_instrument_key: str
    candle_interval: str
    entry_time: str
    exit_time: str
    legs: tuple[Dict[str, Any], ...]
    risk_rules: Dict[str, Any]


_TEMPLATE_CACHE: Optional[Dict[str, TemplateDefinition]] = None


def _iter_template_files(root: Path) -> Iterable[Path]:
    for pattern in ("*.yml", "*.yaml", "*.json"):
        for path in sorted(root.glob(pattern)):
            if path.name.startswith("_"):
                continue
            yield path


def _load_payload(path: Path) -> Dict[str, Any]:
    text = path.read_text(encoding="utf-8")
    if path.suffix.lower() == ".json":
        payload = json.loads(text)
    else:
        payload = yaml.safe_load(text)
    if not isinstance(payload, dict):
        raise ValueError(f"Template file must contain a mapping/object: {path}")
    return payload


def _require_str(payload: Dict[str, Any], field: str, *, path: Path) -> str:
    value = payload.get(field)
    value = "" if value is None else str(value)
    value = value.strip()
    if not value:
        raise ValueError(f"Missing/empty `{field}` in template: {path}")
    return value


def _coerce_template(payload: Dict[str, Any], *, path: Path) -> TemplateDefinition:
    template_id = _require_str(payload, "template_id", path=path)
    display_name = str(payload.get("display_name") or template_id).strip()
    description = str(payload.get("description") or "").strip()
    default_underlying = str(
        payload.get("default_underlying_instrument_key")
        or payload.get("default_underlying")
        or payload.get("default_underlying_key")
        or payload.get("default_underlying_symbol")
        or "NSE_INDEX|Nifty 50"
    ).strip()
    candle_interval = str(payload.get("candle_interval") or payload.get("interval") or "5minute").strip()
    entry_time = str(payload.get("entry_time") or "09:30").strip()
    exit_time = str(payload.get("exit_time") or "15:20").strip()

    legs_raw = payload.get("legs") or []
    if not isinstance(legs_raw, list) or not legs_raw:
        raise ValueError(f"`legs` must be a non-empty list in template: {path}")
    legs: list[Dict[str, Any]] = []
    for idx, leg in enumerate(legs_raw, start=1):
        if not isinstance(leg, dict):
            raise ValueError(f"legs[{idx}] must be an object in template: {path}")
        legs.append(dict(leg))

    risk_rules = payload.get("risk_rules") or {}
    if risk_rules is None:
        risk_rules = {}
    if not isinstance(risk_rules, dict):
        raise ValueError(f"`risk_rules` must be an object if present in template: {path}")

    return TemplateDefinition(
        template_id=template_id,
        display_name=display_name or template_id,
        description=description,
        default_underlying_instrument_key=default_underlying,
        candle_interval=candle_interval,
        entry_time=entry_time,
        exit_time=exit_time,
        legs=tuple(legs),
        risk_rules=dict(risk_rules),
    )


def _load_all() -> Dict[str, TemplateDefinition]:
    root = Path(__file__).resolve().parent
    templates: Dict[str, TemplateDefinition] = {}
    for path in _iter_template_files(root):
        payload = _load_payload(path)
        tmpl = _coerce_template(payload, path=path)
        if tmpl.template_id in templates:
            raise ValueError(f"Duplicate template_id `{tmpl.template_id}` in {path}")
        templates[tmpl.template_id] = tmpl
    return templates


def _templates() -> Dict[str, TemplateDefinition]:
    global _TEMPLATE_CACHE
    if _TEMPLATE_CACHE is None:
        _TEMPLATE_CACHE = _load_all()
    return _TEMPLATE_CACHE


def list_templates() -> list[TemplateDefinition]:
    return sorted(_templates().values(), key=lambda t: (t.display_name.lower(), t.template_id.lower()))


def get_template(template_id: str) -> TemplateDefinition:
    key = str(template_id or "").strip()
    if not key:
        raise KeyError("template_id must be non-empty")
    templates = _templates()
    if key in templates:
        return templates[key]
    # Best-effort lookup by lowercased id / display name.
    lowered = key.lower()
    for cand in templates.values():
        if cand.template_id.lower() == lowered or cand.display_name.lower() == lowered:
            return cand
    raise KeyError(f"Unknown template: {template_id!r}. Available: {sorted(templates)}")


__all__ = [
    "TemplateDefinition",
    "get_template",
    "list_templates",
]

