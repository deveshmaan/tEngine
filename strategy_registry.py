from __future__ import annotations

import importlib
import inspect
import pkgutil
from typing import Dict, List, Type

import strategy
from strategy.base import BaseStrategy

_CACHE: Dict[str, Type[BaseStrategy]] | None = None


def _load() -> Dict[str, Type[BaseStrategy]]:
    strategies: Dict[str, Type[BaseStrategy]] = {}
    for module_info in pkgutil.iter_modules(strategy.__path__):
        module = importlib.import_module(f"{strategy.__name__}.{module_info.name}")
        for _, obj in inspect.getmembers(module, inspect.isclass):
            if obj is BaseStrategy:
                continue
            try:
                if issubclass(obj, BaseStrategy):
                    strategies[obj.__name__] = obj
            except Exception:
                continue
    return strategies


def list_strategies() -> List[str]:
    """Return sorted list of discoverable strategy class names."""

    global _CACHE
    if _CACHE is None:
        _CACHE = _load()
    return sorted(_CACHE.keys())


def get_strategy_class(name: str) -> Type[BaseStrategy]:
    """Return a strategy class by its class name."""

    global _CACHE
    if _CACHE is None:
        _CACHE = _load()
    try:
        return _CACHE[name]
    except KeyError as exc:
        raise KeyError(f"Unknown strategy: {name!r}. Available: {sorted(_CACHE.keys())}") from exc


__all__ = ["get_strategy_class", "list_strategies"]

