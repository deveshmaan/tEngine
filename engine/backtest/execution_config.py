from __future__ import annotations

from dataclasses import dataclass


FILL_MODEL_SAME_TICK = "same_tick"
FILL_MODEL_NEXT_TICK = "next_tick"
ALLOWED_FILL_MODELS = {FILL_MODEL_SAME_TICK, FILL_MODEL_NEXT_TICK}


@dataclass(frozen=True)
class ExecutionConfig:
    """
    Execution simulation knobs for backtesting.

    - fill_model:
        - "same_tick": fills can occur on the same tick as order submission (subject to latency).
        - "next_tick": fills can occur only on a subsequent tick (subject to latency).
    - latency_ms: simulated delay from submission to eligible fill time.
    - allow_partial_fills:
        - True: consume available candle volume; fill partially if needed.
        - False: fill only when candle volume can satisfy remaining qty.
    """

    fill_model: str = FILL_MODEL_SAME_TICK
    latency_ms: int = 0
    allow_partial_fills: bool = False

    def __post_init__(self) -> None:
        fill_model = str(self.fill_model or "").strip().lower()
        if fill_model not in ALLOWED_FILL_MODELS:
            raise ValueError(f"fill_model must be one of {sorted(ALLOWED_FILL_MODELS)}; got {self.fill_model!r}")
        object.__setattr__(self, "fill_model", fill_model)

        try:
            latency_ms = int(self.latency_ms or 0)
        except (TypeError, ValueError) as exc:
            raise ValueError("latency_ms must be an int") from exc
        if latency_ms < 0:
            raise ValueError("latency_ms must be >= 0")
        object.__setattr__(self, "latency_ms", latency_ms)

        object.__setattr__(self, "allow_partial_fills", bool(self.allow_partial_fills))


__all__ = [
    "ExecutionConfig",
    "FILL_MODEL_NEXT_TICK",
    "FILL_MODEL_SAME_TICK",
]

