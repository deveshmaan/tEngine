from __future__ import annotations

import argparse
import asyncio
from pathlib import Path

from engine.config import EngineConfig
from engine.replay import ReplayConfig
from main import EngineApp, _with_replay_run_id


def _parse() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Deterministic replay runner")
    parser.add_argument("--config", type=Path, default=None, help="Override engine config path")
    parser.add_argument("--from-run", dest="from_run", help="Replay using events persisted under run_id")
    parser.add_argument("--from-file", dest="from_file", type=Path, help="Replay from JSONL(.gz) feed")
    parser.add_argument("--speed", type=float, default=1.0, help="Playback speed multiplier")
    parser.add_argument("--seed", type=int, default=None, help="Random seed for deterministic replays")
    args = parser.parse_args()
    if not args.from_run and not args.from_file:
        parser.error("One of --from-run or --from-file is required")
    if args.from_run and args.from_file:
        parser.error("Choose only one replay source")
    return args


def main() -> None:
    args = _parse()
    cfg = EngineConfig.load(args.config)
    replay_cfg = ReplayConfig(
        run_id=args.from_run,
        input_path=args.from_file,
        speed=args.speed,
        seed=args.seed,
    )
    cfg = _with_replay_run_id(cfg, replay_cfg)
    app = EngineApp(cfg)
    try:
        asyncio.run(app.run(replay_cfg=replay_cfg))
    except KeyboardInterrupt:  # pragma: no cover
        pass


if __name__ == "__main__":  # pragma: no cover
    main()
