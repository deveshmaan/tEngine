#!/usr/bin/env bash
set -euo pipefail
ROOT_DIR="$(cd -- "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

if [ -f "$ROOT_DIR/.env" ]; then
  set -a
  # shellcheck source=/dev/null
  source "$ROOT_DIR/.env"
  set +a
fi

: "${UPSTOX_ACCESS_TOKEN:?Set UPSTOX_ACCESS_TOKEN in your env or .env before running}" 

PYTHON_BIN=${PYTHON_BIN:-python3}

$PYTHON_BIN main.py cache-warm --symbol "${1:-NIFTY}" --expiry auto
$PYTHON_BIN main.py tick-demo --csv sample_ticks.csv --post 0.5
$PYTHON_BIN main.py exec-demo --csv sample_ticks.csv --post 0.5
PYTEST_BIN=${PYTEST_BIN:-$PYTHON_BIN -m pytest}
$PYTEST_BIN -q
