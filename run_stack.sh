#!/usr/bin/env bash

set -euo pipefail

ROOT_DIR="$(cd -- "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# Load optional environment overrides (tokens, strategy params, etc.)
if [ -f "$ROOT_DIR/.env" ]; then
  # shellcheck source=/dev/null
  source "$ROOT_DIR/.env"
fi

if ! python - <<'PY' >/dev/null 2>&1; then
import importlib
importlib.import_module("upstox_client")
PY
  pip install --upgrade upstox-python-sdk >/dev/null
  python - <<'PY' >/dev/null 2>&1
import importlib
importlib.import_module("upstox_client")
PY
fi

PROM_BIN="${PROM_BIN:-$ROOT_DIR/prometheus/prometheus}"
PROM_CONFIG="${PROM_CONFIG:-$ROOT_DIR/prometheus/prometheus.yml}"
PROM_STORAGE="${PROM_STORAGE:-$ROOT_DIR/prometheus/data}"
PROM_PORT="${PROM_PORT:-9090}"
METRICS_PORT="${METRICS_PORT:-9103}"
STREAMLIT_PORT="${STREAMLIT_PORT:-8502}"
PYTHON_BIN="${PYTHON_BIN:-$ROOT_DIR/.engine-env/bin/python}"
if [ ! -x "$PYTHON_BIN" ]; then
  PYTHON_BIN="$(command -v python)"
fi
ENGINE_CMD=("$PYTHON_BIN" "$ROOT_DIR/main.py")
STREAMLIT_BIN_DEFAULT="$ROOT_DIR/.engine-env/bin/streamlit"
if [ -x "$STREAMLIT_BIN_DEFAULT" ]; then
  STREAMLIT_BIN="$STREAMLIT_BIN_DEFAULT"
else
  STREAMLIT_BIN=$(command -v streamlit 2>/dev/null || true)
fi
STREAMLIT_CMD=("$STREAMLIT_BIN" run "$ROOT_DIR/streamlit_app.py" --server.port "$STREAMLIT_PORT")
if ! [[ "$METRICS_PORT" =~ ^[0-9]+$ ]]; then
  echo "[STACK] Invalid METRICS_PORT '$METRICS_PORT' - defaulting to 9103"
  METRICS_PORT=9103
fi
export METRICS_PORT STREAMLIT_PORT PROM_PORT
echo "[STACK] Metrics endpoint will listen on port $METRICS_PORT"

mkdir -p "$PROM_STORAGE"

kill_port() {
  local port="$1"
  local pids
  pids=$(lsof -ti tcp:"$port" 2>/dev/null || true)
  if [ -n "$pids" ]; then
    echo "[STACK] Killing existing process on port $port (PID $pids)"
    # shellcheck disable=SC2086
    kill $pids >/dev/null 2>&1 || true
    sleep 1
  fi
}

cleanup() {
  trap - INT TERM
  for pid in "$PROM_PID" "$STREAMLIT_PID" "$ENGINE_PID"; do
    if [ -n "$pid" ] && kill -0 "$pid" >/dev/null 2>&1; then
      kill "$pid" >/dev/null 2>&1 || true
    fi
  done
}

trap cleanup INT TERM

kill_port "$PROM_PORT"
kill_port "$STREAMLIT_PORT"
kill_port "$METRICS_PORT"

PROM_PID=""
if [ -x "$PROM_BIN" ]; then
  echo "[STACK] Starting Prometheus via $PROM_BIN"
  "$PROM_BIN" --config.file="$PROM_CONFIG" --storage.tsdb.path="$PROM_STORAGE" --web.listen-address=":$PROM_PORT" &
  PROM_PID=$!
else
  echo "[STACK] Prometheus binary not found at $PROM_BIN; skipping"
fi

echo "[STACK] Starting Streamlit UI"
if [ -n "$STREAMLIT_BIN" ]; then
  "${STREAMLIT_CMD[@]}" &
  STREAMLIT_PID=$!
else
  echo "[STACK] streamlit executable not found; please install streamlit"
  STREAMLIT_PID=""
fi

echo "[STACK] Starting trading engine"
"${ENGINE_CMD[@]}" "$@" &
ENGINE_PID=$!

for pid in "$PROM_PID" "$STREAMLIT_PID" "$ENGINE_PID"; do
  if [ -n "$pid" ];then
    wait "$pid" || true
  fi
done
cleanup
