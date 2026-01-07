## Metrics & UI

The BUY engine now emits Prometheus metrics by default. Key environment switches:

```bash
export ENABLE_METRICS="true"
export METRICS_PORT="9103"
```

Start the engine/notebook and point your monitoring stack to `http://<engine-host>:9103/metrics`.

### Streamlit Dashboard

```bash
streamlit run streamlit_app.py
```

Set `PROM_URL` if the Prometheus endpoint is not running on `127.0.0.1:9103`.

### Grafana

1. Add a Prometheus datasource that points to your server.
2. Import `grafana/dashboard_buy.json`.
3. Replace the placeholder datasource UID (`__PROM_DS__`) with your Prometheus datasource.

The dashboard includes widgets for PnL, risk halts, order/latency trends, and per-instrument telemetry (LTP, IV, IV z-score).

### Prometheus

The Streamlit/Grafana views expect a full Prometheus server (the engine’s exporter on port `9103` is just the scrape target). A starter config is provided in `prometheus/prometheus.yml`:

```
global:
  scrape_interval: 5s

scrape_configs:
  - job_name: "buy-engine"
    static_configs:
      - targets:
          - "127.0.0.1:9103"
```

Run Prometheus with:

```
./prometheus --config.file=prometheus/prometheus.yml
```

Then point Grafana’s Prometheus datasource at the Prometheus server (default `http://localhost:9090`). Streamlit can continue to read directly from the exporter if desired via `PROM_URL`.

## Environment variables

- `UPSTOX_CLIENT_ID` – OAuth client ID (preferred)
- `UPSTOX_CLIENT_SECRET` – OAuth client secret (preferred)
- `UPSTOX_REDIRECT_URI` – OAuth redirect URL
- `UPSTOX_ACCESS_TOKEN` – OAuth token pulled after login
- `UPSTOX_ALGO_ID` – optional algo header for order placement
- `UNDERLYING` – `NIFTY` (default) or `BANKNIFTY`
- `DRY_RUN` – `true` (default) keeps the engine in simulation mode
- `MARKET_STREAM_MODE` – set to `full_d30` if your Upstox Plus/Pro plan exposes deeper market-data; otherwise leave at the default `full`

## Security & Credentials

- **Never** commit API keys or OAuth tokens. All broker and analytics credentials (Upstox, AngelOne, Finvasia, OpenAI) are loaded through environment variables (see `.env.example` and `docs/setup.md`). Rotate any previously exposed keys before running the engine.
- Export `UPSTOX_ACCESS_TOKEN`, `UPSTOX_CLIENT_ID`, etc. via a secrets manager (`direnv`, `1Password CLI`, `aws secretsmanager`, etc.) before running `main.py`.
- The engine refuses to connect without `UPSTOX_ACCESS_TOKEN`, keeping the repo compliant with Upstox/NSE audits.
- Optional static-IP hardening: set `ALLOWED_IPS` (comma-separated) or `allowed_ips` in `config/app.yml`. The broker will abort startup if the current public IP is not whitelisted. See `docs/setup.md` for the full env checklist.
- Secret scanning: install pre-commit and run gitleaks locally (`pre-commit install`, `pre-commit run --all-files`). CI runs the same scan on push/PR.

## Engine Workflow (BUY-side)

1. **Market data** – `UpstoxBroker` streams `MarketDataStreamerV3` ticks, while `InstrumentCache` tracks contracts/expiries. Stale ticks are flagged via `max_tick_age_seconds`.
2. **Signal stack** – Strategy classes (`AdvancedBuyStrategy`, `ScalpingBuyStrategy`, `OpeningRangeBreakoutStrategy`) enforce:
   - Delta window (`0.25–0.55`), spot-momentum bias, and Zerodha Varsity spread discipline (absolute and % of premium).
   - IV sanity (z-score band) plus a rolling IV percentile gate inspired by Kaushik/Gurjar breakout playbooks.
   - OI percentile filters so BUY orders align with fresh writer covering (per NSE option-chain advisories and the `Nifty_Bank_Option_Strategies_Booklet`).
3. **Execution** – `OMS` places orders using the official `upstox-python-sdk`. `ExitEngine` manages stops/targets and uses REST LTP fallback when streaming is stale.
4. **Risk & Ledger** – Every fill updates SQLite (`engine_state.sqlite`), Prometheus metrics, and the daily realized PnL used by `RiskManager` to halt trading at drawdown/profit caps.
5. **Monitoring** – Streamlit + Grafana dashboards ingest metrics such as IV percentile, OI percentile, last exit reason, and risk halts.

## Strategy Controls & New Env Vars

| Variable | Purpose |
| --- | --- |
| `SPREAD_PCT_MAX` | Max spread as % of premium (default `0.08`). |
| `IV_PERCENTILE_MIN` | Minimum rolling IV percentile to allow BUY entries (default `0.35`). |
| `OI_PERCENTILE_MIN` | Minimum rolling OI percentile to confirm writer capitulation (default `0.25`). |
| `MAX_CONCURRENT_POS`, `CAPITAL_PER_TRADE`, `EXIT_TARGET_PCT_BUY`, `EXIT_STOP_PCT_BUY` | Sizing + exit heuristics as before. |

If your broker/calendar now uses a non-Thursday weekly expiry (e.g., Upstox moved NIFTY to Tuesdays), set `weekly_expiry_weekday` under the `data` block in `config/app.yml` (0 = Monday … 6 = Sunday). The engine uses this hint for expiry discovery and when seeding the instrument cache before it has fetched live metadata.

Set these along with the older knobs to align with your playbook from NSE circulars, Zerodha Varsity Options modules, or Gurjar’s breakout texts. When replaying historical CSVs that lack OI, dial `OI_PERCENTILE_MIN=0.0` to bypass the live-only filter.

**Replay shortcut:** When you run `main.py --replay-only` (or pass the flag through `run_stack.sh`), the engine auto-relaxes IV/OI percentile filters (defaults to zero) so synthetic data can produce trades. Set `RELAX_REPLAY_FILTERS=false` to keep live thresholds, or override the relaxed values with `IV_PERCENTILE_MIN_REPLAY` / `OI_PERCENTILE_MIN_REPLAY` for custom stress tests.

## One-command stack (Engine + Prometheus + Streamlit)

Use the helper script to bring up the entire BUY stack with one command:

```bash
./run_stack.sh --replay-only --runtime-minutes 1
```

The script:

- Loads secrets and overrides from `.env` (if present), so export your `UPSTOX_ACCESS_TOKEN`, etc. there.
- Starts Prometheus with `prometheus/prometheus.yml`, Streamlit (`streamlit_app.py`), and finally `python main.py` (passing any CLI args you supply).
- Cleans up all processes when one of them exits or when you hit `Ctrl+C`.
- Automatically frees default ports (Prometheus 9090, Streamlit 8502, metrics exporter 9103). Override them via `PROM_PORT`, `STREAMLIT_PORT`, or `METRICS_PORT` if you need different bindings.

Override binaries/paths by exporting `PROM_BIN`, `PROM_CONFIG`, or `PROM_STORAGE` before running the script.

## Operational Checklist

- Copy `.env.example` to `.env` and set the required Upstox variables.
- Install pre-commit and enable gitleaks scanning (`pre-commit install`).
- Start the stack with `./run_stack.sh` or run `python main.py`.
- Launch the UI with `streamlit run streamlit_app.py` and (optionally) Grafana + Prometheus.

## Tests

Run the fast test suite locally:

```
./scripts/run_tests.sh
```
