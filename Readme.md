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
2. Import `grafana/dashboard.json`.
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

- `UPSTOX_ACCESS_TOKEN` – OAuth token pulled after login
- `UNDERLYING` – `NIFTY` (default) or `BANKNIFTY`
- `DRY_RUN` – `true` (default) keeps the engine in simulation mode
- `MARKET_STREAM_MODE` – set to `full_d30` if your Upstox Plus/Pro plan exposes deeper market-data; otherwise leave at the default `full`
