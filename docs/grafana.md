# Grafana Panel Notes

- Add gauges for `strategy_short_ma`, `strategy_long_ma`, and counters for `strategy_entry_signals_total` to track entry cadence.
- Real-time panels: `strategy_ma_crossover_state`, `strategy_vol_breakout_state`, `strategy_realized_vol` vs `strategy_realized_vol_avg`, and `strategy_position_size` to monitor signal health and sizing.
- Sentiment/microstructure: add stats for `strategy_imi`, `strategy_pcr`, `strategy_iv_percentile`, and `strategy_oi_trend` (new panels added to the default dashboard).
- Display risk dials: `risk_percent_per_trade`, `capital_base_rupees`, `risk_open_lots`, and `risk_notional_premium_rupees`.
- Chart `order_latency_ms_bucketed` (Histogram→heatmap) and counters `order_timeouts_total` / `order_timeout_retries_total` for execution health.
- Keep existing PnL/risk panels; point the datasource at Prometheus scraping `127.0.0.1:9103`.
