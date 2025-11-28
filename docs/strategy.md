## Advanced intraday buy strategy

The `AdvancedBuyStrategy` layers sentiment and microstructure filters on top of momentum/volatility breakout logic for NIFTY (weekly) and BANKNIFTY (monthly).

**Entry filters**
- Short/long EMAs on 1m closes with volatility-adaptive lookbacks (50% shorter when realized-vol percentile > 0.8).
- Realized-vol breakout: latest 1m return stdev > `strategy.vol_breakout_mult` × trailing average.
- Intraday Momentum Index (IMI): computed on 1m OHLC; requires `IMI < 30` (oversold).
- Put/Call ratio (PCR): derived from option-chain volumes; must stay within [`strategy.pcr_extreme_low`, `strategy.pcr_extreme_high`]; extremes are sentiment warnings.
- IV percentile filter: latest IV percentile across recent samples must be < `strategy.iv_percentile_threshold` and in a breakout regime.
- Event guard: blocks new entries within `strategy.event_halt_minutes` before/after events defined in `strategy.event_file_path` (JSON/YAML with ISO timestamps).

**Strike & size**
- Fetches option-chain every 5 minutes (respects rate limits), compares ATM and next OTM CALL strikes, and picks the strike with higher OI+volume, ignoring strikes below `strategy.oi_volume_min_threshold`.
- Rejects weakening OI when price is rising (falling OI on rising underlying).
- Quantity: `floor((capital_base * risk_percent_per_trade * risk_adjustment_factor) / (premium * lot_size))`, where risk_adjustment_factor shrinks when IV percentile is high or an event window is active.
- Gamma/time guard: blocks entries if minutes-to-expiry < `strategy.min_minutes_to_expiry` or gamma > `strategy.gamma_threshold` (when greeks are available).

**Exits**
- IV reversion: exit when IV percentile > `strategy.iv_exit_percentile`.
- ATR trailing: `exit.at_pct * ATR` below the peak price (ATR from recent option prices).
- OI reversal: rising OI with falling price triggers a defensive exit.
- Existing partial take-profit, time buffers, and trailing stops remain active.

**Metrics & dashboards**
- Prometheus gauges: `strategy_imi`, `strategy_pcr`, `strategy_iv_percentile`, `strategy_oi_trend`, `strategy_selected_strike`, and config gauges for exits.
- Grafana panels added for IMI/PCR/IV percentile/OI trend.

### Event calendar
Provide a JSON/YAML file (see `config/events.yml` sample):
```yaml
- start: "2025-11-28T11:00:00+05:30"
  end: "2025-11-28T12:00:00+05:30"
  name: "RBI policy"
```
The engine skips new entries from `event_halt_minutes` before `start` until `end` + buffer.

### Tuning tips
- `strategy.short_ma`/`long_ma`: higher = smoother; keep `short < long`.
- `strategy.vol_breakout_mult`: raise to reduce signals on calm days; lower to be more aggressive.
- `strategy.iv_percentile_threshold`: tighten to avoid paying high vol; loosen when willing to buy expensive vol.
- `strategy.pcr_extreme_*`: widen to be more tolerant of sentiment extremes.
- `strategy.oi_volume_min_threshold`: raise to trade only liquid strikes.
- `exit.at_pct`: increase to widen ATR-based trailing stops; decrease to hug price.

### Regulatory & operational notes
- BANKNIFTY uses monthly expiries; NIFTY uses nearest weekly by default.
- Tick size/strike steps enforced via the instrument cache and OMS validation.
- Upstox static-IP/rate-limit gates remain in place; order submits will fail fast if broker/watchdog are unhealthy.

This logic is live-only; no paper/backtest hooks are implemented.
