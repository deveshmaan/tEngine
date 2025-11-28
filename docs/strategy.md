## Intraday options strategy

This engine now runs a real-time intraday CALL-buy strategy that combines short-term momentum with a volatility breakout gate.

- **Signals**: a BUY is considered only when the 5m EMA crosses above the 15m EMA on the underlying index _and_ realized 1m return volatility is in breakout (latest > average × configured multiple).
- **Volatility**: realized volatility is computed from rolling 1m returns; the 20-sample average is the reference band. Default multiples are 1.5× for NIFTY and a tighter 1.2× for BANKNIFTY (higher baseline vol).
- **Expiry/strike selection**: NIFTY uses the nearest weekly expiry; BANKNIFTY uses the nearest monthly. The engine picks the closest ATM CALL on 50-point strikes via the instrument cache and Upstox resolver.
- **Sizing**: position size = `floor((capital_base * risk_percent_per_trade) / (option_premium * lot_size))`, capped by `risk.max_open_lots` and blocked by risk gates (PnL stop, notional cap, rate limits, staleness).
- **Order safety**: before submitting orders the strategy checks Upstox connectivity/heartbeat, static IP validation, stale-market-data guards, and the risk manager’s exposure/rate ceilings. Orders are sent as MARKET via the OMS.
- **Exits**: trailing stop starts 20% below entry (25% for BANKNIFTY) and trails by 10% of gains (15% for BANKNIFTY). Half the position is taken off when premium doubles. Any open leg is closed 20 minutes before expiry, and existing time/kill-switch guards still apply.

### Tuning guide
- `strategy.short_ma` / `strategy.long_ma`: raise to reduce noise; lower for faster but choppier signals. Keep `short < long`.
- `strategy.vol_breakout_mult`: >1 dampens signals during calm sessions; <1.5 makes it more aggressive. Use `banknifty.vol_breakout_mult` to tighten only BANKNIFTY.
- `exit.trailing_pct` / `exit.trailing_step`: increase `trailing_pct` to widen the initial stop; decrease `trailing_step` to hug gains more tightly (more stop-outs). Defaults already map to 20%/10% (25%/15% BANKNIFTY).
- `exit.time_buffer_minutes`: enlarge if you want to flatten earlier on expiry day; keep ≥15 to avoid late-day illiquidity.
- `capital_base` / `risk.risk_percent_per_trade`: primary sizing dials; lowering either reduces lot count non-linearly with premium.

### Regulatory & operational notes (India index options)
- Index options list weekly or monthly expiries (BANKNIFTY runs monthly only); selection obeys the exchange calendar and holiday back-adjustment.
- Tick size and strike-step enforcement are applied from the instrument cache (50-point spacing here) and validated by the OMS/risk manager.
- Upstox mandates static-IP allowlists and API rate limits; the broker wrapper enforces static-IP checks at startup and rate ceilings per endpoint. Orders are blocked if WS heartbeat or market-data freshness fails.

Do **not** use this module for backtests or paper trading; the logic is wired for live, real-time execution with OMS/risk integration.
