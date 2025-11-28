BEGIN;
CREATE UNIQUE INDEX IF NOT EXISTS idx_positions_unique ON positions(run_id, symbol, expiry, strike, opt_type);
COMMIT;
