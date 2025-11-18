BEGIN;
CREATE TABLE IF NOT EXISTS cost_ledger (
  run_id TEXT NOT NULL,
  exec_id TEXT NOT NULL,
  category TEXT NOT NULL,
  amount REAL NOT NULL,
  currency TEXT NOT NULL DEFAULT 'INR',
  ts TEXT NOT NULL,
  note TEXT
);

CREATE TABLE IF NOT EXISTS pnl_snapshots (
  run_id TEXT NOT NULL,
  ts TEXT NOT NULL,
  realized REAL NOT NULL,
  unrealized REAL NOT NULL,
  fees REAL NOT NULL,
  net REAL NOT NULL,
  per_symbol TEXT NOT NULL -- JSON
);

CREATE TABLE IF NOT EXISTS control_intents (
  run_id TEXT NOT NULL,
  ts TEXT NOT NULL,
  action TEXT NOT NULL,
  payload_json TEXT
);

CREATE INDEX IF NOT EXISTS idx_execs_run_ts ON executions(run_id, ts);
CREATE INDEX IF NOT EXISTS idx_orders_status ON orders(state);
CREATE INDEX IF NOT EXISTS idx_cost_run_ts ON cost_ledger(run_id, ts);
CREATE INDEX IF NOT EXISTS idx_pnl_run_ts ON pnl_snapshots(run_id, ts);
COMMIT;
