BEGIN;

CREATE TABLE IF NOT EXISTS backtest_runs (
  run_id TEXT PRIMARY KEY,
  created_ts INTEGER NOT NULL,
  spec_json TEXT,
  start_date TEXT,
  end_date TEXT,
  interval TEXT,
  underlying_key TEXT,
  strategy TEXT,
  execution_json TEXT,
  costs_json TEXT,
  errors_json TEXT
);

CREATE TABLE IF NOT EXISTS backtest_summary (
  run_id TEXT NOT NULL,
  metric TEXT NOT NULL,
  value_real REAL,
  value_text TEXT,
  PRIMARY KEY(run_id, metric)
);

CREATE TABLE IF NOT EXISTS backtest_equity (
  run_id TEXT NOT NULL,
  ts INTEGER NOT NULL,
  equity REAL NOT NULL,
  drawdown REAL NOT NULL,
  PRIMARY KEY(run_id, ts)
);

CREATE TABLE IF NOT EXISTS backtest_trades (
  run_id TEXT NOT NULL,
  trade_id TEXT NOT NULL,
  trade_date TEXT,
  opened_ts INTEGER,
  closed_ts INTEGER,
  symbol TEXT,
  pnl_gross REAL,
  fees REAL,
  pnl_net REAL,
  exit_reason TEXT,
  raw_json TEXT,
  PRIMARY KEY(run_id, trade_id)
);

CREATE TABLE IF NOT EXISTS backtest_orders (
  run_id TEXT NOT NULL,
  order_id TEXT NOT NULL,
  ts INTEGER,
  strategy TEXT,
  symbol TEXT,
  side TEXT,
  qty INTEGER,
  fill_qty INTEGER,
  price REAL,
  eff_price REAL,
  status TEXT,
  raw_json TEXT,
  PRIMARY KEY(run_id, order_id)
);

CREATE INDEX IF NOT EXISTS idx_backtest_runs_created_ts ON backtest_runs(created_ts);
CREATE INDEX IF NOT EXISTS idx_backtest_equity_run_ts ON backtest_equity(run_id, ts);
CREATE INDEX IF NOT EXISTS idx_backtest_trades_run_date ON backtest_trades(run_id, trade_date);
CREATE INDEX IF NOT EXISTS idx_backtest_orders_run_ts ON backtest_orders(run_id, ts);

COMMIT;

