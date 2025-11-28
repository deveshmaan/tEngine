BEGIN;
CREATE TABLE IF NOT EXISTS runs (
  run_id TEXT PRIMARY KEY,
  started_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS orders (
  run_id TEXT NOT NULL,
  client_order_id TEXT NOT NULL,
  strategy TEXT NOT NULL,
  symbol TEXT NOT NULL,
  side TEXT NOT NULL,
  qty INTEGER NOT NULL,
  price REAL,
  state TEXT NOT NULL,
  last_update TEXT NOT NULL,
  broker_order_id TEXT,
  idempotency_key TEXT,
  UNIQUE(run_id, client_order_id)
);

CREATE TABLE IF NOT EXISTS order_transitions (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  run_id TEXT NOT NULL,
  client_order_id TEXT NOT NULL,
  prev_state TEXT,
  new_state TEXT NOT NULL,
  ts TEXT NOT NULL,
  reason TEXT
);

CREATE TABLE IF NOT EXISTS executions (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  run_id TEXT NOT NULL,
  order_id TEXT NOT NULL,
  symbol TEXT NOT NULL,
  side TEXT NOT NULL,
  qty INTEGER NOT NULL,
  price REAL NOT NULL,
  ts TEXT NOT NULL,
  venue TEXT,
  idempotency_key TEXT
);

CREATE TABLE IF NOT EXISTS risk_events (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  run_id TEXT NOT NULL,
  ts TEXT NOT NULL,
  code TEXT NOT NULL,
  message TEXT NOT NULL,
  symbol TEXT,
  context TEXT
);

CREATE TABLE IF NOT EXISTS incidents (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  run_id TEXT NOT NULL,
  ts TEXT NOT NULL,
  code TEXT NOT NULL,
  payload TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS positions (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  run_id TEXT NOT NULL,
  symbol TEXT NOT NULL,
  expiry TEXT,
  strike REAL,
  opt_type TEXT,
  qty INTEGER NOT NULL,
  avg_price REAL NOT NULL,
  opened_at TEXT NOT NULL,
  closed_at TEXT
);

CREATE TABLE IF NOT EXISTS event_log (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  run_id TEXT NOT NULL,
  ts TEXT NOT NULL,
  event_type TEXT NOT NULL,
  payload TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_orders_run ON orders(run_id);
CREATE INDEX IF NOT EXISTS idx_exec_run_order ON executions(run_id, order_id);
CREATE INDEX IF NOT EXISTS idx_positions_run_symbol ON positions(run_id, symbol);
CREATE UNIQUE INDEX IF NOT EXISTS idx_positions_unique ON positions(run_id, symbol, expiry, strike, opt_type);
CREATE INDEX IF NOT EXISTS idx_event_log_run_ts ON event_log(run_id, ts);
COMMIT;
