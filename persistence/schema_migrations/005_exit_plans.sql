BEGIN;
CREATE TABLE IF NOT EXISTS exit_plans (
  run_id TEXT NOT NULL,
  instrument TEXT NOT NULL,
  plan_json TEXT NOT NULL,
  updated_at TEXT NOT NULL,
  PRIMARY KEY (run_id, instrument)
);
CREATE INDEX IF NOT EXISTS idx_exit_plans_run ON exit_plans(run_id);
COMMIT;
