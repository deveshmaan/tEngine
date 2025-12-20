BEGIN;
ALTER TABLE executions ADD COLUMN exec_id TEXT;
ALTER TABLE executions ADD COLUMN raw_price REAL;
ALTER TABLE executions ADD COLUMN effective_price REAL;
COMMIT;

