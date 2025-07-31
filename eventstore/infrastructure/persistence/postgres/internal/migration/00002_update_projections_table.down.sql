BEGIN;

DROP TRIGGER IF EXISTS set_updated_at ON eventstore.projections;
DROP FUNCTION IF EXISTS update_updated_at_column();
ALTER TABLE eventstore.projections DROP COLUMN IF EXISTS updated_at;

COMMIT;