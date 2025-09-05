BEGIN;

ALTER TABLE eventstore.projections
    ADD COLUMN updated_at timestamp NOT NULL DEFAULT now();

CREATE OR REPLACE FUNCTION update_updated_at_column()
    RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = now();
    RETURN NEW;
END;
$$ language 'plpgsql';

CREATE TRIGGER set_updated_at
    BEFORE INSERT OR UPDATE ON eventstore.projections
    FOR EACH ROW
EXECUTE PROCEDURE update_updated_at_column();

COMMIT;