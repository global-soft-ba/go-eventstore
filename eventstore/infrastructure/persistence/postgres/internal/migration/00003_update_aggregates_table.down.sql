BEGIN;

DROP PROCEDURE update_aggregates_latest_valid_time;

ALTER TABLE eventstore.aggregates DROP COLUMN latest_valid_time;

COMMIT;