BEGIN;

DROP TABLE eventstore.aggregates;
DROP TABLE eventstore.aggregates_events;
DROP TABLE eventstore.aggregates_snapshots;
DROP SCHEMA eventstore;

COMMIT;
