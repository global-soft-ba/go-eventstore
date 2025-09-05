BEGIN;

CREATE SCHEMA IF NOT EXISTS eventstore;

/* Table Aggregate */
CREATE TABLE IF NOT EXISTS eventstore.aggregates
(
    tenant_id             text   not null,
    aggregate_type        text   not null,
    aggregate_id          text   not null,
    current_version       bigint not null,
    create_time           bigint not null,
    close_time            bigint not null,
    last_transaction_time bigint not null,

    PRIMARY KEY (tenant_id, aggregate_type, aggregate_id)
) PARTITION BY LIST (aggregate_type);

CREATE TABLE eventstore.aggregates_partDefault PARTITION OF eventstore.aggregates DEFAULT;

CREATE INDEX IF NOT EXISTS aggregates_composite_idx on eventstore.aggregates (tenant_id, aggregate_type,aggregate_id);



/* 1) We are using json instead of jsonb. jsonB would be better for searching in these fields; but does not preserve order
   of json elements, which make it hard to test.
   2) We use bigint instead of timestamptz. Postgres has microsecond as lowest precision for time. golang has nanoseconds.
   This mismatch would cause order problems for events. */
CREATE TABLE IF NOT EXISTS eventstore.aggregates_events
(
    id               text   not null,
    tenant_id        text   not null,
    aggregate_type   text   not null,
    aggregate_id     text   not null,
    version          bigint not null,
    type             text   not null,
    class            text   not null,
    transaction_time bigint not null,
    valid_time       bigint not null,
    from_migration   bool   not null,
    data             json,

    PRIMARY KEY (id, aggregate_type)
) PARTITION BY LIST (aggregate_type);

CREATE INDEX IF NOT EXISTS composite_idx on eventstore.aggregates_events (tenant_id, aggregate_type, aggregate_id,
                                                               transaction_time, valid_time, class,type );

CREATE TABLE eventstore.aggregates_events_partDefault PARTITION OF eventstore.aggregates_events DEFAULT;


/* Table for snapshots*/
CREATE TABLE IF NOT EXISTS eventstore.aggregates_snapshots
(
    id               text   not null,
    tenant_id        text   not null,
    aggregate_type   text   not null,
    aggregate_id     text   not null,
    version          bigint not null,
    type             text   not null,
    class            text   not null,
    transaction_time bigint not null,
    valid_time       bigint not null,
    from_migration   bool   not null,
    data             json,

    PRIMARY KEY (id, aggregate_type)
) PARTITION BY LIST (aggregate_type);

CREATE INDEX IF NOT EXISTS snapshot_composite_idx on eventstore.aggregates_snapshots(tenant_id, aggregate_type,aggregate_id,type, valid_time desc);

CREATE TABLE eventstore.aggregates_snapshots_partDefault PARTITION OF eventstore.aggregates_snapshots DEFAULT;


/* Table for projections */
CREATE TABLE IF NOT EXISTS eventstore.projections
(
    tenant_id            text   not null,
    projection_id        text   not null,
    state                text   not null,
    PRIMARY KEY (tenant_id, projection_id)
);
CREATE INDEX IF NOT EXISTS projections_idx on eventstore.projections (tenant_id, projection_id);

/* Table for projections event*/
CREATE TABLE IF NOT EXISTS eventstore.projections_events
(
    projection_id    text   not null,
    id               text   not null,
    tenant_id        text   not null,
    aggregate_type   text   not null,
    aggregate_id     text   not null,
    version          bigint not null,
    type             text   not null,
    class            text   not null,
    transaction_time bigint not null,
    valid_time       bigint not null,
    from_migration   bool   not null,
    data             json,

    PRIMARY KEY (projection_id, id)
) PARTITION BY LIST (projection_id);

CREATE INDEX IF NOT EXISTS projections_composite_idx on  eventstore.projections_events(tenant_id, projection_id, valid_time);

CREATE TABLE eventstore.projections_events_partDefault PARTITION OF eventstore.projections_events DEFAULT;


COMMIT;
