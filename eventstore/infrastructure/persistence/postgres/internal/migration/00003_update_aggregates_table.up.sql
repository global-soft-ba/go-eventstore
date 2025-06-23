BEGIN;

ALTER TABLE eventstore.aggregates ADD COLUMN latest_valid_time bigint NOT NULL DEFAULT 0;

CREATE OR REPLACE PROCEDURE update_aggregates_latest_valid_time(batch_size INT)
    LANGUAGE plpgsql
AS $$
DECLARE
    updated_rows INT;
BEGIN
    LOOP
        WITH max_times AS (
            SELECT tenant_id, aggregate_type, aggregate_id, MAX(valid_time) AS max_valid_time
            FROM eventstore.aggregates_events
            GROUP BY tenant_id, aggregate_type, aggregate_id
        ),
             to_update AS (
                 SELECT a.aggregate_id, a.tenant_id, a.aggregate_type, m.max_valid_time
                 FROM eventstore.aggregates a
                          JOIN max_times m
                               ON a.latest_valid_time = 0
                                   AND a.aggregate_id = m.aggregate_id
                                   AND a.tenant_id = m.tenant_id
                                   AND a.aggregate_type = m.aggregate_type
                 LIMIT batch_size
             )
        UPDATE eventstore.aggregates a
        SET latest_valid_time = u.max_valid_time
        FROM to_update u
        WHERE a.aggregate_id = u.aggregate_id
          AND a.tenant_id = u.tenant_id
          AND a.aggregate_type = u.aggregate_type;

        GET DIAGNOSTICS updated_rows = ROW_COUNT;
        IF updated_rows = 0 THEN
            EXIT;
        END IF;

        COMMIT;
    END LOOP;
END;
$$;

COMMIT;