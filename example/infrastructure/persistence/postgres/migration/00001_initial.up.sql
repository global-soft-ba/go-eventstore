BEGIN;

CREATE TABLE IF NOT EXISTS "items"
(
    "id"            BIGSERIAL NOT NULL, -- 1 to 2,147,483,647
    "tenant_id"     text NOT NULL,
    "item_id"       text NOT NULL,
    "item_name"     text,
    PRIMARY KEY ("tenant_id", "id"),
    UNIQUE ("tenant_id", "item_id")
) PARTITION BY LIST ("tenant_id");

COMMIT;