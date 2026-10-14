CREATE TABLE IF NOT EXISTS __sys_event_catalog (
    id TEXT PRIMARY KEY,
    name TEXT NOT NULL,
    lifecycle TEXT NOT NULL CHECK(lifecycle IN ('creating','updating','active','retiring','deleted')),
    tenant TEXT NOT NULL CHECK(length(CAST(tenant AS BLOB)) BETWEEN 1 AND 128),
    application TEXT NOT NULL CHECK(length(CAST(application AS BLOB)) BETWEEN 1 AND 128),
    family TEXT NOT NULL CHECK(length(CAST(family AS BLOB)) BETWEEN 1 AND 128),
    accepted_revision INTEGER CHECK(accepted_revision IS NULL OR (typeof(accepted_revision)='integer' AND accepted_revision>0)),
    desired_revision INTEGER CHECK(desired_revision IS NULL OR (typeof(desired_revision)='integer' AND desired_revision>0)),
    operation TEXT NOT NULL,
    definition TEXT,
    pending_definition TEXT,
    pending_authority TEXT,
    pending_retirement TEXT,
    digest TEXT NOT NULL,
    operation_authority TEXT NOT NULL,
    retry_at_ms INTEGER NOT NULL,
    attempts INTEGER NOT NULL DEFAULT 0,
    last_error TEXT
);
CREATE UNIQUE INDEX IF NOT EXISTS __sys_event_catalog_live_name ON __sys_event_catalog(name) WHERE lifecycle!='deleted';
CREATE UNIQUE INDEX IF NOT EXISTS __sys_event_catalog_operation ON __sys_event_catalog(operation);
CREATE INDEX IF NOT EXISTS __sys_event_catalog_pending ON __sys_event_catalog(retry_at_ms,id) WHERE pending_definition IS NOT NULL;
CREATE TABLE IF NOT EXISTS __sys_event_catalog_receipts (
    operation TEXT PRIMARY KEY,
    resource TEXT NOT NULL,
    kind TEXT NOT NULL CHECK(kind IN ('configure','retire')),
    digest TEXT NOT NULL,
    principal TEXT NOT NULL,
    grant_id TEXT NOT NULL,
    operator TEXT CHECK(operator IS NULL OR (typeof(operator)='text' AND length(CAST(operator AS BLOB)) BETWEEN 1 AND 256)),
    result TEXT
) WITHOUT ROWID;
