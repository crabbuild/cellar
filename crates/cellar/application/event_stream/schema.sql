CREATE TABLE IF NOT EXISTS __sys_stream (
    singleton INTEGER PRIMARY KEY CHECK(singleton=1),
    definition TEXT NOT NULL,
    revision INTEGER NOT NULL CHECK(typeof(revision)='integer' AND revision>0),
    lifecycle TEXT NOT NULL CHECK(lifecycle IN ('active','deleted')),
    next_offset INTEGER NOT NULL DEFAULT 1 CHECK(typeof(next_offset)='integer' AND next_offset>0),
    earliest_offset INTEGER NOT NULL DEFAULT 1 CHECK(typeof(earliest_offset)='integer' AND earliest_offset>0),
    bytes INTEGER NOT NULL DEFAULT 0 CHECK(typeof(bytes)='integer' AND bytes>=0),
    rows INTEGER NOT NULL DEFAULT 0 CHECK(typeof(rows)='integer' AND rows>=0),
    logical_ms INTEGER NOT NULL DEFAULT 0 CHECK(typeof(logical_ms)='integer' AND logical_ms>=0),
    config_operation TEXT NOT NULL,
    config_digest TEXT NOT NULL,
    config_principal TEXT NOT NULL,
    config_grant TEXT NOT NULL,
    byte_position INTEGER NOT NULL DEFAULT 0 CHECK(typeof(byte_position)='integer' AND byte_position>=0)
);
CREATE TABLE IF NOT EXISTS __sys_stream_events (
    offset INTEGER PRIMARY KEY CHECK(offset>0),
    lane TEXT NOT NULL,
    sequence INTEGER NOT NULL CHECK(sequence>0),
    envelope BLOB NOT NULL,
    size INTEGER NOT NULL CHECK(size>0),
    append_ms INTEGER NOT NULL CHECK(typeof(append_ms)='integer' AND append_ms>=0),
    byte_position INTEGER NOT NULL CHECK(typeof(byte_position)='integer' AND byte_position>=0),
    UNIQUE(lane,sequence)
);
CREATE INDEX IF NOT EXISTS __sys_stream_events_age ON __sys_stream_events(append_ms,offset);
CREATE TABLE IF NOT EXISTS __sys_stream_producers (
    lane TEXT PRIMARY KEY,
    source TEXT NOT NULL,
    incarnation TEXT NOT NULL,
    principal TEXT NOT NULL,
    grant_id TEXT NOT NULL,
    accepted_through INTEGER NOT NULL CHECK(typeof(accepted_through)='integer' AND accepted_through>=0),
    sealed INTEGER NOT NULL DEFAULT 0 CHECK(sealed IN (0,1))
) WITHOUT ROWID;
CREATE TABLE IF NOT EXISTS __sys_stream_subscriptions (
    id TEXT PRIMARY KEY,
    name TEXT NOT NULL,
    generation TEXT NOT NULL,
    start TEXT NOT NULL,
    checkpoint INTEGER NOT NULL CHECK(typeof(checkpoint)='integer' AND checkpoint>=0),
    required INTEGER NOT NULL CHECK(required IN (0,1)),
    state TEXT NOT NULL DEFAULT 'active' CHECK(state IN ('active','paused','expired','deleted')),
    next_lease_generation INTEGER NOT NULL DEFAULT 1 CHECK(typeof(next_lease_generation)='integer' AND next_lease_generation>0),
    retry_at_ms INTEGER NOT NULL DEFAULT 0
);
CREATE INDEX IF NOT EXISTS __sys_stream_subscription_checkpoint ON __sys_stream_subscriptions(required,checkpoint);
CREATE INDEX IF NOT EXISTS __sys_stream_required_frontier ON __sys_stream_subscriptions(checkpoint) WHERE required=1 AND state!='deleted';
CREATE INDEX IF NOT EXISTS __sys_stream_subscription_state ON __sys_stream_subscriptions(state,id);
CREATE UNIQUE INDEX IF NOT EXISTS __sys_stream_subscription_live_name ON __sys_stream_subscriptions(name) WHERE state!='deleted';
CREATE TABLE IF NOT EXISTS __sys_stream_leases (
    subscription TEXT PRIMARY KEY,
    generation INTEGER NOT NULL CHECK(generation>0),
    token TEXT NOT NULL,
    fetch_id TEXT NOT NULL,
    first_offset INTEGER NOT NULL,
    last_offset INTEGER NOT NULL,
    issued_ms INTEGER NOT NULL,
    expires_ms INTEGER NOT NULL,
    CHECK(first_offset>0 AND last_offset>=first_offset AND expires_ms>issued_ms)
);
CREATE INDEX IF NOT EXISTS __sys_stream_lease_expiry ON __sys_stream_leases(expires_ms);
CREATE TABLE IF NOT EXISTS __sys_stream_ack_receipts (
    subscription TEXT PRIMARY KEY,
    receipt TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS __sys_stream_operations (
    sequence INTEGER PRIMARY KEY AUTOINCREMENT,
    operation TEXT NOT NULL UNIQUE,
    digest TEXT NOT NULL,
    principal TEXT NOT NULL,
    grant_id TEXT NOT NULL,
    receipt TEXT NOT NULL,
    size INTEGER NOT NULL CHECK(size>0),
    created_ms INTEGER NOT NULL
);
CREATE TABLE IF NOT EXISTS __sys_stream_retirement (
    singleton INTEGER PRIMARY KEY CHECK(singleton=1),
    operation TEXT NOT NULL,
    digest TEXT NOT NULL,
    request TEXT NOT NULL,
    principal TEXT NOT NULL,
    grant_id TEXT NOT NULL,
    operator TEXT CHECK(operator IS NULL OR (typeof(operator)='text' AND length(CAST(operator AS BLOB)) BETWEEN 1 AND 256)),
    started_ms INTEGER NOT NULL,
    finished_ms INTEGER,
    records INTEGER NOT NULL CHECK(records>=0),
    bytes INTEGER NOT NULL CHECK(bytes>=0)
);
