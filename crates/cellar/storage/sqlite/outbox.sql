
CREATE TABLE IF NOT EXISTS __sys_event_source (
    singleton INTEGER PRIMARY KEY CHECK(singleton=1),
    incarnation TEXT NOT NULL CHECK(length(incarnation)=32),
    protocol INTEGER NOT NULL CHECK(protocol=1)
);
CREATE TABLE IF NOT EXISTS __sys_outbox_lanes (
    lane TEXT PRIMARY KEY,
    binding TEXT NOT NULL,
    next_sequence INTEGER NOT NULL CHECK(typeof(next_sequence)='integer' AND next_sequence>0),
    settled_through INTEGER NOT NULL DEFAULT 0 CHECK(typeof(settled_through)='integer' AND settled_through>=0),
    status TEXT NOT NULL DEFAULT 'pending' CHECK(status IN ('pending','blocked','sealed')),
    retry_at_ms INTEGER NOT NULL,
    attempts INTEGER NOT NULL DEFAULT 0,
    last_error TEXT,
    dispatch_generation INTEGER NOT NULL DEFAULT 0
);
CREATE INDEX IF NOT EXISTS __sys_outbox_lanes_retry ON __sys_outbox_lanes(retry_at_ms,lane);
CREATE TABLE IF NOT EXISTS __sys_outbox (
    lane TEXT NOT NULL,
    sequence INTEGER NOT NULL CHECK(typeof(sequence)='integer' AND sequence>0),
    envelope BLOB NOT NULL,
    size INTEGER NOT NULL CHECK(size>=0),
    created_at_ms INTEGER NOT NULL,
    PRIMARY KEY(lane,sequence)
) WITHOUT ROWID;
CREATE TABLE IF NOT EXISTS __sys_outbox_stats (
    singleton INTEGER PRIMARY KEY CHECK(singleton=1),
    rows INTEGER NOT NULL CHECK(typeof(rows)='integer' AND rows>=0),
    bytes INTEGER NOT NULL CHECK(typeof(bytes)='integer' AND bytes>=0)
);
INSERT OR IGNORE INTO __sys_outbox_stats VALUES(1,0,0);
CREATE TRIGGER IF NOT EXISTS __sys_outbox_insert AFTER INSERT ON __sys_outbox BEGIN
    UPDATE __sys_outbox_stats SET rows=rows+1,bytes=bytes+NEW.size WHERE singleton=1;
END;
CREATE TRIGGER IF NOT EXISTS __sys_outbox_delete AFTER DELETE ON __sys_outbox BEGIN
    UPDATE __sys_outbox_stats SET rows=rows-1,bytes=bytes-OLD.size WHERE singleton=1;
END;

CREATE TABLE IF NOT EXISTS __sys_outbox_seals (
    lane TEXT PRIMARY KEY,
    operation TEXT NOT NULL UNIQUE,
    terminal INTEGER NOT NULL CHECK(typeof(terminal)='integer' AND terminal>=0),
    state TEXT NOT NULL DEFAULT 'pending' CHECK(state IN ('pending','blocked','accepted'))
) WITHOUT ROWID;
CREATE TABLE IF NOT EXISTS __sys_outbox_batch_limits (
    lane TEXT PRIMARY KEY,
    bytes INTEGER NOT NULL CHECK(typeof(bytes)='integer' AND bytes>0),
    records INTEGER NOT NULL CHECK(typeof(records)='integer' AND records>0)
) WITHOUT ROWID;
