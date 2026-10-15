
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
    dispatch_generation INTEGER NOT NULL DEFAULT 0,
    pending_rows INTEGER NOT NULL DEFAULT 0 CHECK(typeof(pending_rows)='integer' AND pending_rows>=0),
    pending_bytes INTEGER NOT NULL DEFAULT 0 CHECK(typeof(pending_bytes)='integer' AND pending_bytes>=0),
    blocked_reason TEXT CHECK(blocked_reason IS NULL OR blocked_reason IN ('Forbidden','DestinationDeleted','SequenceGap','PayloadTooLarge','UnsupportedProtocol','ConfigConflict','ProducerSealed')),
    CHECK(status!='blocked' OR blocked_reason IS NOT NULL),
    CHECK(blocked_reason IS NULL OR status IN ('blocked','sealed'))
);
CREATE INDEX IF NOT EXISTS __sys_outbox_lanes_retry ON __sys_outbox_lanes(retry_at_ms,lane);
CREATE TABLE IF NOT EXISTS __sys_outbox (
    lane TEXT NOT NULL,
    sequence INTEGER NOT NULL CHECK(typeof(sequence)='integer' AND sequence>0),
    envelope BLOB NOT NULL,
    size INTEGER NOT NULL CHECK(size>=0),
    created_at_ms INTEGER NOT NULL CHECK(typeof(created_at_ms)='integer' AND created_at_ms>=0),
    PRIMARY KEY(lane,sequence)
) WITHOUT ROWID;
CREATE INDEX IF NOT EXISTS __sys_outbox_age ON __sys_outbox(created_at_ms);
CREATE INDEX IF NOT EXISTS __sys_outbox_lane_age ON __sys_outbox(lane,created_at_ms);
CREATE TABLE IF NOT EXISTS __sys_outbox_stats (
    singleton INTEGER PRIMARY KEY CHECK(singleton=1),
    rows INTEGER NOT NULL CHECK(typeof(rows)='integer' AND rows>=0),
    bytes INTEGER NOT NULL CHECK(typeof(bytes)='integer' AND bytes>=0)
);
INSERT OR IGNORE INTO __sys_outbox_stats VALUES(1,0,0);
CREATE TRIGGER IF NOT EXISTS __sys_outbox_insert AFTER INSERT ON __sys_outbox BEGIN
    UPDATE __sys_outbox_stats SET rows=rows+1,bytes=bytes+NEW.size WHERE singleton=1;
    UPDATE __sys_outbox_lanes SET pending_rows=pending_rows+1,pending_bytes=pending_bytes+NEW.size WHERE lane=NEW.lane;
END;
CREATE TRIGGER IF NOT EXISTS __sys_outbox_delete AFTER DELETE ON __sys_outbox BEGIN
    UPDATE __sys_outbox_stats SET rows=rows-1,bytes=bytes-OLD.size WHERE singleton=1;
    UPDATE __sys_outbox_lanes SET pending_rows=pending_rows-1,pending_bytes=pending_bytes-OLD.size WHERE lane=OLD.lane;
END;

CREATE TABLE IF NOT EXISTS __sys_outbox_blocked (
    reason TEXT PRIMARY KEY CHECK(reason IN ('Forbidden','DestinationDeleted','SequenceGap','PayloadTooLarge','UnsupportedProtocol','ConfigConflict','ProducerSealed')),
    lanes INTEGER NOT NULL CHECK(typeof(lanes)='integer' AND lanes>=0)
) WITHOUT ROWID;
INSERT OR IGNORE INTO __sys_outbox_blocked VALUES
    ('Forbidden',0),('DestinationDeleted',0),('SequenceGap',0),('PayloadTooLarge',0),
    ('UnsupportedProtocol',0),('ConfigConflict',0),('ProducerSealed',0);
CREATE TRIGGER IF NOT EXISTS __sys_outbox_lane_blocked_insert AFTER INSERT ON __sys_outbox_lanes BEGIN
    UPDATE __sys_outbox_blocked SET lanes=lanes+1 WHERE reason=NEW.blocked_reason;
END;
CREATE TRIGGER IF NOT EXISTS __sys_outbox_lane_blocked_update AFTER UPDATE OF blocked_reason ON __sys_outbox_lanes BEGIN
    UPDATE __sys_outbox_blocked SET lanes=lanes-1 WHERE reason=OLD.blocked_reason;
    UPDATE __sys_outbox_blocked SET lanes=lanes+1 WHERE reason=NEW.blocked_reason;
END;
CREATE TRIGGER IF NOT EXISTS __sys_outbox_lane_blocked_delete AFTER DELETE ON __sys_outbox_lanes BEGIN
    UPDATE __sys_outbox_blocked SET lanes=lanes-1 WHERE reason=OLD.blocked_reason;
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

CREATE TABLE IF NOT EXISTS __sys_cell_facet_names (
    name TEXT PRIMARY KEY CHECK(length(CAST(name AS BLOB))<=256)
) WITHOUT ROWID;
