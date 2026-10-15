
CREATE TABLE IF NOT EXISTS __sys_inbox (
    processing_scope TEXT NOT NULL,
    event_id TEXT NOT NULL,
    digest BLOB NOT NULL CHECK(length(digest)=32),
    processed_ms INTEGER NOT NULL,
    PRIMARY KEY(processing_scope,event_id)
) WITHOUT ROWID;
CREATE INDEX IF NOT EXISTS __sys_inbox_age ON __sys_inbox(processing_scope,processed_ms,event_id);
CREATE TABLE IF NOT EXISTS __sys_inbox_stats (
    singleton INTEGER PRIMARY KEY CHECK(singleton=1),
    rows INTEGER NOT NULL CHECK(typeof(rows)='integer' AND rows>=0)
);
INSERT OR IGNORE INTO __sys_inbox_stats VALUES(1,0);
CREATE TRIGGER IF NOT EXISTS __sys_inbox_insert AFTER INSERT ON __sys_inbox BEGIN
    UPDATE __sys_inbox_stats SET rows=rows+1 WHERE singleton=1;
END;
CREATE TRIGGER IF NOT EXISTS __sys_inbox_delete AFTER DELETE ON __sys_inbox BEGIN
    UPDATE __sys_inbox_stats SET rows=rows-1 WHERE singleton=1;
END;
