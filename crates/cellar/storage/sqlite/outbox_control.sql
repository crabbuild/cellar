CREATE TABLE IF NOT EXISTS __sys_outbox_operations (
    sequence INTEGER PRIMARY KEY AUTOINCREMENT,
    operation TEXT NOT NULL UNIQUE,
    digest TEXT NOT NULL CHECK(length(digest)=64),
    operator TEXT NOT NULL,
    receipt TEXT NOT NULL,
    size INTEGER NOT NULL CHECK(typeof(size)='integer' AND size>=0 AND size<=4096),
    created_ms INTEGER NOT NULL
);
CREATE TABLE IF NOT EXISTS __sys_outbox_operation_stats (
    singleton INTEGER PRIMARY KEY CHECK(singleton=1),
    rows INTEGER NOT NULL CHECK(typeof(rows)='integer' AND rows>=0)
);
INSERT OR IGNORE INTO __sys_outbox_operation_stats VALUES(1,0);
CREATE TRIGGER IF NOT EXISTS __sys_outbox_operation_insert AFTER INSERT ON __sys_outbox_operations BEGIN
    UPDATE __sys_outbox_operation_stats SET rows=rows+1 WHERE singleton=1;
END;
CREATE TRIGGER IF NOT EXISTS __sys_outbox_operation_delete AFTER DELETE ON __sys_outbox_operations BEGIN
    UPDATE __sys_outbox_operation_stats SET rows=rows-1 WHERE singleton=1;
END;
