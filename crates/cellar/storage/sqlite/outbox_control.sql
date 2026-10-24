-- Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

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

CREATE TABLE IF NOT EXISTS __sys_outbox_source_lifecycle (
    singleton INTEGER PRIMARY KEY CHECK(singleton=1),
    incarnation TEXT NOT NULL CHECK(length(incarnation)=32),
    generation INTEGER NOT NULL CHECK(typeof(generation)='integer' AND generation>0),
    state TEXT NOT NULL CHECK(state IN ('checking','verified','cleaning','deleted','live')),
    operation TEXT NOT NULL,
    snapshot TEXT NOT NULL CHECK(length(snapshot)=64),
    work_generation INTEGER NOT NULL DEFAULT 0 CHECK(typeof(work_generation)='integer' AND work_generation>=0)
);
CREATE TABLE IF NOT EXISTS __sys_outbox_destroy_lanes (
    operation TEXT NOT NULL,
    lane TEXT NOT NULL,
    resource TEXT NOT NULL,
    needs_evidence INTEGER NOT NULL CHECK(needs_evidence IN (0,1)),
    evidence TEXT,
    PRIMARY KEY(operation,lane)
) WITHOUT ROWID;
