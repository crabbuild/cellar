-- Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

CREATE TABLE __sys_sandbox_parent (
    singleton INTEGER PRIMARY KEY CHECK(singleton=1),
    record TEXT NOT NULL CHECK(typeof(record)='text' AND length(CAST(record AS BLOB))<=4096)
);
CREATE TABLE __sys_sandbox_allocations (
    name TEXT PRIMARY KEY NOT NULL CHECK(length(CAST(name AS BLOB)) BETWEEN 1 AND 128),
    state TEXT NOT NULL CHECK(state IN ('reserved','ready','deleting','deleted')),
    execution_scope TEXT NOT NULL UNIQUE,
    record TEXT NOT NULL CHECK(typeof(record)='text' AND length(CAST(record AS BLOB))<=4096)
) WITHOUT ROWID;
CREATE INDEX __sys_sandbox_cleanup ON __sys_sandbox_allocations(name) WHERE state!='deleted';
CREATE INDEX __sys_sandbox_deleting ON __sys_sandbox_allocations(name) WHERE state='deleting';
