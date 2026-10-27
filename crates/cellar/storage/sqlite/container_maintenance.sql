-- Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.
CREATE TABLE IF NOT EXISTS __sys_container_binding_maintenance (
    binding TEXT PRIMARY KEY NOT NULL,
    generation INTEGER NOT NULL CHECK(typeof(generation)='integer' AND generation>0),
    version INTEGER NOT NULL CHECK(typeof(version)='integer' AND version>0),
    created_ms INTEGER NOT NULL CHECK(typeof(created_ms)='integer' AND created_ms>=0),
    activity_ms INTEGER NOT NULL CHECK(typeof(activity_ms)='integer' AND activity_ms>=created_ms),
    logical_ms INTEGER NOT NULL CHECK(typeof(logical_ms)='integer' AND logical_ms>=activity_ms),
    expires_ms INTEGER NOT NULL CHECK(typeof(expires_ms)='integer' AND expires_ms>=created_ms),
    due_ms INTEGER NOT NULL CHECK(typeof(due_ms)='integer' AND due_ms>=0),
    policy TEXT NOT NULL CHECK(length(CAST(policy AS BLOB))<=512),
    failed INTEGER NOT NULL CHECK(typeof(failed)='integer' AND failed BETWEEN 0 AND 16)
) WITHOUT ROWID;
