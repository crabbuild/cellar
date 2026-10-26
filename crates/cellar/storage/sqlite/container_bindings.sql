-- Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

CREATE TABLE IF NOT EXISTS __sys_container_bindings (
    binding TEXT PRIMARY KEY NOT NULL CHECK(length(CAST(binding AS BLOB)) BETWEEN 1 AND 128),
    revision INTEGER NOT NULL CHECK(typeof(revision)='integer' AND revision>0),
    activation_epoch INTEGER NOT NULL CHECK(typeof(activation_epoch)='integer' AND activation_epoch>=0),
    record TEXT NOT NULL CHECK(length(CAST(record AS BLOB))<=16384)
) WITHOUT ROWID;
