-- Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

CREATE TABLE IF NOT EXISTS __sys_container_execution (
    binding TEXT PRIMARY KEY NOT NULL,
    sequence INTEGER NOT NULL CHECK(typeof(sequence)='integer' AND sequence>0),
    generation INTEGER NOT NULL CHECK(typeof(generation)='integer' AND generation>0),
    binding_revision INTEGER NOT NULL CHECK(typeof(binding_revision)='integer' AND binding_revision>0),
    owner TEXT NOT NULL CHECK(length(CAST(owner AS BLOB))=64)
) WITHOUT ROWID;
