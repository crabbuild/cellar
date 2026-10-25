-- Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

-- Copyright 2026 Deno Land Inc. Apache-2.0 license.
-- Adapted for Cellar from Celld: crates/celld/js/harness.js.
-- Existing Queue schema and trigger/index definitions.

CREATE TABLE IF NOT EXISTS __sys_queue_messages (
         seq INTEGER PRIMARY KEY AUTOINCREMENT,
         id TEXT NOT NULL UNIQUE,
         body BLOB NOT NULL,
         content_type TEXT NOT NULL,
         size INTEGER NOT NULL,
         enqueued_at INTEGER NOT NULL,
         visible_at INTEGER NOT NULL,
         attempts INTEGER NOT NULL DEFAULT 0,
         lease_id TEXT,
         lease_generation INTEGER NOT NULL DEFAULT 0,
         leased_until INTEGER,
         purge_on_settle INTEGER NOT NULL DEFAULT 0,
         dlq_target TEXT,
         dlq_transfer_id TEXT,
         transfer_kind TEXT,
         dead_letter_source TEXT,
         CHECK ((dlq_target IS NULL AND transfer_kind IS NULL)
             OR (dlq_target IS NOT NULL
                 AND transfer_kind IN ('dead-letter', 'redrive')))
       );

CREATE INDEX IF NOT EXISTS __sys_queue_messages_visible
         ON __sys_queue_messages (visible_at, seq);

CREATE INDEX IF NOT EXISTS __sys_queue_messages_ready
         ON __sys_queue_messages (visible_at, seq)
         WHERE purge_on_settle = 0 AND dlq_target IS NULL
           AND lease_id IS NULL;

CREATE INDEX IF NOT EXISTS __sys_queue_messages_enqueued
         ON __sys_queue_messages (enqueued_at, seq);

CREATE INDEX IF NOT EXISTS __sys_queue_messages_retained
         ON __sys_queue_messages (enqueued_at, seq)
         WHERE purge_on_settle = 0 AND dlq_target IS NULL;

CREATE INDEX IF NOT EXISTS __sys_queue_messages_lease
         ON __sys_queue_messages (leased_until, seq)
         WHERE lease_id IS NOT NULL;

CREATE INDEX IF NOT EXISTS __sys_queue_messages_moving
         ON __sys_queue_messages (seq)
         WHERE dlq_target IS NOT NULL;

CREATE INDEX IF NOT EXISTS __sys_queue_messages_lease_batch
         ON __sys_queue_messages (lease_id, visible_at, seq)
         WHERE lease_id IS NOT NULL;

CREATE UNIQUE INDEX IF NOT EXISTS __sys_queue_messages_dlq_transfer
         ON __sys_queue_messages (dlq_transfer_id)
         WHERE dlq_transfer_id IS NOT NULL AND dlq_target IS NULL;

CREATE TABLE IF NOT EXISTS __sys_queue_meta (
         name TEXT PRIMARY KEY,
         value TEXT NOT NULL
       ) WITHOUT ROWID;

CREATE TABLE IF NOT EXISTS __sys_queue_stats (
         id INTEGER PRIMARY KEY CHECK (id = 1),
         stored_count INTEGER NOT NULL,
         backlog_count INTEGER NOT NULL,
         backlog_bytes INTEGER NOT NULL,
         oldest_message_timestamp INTEGER
       );

INSERT INTO __sys_queue_stats
         (id, stored_count, backlog_count, backlog_bytes,
          oldest_message_timestamp)
       SELECT 1,
              COUNT(*),
              COALESCE(SUM(purge_on_settle = 0), 0),
              COALESCE(SUM(CASE WHEN purge_on_settle = 0 THEN size ELSE 0 END), 0),
              MIN(CASE WHEN purge_on_settle = 0 THEN enqueued_at END)
         FROM __sys_queue_messages
        WHERE true
       ON CONFLICT(id) DO NOTHING;

CREATE TRIGGER IF NOT EXISTS __sys_queue_messages_stored_insert
       AFTER INSERT ON __sys_queue_messages
       BEGIN
         UPDATE __sys_queue_stats
            SET stored_count = stored_count + 1
          WHERE id = 1;
       END;

CREATE TRIGGER IF NOT EXISTS __sys_queue_messages_stored_delete
       AFTER DELETE ON __sys_queue_messages
       BEGIN
         UPDATE __sys_queue_stats
            SET stored_count = stored_count - 1
          WHERE id = 1;
       END;

CREATE TRIGGER IF NOT EXISTS __sys_queue_messages_stats_insert
       AFTER INSERT ON __sys_queue_messages
       WHEN new.purge_on_settle = 0
       BEGIN
         UPDATE __sys_queue_stats
            SET backlog_count = backlog_count + 1,
                backlog_bytes = backlog_bytes + new.size,
                oldest_message_timestamp = CASE
                  WHEN oldest_message_timestamp IS NULL
                    OR new.enqueued_at < oldest_message_timestamp
                  THEN new.enqueued_at ELSE oldest_message_timestamp END
          WHERE id = 1;
       END;

CREATE TRIGGER IF NOT EXISTS __sys_queue_messages_stats_delete
       AFTER DELETE ON __sys_queue_messages
       WHEN old.purge_on_settle = 0
       BEGIN
         UPDATE __sys_queue_stats
            SET backlog_count = backlog_count - 1,
                backlog_bytes = backlog_bytes - old.size,
                oldest_message_timestamp = CASE
                  WHEN backlog_count <= 1 THEN NULL
                  WHEN oldest_message_timestamp = old.enqueued_at
                  THEN (SELECT MIN(enqueued_at) FROM __sys_queue_messages
                         WHERE purge_on_settle = 0)
                  ELSE oldest_message_timestamp END
          WHERE id = 1;
       END;

CREATE TRIGGER IF NOT EXISTS __sys_queue_messages_stats_exclude
       AFTER UPDATE OF purge_on_settle ON __sys_queue_messages
       WHEN old.purge_on_settle = 0 AND new.purge_on_settle != 0
       BEGIN
         UPDATE __sys_queue_stats
            SET backlog_count = backlog_count - 1,
                backlog_bytes = backlog_bytes - old.size,
                oldest_message_timestamp = CASE
                  WHEN backlog_count <= 1 THEN NULL
                  WHEN oldest_message_timestamp = old.enqueued_at
                  THEN (SELECT MIN(enqueued_at) FROM __sys_queue_messages
                         WHERE purge_on_settle = 0)
                  ELSE oldest_message_timestamp END
          WHERE id = 1;
       END;

CREATE TRIGGER IF NOT EXISTS __sys_queue_messages_stats_include
       AFTER UPDATE OF purge_on_settle ON __sys_queue_messages
       WHEN old.purge_on_settle != 0 AND new.purge_on_settle = 0
       BEGIN
         UPDATE __sys_queue_stats
            SET backlog_count = backlog_count + 1,
                backlog_bytes = backlog_bytes + new.size,
                oldest_message_timestamp = CASE
                  WHEN oldest_message_timestamp IS NULL
                    OR new.enqueued_at < oldest_message_timestamp
                  THEN new.enqueued_at ELSE oldest_message_timestamp END
          WHERE id = 1;
       END;

CREATE TABLE IF NOT EXISTS __sys_queue_transfer_receipts (
         transfer_id TEXT PRIMARY KEY,
         message_id TEXT NOT NULL,
         dead_letter_source TEXT,
         expires_at INTEGER NOT NULL
       ) WITHOUT ROWID;

CREATE INDEX IF NOT EXISTS __sys_queue_transfer_receipts_expires
         ON __sys_queue_transfer_receipts (expires_at, transfer_id);
