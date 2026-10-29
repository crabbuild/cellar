// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Large development-store bodies are immutable staged chunks. Each chunk has
//! its own FULL transaction; publishing the pointer and ETag is one small CAS.
//! Readers validate the current ETag in the same snapshot as their chunk read.
//! Retirement is atomic with replacement/deletion, and reclamation uses bounded
//! transactions. An interrupted staging writer expires unless it keeps making
//! progress; an expired/retired writer cannot publish after reclamation starts.

use super::*;

const STAGING_IDLE_MS: i64 = 60_000;
const MAINTENANCE_CHUNKS: usize = 4;

impl LocalStore {
    pub(super) fn stage_body(
        &self,
        connection: &mut Connection,
        key: &str,
        body: &[u8],
    ) -> object_store::Result<i64> {
        let id = {
            let writer = self.writer.lock();
            let transaction = connection
                .transaction_with_behavior(TransactionBehavior::Immediate)
                .map_err(db_error)?;
            let id = transaction.query_row(
                "INSERT INTO __sys_local_bodies(expires_ms, retired) VALUES (?1, 0) RETURNING id",
                [staging_deadline()?], |row| row.get::<_, i64>(0),
            ).map_err(db_error)?;
            drop(writer);
            transaction.commit().map_err(db_error)?;
            id
        };
        for (index, chunk) in body.chunks(READ_CHUNK_BYTES).enumerate() {
            let writer = self.writer.lock();
            let transaction = connection
                .transaction_with_behavior(TransactionBehavior::Immediate)
                .map_err(db_error)?;
            if !renew_stage(&transaction, id)? {
                drop(writer);
                transaction.commit().map_err(db_error)?;
                return Err(message_error("large object staging expired"));
            }
            transaction.execute(
                "INSERT INTO __sys_local_body_chunks(body_id, part_index, body) VALUES (?1, ?2, ?3)",
                params![id, index, chunk],
            ).map_err(db_error)?;
            drop(writer);
            transaction.commit().map_err(db_error)?;
            #[cfg(test)]
            if index == 0 {
                if let Some((prefix, gate)) = &self.body_gate {
                    if key.starts_with(prefix) {
                        gate.wait();
                    }
                }
            }
        }
        // The key is used only by the private fault seam; no key is persisted
        // in staging or emitted in timing evidence.
        let _ = key;
        Ok(id)
    }

    pub(super) fn retire_body_for_key(
        transaction: &rusqlite::Transaction<'_>,
        key: &str,
    ) -> object_store::Result<Option<i64>> {
        let body: Option<i64> = transaction
            .query_row(
                "SELECT body_id FROM __sys_local_objects WHERE key = ?1",
                [key],
                |row| row.get(0),
            )
            .optional()
            .map_err(db_error)?
            .flatten();
        if let Some(id) = body {
            transaction
                .execute(
                    "UPDATE __sys_local_bodies SET retired = 1 WHERE id = ?1",
                    [id],
                )
                .map_err(db_error)?;
        }
        Ok(body)
    }

    pub(super) fn publish_body(
        transaction: &rusqlite::Transaction<'_>,
        id: i64,
    ) -> object_store::Result<()> {
        if !renew_stage(transaction, id)? {
            return Err(message_error(
                "large object staging expired before publication",
            ));
        }
        transaction
            .execute(
                "UPDATE __sys_local_bodies SET expires_ms = NULL WHERE id = ?1",
                [id],
            )
            .map_err(db_error)?;
        Ok(())
    }

    pub(super) fn retire_staged_body(
        &self,
        connection: &mut Connection,
        id: i64,
    ) -> object_store::Result<()> {
        let writer = self.writer.lock();
        let transaction = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(db_error)?;
        // A failed/ambiguous publish must never retire a body now named by an
        // object. The small metadata transaction is the sole publication point.
        transaction
            .execute(
                "UPDATE __sys_local_bodies SET retired = 1 WHERE id = ?1 AND NOT EXISTS
             (SELECT 1 FROM __sys_local_objects WHERE body_id = ?1)",
                [id],
            )
            .map_err(db_error)?;
        drop(writer);
        transaction.commit().map_err(db_error)
    }

    /// Reclaim a known retired body completely, releasing admission between
    /// chunks. Concurrent readers hold SQLite snapshots; streamed reads which
    /// have not entered yet must still validate their object's current ETag.
    pub(super) fn drain_body(
        &self,
        connection: &mut Connection,
        id: i64,
    ) -> object_store::Result<()> {
        while self.reap_body_chunk(connection, Some(id))? {}
        Ok(())
    }

    pub(super) fn maintain_bodies(&self, connection: &mut Connection) -> object_store::Result<()> {
        for _ in 0..MAINTENANCE_CHUNKS {
            if !self.reap_body_chunk(connection, None)? {
                break;
            }
        }
        Ok(())
    }

    fn reap_body_chunk(
        &self,
        connection: &mut Connection,
        selected: Option<i64>,
    ) -> object_store::Result<bool> {
        let writer = self.writer.lock();
        let transaction = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(db_error)?;
        let id: Option<i64> = if let Some(id) = selected {
            transaction
                .query_row(
                    "SELECT id FROM __sys_local_bodies WHERE id = ?1 AND retired = 1",
                    [id],
                    |row| row.get(0),
                )
                .optional()
                .map_err(db_error)?
        } else {
            transaction.query_row(
                "SELECT id FROM __sys_local_bodies WHERE retired = 1 LIMIT 1", [], |row| row.get(0),
            ).optional().map_err(db_error)?.or(transaction.query_row(
                "SELECT id FROM __sys_local_bodies WHERE expires_ms <= ?1 ORDER BY expires_ms, id LIMIT 1",
                [crate::asyncrt::wall_ms()], |row| row.get(0),
            ).optional().map_err(db_error)?)
        };
        let Some(id) = id else {
            // No mutation occurred; do not create a new WAL commit merely for
            // an empty maintenance check.
            return Ok(false);
        };
        transaction
            .execute(
                "UPDATE __sys_local_bodies SET retired = 1 WHERE id = ?1",
                [id],
            )
            .map_err(db_error)?;
        transaction
            .execute(
                "DELETE FROM __sys_local_body_chunks WHERE body_id = ?1 AND part_index =
             (SELECT min(part_index) FROM __sys_local_body_chunks WHERE body_id = ?1)",
                [id],
            )
            .map_err(db_error)?;
        transaction
            .execute(
                "DELETE FROM __sys_local_bodies WHERE id = ?1 AND NOT EXISTS
             (SELECT 1 FROM __sys_local_body_chunks WHERE body_id = ?1)",
                [id],
            )
            .map_err(db_error)?;
        drop(writer);
        transaction.commit().map_err(db_error)?;
        Ok(true)
    }

    pub(super) fn read_body_range(
        transaction: &rusqlite::Transaction<'_>,
        id: i64,
        offset: u64,
        bytes: &mut [u8],
    ) -> object_store::Result<()> {
        let mut copied = 0;
        while copied < bytes.len() {
            let position = offset + copied as u64;
            let index = position / READ_CHUNK_BYTES as u64;
            let within = (position % READ_CHUNK_BYTES as u64) as usize;
            let length = (READ_CHUNK_BYTES - within).min(bytes.len() - copied);
            let chunk: Vec<u8> = transaction.query_row(
                "SELECT substr(body, ?3, ?4) FROM __sys_local_body_chunks WHERE body_id = ?1 AND part_index = ?2",
                params![id, index, within + 1, length], |row| row.get(0),
            ).map_err(db_error)?;
            if chunk.len() != length {
                return Err(message_error("incomplete large object chunk"));
            }
            bytes[copied..copied + length].copy_from_slice(&chunk);
            copied += length;
        }
        Ok(())
    }
}

fn staging_deadline() -> object_store::Result<i64> {
    crate::asyncrt::wall_ms()
        .checked_add(STAGING_IDLE_MS)
        .ok_or_else(|| message_error("large object staging deadline overflow"))
}

fn renew_stage(transaction: &rusqlite::Transaction<'_>, id: i64) -> object_store::Result<bool> {
    let changed = transaction.execute(
        "UPDATE __sys_local_bodies SET expires_ms = ?2 WHERE id = ?1 AND retired = 0 AND expires_ms > ?3",
        params![id, staging_deadline()?, crate::asyncrt::wall_ms()],
    ).map_err(db_error)?;
    if changed == 0 {
        transaction.execute("UPDATE __sys_local_bodies SET retired = 1 WHERE id = ?1 AND expires_ms IS NOT NULL", [id]).map_err(db_error)?;
    }
    Ok(changed == 1)
}
