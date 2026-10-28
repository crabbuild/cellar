// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Retire expired identities before reclaiming their parts. The tombstone
//! survives interruptions and clock rollback until its last part is gone.

use super::*;
use crate::object_multipart::MultipartGcProgress;

const UPLOADS_PER_PASS: usize = 64;
const PARTS_PER_PASS: usize = 1;

impl LocalStore {
    pub(super) fn reap_expired_sync(
        &self,
        scope: &str,
    ) -> object_store::Result<MultipartGcProgress> {
        let _writer = self.writer.lock();
        let mut connection = self.connect()?;
        let transaction = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(db_error)?;
        let now = crate::asyncrt::wall_ms();
        // Exact scope + status + deadline use the maintenance index. No LIKE
        // pattern, tenant-wide scan or materialized BLOB is needed.
        let retired_uploads = transaction
            .execute(
                "UPDATE __sys_local_uploads SET retired = 1 WHERE id IN (
               SELECT id FROM __sys_local_uploads
               WHERE scope = ?1 AND retired = 0 AND expires_ms <= ?2
               ORDER BY expires_ms, id LIMIT ?3
             )",
                params![scope, now, UPLOADS_PER_PASS],
            )
            .map_err(db_error)?;
        let ids = {
            let mut query = transaction
                .prepare(
                    "SELECT id FROM __sys_local_uploads WHERE scope = ?1 AND retired = 1
                 ORDER BY expires_ms, id LIMIT ?2",
                )
                .map_err(db_error)?;
            let ids = query
                .query_map(params![scope, UPLOADS_PER_PASS], |row| {
                    row.get::<_, String>(0)
                })
                .map_err(db_error)?
                .collect::<rusqlite::Result<Vec<_>>>()
                .map_err(db_error)?;
            ids
        };
        let mut progress = MultipartGcProgress {
            retired_uploads,
            ..Default::default()
        };
        for id in ids {
            if progress.deleted_parts < PARTS_PER_PASS {
                progress.deleted_parts += transaction
                    .execute(
                        "DELETE FROM __sys_local_parts WHERE rowid IN (
                       SELECT rowid FROM __sys_local_parts WHERE upload_id = ?1
                       ORDER BY part_index LIMIT ?2
                     )",
                        params![id, PARTS_PER_PASS - progress.deleted_parts],
                    )
                    .map_err(db_error)?;
            }
            progress.deleted_uploads += transaction
                .execute(
                    "DELETE FROM __sys_local_uploads WHERE id = ?1 AND retired = 1
                 AND NOT EXISTS(SELECT 1 FROM __sys_local_parts WHERE upload_id = ?1)",
                    [&id],
                )
                .map_err(db_error)?;
        }
        // Two bounded index probes avoid an OR forcing a scan of active rows.
        progress.pending = transaction.query_row(
            "SELECT EXISTS(SELECT 1 FROM __sys_local_uploads WHERE scope = ?1 AND retired = 1)
                 OR EXISTS(SELECT 1 FROM __sys_local_uploads WHERE scope = ?1 AND retired = 0 AND expires_ms <= ?2)",
            params![scope, now], |row| row.get(0),
        ).map_err(db_error)?;
        transaction.commit().map_err(db_error)?;
        Ok(progress)
    }
}
