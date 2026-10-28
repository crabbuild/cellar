// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Persistent, replaceable parts. Completion and retirement share one SQLite
//! transaction; an invalid receipt leaves both the upload and object untouched.

use super::*;
use object_store::{
    multipart::{MultipartStore, PartId},
    MultipartId,
};
use rand::RngCore as _;
use sha2::{Digest as _, Sha256};

const MAX_PARTS: usize = 10_000;

pub(super) fn check_part(index: usize) -> object_store::Result<i64> {
    if index >= MAX_PARTS {
        return Err(message_error("multipart part index exceeds 9999"));
    }
    i64::try_from(index).map_err(db_error)
}

impl LocalStore {
    pub(super) fn upload_attributes(
        connection: &rusqlite::Transaction<'_>,
        key: &str,
        id: &str,
    ) -> object_store::Result<Option<String>> {
        let (attributes, expires_ms, retired): (String, Option<i64>, bool) = connection
            .query_row(
                "SELECT attributes, expires_ms, retired FROM __sys_local_uploads WHERE id = ?1 AND key = ?2",
                params![id, key],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
            )
            .optional()
            .map_err(db_error)?
            .ok_or_else(|| not_found(key))?;
        if retired || expires_ms.is_some_and(|deadline| deadline <= crate::asyncrt::wall_ms()) {
            connection
                .execute(
                    "UPDATE __sys_local_uploads SET retired = 1 WHERE id = ?1 AND key = ?2",
                    params![id, key],
                )
                .map_err(db_error)?;
            // The caller must commit retirement before returning NotFound.
            // Rolling back this observation could revive it after clock rollback.
            return Ok(None);
        }
        Ok(Some(attributes))
    }

    pub(super) fn validate_upload_sync(&self, key: &str, id: &str) -> object_store::Result<()> {
        let _writer = self.writer.lock();
        let mut connection = self.connect()?;
        let transaction = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(db_error)?;
        let live = Self::upload_attributes(&transaction, key, id)?.is_some();
        transaction.commit().map_err(db_error)?;
        if live {
            Ok(())
        } else {
            Err(not_found(key))
        }
    }

    fn put_part_sync(
        &self,
        key: String,
        id: String,
        index: i64,
        body: Bytes,
    ) -> object_store::Result<PartId> {
        let receipt = hex::encode(Sha256::digest(&body));
        self.install_part_sync(&key, &id, index, body.len(), receipt, |part| {
            part.write_all_at(&body, 0).map_err(db_error)
        })
    }

    pub(super) fn install_part_sync(
        &self,
        key: &str,
        id: &str,
        index: i64,
        length: usize,
        receipt: String,
        write: impl FnOnce(&mut rusqlite::blob::Blob<'_>) -> object_store::Result<()>,
    ) -> object_store::Result<PartId> {
        let _writer = self.writer.lock();
        let mut connection = self.connect()?;
        let transaction = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(db_error)?;
        if Self::upload_attributes(&transaction, key, id)?.is_none() {
            transaction.commit().map_err(db_error)?;
            return Err(not_found(key));
        }
        let rowid = transaction.query_row(
            "INSERT INTO __sys_local_parts(upload_id, part_index, etag, body)
             VALUES (?1, ?2, ?3, ?4)
             ON CONFLICT(upload_id, part_index) DO UPDATE SET etag = excluded.etag, body = excluded.body
             RETURNING rowid",
            params![id, index, receipt, rusqlite::blob::ZeroBlob(i32::try_from(length).map_err(db_error)?)],
            |row| row.get::<_, i64>(0),
        ).map_err(db_error)?;
        {
            let mut part = transaction
                .blob_open(rusqlite::MAIN_DB, "__sys_local_parts", "body", rowid, false)
                .map_err(db_error)?;
            write(&mut part)?;
        }
        transaction.commit().map_err(db_error)?;
        Ok(PartId {
            content_id: receipt,
        })
    }

    fn complete_parts_sync(
        &self,
        key: String,
        id: String,
        parts: Vec<PartId>,
    ) -> object_store::Result<PutResult> {
        if parts.is_empty() || parts.len() > MAX_PARTS {
            return Err(message_error(
                "multipart completion requires 1 to 10000 parts",
            ));
        }
        let _writer = self.writer.lock();
        let mut connection = self.connect()?;
        let transaction = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(db_error)?;
        let Some(attributes) = Self::upload_attributes(&transaction, &key, &id)? else {
            transaction.commit().map_err(db_error)?;
            return Err(not_found(&key));
        };
        let mut inputs = Vec::with_capacity(parts.len());
        let mut size = 0_usize;
        for (index, receipt) in parts.iter().enumerate() {
            let part = transaction.query_row(
                "SELECT rowid, length(body), etag FROM __sys_local_parts WHERE upload_id = ?1 AND part_index = ?2",
                params![id, check_part(index)?],
                |row| Ok((row.get::<_, i64>(0)?, row.get::<_, usize>(1)?, row.get::<_, String>(2)?)),
            ).optional().map_err(db_error)?.ok_or_else(|| message_error("multipart completion names a missing part"))?;
            if part.2 != receipt.content_id {
                return Err(precondition(&key));
            }
            size = size
                .checked_add(part.1)
                .ok_or_else(|| message_error("multipart size overflow"))?;
            inputs.push((part.0, part.1));
        }
        let size = i32::try_from(size).map_err(db_error)?;
        let etag = transaction.query_row(
            "UPDATE __sys_local_sequence SET next_etag = next_etag + 1 WHERE singleton = 1 RETURNING next_etag - 1",
            [], |row| row.get::<_, i64>(0),
        ).map_err(db_error)?;
        let output = transaction.query_row(
            "INSERT INTO __sys_local_objects(key, etag, modified_ms, attributes, body)
             VALUES (?1, ?2, ?3, ?4, ?5)
             ON CONFLICT(key) DO UPDATE SET etag = excluded.etag, modified_ms = excluded.modified_ms,
               attributes = excluded.attributes, body = excluded.body RETURNING rowid",
            params![key, etag, crate::asyncrt::wall_ms(), attributes, rusqlite::blob::ZeroBlob(size)],
            |row| row.get::<_, i64>(0),
        ).map_err(db_error)?;
        {
            // Object size does not determine resident memory: copy one bounded
            // slice at a time while the upload rows and output share a snapshot.
            let mut output = transaction
                .blob_open(
                    rusqlite::MAIN_DB,
                    "__sys_local_objects",
                    "body",
                    output,
                    false,
                )
                .map_err(db_error)?;
            let mut buffer = vec![0; READ_CHUNK_BYTES];
            let mut destination = 0;
            for (row, size) in inputs {
                let input = transaction
                    .blob_open(rusqlite::MAIN_DB, "__sys_local_parts", "body", row, true)
                    .map_err(db_error)?;
                let mut offset = 0;
                while offset < size {
                    let length = (size - offset).min(buffer.len());
                    input
                        .read_at_exact(&mut buffer[..length], offset)
                        .map_err(db_error)?;
                    output
                        .write_all_at(&buffer[..length], destination)
                        .map_err(db_error)?;
                    offset += length;
                    destination += length;
                }
            }
        }
        transaction
            .execute("DELETE FROM __sys_local_parts WHERE upload_id = ?1", [&id])
            .map_err(db_error)?;
        transaction
            .execute("DELETE FROM __sys_local_uploads WHERE id = ?1", [&id])
            .map_err(db_error)?;
        transaction.commit().map_err(db_error)?;
        Ok(PutResult {
            e_tag: Some(etag.to_string()),
            version: None,
        })
    }
}

impl LocalStore {
    pub(super) async fn create_multipart_opts(
        &self,
        path: &Path,
        options: PutMultipartOptions,
    ) -> object_store::Result<MultipartId> {
        self.create_scoped_multipart(path, options, "").await
    }

    async fn create_scoped_multipart(
        &self,
        path: &Path,
        options: PutMultipartOptions,
        scope: &str,
    ) -> object_store::Result<MultipartId> {
        if !scope.is_empty() && (!scope.ends_with('/') || !path.as_ref().starts_with(scope)) {
            return Err(message_error(
                "multipart key is outside its canonical scope",
            ));
        }
        if !options.tags.encoded().is_empty() {
            return Err(Error::NotSupported {
                source: "local multipart tags are unsupported".into(),
            });
        }
        let mut random = [0; 32];
        crate::asyncrt::rng("local-multipart-upload")
            .try_fill_bytes(&mut random)
            .map_err(db_error)?;
        let id = hex::encode(random);
        let key = path.to_string();
        let scope = scope.to_string();
        let attributes = encode_attributes(&options.attributes)?;
        let guard = options.extensions.get::<crate::bucket::IoGuard>().cloned();
        let store = self.clone();
        store_blocking!(move || {
            let _capacity = guard;
            let _writer = store.writer.lock();
            let mut connection = store.connect()?;
            let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate).map_err(db_error)?;
            // Capture time after SQLite writer admission, including cross-process waits.
            let expires_ms = store.multipart_policy.upload_ttl.map(|ttl| {
                crate::asyncrt::wall_ms().checked_add(i64::try_from(ttl.as_millis()).map_err(db_error)?)
                    .ok_or_else(|| message_error("multipart deadline overflow"))
            }).transpose()?;
            transaction.execute(
                    "INSERT INTO __sys_local_uploads(id, key, attributes, scope, expires_ms, retired) VALUES (?1, ?2, ?3, ?4, ?5, 0)",
                    params![id, key, attributes, scope, expires_ms],
                )
                .map_err(db_error)?;
            transaction.commit().map_err(db_error)?;
            Ok(id)
        })
        .await
        .map_err(db_error)?
    }
}

#[async_trait]
impl MultipartStore for LocalStore {
    async fn create_multipart(&self, path: &Path) -> object_store::Result<MultipartId> {
        self.create_multipart_opts(path, PutMultipartOptions::default())
            .await
    }

    async fn put_part(
        &self,
        path: &Path,
        id: &MultipartId,
        index: usize,
        data: PutPayload,
    ) -> object_store::Result<PartId> {
        let index = check_part(index)?;
        let (store, key, id, body) = (
            self.clone(),
            path.to_string(),
            id.clone(),
            Bytes::from(data),
        );
        store_blocking!(move || store.put_part_sync(key, id, index, body))
            .await
            .map_err(db_error)?
    }

    async fn complete_multipart(
        &self,
        path: &Path,
        id: &MultipartId,
        parts: Vec<PartId>,
    ) -> object_store::Result<PutResult> {
        let (store, key, id) = (self.clone(), path.to_string(), id.clone());
        store_blocking!(move || store.complete_parts_sync(key, id, parts))
            .await
            .map_err(db_error)?
    }

    async fn abort_multipart(&self, path: &Path, id: &MultipartId) -> object_store::Result<()> {
        let (store, key, id) = (self.clone(), path.to_string(), id.clone());
        store_blocking!(move || {
            let _writer = store.writer.lock();
            let mut connection = store.connect()?;
            let transaction = connection
                .transaction_with_behavior(TransactionBehavior::Immediate)
                .map_err(db_error)?;
            let owner = transaction
                .query_row(
                    "SELECT key FROM __sys_local_uploads WHERE id = ?1",
                    [&id],
                    |row| row.get::<_, String>(0),
                )
                .optional()
                .map_err(db_error)?;
            if owner.as_ref().is_some_and(|owner| owner != &key) {
                return Err(not_found(&key));
            }
            transaction
                .execute("DELETE FROM __sys_local_parts WHERE upload_id = ?1", [&id])
                .map_err(db_error)?;
            transaction
                .execute("DELETE FROM __sys_local_uploads WHERE id = ?1", [&id])
                .map_err(db_error)?;
            transaction.commit().map_err(db_error)?;
            Ok(())
        })
        .await
        .map_err(db_error)?
    }
}

#[async_trait]
impl crate::object_multipart::ResumableMultipartStore for LocalStore {
    fn maintenance_policy(&self) -> Option<crate::object_multipart::MultipartMaintenancePolicy> {
        Some(self.multipart_policy)
    }

    async fn reap_expired(
        &self,
        scope: &str,
    ) -> object_store::Result<crate::object_multipart::MultipartGcProgress> {
        let store = self.clone();
        let scope = scope.to_string();
        store_blocking!(move || store.reap_expired_sync(&scope))
            .await
            .map_err(db_error)?
    }

    async fn create_scoped(
        &self,
        path: &Path,
        options: PutMultipartOptions,
        scope: &str,
    ) -> object_store::Result<MultipartId> {
        self.create_scoped_multipart(path, options, scope).await
    }

    fn supports_streamed_parts(&self) -> bool {
        true
    }

    async fn begin_part(
        &self,
        path: &Path,
        id: &MultipartId,
        index: usize,
        length: u64,
        capacity: Arc<dyn Send + Sync>,
    ) -> object_store::Result<Box<dyn crate::object_multipart::StreamedPart>> {
        super::streamed_part::begin(
            self.clone(),
            path.to_string(),
            id.clone(),
            index,
            length,
            capacity,
        )
        .await
    }

    fn supports_metadata(&self) -> bool {
        true
    }

    async fn create_with_options(
        &self,
        path: &Path,
        options: PutMultipartOptions,
    ) -> object_store::Result<MultipartId> {
        self.create_multipart_opts(path, options).await
    }
}
