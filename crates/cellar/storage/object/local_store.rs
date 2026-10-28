// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

// Copyright 2026 Deno Land Inc. Apache-2.0 license.
// Modified for Cellar.

//! The persistent object store for `cellar dev`.
//!
//! The development supervisor and its node are separate processes, and both
//! write the fleet store during a reload. SQLite supplies the cross-process
//! transaction that a directory of files cannot: a conditional update checks
//! its ETag and installs the new object in one commit. This backend is local to
//! one development machine. It is not a shared-filesystem production mode.
//!
//! Operation connections leave committed frames in the WAL. Closing an idle
//! connection must not acquire the exclusive lock for a final checkpoint while
//! another worker is trying to renew a lease. FULL commits and SQLite's normal
//! automatic checkpoints remain enabled; preserve the database and its WAL
//! together when moving local development state.
//! One idle connection keeps the WAL index alive between operations. Without
//! it, reopening recovers the index and loses checkpoint progress, repeatedly
//! scanning and checkpointing a large WAL for even a one-byte write.

use async_trait::async_trait;
use bytes::Bytes;
use futures_util::future::BoxFuture;
use futures_util::stream::{self, BoxStream};
use futures_util::FutureExt as _;
use futures_util::{StreamExt as _, TryStreamExt as _};
use object_store::list::{PaginatedListOptions, PaginatedListResult, PaginatedListStore};
use object_store::path::Path;
use object_store::{
    Attribute, AttributeValue, Attributes, Error, GetOptions, GetResult, GetResultPayload,
    ListResult, MultipartUpload, ObjectMeta, ObjectStore, PutMode, PutMultipartOptions, PutOptions,
    PutPayload, PutResult,
};
use rusqlite::{config::DbConfig, params, Connection, OptionalExtension as _, TransactionBehavior};
use serde::{Deserialize, Serialize};
use std::collections::BTreeSet;
use std::fmt;
use std::path::{Path as FsPath, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime};

const STORE: &str = "cellar development store";
const READ_CHUNK_BYTES: usize = 128 << 10;

#[path = "local_store_bodies.rs"]
mod bodies;
#[cfg(test)]
#[path = "local_store_bodies_tests.rs"]
mod bodies_tests;
#[path = "local_store_timing.rs"]
mod timing;

#[cfg(test)]
#[path = "local_store_timing_tests.rs"]
mod timing_tests;

#[derive(Clone, Debug)]
pub(crate) struct LocalStore {
    database: PathBuf,
    multipart_policy: crate::object_multipart::MultipartMaintenancePolicy,
    // No statements or transactions remain on this connection after open.
    // Clones share its lifetime so operation connections can recycle the WAL
    // after automatic checkpoints. Mutex provides Sync for the idle handle;
    // operations still own separate connections and never lock this mutex.
    _wal_index: Arc<Mutex<Connection>>,
    // Admit transaction starts fairly among clones. The SQLite write lock
    // protects the transaction after this guard is released for COMMIT, whose
    // automatic checkpoint must not block unrelated lease writers behind it.
    // Other instances/processes still arbitrate through SQLite's write lock.
    writer: Arc<parking_lot::FairMutex<()>>,
    #[cfg(all(test, unix, not(cellar_internal_tests)))]
    checkpoint_vfs: Option<Arc<checkpoint_tests::CheckpointVfs>>,
    #[cfg(test)]
    read_gate: Option<Arc<read_tests::ReadGate>>,
    #[cfg(test)]
    metadata_gate: Option<Arc<read_tests::ReadGate>>,
    #[cfg(test)]
    list_gate: Option<Arc<read_tests::ReadGate>>,
    #[cfg(test)]
    put_gate: Option<(String, Arc<read_tests::ReadGate>)>,
    #[cfg(test)]
    body_gate: Option<(String, Arc<read_tests::ReadGate>)>,
}

#[derive(Debug)]
struct StoredObject {
    key: String,
    body: Vec<u8>,
    size: u64,
    etag: i64,
    modified_ms: i64,
    attributes: String,
}

#[derive(Debug, Serialize, Deserialize)]
struct StoredAttribute {
    kind: String,
    name: Option<String>,
    value: String,
}

impl LocalStore {
    pub(crate) fn open(database: impl AsRef<FsPath>) -> object_store::Result<Self> {
        Self::open_with_multipart_policy(database, Default::default())
    }

    pub(crate) fn open_with_multipart_policy(
        database: impl AsRef<FsPath>,
        multipart_policy: crate::object_multipart::MultipartMaintenancePolicy,
    ) -> object_store::Result<Self> {
        multipart_policy.validate()?;
        let database = database.as_ref().to_path_buf();
        let connection = Connection::open(&database).map_err(db_error)?;
        let has_objects: bool = connection
            .query_row(
                "SELECT EXISTS(SELECT 1 FROM sqlite_schema WHERE name = '__sys_local_objects')",
                [],
                |row| row.get(0),
            )
            .map_err(db_error)?;
        if has_objects {
            connection
                .prepare("SELECT body_id, body_size FROM __sys_local_objects LIMIT 0")
                .map_err(|_| {
                    message_error("incompatible local object schema; use a fresh development store")
                })?;
        }
        // Internal format hard cutover: refuse old upload records rather than
        // guessing deadlines, migrating state or discarding application data.
        let has_uploads: bool = connection
            .query_row(
                "SELECT EXISTS(SELECT 1 FROM sqlite_schema WHERE name = '__sys_local_uploads')",
                [],
                |row| row.get(0),
            )
            .map_err(db_error)?;
        if has_uploads {
            connection.prepare(
                "SELECT id, key, attributes, scope, expires_ms, retired FROM __sys_local_uploads LIMIT 0",
            ).map_err(|_| message_error("incompatible local multipart schema; use a fresh development store"))?;
        }
        configure_connection(&connection)?;
        connection
            .pragma_update(None, "journal_mode", "WAL")
            .map_err(db_error)?;
        // A trailing BLOB preserves SQLite's zero-tail optimization while
        // incremental writes fill the space reserved by ZeroBlob.
        connection
            .execute_batch(
                "CREATE TABLE IF NOT EXISTS __sys_local_objects (
                   key TEXT PRIMARY KEY,
                   etag INTEGER NOT NULL,
                   modified_ms INTEGER NOT NULL,
                   attributes TEXT NOT NULL,
                   body_id INTEGER,
                   body_size INTEGER,
                   body BLOB NOT NULL,
                   CHECK ((body_id IS NULL AND body_size IS NULL) OR
                     (body_id IS NOT NULL AND body_size > 0 AND length(body) = 0))
                 );
                 CREATE UNIQUE INDEX IF NOT EXISTS __sys_local_object_body ON __sys_local_objects(body_id);
                 CREATE TABLE IF NOT EXISTS __sys_local_bodies (
                   id INTEGER PRIMARY KEY AUTOINCREMENT,
                   expires_ms INTEGER,
                   retired INTEGER NOT NULL CHECK (retired IN (0, 1))
                 );
                 CREATE INDEX IF NOT EXISTS __sys_local_body_expiry ON __sys_local_bodies(expires_ms, id);
                 CREATE INDEX IF NOT EXISTS __sys_local_body_retired ON __sys_local_bodies(retired, id);
                 CREATE TABLE IF NOT EXISTS __sys_local_body_chunks (
                   body_id INTEGER NOT NULL,
                   part_index INTEGER NOT NULL,
                   body BLOB NOT NULL,
                   PRIMARY KEY (body_id, part_index)
                 ) WITHOUT ROWID;
                 CREATE TABLE IF NOT EXISTS __sys_local_sequence (
                   singleton INTEGER PRIMARY KEY CHECK (singleton = 1),
                   next_etag INTEGER NOT NULL
                 );
                 CREATE TABLE IF NOT EXISTS __sys_local_uploads (
                   id TEXT PRIMARY KEY,
                   key TEXT NOT NULL,
                   attributes TEXT NOT NULL,
                   scope TEXT NOT NULL,
                   expires_ms INTEGER,
                   retired INTEGER NOT NULL CHECK (retired IN (0, 1))
                 );
                 CREATE INDEX IF NOT EXISTS __sys_local_upload_expiry
                   ON __sys_local_uploads(scope, retired, expires_ms, id);
                 CREATE TABLE IF NOT EXISTS __sys_local_parts (
                   upload_id TEXT NOT NULL,
                   part_index INTEGER NOT NULL,
                   etag TEXT NOT NULL,
                   body BLOB NOT NULL,
                   PRIMARY KEY (upload_id, part_index)
                 );
                 INSERT OR IGNORE INTO __sys_local_sequence(singleton, next_etag) VALUES (1, 1);",
            )
            .map_err(db_error)?;
        Ok(Self {
            database,
            multipart_policy,
            _wal_index: Arc::new(Mutex::new(connection)),
            writer: Arc::new(parking_lot::FairMutex::new(())),
            #[cfg(all(test, unix, not(cellar_internal_tests)))]
            checkpoint_vfs: None,
            #[cfg(test)]
            read_gate: None,
            #[cfg(test)]
            metadata_gate: None,
            #[cfg(test)]
            list_gate: None,
            #[cfg(test)]
            put_gate: None,
            #[cfg(test)]
            body_gate: None,
        })
    }

    fn connect(&self) -> object_store::Result<Connection> {
        #[cfg(all(test, unix, not(cellar_internal_tests)))]
        if let Some(vfs) = &self.checkpoint_vfs {
            let connection = Connection::open_with_flags_and_vfs(
                &self.database,
                rusqlite::OpenFlags::default(),
                vfs.name(),
            )
            .map_err(db_error)?;
            configure_connection(&connection)?;
            return Ok(connection);
        }
        let connection = Connection::open(&self.database).map_err(db_error)?;
        configure_connection(&connection)?;
        Ok(connection)
    }

    fn connect_reader(&self) -> object_store::Result<Connection> {
        // Reads must neither recreate a missing backend nor checkpoint unrelated
        // object data when their short-lived connection happens to close last.
        // Writers retain FULL commits and ordinary automatic checkpoints.
        let flags =
            rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY | rusqlite::OpenFlags::SQLITE_OPEN_NO_MUTEX;
        #[cfg(all(test, unix, not(cellar_internal_tests)))]
        let connection = match &self.checkpoint_vfs {
            Some(vfs) => Connection::open_with_flags_and_vfs(&self.database, flags, vfs.name()),
            None => Connection::open_with_flags(&self.database, flags),
        }
        .map_err(db_error)?;
        #[cfg(any(not(test), not(unix), cellar_internal_tests))]
        let connection = Connection::open_with_flags(&self.database, flags).map_err(db_error)?;
        configure_connection(&connection)?;
        connection
            .set_db_config(
                rusqlite::config::DbConfig::SQLITE_DBCONFIG_NO_CKPT_ON_CLOSE,
                true,
            )
            .map_err(db_error)?;
        Ok(connection)
    }

    fn read(&self, key: &str) -> object_store::Result<StoredObject> {
        let mut object = self.read_metadata(key, true)?;
        if object.size > READ_CHUNK_BYTES as u64 {
            let size = usize::try_from(object.size).map_err(db_error)?;
            object.body.reserve(size);
            for offset in (0..size).step_by(READ_CHUNK_BYTES) {
                object.body.extend_from_slice(&self.read_chunk(
                    key,
                    object.etag,
                    object.size,
                    offset as u64,
                    (size - offset).min(READ_CHUNK_BYTES),
                )?);
            }
        }
        Ok(object)
    }

    fn read_metadata(
        &self,
        key: &str,
        include_small_body: bool,
    ) -> object_store::Result<StoredObject> {
        self.connect_reader()?
            .query_row(
                "SELECT key, etag, modified_ms, attributes, COALESCE(body_size, length(body)),
                        CASE WHEN ?2 AND COALESCE(body_size, length(body)) <= ?3 THEN body ELSE NULL END
                 FROM __sys_local_objects WHERE key = ?1",
                params![key, include_small_body, READ_CHUNK_BYTES as i64],
                |row| {
                    #[cfg(test)]
                    if let Some(gate) = &self.metadata_gate {
                        gate.wait();
                    }
                    Ok(StoredObject {
                        key: row.get(0)?,
                        // Lease renewals may replace the object before its
                        // response body is polled. Capture small bodies in
                        // this same statement, without loading large BLOBs
                        // or keeping a transaction open in an idle stream.
                        body: row.get::<_, Option<Vec<u8>>>(5)?.unwrap_or_default(),
                        size: row.get(4)?,
                        etag: row.get(1)?,
                        modified_ms: row.get(2)?,
                        attributes: row.get(3)?,
                    })
                },
            )
            .optional()
            .map_err(db_error)?
            .ok_or_else(|| not_found(key))
    }

    fn read_chunk(
        &self,
        key: &str,
        etag: i64,
        size: u64,
        offset: u64,
        length: usize,
    ) -> object_store::Result<Bytes> {
        let mut connection = self.connect_reader()?;
        let transaction = connection.transaction().map_err(db_error)?;
        let (rowid, body_id) = transaction
            .query_row(
                "SELECT rowid, body_id FROM __sys_local_objects
                 WHERE key = ?1 AND etag = ?2 AND COALESCE(body_size, length(body)) = ?3",
                params![key, etag, size],
                |row| Ok((row.get::<_, i64>(0)?, row.get::<_, Option<i64>>(1)?)),
            )
            .optional()
            .map_err(db_error)?
            .ok_or_else(|| precondition(key))?;
        let mut bytes = vec![0; length];
        #[cfg(test)]
        if let Some(gate) = &self.read_gate {
            gate.wait();
        }
        if let Some(id) = body_id {
            Self::read_body_range(&transaction, id, offset, &mut bytes)?;
        } else {
            // SQL BLOB extraction can materialize the whole value inside
            // SQLite even when the Rust caller only retains a small slice.
            // Incremental I/O pins this row/version in the read transaction.
            let blob = transaction
                .blob_open(
                    rusqlite::MAIN_DB,
                    "__sys_local_objects",
                    "body",
                    rowid,
                    true,
                )
                .map_err(db_error)?;
            blob.read_at_exact(&mut bytes, usize::try_from(offset).map_err(db_error)?)
                .map_err(db_error)?;
        }
        transaction.commit().map_err(db_error)?;
        Ok(bytes.into())
    }

    fn metadata(&self, prefix: &str) -> object_store::Result<Vec<StoredObject>> {
        let connection = self.connect_reader()?;
        #[cfg(test)]
        if let Some(gate) = &self.list_gate {
            gate.wait();
        }
        // Restoration lists one cell/epoch at a time. Restrict the indexed
        // scan before decoding rows so its cost does not grow with unrelated
        // cells and their histories. BINARY ordering preserves literal '%' and
        // '_' characters; LIKE would give those characters wildcard semantics.
        let upper = prefix_upper_bound(prefix);
        let sql = format!(
            "SELECT key, CAST(X'' AS BLOB), etag, modified_ms, attributes, COALESCE(body_size, length(body))
             FROM __sys_local_objects WHERE key >= ?1 {} ORDER BY key",
            if upper.is_some() { "AND key < ?2" } else { "" },
        );
        let mut statement = connection.prepare(&sql).map_err(db_error)?;
        let rows = statement
            .query_map(
                rusqlite::params_from_iter(std::iter::once(prefix).chain(upper.as_deref())),
                |row| {
                    Ok(StoredObject {
                        key: row.get(0)?,
                        body: row.get(1)?,
                        size: row.get(5)?,
                        etag: row.get(2)?,
                        modified_ms: row.get(3)?,
                        attributes: row.get(4)?,
                    })
                },
            )
            .map_err(db_error)?;
        rows.collect::<Result<Vec<_>, _>>().map_err(db_error)
    }

    fn put_sync(
        &self,
        key: String,
        body: Bytes,
        options: PutOptions,
    ) -> object_store::Result<PutResult> {
        self.put_sync_timed(key, body, options, &timing::Timing::default())
    }

    fn put_sync_timed(
        &self,
        key: String,
        body: Bytes,
        options: PutOptions,
        timing: &timing::Timing,
    ) -> object_store::Result<PutResult> {
        let attributes = encode_attributes(&options.attributes)?;
        let modified_ms = crate::asyncrt::wall_ms();
        let mut connection = self.connect()?;
        timing.mark("connected");
        // The fixture observes a real entered SQLite connection without
        // locking out unrelated metadata/claim I/O while the worker pauses.
        #[cfg(test)]
        if let Some((prefix, gate)) = &self.put_gate {
            if key.starts_with(prefix) {
                gate.wait();
            }
        }
        let staged = if body.len() > READ_CHUNK_BYTES {
            timing.mark("body_staging");
            let id = self.stage_body(&mut connection, &key, &body)?;
            timing.mark("body_staged");
            Some(id)
        } else {
            None
        };
        let published = (|| {
            let writer = self.writer.lock();
            timing.mark("writer_admitted");
            let transaction = connection
                .transaction_with_behavior(TransactionBehavior::Immediate)
                .map_err(db_error)?;
            timing.mark("transaction_started");
            let current = transaction
                .query_row(
                    "SELECT etag FROM __sys_local_objects WHERE key = ?1",
                    [&key],
                    |row| row.get::<_, i64>(0),
                )
                .optional()
                .map_err(db_error)?;
            match options.mode {
                PutMode::Overwrite => {}
                PutMode::Create if current.is_some() => return Err(already_exists(&key)),
                PutMode::Create => {}
                PutMode::Update(version) => {
                    let current = current.map(|etag| etag.to_string());
                    if current.as_deref().is_none()
                        || version.e_tag.as_deref() != current.as_deref()
                    {
                        return Err(precondition(&key));
                    }
                }
            }
            let retired = Self::retire_body_for_key(&transaction, &key)?;
            if let Some(id) = staged {
                Self::publish_body(&transaction, id)?;
            }
            let etag = transaction
                .query_row(
                    "UPDATE __sys_local_sequence SET next_etag = next_etag + 1
                 WHERE singleton = 1 RETURNING next_etag - 1",
                    [],
                    |row| row.get::<_, i64>(0),
                )
                .map_err(db_error)?;
            // Reserve the body without binding a full-size copy. Incremental
            // writes share the transaction with the ETag check and metadata.
            let rowid = transaction
            .query_row(
                "INSERT INTO __sys_local_objects(key, body, etag, modified_ms, attributes, body_id, body_size)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)
                 ON CONFLICT(key) DO UPDATE SET
                   body = excluded.body,
                   etag = excluded.etag,
                   modified_ms = excluded.modified_ms,
                   attributes = excluded.attributes,
                   body_id = excluded.body_id,
                   body_size = excluded.body_size
                 RETURNING rowid",
                params![
                    key,
                    rusqlite::blob::ZeroBlob(if staged.is_some() { 0 } else { i32::try_from(body.len()).map_err(db_error)? }),
                    etag,
                    modified_ms,
                    attributes,
                    staged,
                    staged.map(|_| body.len())
                ],
                |row| row.get::<_, i64>(0),
            )
            .map_err(db_error)?;
            if staged.is_none() {
                let mut blob = transaction
                    .blob_open(
                        rusqlite::MAIN_DB,
                        "__sys_local_objects",
                        "body",
                        rowid,
                        false,
                    )
                    .map_err(db_error)?;
                blob.write_all_at(&body, 0).map_err(db_error)?;
            }
            // SQLite still owns the write lock, so the CAS remains atomic. Release
            // only our admission guard: COMMIT's post-transaction checkpoint can
            // stall on main-file I/O after SQLite has already allowed new writers.
            drop(writer);
            timing.mark("commit_started");
            transaction.commit().map_err(db_error)?;
            timing.mark("committed");
            Ok::<_, object_store::Error>((
                PutResult {
                    e_tag: Some(etag.to_string()),
                    version: None,
                },
                retired,
            ))
        })();
        if published.is_err() {
            if let Some(id) = staged {
                self.retire_staged_body(&mut connection, id)?;
                self.drain_body(&mut connection, id)?;
            }
        }
        let (result, retired) = published?;
        if let Some(id) = retired {
            self.drain_body(&mut connection, id)?;
        }
        self.maintain_bodies(&mut connection)?;
        Ok(result)
    }

    fn copy_sync(&self, from: &str, to: &str, create: bool) -> object_store::Result<()> {
        let source = self.read(from)?;
        let attributes = decode_attributes(&source.attributes)?;
        self.put_sync(
            to.to_string(),
            source.body.into(),
            PutOptions {
                mode: if create {
                    PutMode::Create
                } else {
                    PutMode::Overwrite
                },
                attributes,
                ..PutOptions::default()
            },
        )?;
        Ok(())
    }
}

fn configure_connection(connection: &Connection) -> object_store::Result<()> {
    // Every request owns a connection, including metadata reads. A last-close
    // checkpoint takes an exclusive lock and can make new connections back off
    // through a lease's entire renewal window. Retain the WAL between requests;
    // automatic checkpoints still run, and FULL still syncs each commit.
    connection
        .set_db_config(DbConfig::SQLITE_DBCONFIG_NO_CKPT_ON_CLOSE, true)
        .map_err(db_error)?;
    // `synchronous` is connection-local. Setting it only while the schema is
    // created leaves later writes at the bundled SQLite default, so a build
    // configuration can silently weaken the development store's durability.
    connection
        .pragma_update(None, "synchronous", "FULL")
        .map_err(db_error)?;
    // Each operation owns a connection. Its freed page cache can remain in
    // a blocking worker's allocator arena; cap it at one read chunk.
    connection
        .pragma_update(None, "cache_size", -(READ_CHUNK_BYTES as i64 / 1024))
        .map_err(db_error)?;
    connection
        .busy_timeout(Duration::from_secs(30))
        .map_err(db_error)
}

impl fmt::Display for LocalStore {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "LocalStore({})", self.database.display())
    }
}

// The externally supplied deterministic backend retains its existing adapter.
// Production admission is fallible when its execution domain is closing.
macro_rules! store_blocking {
    ($operation:expr) => {{
        #[cfg(not(cellar_internal_tests))]
        {
            crate::asyncrt::try_blocking($operation).map_err(db_error)?
        }
        #[cfg(cellar_internal_tests)]
        {
            crate::asyncrt::blocking($operation)
        }
    }};
}

#[path = "local_store_multipart.rs"]
mod multipart;

#[path = "local_store_expiry.rs"]
mod expiry;

#[cfg(test)]
#[path = "local_store_expiry_tests.rs"]
mod expiry_tests;

#[path = "local_store_streamed_part.rs"]
mod streamed_part;

#[cfg(test)]
#[path = "local_store_streamed_part_tests.rs"]
mod streamed_part_tests;

#[cfg(test)]
#[path = "local_store_multipart_tests.rs"]
mod multipart_tests;

#[async_trait]
impl ObjectStore for LocalStore {
    async fn put_opts(
        &self,
        location: &Path,
        payload: PutPayload,
        options: PutOptions,
    ) -> object_store::Result<PutResult> {
        let store = self.clone();
        let key = location.to_string();
        let body: Bytes = payload.into();
        let reservation = options
            .extensions
            .get::<crate::bucket::IoGuard>()
            .map(crate::bucket::IoGuard::keep_alive);
        let mut caller = timing::Caller::new();
        let timing = caller.timing.clone();
        let result = store_blocking!(move || {
            timing.mark("native_entered");
            // The caller may cancel after native entry. Return capacity only
            // after SQLite has released its input, connection and transaction.
            let result = store.put_sync_timed(key, body, options, &timing);
            drop(reservation);
            timing.mark("native_complete");
            result
        })
        .await;
        caller.delivered();
        result.map_err(db_error)?
    }

    async fn put_multipart_opts(
        &self,
        location: &Path,
        options: PutMultipartOptions,
    ) -> object_store::Result<Box<dyn MultipartUpload>> {
        Ok(Box::new(LocalUpload {
            store: self.clone(),
            location: location.clone(),
            attributes: options.attributes,
            extensions: options.extensions,
            parts: Arc::new(Mutex::new(Vec::new())),
        }))
    }

    async fn get_opts(
        &self,
        location: &Path,
        options: GetOptions,
    ) -> object_store::Result<GetResult> {
        let store = self.clone();
        let key = location.to_string();
        #[cfg(not(cellar_internal_tests))]
        let owner = crate::asyncrt::services()
            .domain_token()
            .ok_or_else(|| domain_error(crate::asyncrt::DomainClosed))?;
        let guard = options.extensions.get::<crate::bucket::IoGuard>().cloned();
        let metadata_guard = guard.as_ref().map(crate::bucket::IoGuard::keep_alive);
        let include_small_body = !options.head;
        let object = store_blocking!(move || {
            let _reservation = metadata_guard;
            store.read_metadata(&key, include_small_body)
        })
        .await
        .map_err(db_error)??;
        let meta = object_meta(&object)?;
        options.check_preconditions(&meta)?;
        let full = 0..object.size;
        let range = match options.range {
            Some(range) => range.as_range(full.end).map_err(db_error)?,
            None => full,
        };
        let payload = if options.head {
            stream::empty().boxed()
        } else {
            let body = ReadBody {
                store: self.clone(),
                key: object.key.clone(),
                etag: object.etag,
                size: object.size,
                cursor: range.start,
                end: range.end,
                inline: (object.size <= READ_CHUNK_BYTES as u64).then(|| Bytes::from(object.body)),
                guard,
                #[cfg(not(cellar_internal_tests))]
                owner,
            };
            stream::try_unfold(body, ReadBody::next_chunk).boxed()
        };
        Ok(GetResult {
            payload: GetResultPayload::Stream(payload),
            meta,
            range,
            attributes: decode_attributes(&object.attributes)?,
        })
    }

    async fn delete(&self, location: &Path) -> object_store::Result<()> {
        let store = self.clone();
        let key = location.to_string();
        store_blocking!(move || {
            let writer;
            let mut connection = store.connect()?;
            writer = store.writer.lock();
            let transaction = connection
                .transaction_with_behavior(TransactionBehavior::Immediate)
                .map_err(db_error)?;
            let retired = Self::retire_body_for_key(&transaction, &key)?;
            transaction
                .execute("DELETE FROM __sys_local_objects WHERE key = ?1", [key])
                .map_err(db_error)?;
            // Match put/CAS: SQLite serializes COMMIT, while its subsequent
            // automatic checkpoint must leave writer admission available.
            drop(writer);
            transaction.commit().map_err(db_error)?;
            if let Some(id) = retired {
                store.drain_body(&mut connection, id)?;
            }
            store.maintain_bodies(&mut connection)?;
            Ok(())
        })
        .await
        .map_err(db_error)?
    }

    fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
        let prefix = prefix.cloned();
        let store = self.clone();
        // A returned stream may be polled by another domain. Its SQLite work
        // remains owned by the domain that created it, including after caller
        // cancellation; a closed original owner refuses new I/O.
        #[cfg(not(cellar_internal_tests))]
        let owner = crate::asyncrt::services().domain_token();
        let operation = move || {
            store
                .metadata(prefix.as_ref().map_or("", |path| path.as_ref()))
                .and_then(|objects| {
                    objects
                        .into_iter()
                        .map(|object| object_meta(&object))
                        .filter(|meta| {
                            meta.as_ref().map_or(true, |meta| {
                                prefix.as_ref().is_none_or(|prefix| {
                                    meta.location
                                        .prefix_match(prefix)
                                        .is_some_and(|mut remainder| remainder.next().is_some())
                                })
                            })
                        })
                        .collect::<object_store::Result<Vec<_>>>()
                })
        };
        stream::once(async move {
            #[cfg(not(cellar_internal_tests))]
            let result = owner
                .ok_or_else(|| domain_error(crate::asyncrt::DomainClosed))?
                .blocking(operation)
                .map_err(domain_error)?
                .await
                .map_err(db_error)?;
            #[cfg(cellar_internal_tests)]
            let result = store_blocking!(operation).await.map_err(db_error)?;
            result
        })
        .map_ok(|objects| stream::iter(objects.into_iter().map(Ok)))
        .try_flatten()
        .boxed()
    }

    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> object_store::Result<ListResult> {
        let prefix = prefix.cloned().unwrap_or_default();
        let store = self.clone();
        store_blocking!(move || {
            let mut common_prefixes = BTreeSet::new();
            let mut objects = Vec::new();
            for object in store.metadata(prefix.as_ref())? {
                let location = stored_location(&object.key)?;
                let Some(mut remainder) = location.prefix_match(&prefix) else {
                    continue;
                };
                let Some(child) = remainder.next() else {
                    continue;
                };
                if remainder.next().is_some() {
                    common_prefixes.insert(prefix.child(child));
                } else {
                    objects.push(object_meta(&object)?);
                }
            }
            Ok(ListResult {
                common_prefixes: common_prefixes.into_iter().collect(),
                objects,
            })
        })
        .await
        .map_err(db_error)?
    }

    async fn copy(&self, from: &Path, to: &Path) -> object_store::Result<()> {
        let store = self.clone();
        let from = from.to_string();
        let to = to.to_string();
        store_blocking!(move || store.copy_sync(&from, &to, false))
            .await
            .map_err(db_error)?
    }

    async fn copy_if_not_exists(&self, from: &Path, to: &Path) -> object_store::Result<()> {
        let store = self.clone();
        let from = from.to_string();
        let to = to.to_string();
        store_blocking!(move || store.copy_sync(&from, &to, true))
            .await
            .map_err(db_error)?
    }
}

struct ReadBody {
    store: LocalStore,
    key: String,
    etag: i64,
    size: u64,
    cursor: u64,
    end: u64,
    inline: Option<Bytes>,
    guard: Option<crate::bucket::IoGuard>,
    #[cfg(not(cellar_internal_tests))]
    owner: crate::asyncrt::DomainToken,
}

impl ReadBody {
    async fn next_chunk(mut self) -> object_store::Result<Option<(Bytes, Self)>> {
        if self.cursor == self.end {
            return Ok(None);
        }
        if let Some(bytes) = self.inline.take() {
            let bytes = bytes.slice(
                usize::try_from(self.cursor).map_err(db_error)?
                    ..usize::try_from(self.end).map_err(db_error)?,
            );
            self.cursor = self.end;
            // Already captured bytes do not authorize output after the
            // response's original execution domain closes.
            #[cfg(not(cellar_internal_tests))]
            self.owner.mono_ms().map_err(domain_error)?;
            return Ok(Some((bytes, self)));
        }
        let length = usize::try_from((self.end - self.cursor).min(READ_CHUNK_BYTES as u64))
            .map_err(db_error)?;
        let (store, key, etag, size, offset) = (
            self.store.clone(),
            self.key.clone(),
            self.etag,
            self.size,
            self.cursor,
        );
        let guard = self.guard.as_ref().map(crate::bucket::IoGuard::keep_alive);
        let operation = move || {
            let _reservation = guard;
            store.read_chunk(&key, etag, size, offset, length)
        };
        #[cfg(not(cellar_internal_tests))]
        let bytes = {
            let result = self.owner.blocking(operation).map_err(domain_error)?.await;
            // Closure wins an undelivered completion, including when another
            // domain polls this response after the original owner closes.
            self.owner.mono_ms().map_err(domain_error)?;
            result.map_err(db_error)??
        };
        #[cfg(cellar_internal_tests)]
        let bytes = store_blocking!(operation).await.map_err(db_error)??;
        self.cursor += length as u64;
        Ok(Some((bytes, self)))
    }
}

#[cfg(not(cellar_internal_tests))]
fn domain_error(error: crate::asyncrt::DomainClosed) -> Error {
    Error::Generic {
        store: STORE,
        source: Box::new(error),
    }
}

#[cfg(test)]
#[path = "local_store_read_tests.rs"]
mod read_tests;

#[cfg(all(test, unix, not(cellar_internal_tests)))]
#[path = "local_store_checkpoint_tests.rs"]
mod checkpoint_tests;

#[cfg(test)]
#[path = "local_store_list_tests.rs"]
mod list_tests;

// Shared native-I/O fixture; absent from the production API.
#[cfg(all(test, not(cellar_internal_tests)))]
pub(crate) use read_tests::{held_upload_store, UploadGate};

#[async_trait]
impl PaginatedListStore for LocalStore {
    async fn list_paginated(
        &self,
        prefix: Option<&str>,
        options: PaginatedListOptions,
    ) -> object_store::Result<PaginatedListResult> {
        let prefix = prefix.unwrap_or_default().to_owned();
        let store = self.clone();
        store_blocking!(move || {
            let after = options.page_token.as_deref().or(options.offset.as_deref());
            let delimiter = options.delimiter.as_deref();
            let limit = options.max_keys.unwrap_or(usize::MAX);
            let mut entries: Vec<(String, Option<Path>, Option<ObjectMeta>)> = Vec::new();
            for object in store.metadata(&prefix)? {
                let Some(remainder) = object.key.strip_prefix(&prefix) else {
                    continue;
                };
                if after.is_some_and(|after| object.key.as_str() <= after) {
                    continue;
                }
                let common = delimiter.and_then(|delimiter| {
                    remainder.find(delimiter).map(|index| {
                        stored_location(&format!("{prefix}{}{}", &remainder[..index], delimiter))
                    })
                });
                let common = common.transpose()?;
                if let Some(common) = common {
                    if let Some((last, Some(previous), _)) = entries.last_mut() {
                        if *previous == common {
                            *last = object.key;
                            continue;
                        }
                    }
                    entries.push((object.key, Some(common), None));
                } else {
                    entries.push((object.key.clone(), None, Some(object_meta(&object)?)));
                }
            }
            let truncated = entries.len() > limit;
            entries.truncate(limit);
            let page_token = truncated
                .then(|| entries.last().map(|(last, _, _)| last.clone()))
                .flatten();
            let mut result = ListResult {
                common_prefixes: Vec::new(),
                objects: Vec::new(),
            };
            for (_, common, object) in entries {
                if let Some(common) = common {
                    result.common_prefixes.push(common);
                }
                if let Some(object) = object {
                    result.objects.push(object);
                }
            }
            Ok(PaginatedListResult { result, page_token })
        })
        .await
        .map_err(db_error)?
    }
}

#[derive(Debug)]
struct LocalUpload {
    store: LocalStore,
    location: Path,
    attributes: Attributes,
    parts: Arc<Mutex<Vec<Bytes>>>,
    // Release accumulated payload before its capacity context on cancellation.
    extensions: object_store::Extensions,
}

#[async_trait]
impl MultipartUpload for LocalUpload {
    fn put_part(&mut self, data: PutPayload) -> BoxFuture<'static, object_store::Result<()>> {
        self.parts.lock().unwrap().push(data.into());
        async { Ok(()) }.boxed()
    }

    async fn complete(&mut self) -> object_store::Result<PutResult> {
        let parts = std::mem::take(&mut *self.parts.lock().unwrap());
        let bytes = parts.iter().map(Bytes::len).sum();
        let mut body = Vec::with_capacity(bytes);
        for part in parts {
            body.extend_from_slice(&part);
        }
        self.store
            .put_opts(
                &self.location,
                body.into(),
                PutOptions {
                    attributes: self.attributes.clone(),
                    extensions: self.extensions.clone(),
                    ..PutOptions::default()
                },
            )
            .await
    }

    async fn abort(&mut self) -> object_store::Result<()> {
        self.parts.lock().unwrap().clear();
        Ok(())
    }
}

// The smallest valid UTF-8 string above every string with this prefix. SQLite
// BINARY text ordering follows UTF-8/code-point order. Empty/all-maximal
// prefixes have no upper bound; skipping the surrogate gap keeps the bound a
// valid SQLite TEXT parameter rather than a differently ordered BLOB.
fn prefix_upper_bound(prefix: &str) -> Option<String> {
    let mut upper = prefix.to_owned();
    while let Some(last) = upper.pop() {
        if let Some(next) = (u32::from(last) + 1..=u32::from(char::MAX)).find_map(char::from_u32) {
            upper.push(next);
            return Some(upper);
        }
    }
    None
}

fn object_meta(object: &StoredObject) -> object_store::Result<ObjectMeta> {
    let modified = SystemTime::UNIX_EPOCH
        .checked_add(Duration::from_millis(object.modified_ms.max(0) as u64))
        .ok_or_else(|| message_error("the object timestamp is outside the system clock range"))?;
    Ok(ObjectMeta {
        location: stored_location(&object.key)?,
        last_modified: modified.into(),
        size: object.size,
        e_tag: Some(object.etag.to_string()),
        version: None,
    })
}

fn encode_attributes(attributes: &Attributes) -> object_store::Result<String> {
    let mut stored = Vec::with_capacity(attributes.len());
    for (attribute, value) in attributes {
        let (kind, name) = match attribute {
            Attribute::ContentDisposition => ("content-disposition", None),
            Attribute::ContentEncoding => ("content-encoding", None),
            Attribute::ContentLanguage => ("content-language", None),
            Attribute::ContentType => ("content-type", None),
            Attribute::CacheControl => ("cache-control", None),
            Attribute::StorageClass => ("storage-class", None),
            Attribute::Metadata(name) => ("metadata", Some(name.as_ref().to_string())),
            _ => {
                return Err(message_error(
                    "the development store does not support this attribute",
                ))
            }
        };
        stored.push(StoredAttribute {
            kind: kind.to_string(),
            name,
            value: value.as_ref().to_string(),
        });
    }
    serde_json::to_string(&stored).map_err(db_error)
}

fn decode_attributes(encoded: &str) -> object_store::Result<Attributes> {
    let stored: Vec<StoredAttribute> = serde_json::from_str(encoded).map_err(db_error)?;
    let mut attributes = Attributes::with_capacity(stored.len());
    for stored in stored {
        let attribute = match stored.kind.as_str() {
            "content-disposition" => Attribute::ContentDisposition,
            "content-encoding" => Attribute::ContentEncoding,
            "content-language" => Attribute::ContentLanguage,
            "content-type" => Attribute::ContentType,
            "cache-control" => Attribute::CacheControl,
            "storage-class" => Attribute::StorageClass,
            "metadata" => Attribute::Metadata(stored.name.unwrap_or_default().into()),
            kind => return Err(message_error(format!("unknown stored attribute {kind:?}"))),
        };
        attributes.insert(attribute, AttributeValue::from(stored.value));
    }
    Ok(attributes)
}

#[cfg(all(test, cellar_internal_tests))]
mod internal_tests {
    include!(env!("CELLAR_INTERNAL_LOCAL_STORE_TESTS"));
}

/// The [`Path`] a stored key came from. Every key is stored as
/// `location.to_string()`, which is already percent-encoded, so this parses
/// that form back. `Path::from` would encode it a second time, and a listed
/// `přehled.html` would come back as `p%25C5%2599ehled.html`, a key that
/// names no object (denoland/celld#232).
fn stored_location(key: &str) -> object_store::Result<Path> {
    Ok(Path::parse(key)?)
}

fn db_error(error: impl fmt::Display) -> Error {
    message_error(error.to_string())
}

fn message_error(message: impl Into<String>) -> Error {
    Error::Generic {
        store: STORE,
        source: Box::new(std::io::Error::other(message.into())),
    }
}

fn not_found(path: &str) -> Error {
    Error::NotFound {
        path: path.to_string(),
        source: Box::new(std::io::Error::new(
            std::io::ErrorKind::NotFound,
            "the object does not exist",
        )),
    }
}

fn already_exists(path: &str) -> Error {
    Error::AlreadyExists {
        path: path.to_string(),
        source: Box::new(std::io::Error::new(
            std::io::ErrorKind::AlreadyExists,
            "the object already exists",
        )),
    }
}

fn precondition(path: &str) -> Error {
    Error::Precondition {
        path: path.to_string(),
        source: Box::new(std::io::Error::other("the ETag does not match")),
    }
}

#[cfg(test)]
mod schema_tests {
    use super::*;

    #[test]
    fn failed_reads_do_not_recreate_a_missing_store() -> object_store::Result<()> {
        use cellar_ltx::{DirectFileSystem, FileSystem as _};

        let directory = tempfile::tempdir().map_err(db_error)?;
        let path = directory.path().join("store.sqlite");
        let held = directory.path().join("unavailable.sqlite");
        let store = LocalStore::open(&path)?;
        store.put_sync(
            "key".into(),
            Bytes::from_static(b"retained"),
            PutOptions::default(),
        )?;
        DirectFileSystem.rename(&path, &held).map_err(db_error)?;
        assert!(store.read("key").is_err());
        assert!(store.read_metadata("key", false).is_err());
        assert!(store.read_chunk("key", 1, 8, 0, 8).is_err());
        assert!(store.metadata("").is_err());
        assert_eq!(
            DirectFileSystem.metadata(&path).unwrap_err().kind(),
            std::io::ErrorKind::NotFound
        );
        DirectFileSystem.rename(&held, &path).map_err(db_error)?;
        assert_eq!(store.read("key")?.body, b"retained");
        Ok(())
    }

    #[test]
    fn metadata_reads_leave_unrelated_committed_wal_for_checkpointing() -> object_store::Result<()>
    {
        use cellar_ltx::{DirectFileSystem, FileSystem as _};
        use rusqlite::config::DbConfig;

        let directory = tempfile::tempdir().map_err(db_error)?;
        let path = directory.path().join("store.sqlite");
        let wal = directory.path().join("store.sqlite-wal");
        let store = LocalStore::open(&path)?;
        // Model a committed WAL left by another process. No writer or read
        // transaction remains active when the metadata request starts.
        let anchor = store.connect()?;
        anchor
            .set_db_config(DbConfig::SQLITE_DBCONFIG_NO_CKPT_ON_CLOSE, true)
            .map_err(db_error)?;
        let payload = Bytes::from(vec![0x5a; 1024 * 1024]);
        let first = store.put_sync("large".into(), payload.clone(), PutOptions::default())?;
        drop(anchor);
        let before = DirectFileSystem.metadata(&path).map_err(db_error)?.len;
        assert!(before < payload.len() as u64);
        assert!(DirectFileSystem.metadata(&wal).map_err(db_error)?.len > payload.len() as u64);

        let metadata = store.read_metadata("large", false)?;
        assert_eq!(metadata.size, payload.len() as u64);
        assert!(metadata.body.is_empty());
        assert_eq!(
            DirectFileSystem.metadata(&path).map_err(db_error)?.len,
            before,
            "a metadata read checkpointed unrelated object data"
        );
        assert!(DirectFileSystem.metadata(&wal).map_err(db_error)?.len > payload.len() as u64);
        drop(store);

        let reopened = LocalStore::open(&path)?;
        assert_eq!(reopened.read("large")?.body, payload);
        let second = reopened.put_sync(
            "large".into(),
            Bytes::from_static(b"replacement"),
            PutOptions {
                mode: PutMode::Update(object_store::UpdateVersion {
                    e_tag: first.e_tag,
                    version: None,
                }),
                ..Default::default()
            },
        )?;
        assert_eq!(second.e_tag.as_deref(), Some("2"));
        let connection = reopened.connect()?;
        assert_eq!(
            connection
                .query_row("PRAGMA synchronous", [], |row| row.get::<_, u32>(0))
                .map_err(db_error)?,
            2
        );
        assert!(
            connection
                .query_row("PRAGMA wal_autocheckpoint", [], |row| row.get::<_, u32>(0))
                .map_err(db_error)?
                > 0
        );
        connection
            .execute_batch("PRAGMA wal_checkpoint(TRUNCATE)")
            .map_err(db_error)?;
        drop(connection);
        drop(reopened);
        let final_store = LocalStore::open(&path)?;
        assert_eq!(final_store.read("large")?.body, b"replacement");
        Ok(())
    }

    #[test]
    fn system_tables_and_etags_survive_reopen() -> object_store::Result<()> {
        let directory = tempfile::tempdir().map_err(db_error)?;
        let path = directory.path().join("store.sqlite");
        let store = LocalStore::open(&path)?;
        let first = store.put_sync(
            "key".into(),
            Bytes::from_static(b"one"),
            PutOptions::default(),
        )?;
        let store = LocalStore::open(&path)?;
        assert_eq!(store.read("key")?.body, b"one");
        let second = store.put_sync(
            "key".into(),
            Bytes::from_static(b"two"),
            PutOptions {
                mode: PutMode::Update(object_store::UpdateVersion {
                    e_tag: first.e_tag,
                    version: None,
                }),
                ..Default::default()
            },
        )?;
        assert_eq!(second.e_tag.as_deref(), Some("2"));
        let connection = store.connect()?;
        let mut statement = connection.prepare("SELECT name FROM sqlite_schema WHERE type='table' AND name NOT LIKE 'sqlite_%' ORDER BY name").map_err(db_error)?;
        let names = statement
            .query_map([], |row| row.get::<_, String>(0))
            .map_err(db_error)?
            .collect::<rusqlite::Result<Vec<_>>>()
            .map_err(db_error)?;
        assert_eq!(
            names,
            [
                "__sys_local_bodies",
                "__sys_local_body_chunks",
                "__sys_local_objects",
                "__sys_local_parts",
                "__sys_local_sequence",
                "__sys_local_uploads"
            ]
        );
        Ok(())
    }
}
