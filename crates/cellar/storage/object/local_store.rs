// Copyright 2026 Deno Land Inc. Apache-2.0 license.
// Modified for Cellar.

//! The persistent object store for `cellar dev`.
//!
//! The development supervisor and its node are separate processes, and both
//! write the fleet store during a reload. SQLite supplies the cross-process
//! transaction that a directory of files cannot: a conditional update checks
//! its ETag and installs the new object in one commit. This backend is local to
//! one development machine. It is not a shared-filesystem production mode.

use async_trait::async_trait;
use bytes::Bytes;
use futures_util::future::BoxFuture;
use futures_util::stream::{self, BoxStream};
use futures_util::FutureExt as _;
use futures_util::StreamExt as _;
use object_store::list::{PaginatedListOptions, PaginatedListResult, PaginatedListStore};
use object_store::path::Path;
use object_store::{
    Attribute, AttributeValue, Attributes, Error, GetOptions, GetResult, GetResultPayload,
    ListResult, MultipartUpload, ObjectMeta, ObjectStore, PutMode, PutMultipartOptions, PutOptions,
    PutPayload, PutResult,
};
use rusqlite::{params, Connection, OptionalExtension as _, TransactionBehavior};
use serde::{Deserialize, Serialize};
use std::collections::BTreeSet;
use std::fmt;
use std::path::{Path as FsPath, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime};

const STORE: &str = "cellar development store";
const READ_CHUNK_BYTES: usize = 128 << 10;

#[derive(Clone, Debug)]
pub(crate) struct LocalStore {
    database: PathBuf,
    #[cfg(test)]
    read_gate: Option<Arc<read_tests::ReadGate>>,
    #[cfg(test)]
    metadata_gate: Option<Arc<read_tests::ReadGate>>,
    #[cfg(test)]
    put_gate: Option<(String, Arc<read_tests::ReadGate>)>,
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
        let database = database.as_ref().to_path_buf();
        let store = Self {
            database,
            #[cfg(test)]
            read_gate: None,
            #[cfg(test)]
            metadata_gate: None,
            #[cfg(test)]
            put_gate: None,
        };
        let connection = store.connect()?;
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
                   body BLOB NOT NULL
                 );
                 CREATE TABLE IF NOT EXISTS __sys_local_sequence (
                   singleton INTEGER PRIMARY KEY CHECK (singleton = 1),
                   next_etag INTEGER NOT NULL
                 );
                 INSERT OR IGNORE INTO __sys_local_sequence(singleton, next_etag) VALUES (1, 1);",
            )
            .map_err(db_error)?;
        Ok(store)
    }

    fn connect(&self) -> object_store::Result<Connection> {
        let connection = Connection::open(&self.database).map_err(db_error)?;
        configure_connection(&connection)?;
        Ok(connection)
    }

    fn read(&self, key: &str) -> object_store::Result<StoredObject> {
        self.connect()?
            .query_row(
                "SELECT key, body, etag, modified_ms, attributes, length(body)
                 FROM __sys_local_objects WHERE key = ?1",
                [key],
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
            .optional()
            .map_err(db_error)?
            .ok_or_else(|| not_found(key))
    }

    fn read_metadata(
        &self,
        key: &str,
        include_small_body: bool,
    ) -> object_store::Result<StoredObject> {
        self.connect()?
            .query_row(
                "SELECT key, etag, modified_ms, attributes, length(body),
                        CASE WHEN ?2 AND length(body) <= ?3 THEN body ELSE NULL END
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
        let mut connection = self.connect()?;
        let transaction = connection.transaction().map_err(db_error)?;
        let rowid = transaction
            .query_row(
                "SELECT rowid FROM __sys_local_objects
                 WHERE key = ?1 AND etag = ?2 AND length(body) = ?3",
                params![key, etag, size],
                |row| row.get::<_, i64>(0),
            )
            .optional()
            .map_err(db_error)?
            .ok_or_else(|| precondition(key))?;
        let mut bytes = vec![0; length];
        {
            // SQL BLOB extraction can materialize the whole value inside
            // SQLite even when the Rust caller only retains a small slice.
            // Incremental I/O pins this row/version in the read transaction.
            let blob = transaction
                .blob_open(
                    rusqlite::DatabaseName::Main,
                    "__sys_local_objects",
                    "body",
                    rowid,
                    true,
                )
                .map_err(db_error)?;
            #[cfg(test)]
            if let Some(gate) = &self.read_gate {
                gate.wait();
            }
            blob.read_at_exact(&mut bytes, usize::try_from(offset).map_err(db_error)?)
                .map_err(db_error)?;
        }
        transaction.commit().map_err(db_error)?;
        Ok(bytes.into())
    }

    fn metadata(&self) -> object_store::Result<Vec<StoredObject>> {
        let connection = self.connect()?;
        let mut statement = connection
            .prepare(
                "SELECT key, CAST(X'' AS BLOB), etag, modified_ms, attributes, length(body)
                 FROM __sys_local_objects ORDER BY key",
            )
            .map_err(db_error)?;
        let rows = statement
            .query_map([], |row| {
                Ok(StoredObject {
                    key: row.get(0)?,
                    body: row.get(1)?,
                    size: row.get(5)?,
                    etag: row.get(2)?,
                    modified_ms: row.get(3)?,
                    attributes: row.get(4)?,
                })
            })
            .map_err(db_error)?;
        rows.collect::<Result<Vec<_>, _>>().map_err(db_error)
    }

    fn put_sync(
        &self,
        key: String,
        body: Bytes,
        options: PutOptions,
    ) -> object_store::Result<PutResult> {
        let attributes = encode_attributes(&options.attributes)?;
        let modified_ms = crate::asyncrt::wall_ms();
        let mut connection = self.connect()?;
        // The fixture observes a real entered SQLite connection without
        // locking out unrelated metadata/claim I/O while the worker pauses.
        #[cfg(test)]
        if let Some((prefix, gate)) = &self.put_gate {
            if key.starts_with(prefix) {
                gate.wait();
            }
        }
        let transaction = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(db_error)?;
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
                if current.as_deref().is_none() || version.e_tag.as_deref() != current.as_deref() {
                    return Err(precondition(&key));
                }
            }
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
                "INSERT INTO __sys_local_objects(key, body, etag, modified_ms, attributes)
                 VALUES (?1, ?2, ?3, ?4, ?5)
                 ON CONFLICT(key) DO UPDATE SET
                   body = excluded.body,
                   etag = excluded.etag,
                   modified_ms = excluded.modified_ms,
                   attributes = excluded.attributes
                 RETURNING rowid",
                params![
                    key,
                    rusqlite::blob::ZeroBlob(i32::try_from(body.len()).map_err(db_error)?),
                    etag,
                    modified_ms,
                    attributes
                ],
                |row| row.get::<_, i64>(0),
            )
            .map_err(db_error)?;
        {
            let mut blob = transaction
                .blob_open(
                    rusqlite::DatabaseName::Main,
                    "__sys_local_objects",
                    "body",
                    rowid,
                    false,
                )
                .map_err(db_error)?;
            blob.write_all_at(&body, 0).map_err(db_error)?;
        }
        transaction.commit().map_err(db_error)?;
        Ok(PutResult {
            e_tag: Some(etag.to_string()),
            version: None,
        })
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
        store_blocking!(move || {
            // The caller may cancel after native entry. Return capacity only
            // after SQLite has released its input, connection and transaction.
            let result = store.put_sync(key, body, options);
            drop(reservation);
            result
        })
        .await
        .map_err(db_error)?
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
            store
                .connect()?
                .execute("DELETE FROM __sys_local_objects WHERE key = ?1", [key])
                .map_err(db_error)?;
            Ok(())
        })
        .await
        .map_err(db_error)?
    }

    fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
        let prefix = prefix.cloned();
        let result = self.metadata().and_then(|objects| {
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
        });
        match result {
            Ok(objects) => stream::iter(objects.into_iter().map(Ok)).boxed(),
            Err(error) => stream::once(async move { Err(error) }).boxed(),
        }
    }

    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> object_store::Result<ListResult> {
        let prefix = prefix.cloned().unwrap_or_default();
        let mut common_prefixes = BTreeSet::new();
        let mut objects = Vec::new();
        for object in self.metadata()? {
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
        let prefix = prefix.unwrap_or_default();
        let after = options.page_token.as_deref().or(options.offset.as_deref());
        let delimiter = options.delimiter.as_deref();
        let limit = options.max_keys.unwrap_or(usize::MAX);
        let mut entries: Vec<(String, Option<Path>, Option<ObjectMeta>)> = Vec::new();
        for object in self.metadata()? {
            let Some(remainder) = object.key.strip_prefix(prefix) else {
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
        assert_eq!(names, ["__sys_local_objects", "__sys_local_sequence"]);
        Ok(())
    }
}
