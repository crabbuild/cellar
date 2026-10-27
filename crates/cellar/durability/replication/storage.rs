// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

// Copyright 2026 Deno Land Inc. Apache-2.0 license.
// Modified for Cellar.

//! Caller-owned replication storage and page-read infrastructure.

use cellar_ltx::{ObjectStoreClient, ObjectStoreConfig, TimestampMetadataKey};
use object_store::{aws::AmazonS3, ObjectStore};
use std::sync::Arc;

/// The key space and upload metadata of a supplied replication store.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct ReplicaStorageOptions {
    /// Relative fleet prefix inside the supplied store. Empty selects its root.
    /// Pass an unprefixed client when using this option.
    pub prefix: String,
    /// Multipart part size in bytes; zero selects the LTX default.
    pub part_size: u64,
    /// Azure requires `Underscore`; other stores normally use `Litestream`.
    pub timestamp_metadata_key: TimestampMetadataKey,
}

/// An application-supplied blocking page-reader factory. Applications can own
/// their page-read executors and transport instead of using the default
/// object-store helper or signed S3 fault reader. Called inside the owning
/// execution domain; `prefix` identifies one cell's LTX epoch.
#[async_trait::async_trait]
pub trait ReplicaRangeReaderFactory: Send + Sync {
    async fn create(&self, prefix: &str)
        -> anyhow::Result<Box<dyn cellar_ltx::paged::RangeReader>>;
}

/// One shared storage client used by the production replication loops.
/// Ordinary operations use that client's connection pool, retries, credentials
/// and provider refresh. Per-cell clients only change the object key prefix.
/// Signed blocking S3 reads are an explicit opt-in; a supplied page-reader
/// factory can own their transport and executor.
/// The default generic-store page bridge polls each range under the activation's
/// captured execution domain. Closing that domain refuses later faults, even
/// when the caller has entered another domain.
#[derive(Clone)]
pub struct ReplicaStorage {
    store: Arc<dyn ObjectStore>,
    s3: Option<Arc<AmazonS3>>,
    config: ObjectStoreConfig,
    range_readers: Option<Arc<dyn ReplicaRangeReaderFactory>>,
}

pub(crate) fn normalize_prefix(prefix: &str) -> anyhow::Result<String> {
    anyhow::ensure!(!prefix.starts_with('/'), "storage prefix must be relative");
    let key = prefix.strip_suffix('/').unwrap_or(prefix);
    let path = object_store::path::Path::parse(key)?;
    anyhow::ensure!(
        path.as_ref() == key,
        "storage prefix must be a canonical relative object path"
    );
    Ok(if key.is_empty() {
        String::new()
    } else {
        format!("{key}/")
    })
}

impl ReplicaStorage {
    /// Use an already configured client for any object-store backend. Paged
    /// reads use this exact client unless a custom reader factory is supplied.
    pub fn new(
        store: Arc<dyn ObjectStore>,
        options: ReplicaStorageOptions,
    ) -> anyhow::Result<Self> {
        Ok(Self {
            store,
            s3: None,
            config: ObjectStoreConfig {
                bucket: String::new(),
                path: normalize_prefix(&options.prefix)?,
                region: String::new(),
                endpoint: String::new(),
                access_key_id: String::new(),
                secret_access_key: String::new(),
                session_token: String::new(),
                force_path_style: false,
                skip_verify: false,
                part_size: options.part_size,
                timestamp_metadata_key: options.timestamp_metadata_key,
            },
            range_readers: None,
        })
    }

    /// Use the supplied S3 client and its credential provider for signed
    /// blocking page reads. `config` must match its endpoint, bucket, region
    /// and addressing; `config.path` selects the fleet prefix.
    /// Credential fields in `config` are ignored; the supplied provider is
    /// authoritative for both initial reads and refresh.
    pub fn s3(store: Arc<AmazonS3>, mut config: ObjectStoreConfig) -> anyhow::Result<Self> {
        config.path = normalize_prefix(&config.path)?;
        Ok(Self {
            store: store.clone(),
            s3: Some(store),
            config,
            range_readers: None,
        })
    }

    pub fn with_range_readers(mut self, factory: Arc<dyn ReplicaRangeReaderFactory>) -> Self {
        self.range_readers = Some(factory);
        self
    }

    pub fn object_store(&self) -> Arc<dyn ObjectStore> {
        self.store.clone()
    }

    pub fn prefix(&self) -> &str {
        &self.config.path
    }

    pub(crate) fn client_for(&self, cell: &str, epoch: u64) -> ObjectStoreClient {
        let mut config = self.config.clone();
        config.path = format!("{}cells/{cell}/ltx/e{epoch}", self.config.path);
        match &self.s3 {
            Some(store) => ObjectStoreClient::with_s3_store(config, store.clone()),
            None => ObjectStoreClient::with_store(config, self.store.clone()),
        }
    }

    pub(crate) async fn range_reader(
        &self,
        cell: &str,
        epoch: u64,
    ) -> anyhow::Result<Box<dyn cellar_ltx::paged::RangeReader>> {
        match &self.range_readers {
            Some(factory) => {
                factory
                    .create(&format!("{}cells/{cell}/ltx/e{epoch}", self.config.path))
                    .await
            }
            None if self.s3.is_some() => {
                Ok(self.client_for(cell, epoch).blocking_range_reader().await?)
            }
            None => {
                // The LTX helper owns a separate runtime. Supplied stores can
                // require Cellar services while polling metadata/body reads,
                // so retain this activation's domain across that boundary.
                let owner = crate::asyncrt::services()
                    .domain_token()
                    .ok_or(crate::asyncrt::DomainClosed)?;
                Ok(Box::new(
                    cellar_ltx::client::object_store::StoreRangeReader::new(
                        Arc::new(PageReadStore {
                            inner: self.store.clone(),
                            owner,
                        }),
                        format!("{}cells/{cell}/ltx/e{epoch}", self.config.path),
                    ),
                ))
            }
        }
    }
}

/// Fault-only facade: exactly the supplied client's range operation, polled and
/// dropped under its captured owner. Ordinary replication clients and custom
/// page-reader factories retain their own contracts. This facade permits no
/// mutation or listing, and never substitutes a process-global owner.
struct PageReadStore {
    inner: Arc<dyn ObjectStore>,
    owner: crate::asyncrt::DomainToken,
}

impl std::fmt::Debug for PageReadStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("PageReadStore")
    }
}

impl std::fmt::Display for PageReadStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("owner-bound replica page reads")
    }
}

fn unsupported_page_operation() -> object_store::Error {
    object_store::Error::NotSupported {
        source: Box::new(std::io::Error::new(
            std::io::ErrorKind::Unsupported,
            "replica page adapter permits only range reads",
        )),
    }
}

#[async_trait::async_trait]
impl ObjectStore for PageReadStore {
    async fn get_range(
        &self,
        path: &object_store::path::Path,
        range: std::ops::Range<u64>,
    ) -> object_store::Result<bytes::Bytes> {
        self.owner
            .run(self.inner.get_range(path, range))
            .await
            .map_err(|error| object_store::Error::Generic {
                store: "owner-bound replica page reads",
                source: Box::new(error),
            })?
    }

    async fn put_opts(
        &self,
        _: &object_store::path::Path,
        _: object_store::PutPayload,
        _: object_store::PutOptions,
    ) -> object_store::Result<object_store::PutResult> {
        Err(unsupported_page_operation())
    }

    async fn put_multipart_opts(
        &self,
        _: &object_store::path::Path,
        _: object_store::PutMultipartOptions,
    ) -> object_store::Result<Box<dyn object_store::MultipartUpload>> {
        Err(unsupported_page_operation())
    }

    async fn get_opts(
        &self,
        _: &object_store::path::Path,
        _: object_store::GetOptions,
    ) -> object_store::Result<object_store::GetResult> {
        Err(unsupported_page_operation())
    }

    async fn delete(&self, _: &object_store::path::Path) -> object_store::Result<()> {
        Err(unsupported_page_operation())
    }

    fn list(
        &self,
        _: Option<&object_store::path::Path>,
    ) -> futures_util::stream::BoxStream<'static, object_store::Result<object_store::ObjectMeta>>
    {
        Box::pin(futures_util::stream::once(async {
            Err(unsupported_page_operation())
        }))
    }

    async fn list_with_delimiter(
        &self,
        _: Option<&object_store::path::Path>,
    ) -> object_store::Result<object_store::ListResult> {
        Err(unsupported_page_operation())
    }

    async fn copy(
        &self,
        _: &object_store::path::Path,
        _: &object_store::path::Path,
    ) -> object_store::Result<()> {
        Err(unsupported_page_operation())
    }

    async fn copy_if_not_exists(
        &self,
        _: &object_store::path::Path,
        _: &object_store::path::Path,
    ) -> object_store::Result<()> {
        Err(unsupported_page_operation())
    }
}

#[cfg(test)]
#[path = "storage_tests.rs"]
mod tests;
