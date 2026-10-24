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
            None => Ok(self.client_for(cell, epoch).blocking_range_reader().await?),
        }
    }
}
