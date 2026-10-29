// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Keep the real retirement authority readable while faulting replica recovery.
//! Both clients access the same bucket; no response or durability proof is faked.
use futures_util::stream::BoxStream;
use object_store::{path::Path, *};
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc,
};

#[derive(Debug)]
pub(super) struct RecoveryStore {
    healthy: Arc<dyn ObjectStore>,
    faulted: Arc<dyn ObjectStore>,
    retirement_prefix: String,
    pub(super) authority_reads: AtomicUsize,
    pub(super) replica_reads: AtomicUsize,
}

impl RecoveryStore {
    pub(super) fn new(
        healthy: Arc<dyn ObjectStore>,
        faulted: Arc<dyn ObjectStore>,
        prefix: &str,
    ) -> Arc<Self> {
        Arc::new(Self {
            healthy,
            faulted,
            retirement_prefix: format!("{prefix}cell-retirement/"),
            authority_reads: AtomicUsize::new(0),
            replica_reads: AtomicUsize::new(0),
        })
    }

    fn reader(&self, path: Option<&Path>) -> &dyn ObjectStore {
        if path.is_some_and(|path| path.as_ref().starts_with(&self.retirement_prefix)) {
            self.authority_reads.fetch_add(1, Ordering::SeqCst);
            self.healthy.as_ref()
        } else {
            self.replica_reads.fetch_add(1, Ordering::SeqCst);
            self.faulted.as_ref()
        }
    }
}

impl std::fmt::Display for RecoveryStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("filesystem replica recovery fault with healthy retirement authority")
    }
}

#[async_trait::async_trait]
impl ObjectStore for RecoveryStore {
    async fn get_opts(&self, path: &Path, options: GetOptions) -> Result<GetResult> {
        self.reader(Some(path)).get_opts(path, options).await
    }

    fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, Result<ObjectMeta>> {
        self.reader(prefix).list(prefix)
    }

    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> Result<ListResult> {
        self.reader(prefix).list_with_delimiter(prefix).await
    }

    async fn put_opts(
        &self,
        path: &Path,
        payload: PutPayload,
        options: PutOptions,
    ) -> Result<PutResult> {
        self.faulted.put_opts(path, payload, options).await
    }

    async fn put_multipart_opts(
        &self,
        path: &Path,
        options: PutMultipartOptions,
    ) -> Result<Box<dyn MultipartUpload>> {
        self.faulted.put_multipart_opts(path, options).await
    }

    async fn delete(&self, path: &Path) -> Result<()> {
        self.faulted.delete(path).await
    }

    async fn copy(&self, from: &Path, to: &Path) -> Result<()> {
        self.faulted.copy(from, to).await
    }

    async fn copy_if_not_exists(&self, from: &Path, to: &Path) -> Result<()> {
        self.faulted.copy_if_not_exists(from, to).await
    }
}
