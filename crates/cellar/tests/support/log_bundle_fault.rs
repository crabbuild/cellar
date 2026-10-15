//! Exclude only session-bundle publication from follower-only proof fixtures.
//! Lease CAS, reads, retained-object injection and loss diagnostics stay real.

use futures_util::stream::BoxStream;
use object_store::{path::Path, *};
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc,
};

#[derive(Debug)]
pub(super) struct BundleFault {
    store: Arc<dyn ObjectStore>,
    prefix: String,
    refused: AtomicUsize,
}

impl BundleFault {
    pub fn new(store: Arc<dyn ObjectStore>, prefix: String) -> Arc<Self> {
        Arc::new(Self {
            store,
            prefix,
            refused: AtomicUsize::new(0),
        })
    }

    pub fn refused(&self) -> usize {
        self.refused.load(Ordering::Acquire)
    }

    fn check(&self, path: &Path) -> object_store::Result<()> {
        if path.as_ref().starts_with(&self.prefix) {
            self.refused.fetch_add(1, Ordering::AcqRel);
            return Err(object_store::Error::Generic {
                store: "follower-only-bundle",
                source: "session bundle publication excluded by the private proof fixture".into(),
            });
        }
        Ok(())
    }
}

impl std::fmt::Display for BundleFault {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("follower-only bundle exclusion")
    }
}

#[async_trait::async_trait]
impl ObjectStore for BundleFault {
    async fn put_opts(
        &self,
        path: &Path,
        payload: PutPayload,
        opts: PutOptions,
    ) -> object_store::Result<PutResult> {
        self.check(path)?;
        self.store.put_opts(path, payload, opts).await
    }

    async fn put_multipart_opts(
        &self,
        path: &Path,
        opts: PutMultipartOptions,
    ) -> object_store::Result<Box<dyn MultipartUpload>> {
        self.check(path)?;
        self.store.put_multipart_opts(path, opts).await
    }

    async fn get_opts(&self, path: &Path, opts: GetOptions) -> object_store::Result<GetResult> {
        self.store.get_opts(path, opts).await
    }

    async fn delete(&self, path: &Path) -> object_store::Result<()> {
        self.store.delete(path).await
    }

    fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
        self.store.list(prefix)
    }

    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> object_store::Result<ListResult> {
        self.store.list_with_delimiter(prefix).await
    }

    async fn copy(&self, from: &Path, to: &Path) -> object_store::Result<()> {
        self.store.copy(from, to).await
    }

    async fn copy_if_not_exists(&self, from: &Path, to: &Path) -> object_store::Result<()> {
        self.store.copy_if_not_exists(from, to).await
    }
}
