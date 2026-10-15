//! Lost replica deletion replies with a retained, independent deletion ledger.

use futures_util::{stream::BoxStream, StreamExt};
use object_store::path::Path;
use object_store::*;
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc, Mutex,
};

#[derive(Debug)]
pub(super) struct CleanupFault {
    store: Arc<dyn ObjectStore>,
    prefix: String,
    pub armed: AtomicBool,
    pub deleted: Mutex<Vec<Path>>,
}

impl CleanupFault {
    pub fn new(store: Arc<dyn ObjectStore>, prefix: String) -> Arc<Self> {
        Arc::new(Self {
            store,
            prefix,
            armed: AtomicBool::new(false),
            deleted: Mutex::new(Vec::new()),
        })
    }

    fn matches(&self, path: &Path) -> bool {
        path.as_ref().starts_with(&self.prefix)
    }

    fn failure() -> Error {
        Error::Generic {
            store: "source-cleanup-fault",
            source: "injected unavailable deletion response".into(),
        }
    }
}

impl std::fmt::Display for CleanupFault {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("source cleanup reply fault")
    }
}

#[async_trait::async_trait]
impl ObjectStore for CleanupFault {
    async fn put_opts(
        &self,
        path: &Path,
        payload: PutPayload,
        opts: PutOptions,
    ) -> Result<PutResult> {
        self.store.put_opts(path, payload, opts).await
    }

    async fn put_multipart_opts(
        &self,
        path: &Path,
        opts: PutMultipartOptions,
    ) -> Result<Box<dyn MultipartUpload>> {
        self.store.put_multipart_opts(path, opts).await
    }

    async fn get_opts(&self, path: &Path, opts: GetOptions) -> Result<GetResult> {
        self.store.get_opts(path, opts).await
    }

    async fn delete(&self, path: &Path) -> Result<()> {
        self.store.delete(path).await?;
        if self.matches(path) && self.armed.load(Ordering::Acquire) {
            self.deleted.lock().unwrap().push(path.clone());
            return Err(Self::failure());
        }
        Ok(())
    }

    fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, Result<ObjectMeta>> {
        // After one actual deletion, refuse discovery until the test changes
        // owner. Empty listings must not accidentally complete the operation.
        if prefix.is_some_and(|path| self.matches(path))
            && self.armed.load(Ordering::Acquire)
            && !self.deleted.lock().unwrap().is_empty()
        {
            return futures_util::stream::once(async { Err(Self::failure()) }).boxed();
        }
        self.store.list(prefix)
    }

    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> Result<ListResult> {
        self.store.list_with_delimiter(prefix).await
    }

    async fn copy(&self, from: &Path, to: &Path) -> Result<()> {
        self.store.copy(from, to).await
    }

    async fn copy_if_not_exists(&self, from: &Path, to: &Path) -> Result<()> {
        self.store.copy_if_not_exists(from, to).await
    }
}
