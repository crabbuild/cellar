//! A per-host catalog read gate; other hosts and the controller keep progressing.
use object_store::{
    path::Path, GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta, ObjectStore,
    PutMultipartOptions, PutOptions, PutPayload, PutResult,
};
use std::sync::{
    atomic::{AtomicBool, AtomicUsize, Ordering},
    Arc,
};

#[derive(Debug)]
pub(super) struct RootGate {
    inner: Arc<dyn ObjectStore>,
    held: AtomicBool,
    pub(super) active: AtomicUsize,
    resume: tokio::sync::Notify,
}

impl std::fmt::Display for RootGate {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "per-host root gate")
    }
}

pub(super) struct Held<'a>(&'a RootGate);

impl Drop for Held<'_> {
    fn drop(&mut self) {
        self.0.held.store(false, Ordering::Release);
        self.0.resume.notify_waiters();
    }
}

struct Reading<'a>(&'a AtomicUsize);

impl Drop for Reading<'_> {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::AcqRel);
    }
}

impl RootGate {
    pub(super) fn new(inner: Arc<dyn ObjectStore>) -> Self {
        Self {
            inner,
            held: AtomicBool::new(false),
            active: AtomicUsize::new(0),
            resume: tokio::sync::Notify::new(),
        }
    }

    pub(super) fn hold(&self) -> Held<'_> {
        assert!(
            !self.held.swap(true, Ordering::AcqRel),
            "root gate already held"
        );
        Held(self)
    }
}

#[async_trait::async_trait]
impl ObjectStore for RootGate {
    async fn put_opts(
        &self,
        path: &Path,
        payload: PutPayload,
        options: PutOptions,
    ) -> object_store::Result<PutResult> {
        self.inner.put_opts(path, payload, options).await
    }

    async fn put_multipart_opts(
        &self,
        path: &Path,
        options: PutMultipartOptions,
    ) -> object_store::Result<Box<dyn MultipartUpload>> {
        self.inner.put_multipart_opts(path, options).await
    }

    async fn get_opts(&self, path: &Path, options: GetOptions) -> object_store::Result<GetResult> {
        if path.as_ref().ends_with("/fleet/head") && self.held.load(Ordering::Acquire) {
            self.active.fetch_add(1, Ordering::AcqRel);
            let _reading = Reading(&self.active);
            loop {
                let resumed = self.resume.notified();
                tokio::pin!(resumed);
                resumed.as_mut().enable();
                if !self.held.load(Ordering::Acquire) {
                    break;
                }
                resumed.await;
            }
        }
        self.inner.get_opts(path, options).await
    }

    async fn delete(&self, path: &Path) -> object_store::Result<()> {
        self.inner.delete(path).await
    }

    fn list(
        &self,
        prefix: Option<&Path>,
    ) -> futures_util::stream::BoxStream<'static, object_store::Result<ObjectMeta>> {
        self.inner.list(prefix)
    }

    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> object_store::Result<ListResult> {
        self.inner.list_with_delimiter(prefix).await
    }

    async fn copy(&self, from: &Path, to: &Path) -> object_store::Result<()> {
        self.inner.copy(from, to).await
    }

    async fn copy_if_not_exists(&self, from: &Path, to: &Path) -> object_store::Result<()> {
        self.inner.copy_if_not_exists(from, to).await
    }
}
