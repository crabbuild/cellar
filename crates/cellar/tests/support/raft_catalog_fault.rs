//! Conditional writes are real InMemory operations; faults lose responses only.
use futures_util::stream::BoxStream;
use object_store::{
    path::Path, GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta, ObjectStore,
    PutMode, PutMultipartOptions, PutOptions, PutPayload, PutResult,
};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

#[derive(Debug, Default)]
pub struct Store {
    pub inner: object_store::memory::InMemory,
    pub fail_before: AtomicBool,
    pub fail_after: AtomicBool,
    pub fail_read: AtomicBool,
    pub fail_confirmation: AtomicBool,
    pub fail_head_after: AtomicBool,
    pub fail_head_confirmation: AtomicBool,
    pub archive_unavailable: AtomicBool,
    pub archive_hang: AtomicBool,
    pub archive_resume: tokio::sync::Notify,
    pub archive_attempts: AtomicUsize,
    pub non_create: AtomicUsize,
}

impl std::fmt::Display for Store {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "catalog fault store")
    }
}

fn failure() -> object_store::Error {
    object_store::Error::Generic {
        store: "catalog-test",
        source: Box::new(std::io::Error::other("lost store response")),
    }
}

impl Store {
    async fn archive_fault(&self, path: &Path) -> object_store::Result<()> {
        if path.as_ref().contains("/archives/") {
            self.archive_attempts.fetch_add(1, Ordering::AcqRel);
            loop {
                let resumed = self.archive_resume.notified();
                tokio::pin!(resumed);
                resumed.as_mut().enable();
                if !self.archive_hang.load(Ordering::Acquire) {
                    break;
                }
                resumed.await;
            }
            if self.archive_unavailable.load(Ordering::Acquire) {
                return Err(failure());
            }
        }
        Ok(())
    }
}

#[async_trait::async_trait]
impl ObjectStore for Store {
    async fn put_opts(
        &self,
        path: &Path,
        payload: PutPayload,
        options: PutOptions,
    ) -> object_store::Result<PutResult> {
        tokio::task::yield_now().await;
        self.archive_fault(path).await?;
        if !matches!(options.mode, PutMode::Create) {
            self.non_create.fetch_add(1, Ordering::AcqRel);
        }
        if self.fail_before.swap(false, Ordering::AcqRel) {
            return Err(failure());
        }
        let result = self.inner.put_opts(path, payload, options).await?;
        let head = path.filename() == Some("head");
        if head && self.fail_head_confirmation.swap(false, Ordering::AcqRel) {
            self.fail_read.store(true, Ordering::Release);
        }
        if head && self.fail_head_after.swap(false, Ordering::AcqRel) {
            return Err(failure());
        }
        if self.fail_confirmation.swap(false, Ordering::AcqRel) {
            self.fail_read.store(true, Ordering::Release);
        }
        if self.fail_after.swap(false, Ordering::AcqRel) {
            return Err(failure());
        }
        Ok(result)
    }

    async fn put_multipart_opts(
        &self,
        path: &Path,
        options: PutMultipartOptions,
    ) -> object_store::Result<Box<dyn MultipartUpload>> {
        self.inner.put_multipart_opts(path, options).await
    }

    async fn get_opts(&self, path: &Path, options: GetOptions) -> object_store::Result<GetResult> {
        tokio::task::yield_now().await;
        self.archive_fault(path).await?;
        if self.fail_read.swap(false, Ordering::AcqRel) {
            return Err(failure());
        }
        self.inner.get_opts(path, options).await
    }

    async fn delete(&self, path: &Path) -> object_store::Result<()> {
        self.inner.delete(path).await
    }

    fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
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
