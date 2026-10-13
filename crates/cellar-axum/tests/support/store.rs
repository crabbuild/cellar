//! Fault injection affects replication writes, while real ownership CAS continues.
use async_trait::async_trait;
use futures_util::stream::BoxStream;
use object_store::{path::Path, *};
use std::{
    fmt,
    sync::{
        atomic::{AtomicU8, Ordering},
        Arc,
    },
};
use tokio::sync::{Notify, Semaphore};

#[derive(Debug)]
pub struct ProofControl {
    pub mode: AtomicU8,
    pub entered: Notify,
    pub release: Semaphore,
}

impl Default for ProofControl {
    fn default() -> Self {
        Self {
            mode: AtomicU8::new(0),
            entered: Notify::new(),
            release: Semaphore::new(0),
        }
    }
}

#[derive(Debug)]
pub struct ProofStore {
    pub inner: Arc<dyn ObjectStore>,
    pub control: Arc<ProofControl>,
}

impl fmt::Display for ProofStore {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "controlled replication: {}", self.inner)
    }
}

#[async_trait]
impl ObjectStore for ProofStore {
    async fn put_opts(
        &self,
        location: &Path,
        payload: PutPayload,
        options: PutOptions,
    ) -> Result<PutResult> {
        if location.as_ref().ends_with(".ltx") {
            if self.control.mode.load(Ordering::SeqCst) == 1 {
                self.control.entered.notify_one();
                self.control
                    .release
                    .acquire()
                    .await
                    .map_err(|e| Error::Generic {
                        store: "proof-test",
                        source: Box::new(e),
                    })?
                    .forget();
            }
            if self.control.mode.load(Ordering::SeqCst) == 2 {
                return Err(Error::Generic {
                    store: "proof-test",
                    source: Box::new(std::io::Error::other("injected replication failure")),
                });
            }
        }
        self.inner.put_opts(location, payload, options).await
    }

    async fn put_multipart_opts(
        &self,
        path: &Path,
        options: PutMultipartOptions,
    ) -> Result<Box<dyn MultipartUpload>> {
        self.inner.put_multipart_opts(path, options).await
    }

    async fn get_opts(&self, path: &Path, options: GetOptions) -> Result<GetResult> {
        self.inner.get_opts(path, options).await
    }

    async fn delete(&self, path: &Path) -> Result<()> {
        self.inner.delete(path).await
    }

    fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, Result<ObjectMeta>> {
        self.inner.list(prefix)
    }

    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> Result<ListResult> {
        self.inner.list_with_delimiter(prefix).await
    }

    async fn copy(&self, from: &Path, to: &Path) -> Result<()> {
        self.inner.copy(from, to).await
    }

    async fn copy_if_not_exists(&self, from: &Path, to: &Path) -> Result<()> {
        self.inner.copy_if_not_exists(from, to).await
    }
}
