// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Instrument the actual in-memory object-store adapter, without replacing its
//! conditional-create semantics, bytes or physical checkpoint verification.
use futures_util::stream::BoxStream;
use object_store::{path::Path, *};
use std::sync::{
    atomic::{AtomicBool, AtomicUsize, Ordering},
    Arc, Mutex,
};
use tokio::sync::Notify;

#[derive(Debug, Default)]
pub struct Gate {
    pub entered: Notify,
    pub release: Notify,
}

#[derive(Debug)]
pub struct Store {
    pub inner: Arc<memory::InMemory>,
    pub puts: AtomicUsize,
    pub lose_put_response: AtomicBool,
    held: Mutex<Option<(Path, Arc<Gate>)>>,
}

impl Store {
    pub fn new() -> Arc<Self> {
        Arc::new(Self {
            inner: Arc::new(memory::InMemory::new()),
            puts: AtomicUsize::new(0),
            lose_put_response: AtomicBool::new(false),
            held: Mutex::new(None),
        })
    }

    pub fn hold(&self, path: Path) -> Arc<Gate> {
        let gate = Arc::new(Gate::default());
        *self.held.lock().unwrap() = Some((path, gate.clone()));
        gate
    }

    pub fn clear(&self) {
        self.held.lock().unwrap().take();
    }
}

impl std::fmt::Display for Store {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("cell-group archive instrumented store")
    }
}

#[async_trait::async_trait]
impl ObjectStore for Store {
    async fn put_opts(
        &self,
        path: &Path,
        payload: PutPayload,
        opts: PutOptions,
    ) -> Result<PutResult> {
        self.puts.fetch_add(1, Ordering::AcqRel);
        let result = self.inner.put_opts(path, payload, opts).await?;
        if self.lose_put_response.swap(false, Ordering::AcqRel) {
            return Err(Error::Generic {
                store: "lost PUT response",
                source: "injected".into(),
            });
        }
        Ok(result)
    }

    async fn put_multipart_opts(
        &self,
        path: &Path,
        opts: PutMultipartOptions,
    ) -> Result<Box<dyn MultipartUpload>> {
        self.inner.put_multipart_opts(path, opts).await
    }

    async fn get_opts(&self, path: &Path, opts: GetOptions) -> Result<GetResult> {
        let gate = self
            .held
            .lock()
            .unwrap()
            .as_ref()
            .filter(|(held, _)| held == path)
            .map(|(_, gate)| gate.clone());
        if let Some(gate) = gate {
            gate.entered.notify_one();
            gate.release.notified().await;
        }
        self.inner.get_opts(path, opts).await
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
