// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Instrument the selected object-store adapter, without replacing its
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
    pub inner: Arc<dyn ObjectStore>,
    pub puts: AtomicUsize,
    pub lose_put_response: AtomicBool,
    pub deletes: AtomicUsize,
    pub lose_delete_response: AtomicBool,
    held: Mutex<Option<(Path, usize, Arc<Gate>)>>,
    held_delete: Mutex<Option<(Path, Arc<Gate>)>>,
    publication: Mutex<Option<Arc<Gate>>>,
    pub publication_cuts: Mutex<Vec<u64>>,
    pub outage: AtomicBool,
    pub lose_publication_and_reads: AtomicBool,
}

impl Store {
    pub fn new() -> Arc<Self> {
        Self::around(Arc::new(memory::InMemory::new()))
    }

    pub fn around(inner: Arc<dyn ObjectStore>) -> Arc<Self> {
        Arc::new(Self {
            inner,
            puts: AtomicUsize::new(0),
            lose_put_response: AtomicBool::new(false),
            deletes: AtomicUsize::new(0),
            lose_delete_response: AtomicBool::new(false),
            held: Mutex::new(None),
            held_delete: Mutex::new(None),
            publication: Mutex::new(None),
            publication_cuts: Mutex::new(Vec::new()),
            outage: AtomicBool::new(false),
            lose_publication_and_reads: AtomicBool::new(false),
        })
    }

    pub fn hold_publication(&self) -> Arc<Gate> {
        let gate = Arc::new(Gate::default());
        *self.publication.lock().unwrap() = Some(gate.clone());
        gate
    }

    fn available(&self) -> Result<()> {
        if self.outage.load(Ordering::Acquire) {
            return Err(Error::Generic {
                store: "archive outage",
                source: "injected".into(),
            });
        }
        Ok(())
    }

    pub fn hold(&self, path: Path) -> Arc<Gate> {
        self.hold_after(path, 0)
    }

    pub fn hold_after(&self, path: Path, puts: usize) -> Arc<Gate> {
        let gate = Arc::new(Gate::default());
        *self.held.lock().unwrap() = Some((path, puts, gate.clone()));
        gate
    }

    pub fn hold_delete(&self, path: Path) -> Arc<Gate> {
        let gate = Arc::new(Gate::default());
        *self.held_delete.lock().unwrap() = Some((path, gate.clone()));
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
        self.available()?;
        self.puts.fetch_add(1, Ordering::AcqRel);
        let publication = if path.as_ref().ends_with("/ownership/registry") {
            let bytes: Vec<u8> = payload
                .iter()
                .flat_map(|bytes| bytes.iter().copied())
                .collect();
            let value: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
            value["selected"]["marker"]["index"].as_u64()
        } else {
            None
        };
        if publication.is_some() {
            let gate = self.publication.lock().unwrap().take();
            if let Some(gate) = gate {
                gate.entered.notify_one();
                gate.release.notified().await;
            }
        }
        let result = self.inner.put_opts(path, payload, opts).await?;
        if let Some(index) = publication {
            self.publication_cuts.lock().unwrap().push(index);
        }
        if publication.is_some()
            && self
                .lose_publication_and_reads
                .swap(false, Ordering::AcqRel)
        {
            self.outage.store(true, Ordering::Release);
            return Err(Error::Generic {
                store: "lost archive publication",
                source: "injected".into(),
            });
        }
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
        self.available()?;
        let gate = self
            .held
            .lock()
            .unwrap()
            .as_ref()
            .filter(|(held, puts, _)| held == path && self.puts.load(Ordering::Acquire) >= *puts)
            .map(|(_, _, gate)| gate.clone());
        if let Some(gate) = gate {
            gate.entered.notify_one();
            gate.release.notified().await;
        }
        self.inner.get_opts(path, opts).await
    }

    async fn delete(&self, path: &Path) -> Result<()> {
        self.deletes.fetch_add(1, Ordering::AcqRel);
        let gate = self
            .held_delete
            .lock()
            .unwrap()
            .as_ref()
            .filter(|(held, _)| held == path)
            .map(|(_, gate)| gate.clone());
        if let Some(gate) = gate {
            gate.entered.notify_one();
            gate.release.notified().await;
        }
        self.inner.delete(path).await?;
        if self.lose_delete_response.swap(false, Ordering::AcqRel) {
            return Err(Error::Generic {
                store: "lost DELETE response",
                source: "injected".into(),
            });
        }
        Ok(())
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
