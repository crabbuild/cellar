// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Instrument the selected object-store adapter, without replacing its
//! conditional-create semantics, bytes or physical checkpoint verification.
use futures_util::stream::BoxStream;
use object_store::{path::Path, *};
use std::sync::{
    atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering},
    Arc, Mutex,
};
use tokio::sync::Notify;

#[derive(Debug, Default)]
pub struct Gate {
    pub entered: Notify,
    pub release: Notify,
    pub publication_index: AtomicU64,
}

#[derive(Debug)]
pub struct Store {
    pub inner: Arc<dyn ObjectStore>,
    pub puts: AtomicUsize,
    pub gets: AtomicUsize,
    pub lose_put_response: AtomicBool,
    pub lose_immutable_put_response: AtomicBool,
    pub lose_immutable_put_and_reads: AtomicBool,
    pub lose_directory_head_and_reads: AtomicBool,
    pub deletes: AtomicUsize,
    pub lose_delete_response: AtomicBool,
    held: Mutex<Option<(Path, usize, Arc<Gate>)>>,
    held_put: Mutex<Option<(Path, Arc<Gate>)>>,
    held_delete: Mutex<Option<(Path, Arc<Gate>)>>,
    publication: Mutex<Option<(u64, Arc<Gate>)>>,
    pub publication_cuts: Mutex<Vec<u64>>,
    pub deny_generation_writes: AtomicBool,
    pub deny_chunk_reads: AtomicBool,
    pub denied_chunk_reads: AtomicUsize,
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
            gets: AtomicUsize::new(0),
            lose_put_response: AtomicBool::new(false),
            lose_immutable_put_response: AtomicBool::new(false),
            lose_immutable_put_and_reads: AtomicBool::new(false),
            lose_directory_head_and_reads: AtomicBool::new(false),
            deletes: AtomicUsize::new(0),
            lose_delete_response: AtomicBool::new(false),
            held: Mutex::new(None),
            held_put: Mutex::new(None),
            held_delete: Mutex::new(None),
            publication: Mutex::new(None),
            publication_cuts: Mutex::new(Vec::new()),
            deny_generation_writes: AtomicBool::new(false),
            deny_chunk_reads: AtomicBool::new(false),
            denied_chunk_reads: AtomicUsize::new(0),
            outage: AtomicBool::new(false),
            lose_publication_and_reads: AtomicBool::new(false),
        })
    }

    /// Hold the next registry write containing a selected root, including
    /// reservation/hold changes. Use `hold_publication_after` for a newer cut.
    pub fn hold_publication(&self) -> Arc<Gate> {
        self.hold_publication_after(0)
    }

    /// Reservations retain the previous selected root. Wait for a genuinely
    /// newer selected cut, whose independent verification already finished.
    pub fn hold_publication_after(&self, index: u64) -> Arc<Gate> {
        let gate = Arc::new(Gate::default());
        *self.publication.lock().unwrap() = Some((index, gate.clone()));
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

    pub fn hold_put(&self, path: Path) -> Arc<Gate> {
        let gate = Arc::new(Gate::default());
        *self.held_put.lock().unwrap() = Some((path, gate.clone()));
        gate
    }

    pub fn clear_put(&self) {
        self.held_put.lock().unwrap().take();
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
        if path.as_ref().contains("/generations/")
            && self.deny_generation_writes.load(Ordering::Acquire)
        {
            return Err(Error::Generic {
                store: "archive generation writes denied",
                source: "injected".into(),
            });
        }
        self.puts.fetch_add(1, Ordering::AcqRel);
        let gate = self
            .held_put
            .lock()
            .unwrap()
            .as_ref()
            .filter(|(held, _)| held == path)
            .map(|(_, gate)| gate.clone());
        if let Some(gate) = gate {
            gate.entered.notify_one();
            gate.release.notified().await;
        }
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
        if let Some(index) = publication {
            let gate = {
                let mut pending = self.publication.lock().unwrap();
                if pending
                    .as_ref()
                    .is_some_and(|(previous, _)| index > *previous)
                {
                    pending.take().map(|(_, gate)| gate)
                } else {
                    None
                }
            };
            if let Some(gate) = gate {
                gate.publication_index.store(index, Ordering::Release);
                gate.entered.notify_one();
                gate.release.notified().await;
            }
        }
        let immutable =
            matches!(opts.mode, PutMode::Create) && path.as_ref().contains("/generations/");
        let result = self.inner.put_opts(path, payload, opts).await?;
        if immutable
            && self
                .lose_immutable_put_and_reads
                .swap(false, Ordering::AcqRel)
        {
            self.outage.store(true, Ordering::Release);
            return Err(Error::Generic {
                store: "lost immutable archive PUT and subsequent reads",
                source: "injected after accepted create".into(),
            });
        }
        if immutable
            && self
                .lose_immutable_put_response
                .swap(false, Ordering::AcqRel)
        {
            return Err(Error::Generic {
                store: "lost immutable archive PUT response",
                source: "injected after accepted create".into(),
            });
        }
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
        if path.as_ref().ends_with("/cell-groups/head")
            && self
                .lose_directory_head_and_reads
                .swap(false, Ordering::AcqRel)
        {
            self.outage.store(true, Ordering::Release);
            return Err(Error::Generic {
                store: "lost directory head response",
                source: "injected after real CAS".into(),
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
        self.gets.fetch_add(1, Ordering::AcqRel);
        self.available()?;
        if path.as_ref().contains("/chunks/") && self.deny_chunk_reads.load(Ordering::Acquire) {
            self.denied_chunk_reads.fetch_add(1, Ordering::AcqRel);
            return Err(Error::Generic {
                store: "archive chunk reads denied",
                source: "injected after independent verification".into(),
            });
        }
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
