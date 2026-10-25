// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Caller-owned faults around real authenticated log transport and bucket I/O.
use anyhow::Result;
use cellar::sdk::{LogStreamIo, LogTransport};
use futures_util::stream::BoxStream;
use object_store::{
    path::Path, GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta, ObjectStore,
    PutMultipartOptions, PutOptions, PutPayload, PutResult,
};
use std::{
    collections::BTreeSet,
    fmt,
    future::Future,
    pin::Pin,
    sync::{
        atomic::{AtomicU8, AtomicUsize, Ordering},
        Arc, Mutex,
    },
    time::Duration,
};
use tokio::sync::{Notify, OwnedRwLockReadGuard, RwLock};

#[derive(Default)]
pub struct Gate {
    mode: AtomicU8,
    pub entered: AtomicUsize,
    pub completed: AtomicUsize,
    pub lost_acks: AtomicUsize,
    pub members: Mutex<BTreeSet<String>>,
    changed: Notify,
    publications: Arc<RwLock<()>>,
}

enum Admission {
    Proceed,
    Fail,
    CommitThenFail,
}

impl Gate {
    pub fn hold(&self) {
        self.mode.store(1, Ordering::Release);
    }

    pub fn is_held(&self) -> bool {
        self.mode.load(Ordering::Acquire) == 1
    }

    pub async fn hold_and_drain(&self) {
        self.hold();
        // A held caller drops its read permit before awaiting release. Only
        // already-admitted underlying writes remain here, including multipart
        // uploads and their in-flight parts.
        drop(self.publications.write().await);
    }

    pub fn open(&self) {
        self.mode.store(0, Ordering::Release);
        self.changed.notify_waiters();
    }

    pub fn fail(&self) {
        self.mode.store(2, Ordering::Release);
        self.changed.notify_waiters();
    }

    /// Let object-store `put_opts` commit, then return an error instead of its
    /// receipt. Lease renewal and reads remain outside this data fault.
    pub fn lose_ack(&self) {
        self.mode.store(3, Ordering::Release);
        self.changed.notify_waiters();
    }

    async fn enter(&self) -> Result<()> {
        match self.admission().await {
            Admission::Proceed => Ok(()),
            _ => anyhow::bail!("controlled durability transport failure"),
        }
    }

    async fn admission(&self) -> Admission {
        self.entered.fetch_add(1, Ordering::Relaxed);
        loop {
            let changed = self.changed.notified();
            match self.mode.load(Ordering::Acquire) {
                0 => return Admission::Proceed,
                1 => changed.await,
                3 => return Admission::CommitThenFail,
                _ => return Admission::Fail,
            }
        }
    }

    async fn publication(&self) -> (Admission, OwnedRwLockReadGuard<()>) {
        self.entered.fetch_add(1, Ordering::Relaxed);
        loop {
            let changed = self.changed.notified();
            let permit = self.publications.clone().read_owned().await;
            match self.mode.load(Ordering::Acquire) {
                0 => return (Admission::Proceed, permit),
                3 => return (Admission::CommitThenFail, permit),
                1 => {
                    drop(permit);
                    changed.await;
                }
                _ => return (Admission::Fail, permit),
            }
        }
    }
}

pub struct Release(pub Arc<Gate>, pub Arc<Gate>);

impl Drop for Release {
    fn drop(&mut self) {
        self.0.open();
        self.1.open();
    }
}

pub struct Peer {
    pub inner: Arc<dyn LogTransport>,
    pub gate: Arc<Gate>,
}

impl LogTransport for Peer {
    fn post<'a>(
        &'a self,
        node: &'a str,
        address: &'a str,
        path: &'a str,
        body: Vec<u8>,
        deadline: Option<Duration>,
    ) -> Pin<Box<dyn Future<Output = Result<bytes::Bytes>> + Send + 'a>> {
        Box::pin(async move {
            if path == "/peer/log/append" {
                self.gate.enter().await?;
            }
            let result = self.inner.post(node, address, path, body, deadline).await?;
            if path == "/peer/log/append"
                && serde_json::from_slice::<cellar::node_log::AppendResp>(&result)?.ok
            {
                self.gate.completed.fetch_add(1, Ordering::Relaxed);
                self.gate.members.lock().unwrap().insert(node.to_owned());
            }
            Ok(result)
        })
    }

    fn open_stream<'a>(
        &'a self,
        node: &'a str,
        address: &'a str,
        path: &'a str,
    ) -> Pin<Box<dyn Future<Output = Result<LogStreamIo>> + Send + 'a>> {
        self.inner.open_stream(node, address, path)
    }
}

pub struct Store {
    pub inner: Arc<dyn ObjectStore>,
    pub gate: Arc<Gate>,
}

impl fmt::Debug for Store {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("DurabilityFaultStore")
    }
}

impl fmt::Display for Store {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("durability fault store")
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
        // Node leases stay independent of the data cut and its drain.
        let (admission, _publication) = if path.as_ref().starts_with("nodes/") {
            (Admission::Proceed, None)
        } else {
            let (admission, permit) = self.gate.publication().await;
            (admission, Some(permit))
        };
        let failure = || object_store::Error::Generic {
            store: "durability qualification",
            source: anyhow::anyhow!("controlled durability transport failure").into(),
        };
        if matches!(admission, Admission::Fail) {
            return Err(failure());
        }
        let result = self.inner.put_opts(path, payload, options).await?;
        if matches!(admission, Admission::CommitThenFail) {
            self.gate.lost_acks.fetch_add(1, Ordering::Relaxed);
            return Err(failure());
        }
        self.gate.completed.fetch_add(1, Ordering::Relaxed);
        Ok(result)
    }

    async fn put_multipart_opts(
        &self,
        path: &Path,
        options: PutMultipartOptions,
    ) -> object_store::Result<Box<dyn MultipartUpload>> {
        if path.as_ref().starts_with("nodes/") {
            return self.inner.put_multipart_opts(path, options).await;
        }
        let (admission, permit) = self.gate.publication().await;
        if !matches!(admission, Admission::Proceed) {
            return Err(object_store::Error::Generic {
                store: "durability qualification",
                source: "controlled durability transport failure".into(),
            });
        }
        let upload = self.inner.put_multipart_opts(path, options).await?;
        Ok(Box::new(PublicationUpload {
            upload,
            permit: Some(Arc::new(permit)),
        }))
    }

    async fn get_opts(&self, path: &Path, options: GetOptions) -> object_store::Result<GetResult> {
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

#[derive(Debug)]
struct PublicationUpload {
    upload: Box<dyn MultipartUpload>,
    permit: Option<Arc<OwnedRwLockReadGuard<()>>>,
}

#[async_trait::async_trait]
impl MultipartUpload for PublicationUpload {
    fn put_part(&mut self, payload: PutPayload) -> object_store::UploadPart {
        let part = self.upload.put_part(payload);
        let permit = self.permit.clone();
        Box::pin(async move {
            let _permit = permit;
            part.await
        })
    }

    async fn complete(&mut self) -> object_store::Result<PutResult> {
        let _permit = self.permit.take();
        self.upload.complete().await
    }

    async fn abort(&mut self) -> object_store::Result<()> {
        let _permit = self.permit.take();
        self.upload.abort().await
    }
}
