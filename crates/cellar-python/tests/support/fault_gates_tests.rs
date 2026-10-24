//! Fault-control ordering only; delayed in-memory writes are not durable proof.
use super::faults::{Gate, Store};
use anyhow::Result;
use futures_util::stream::BoxStream;
use object_store::{
    path::Path, GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta, ObjectStore,
    PutMultipartOptions, PutOptions, PutPayload, PutResult,
};
use std::{
    fmt,
    sync::{atomic::Ordering, Arc},
    time::Duration,
};
use tokio::sync::Semaphore;

#[derive(Debug)]
struct Delayed {
    inner: object_store::memory::InMemory,
    entered: Semaphore,
    release: Semaphore,
}

impl fmt::Display for Delayed {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("delayed publication")
    }
}

#[async_trait::async_trait]
impl ObjectStore for Delayed {
    async fn put_opts(
        &self,
        path: &Path,
        payload: PutPayload,
        options: PutOptions,
    ) -> object_store::Result<PutResult> {
        self.entered.add_permits(1);
        self.release.acquire().await.unwrap().forget();
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

#[tokio::test]
async fn publication_cut_joins_an_already_admitted_upload_before_snapshot() -> Result<()> {
    let delayed = Arc::new(Delayed {
        inner: Default::default(),
        entered: Semaphore::new(0),
        release: Semaphore::new(0),
    });
    let gate = Arc::new(Gate::default());
    let store = Arc::new(Store {
        inner: delayed.clone(),
        gate: gate.clone(),
    });
    let uploading = {
        let store = store.clone();
        tokio::spawn(async move {
            store
                .put_opts(
                    &Path::from("cells/counter/ltx/warm"),
                    bytes::Bytes::from_static(b"warm proof").into(),
                    Default::default(),
                )
                .await
        })
    };
    tokio::time::timeout(Duration::from_secs(1), delayed.entered.acquire())
        .await??
        .forget();
    let mut cut = {
        let gate = gate.clone();
        tokio::spawn(async move { gate.hold_and_drain().await })
    };
    assert!(
        tokio::time::timeout(Duration::from_millis(30), &mut cut)
            .await
            .is_err(),
        "publication cut returned before its already-admitted write committed"
    );
    let mut next = {
        let store = store.clone();
        tokio::spawn(async move {
            store
                .put_opts(
                    &Path::from("cells/counter/ltx/next"),
                    bytes::Bytes::from_static(b"next proof").into(),
                    Default::default(),
                )
                .await
        })
    };
    tokio::time::timeout(Duration::from_secs(1), async {
        while gate.entered.load(Ordering::Acquire) < 2 {
            tokio::task::yield_now().await;
        }
    })
    .await?;
    delayed.release.add_permits(1);
    uploading.await??;
    tokio::time::timeout(Duration::from_secs(1), &mut cut).await??;
    assert_eq!(
        store
            .get_opts(&Path::from("cells/counter/ltx/warm"), Default::default())
            .await?
            .bytes()
            .await?
            .as_ref(),
        b"warm proof"
    );
    assert!(
        tokio::time::timeout(Duration::from_millis(30), &mut next)
            .await
            .is_err(),
        "new write escaped its publication cut"
    );
    assert_eq!(
        delayed.entered.available_permits(),
        0,
        "held write reached the underlying store"
    );
    gate.open();
    tokio::time::timeout(Duration::from_secs(1), delayed.entered.acquire())
        .await??
        .forget();
    delayed.release.add_permits(1);
    next.await??;
    Ok(())
}

#[tokio::test]
async fn publication_cut_keeps_node_lease_updates_independent() -> Result<()> {
    let gate = Arc::new(Gate::default());
    let store = Store {
        inner: Arc::new(object_store::memory::InMemory::new()),
        gate: gate.clone(),
    };
    gate.hold_and_drain().await;
    tokio::time::timeout(
        Duration::from_secs(1),
        store.put_opts(
            &Path::from("nodes/witness.json"),
            bytes::Bytes::from_static(b"lease").into(),
            Default::default(),
        ),
    )
    .await??;
    assert_eq!(gate.entered.load(Ordering::Acquire), 0);
    gate.open();
    Ok(())
}

#[tokio::test]
async fn publication_cut_joins_multipart_completion_abort_and_drop() -> Result<()> {
    for end in [0, 1, 2] {
        let gate = Arc::new(Gate::default());
        let store = Store {
            inner: Arc::new(object_store::memory::InMemory::new()),
            gate: gate.clone(),
        };
        let mut upload = store
            .put_multipart_opts(
                &Path::from("cells/counter/ltx/multipart"),
                Default::default(),
            )
            .await?;
        let part = upload.put_part(bytes::Bytes::from_static(b"proof part").into());
        let mut cut = {
            let gate = gate.clone();
            tokio::spawn(async move { gate.hold_and_drain().await })
        };
        assert!(
            tokio::time::timeout(Duration::from_millis(30), &mut cut)
                .await
                .is_err(),
            "cut lost its multipart reservation"
        );
        part.await?;
        match end {
            0 => {
                upload.complete().await?;
            }
            1 => upload.abort().await?,
            _ => drop(upload),
        }
        tokio::time::timeout(Duration::from_secs(1), &mut cut).await??;
        gate.open();
    }
    Ok(())
}
