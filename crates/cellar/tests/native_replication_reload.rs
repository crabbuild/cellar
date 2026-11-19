// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

use cellar::replication::ActivationOptions;
use cellar::sdk::{
    ExecutionDomain, ReplicaStorage, ReplicaStorageOptions, Replication, ReplicationOptions,
};
use futures_util::stream::BoxStream;
use object_store::ObjectStore;
use object_store::{
    path::Path as ObjectPath, GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta,
    PutMultipartOptions, PutOptions, PutPayload, PutResult,
};
use std::sync::atomic::AtomicI64;
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc,
};
use std::time::Duration;
use tokio::sync::{Notify, Semaphore};

#[derive(Debug)]
struct PausedUpload {
    store: object_store::memory::InMemory,
    pause: AtomicBool,
    started: Notify,
    released: Semaphore,
}

impl std::fmt::Display for PausedUpload {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("paused final replica upload")
    }
}

#[async_trait::async_trait]
impl ObjectStore for PausedUpload {
    async fn put_opts(
        &self,
        path: &ObjectPath,
        payload: PutPayload,
        options: PutOptions,
    ) -> object_store::Result<PutResult> {
        if path.as_ref().ends_with(".ltx") && self.pause.swap(false, Ordering::SeqCst) {
            self.started.notify_one();
            let _permit =
                self.released
                    .acquire()
                    .await
                    .map_err(|error| object_store::Error::Generic {
                        store: "paused upload",
                        source: Box::new(error),
                    })?;
        }
        self.store.put_opts(path, payload, options).await
    }

    async fn put_multipart_opts(
        &self,
        path: &ObjectPath,
        options: PutMultipartOptions,
    ) -> object_store::Result<Box<dyn MultipartUpload>> {
        self.store.put_multipart_opts(path, options).await
    }

    async fn get_opts(
        &self,
        path: &ObjectPath,
        options: GetOptions,
    ) -> object_store::Result<GetResult> {
        self.store.get_opts(path, options).await
    }

    async fn delete(&self, path: &ObjectPath) -> object_store::Result<()> {
        self.store.delete(path).await
    }

    fn list(
        &self,
        prefix: Option<&ObjectPath>,
    ) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
        self.store.list(prefix)
    }

    async fn list_with_delimiter(
        &self,
        prefix: Option<&ObjectPath>,
    ) -> object_store::Result<ListResult> {
        self.store.list_with_delimiter(prefix).await
    }

    async fn copy(&self, from: &ObjectPath, to: &ObjectPath) -> object_store::Result<()> {
        self.store.copy(from, to).await
    }

    async fn copy_if_not_exists(
        &self,
        from: &ObjectPath,
        to: &ObjectPath,
    ) -> object_store::Result<()> {
        self.store.copy_if_not_exists(from, to).await
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[expect(
    clippy::disallowed_methods,
    reason = "The external test driver seeds its clock before entering a domain."
)]
async fn reload_during_an_aged_checkpoint_keeps_cold_restore_contiguous() -> anyhow::Result<()> {
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)?
        .as_millis() as i64;
    let clock = Arc::new(AtomicI64::new(now));
    let selected = clock.clone();
    let domain = ExecutionDomain::with_wall_clock(
        tokio::runtime::Handle::current(),
        Arc::new(move || selected.load(Ordering::SeqCst)),
    );
    let result = domain.run(async {
        let directory = tempfile::tempdir()?;
        let objects = Arc::new(PausedUpload {
            store: Default::default(), pause: AtomicBool::new(true),
            started: Notify::new(), released: Semaphore::new(0),
        });
        let storage = || ReplicaStorage::new(objects.clone(), ReplicaStorageOptions::default());
        let options = ReplicationOptions {
            flush_interval: Duration::ZERO, compaction: None,
            hydrate_bytes_per_second: 0, paged_restore: false, ..Default::default()
        };
        let backend = Replication::start(&directory.path().join("local"), storage()?, options.clone())?;
        let cell = "Counter:reload";
        let activate = |fresh, resume_local| ActivationOptions {
            cell, epoch: 1, fresh, took_over: false, resume_local, prior: None,
        };
        let active = backend.ltx().activate(activate(true, false)).await?;
        let writer = rusqlite::Connection::open(&active.path)?;
        writer.execute_batch("CREATE TABLE counter(value INTEGER); INSERT INTO counter VALUES (7)")?;
        let close = backend.close_for_reload(cell, 1);
        tokio::pin!(close);
        cellar::asyncrt::select! {
            result = &mut close => { result?; anyhow::bail!("close completed before its upload"); }
            () = objects.started.notified() => {}
        }
        // Make the real age-maintenance owner eligible while the final
        // uploaded cut is fixed. It ticks every 10 ms; this is not a WAL writer.
        clock.fetch_add(120_000, Ordering::SeqCst);
        cellar::asyncrt::sleep(Duration::from_millis(250)).await;
        objects.released.add_permits(1);
        close.await?;
        drop(writer);
        let resumed = backend.ltx().activate(activate(false, true)).await?;
        let connection = rusqlite::Connection::open(&resumed.path)?;
        connection.execute("UPDATE counter SET value = 8", [])?;
        drop(connection);
        backend.close_for_reload(cell, 1).await?;
        // An independent local directory forces the public recovery path to
        // prove the remote chain, rather than trusting the retained database.
        let cold = Replication::start(&directory.path().join("cold"), storage()?, options)?;
        let restored = cold.ltx().activate(ActivationOptions {cell, epoch: 2, fresh: false, took_over: false, resume_local: false, prior: None}).await?;
        let connection = rusqlite::Connection::open(restored.path)?;
        assert_eq!(connection.query_row("SELECT value FROM counter", [], |row| row.get::<_, i64>(0))?, 8);
        Ok::<_, anyhow::Error>(())
    }).await?;
    domain.shutdown().await;
    result
}
