// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Observe real journal replacement and hold its directory barrier while a
//! controller advances a three-voter group through predecessor retirement.
use super::*;
use anyhow::ensure;
use cellar_ltx::{FileSystem, HostDirEntry, HostDirIterator, HostFile, HostFileLock, HostMetadata};
use std::{
    io,
    path::Path,
    sync::{atomic::AtomicBool, Mutex},
};

struct JournalFileSystem {
    inner: fault::FaultFileSystem,
    armed: AtomicBool,
    first: Mutex<Option<serde_json::Value>>,
}

impl FileSystem for JournalFileSystem {
    fn temporary_file(&self, path: Option<&Path>) -> io::Result<HostFile> {
        self.inner.temporary_file(path)
    }

    fn create_read_write(&self, path: &Path) -> io::Result<HostFile> {
        self.inner.create_read_write(path)
    }

    fn open_read_write(&self, path: &Path) -> io::Result<HostFile> {
        self.inner.open_read_write(path)
    }

    fn lock_file(&self, path: &Path) -> io::Result<HostFileLock> {
        self.inner.lock_file(path)
    }

    fn read(&self, path: &Path) -> io::Result<Vec<u8>> {
        self.inner.read(path)
    }

    fn read_dir(&self, path: &Path) -> io::Result<Vec<HostDirEntry>> {
        self.inner.read_dir(path)
    }

    fn read_dir_iter(&self, path: &Path) -> io::Result<HostDirIterator> {
        self.inner.read_dir_iter(path)
    }

    fn metadata(&self, path: &Path) -> io::Result<HostMetadata> {
        self.inner.metadata(path)
    }

    fn symlink_metadata(&self, path: &Path) -> io::Result<HostMetadata> {
        self.inner.symlink_metadata(path)
    }

    fn create(&self, path: &Path) -> io::Result<HostFile> {
        self.inner.create(path)
    }

    fn create_new(&self, path: &Path) -> io::Result<HostFile> {
        self.inner.create_new(path)
    }

    fn open(&self, path: &Path) -> io::Result<HostFile> {
        self.inner.open(path)
    }

    fn write(&self, path: &Path, bytes: &[u8]) -> io::Result<()> {
        self.inner.write(path, bytes)
    }

    fn sync_all(&self, path: &Path) -> io::Result<()> {
        self.inner.sync_all(path)
    }

    fn rename(&self, from: &Path, to: &Path) -> io::Result<()> {
        self.inner.rename(from, to)?;
        if self.armed.load(Ordering::Acquire)
            && to.file_name().is_some_and(|name| name == "state")
            && to
                .parent()
                .and_then(Path::parent)
                .and_then(Path::file_name)
                .is_some_and(|name| name == "cell-group-archive-journals")
        {
            let mut first = self.first.lock().unwrap();
            if first.is_none() {
                let metadata = self.inner.metadata(to)?;
                if metadata.len > 32 * 1024 {
                    return Err(io::Error::other("archive journal exceeds its byte bound"));
                }
                *first = Some(serde_json::from_slice(&self.inner.read(to)?)?);
            }
        }
        Ok(())
    }

    fn remove_file(&self, path: &Path) -> io::Result<()> {
        self.inner.remove_file(path)
    }

    fn remove_dir_all(&self, path: &Path) -> io::Result<()> {
        self.inner.remove_dir_all(path)
    }

    fn remove_dir_batch(&self, path: &Path, limit: usize) -> io::Result<bool> {
        self.inner.remove_dir_batch(path, limit)
    }

    fn create_dir_all(&self, path: &Path) -> io::Result<()> {
        self.inner.create_dir_all(path)
    }
}

pub(crate) async fn archive_retirement_joins_next_intent(
    rooted: bool,
    store: Arc<dyn ObjectStore>,
) -> Result<()> {
    let gate = fault::Gate::new(None);
    let filesystem = Arc::new(JournalFileSystem {
        inner: fault::FaultFileSystem { gate: gate.clone() },
        armed: AtomicBool::new(false),
        first: Mutex::new(None),
    });
    let domain = ExecutionDomain::with_filesystem(
        tokio::runtime::Handle::current(),
        filesystem.clone(),
        None,
    );
    let cluster = Cluster::with_store_birth_domain(
        Default::default(),
        tempfile::tempdir()?,
        Arc::new(DirectFileSystem),
        [81; 32],
        store,
        ClusterBirthOptions {
            group: GroupId::new([3; 16])?,
            image_storage: if rooted {
                RaftImageStorage::Extents
            } else {
                RaftImageStorage::Materialized
            },
            minimum: Durability::Quorum,
            log: Default::default(),
        },
        domain,
    )
    .await?;
    let (mut fixture, route, _, _) = Box::pin(registered::setup_cluster(
        rooted,
        cluster,
        Arc::new(DirectFileSystem),
    ))
    .await?;
    fixture.cluster._root.disable_cleanup(true);
    eprintln!(
        "retained archive retirement disk: {}",
        fixture.cluster._root.path().display()
    );
    let (mut config, remote) = registration(&fixture, &route, rooted).await?;
    // The third cut needs a new checkpoint, so failed chunk writes leave the
    // real reservation intent available for exact-ticket reconciliation.
    config.tails = CellGroupArchiveTailPool::new(CellGroupArchiveTailLimits {
        max_entries: 1,
        ..Default::default()
    })?;
    let controllers = controller::start_with_limits(&mut fixture, controller::limits()).await?;
    let mut retirement = None;
    let mut barrier = None;
    let work = async {
        controllers[0].archive(config.clone())?.response().await?;
        let initial = controllers[0]
            .archive_status(route.history)?
            .context("archive status")?
            .published
            .context("initial archive")?;
        let held = remote.hold_retirement_after(initial.position().index.get());
        retirement = Some(held.clone());
        let second = add(&fixture, &route, rooted, 1).await?;
        tokio::time::timeout(Duration::from_secs(10), held.entered.notified())
            .await
            .context("predecessor retirement did not enter")?;
        published(&controllers[0], route.history, second).await?;
        let base = fixture
            .cluster
            ._root
            .path()
            .join("node-1/log/cell-group-archive-journals");
        let directory = fixture
            .cluster
            .domain
            .blocking(move || -> Result<_> {
                let paths = std::fs::read_dir(base)?
                    .map(|entry| entry.map(|entry| entry.path()))
                    .collect::<std::io::Result<Vec<_>>>()?;
                ensure!(paths.len() == 1, "expected one journal");
                Ok(paths[0].clone())
            })?
            .await??;
        *gate.sync_path.lock().unwrap() = Some(directory);
        filesystem.armed.store(true, Ordering::Release);
        barrier = Some(gate.arm(fault::HOLD_SELECTED_DIRECTORY_SYNC));
        remote.deny_chunk_creates.store(true, Ordering::Release);
        let third = add(&fixture, &route, rooted, 2).await?;
        held.release.notify_one();
        retirement = None;
        tokio::time::timeout(Duration::from_secs(10), gate.entered.notified())
            .await
            .context("combined journal barrier did not enter")?;
        let observed = filesystem
            .first
            .lock()
            .unwrap()
            .clone()
            .context("journal replacement")?;
        ensure!(
            observed["record"]["retire"].is_null(),
            "released predecessor retained in next intent"
        );
        let saved = observed["record"]["pending"].get("Reserve").context(
            "retirement wrote a standalone cleanup record before the next durable intent",
        )?;
        let intent: CellGroupArchiveIntent = serde_json::from_value(saved.clone())?;
        let writes = remote.puts.load(Ordering::Acquire);
        tokio::time::sleep(Duration::from_millis(30)).await;
        ensure!(
            remote.puts.load(Ordering::Acquire) == writes,
            "remote allocation escaped the journal barrier"
        );
        ensure!(
            remote.denied_chunk_creates.load(Ordering::Acquire) == 0,
            "upload escaped the journal barrier"
        );
        drop(barrier.take());
        controller::wait_for(async || Ok(remote.denied_chunk_creates.load(Ordering::Acquire) > 0))
            .await?;
        let registry = config
            .directory
            .open_group_archive(&fixture.cluster.domain, &config.io, &config.birth)
            .await?;
        let ticket = registry.reserve(&fixture.cluster.domain, &intent).await?;
        ensure!(
            ticket == registry.reserve(&fixture.cluster.domain, &intent).await?,
            "retry changed the reservation identity"
        );
        remote.deny_chunk_creates.store(false, Ordering::Release);
        published(&controllers[0], route.history, third).await?;
        Ok::<_, anyhow::Error>(())
    }
    .await;
    if let Some(held) = retirement {
        held.release.notify_one();
    }
    drop(barrier);
    remote.deny_chunk_creates.store(false, Ordering::Release);
    let mut cleanup = Vec::new();
    for controller in controllers {
        if let Err(error) = controller.close().await {
            cleanup.push(error.to_string());
        }
    }
    let audited = if work.is_ok() && cleanup.is_empty() {
        audit(&fixture, &config, rooted, 2).await
    } else {
        Ok(())
    };
    drop(config);
    let joined = fixture.close().await;
    work?;
    ensure!(
        cleanup.is_empty(),
        "archive retirement cleanup: {cleanup:?}"
    );
    audited?;
    joined
}
