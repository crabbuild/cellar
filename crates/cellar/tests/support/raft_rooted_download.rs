// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Hold real destination verification while another archive GET progresses.
use super::*;
use anyhow::{ensure, Context as _};
use std::sync::atomic::Ordering;

struct Download {
    _root: tempfile::TempDir,
    domain: Arc<ExecutionDomain>,
    source: Host,
    remote: Arc<archive_faults::Store>,
    archive: CellArchive,
    reference: RootArchiveReference,
    target: NodeExtentStore,
    gate: Arc<fault::Gate>,
}

impl Download {
    async fn new() -> Result<Self> {
        let root = tempfile::tempdir()?;
        let remote = Arc::new(archive_faults::Store::default());
        let (namespace, birth, _) = archive::history(remote.clone()).await?;
        let domain = Arc::new(ExecutionDomain::new(tokio::runtime::Handle::current()));
        let clock = cellar_ltx::LtxHost::default();
        let age = clock.clone();
        let read = domain.clone();
        let blocking = domain.clone();
        let host = cellar_ltx::LtxHost::new(
            move || clock.now_unix_millis(),
            move |path| age.file_age(path),
            move |path| {
                let domain = read.clone();
                async move {
                    domain
                        .blocking(move || std::fs::read(path))
                        .map_err(std::io::Error::other)?
                        .await
                        .map_err(std::io::Error::other)?
                }
            },
            move |job| {
                let domain = blocking.clone();
                async move {
                    domain
                        .blocking(job)
                        .map_err(|error| cellar_ltx::HostTaskError::new(error.to_string()))?
                        .await
                        .map_err(|error| cellar_ltx::HostTaskError::new(error.to_string()))
                }
            },
        );
        let archive = namespace.cell_archive(&birth, host, ArchiveLimits::default())?;
        let (source, captured) = snapshots::source_members(
            &root.path().join("source"),
            birth.digest(),
            vec![MemberId::new(2)?],
        )
        .await?;
        let reference = archive.store_root_snapshot(Arc::new(captured)).await?;
        remote.root_extent_reads.store(0, Ordering::Release);
        remote.root_extent_ready.store(0, Ordering::Release);
        remote.hold_root_extents_after.store(1, Ordering::Release);
        let gate = fault::Gate::new(None);
        let target = NodeExtentStore::open(
            root.path().join("target"),
            Arc::new(fault::FaultFileSystem { gate: gate.clone() }),
            Default::default(),
        )?;
        Ok(Self {
            _root: root,
            domain,
            source,
            remote,
            archive,
            reference,
            target,
            gate,
        })
    }

    fn release_network(&self) {
        self.remote
            .hold_root_extents_after
            .store(0, Ordering::Release);
        self.remote.root_extents_resume.notify_waiters();
    }

    async fn second_download(&self) -> Result<()> {
        tokio::time::timeout(Duration::from_secs(5), async {
            while self.remote.root_extent_ready.load(Ordering::Acquire) < 2 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .context("second GET was not polled while the first disk install was held")
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn root_download_progresses_while_destination_verification_is_held() -> Result<()> {
    let fixture = Download::new().await?;
    let release = fixture.gate.arm(fault::HOLD_EXTENT_READ);
    let mut load = Box::pin(
        fixture
            .archive
            .load_root_snapshot_at(&fixture.reference, &fixture.target),
    );
    let overlap = async {
        fixture.gate.entered.notified().await;
        fixture.release_network();
        fixture.second_download().await
    };
    let observed = cellar::asyncrt::select! {
        result = &mut load => panic!("held install returned: {:?}", result.err()),
        result = overlap => result,
    };
    let reads = fixture.remote.root_extent_reads.load(Ordering::Acquire);
    let busy = fixture.target.usage()?.busy;
    drop(release);
    fixture.release_network();
    let restored = tokio::time::timeout(Duration::from_secs(10), load).await?;
    drop(restored?);
    fixture.domain.shutdown().await;
    fixture.source.close().await?;
    observed?;
    ensure!(
        busy && reads == 2,
        "download/install exceeded the two-object window"
    );
    ensure!(!fixture.target.usage()?.busy);
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn failed_root_download_drains_the_entered_install_before_returning() -> Result<()> {
    let fixture = Download::new().await?;
    fixture.remote.fail_root_extent.store(2, Ordering::Release);
    let release = fixture.gate.arm(fault::HOLD_EXTENT_READ);
    let mut load = Box::pin(
        fixture
            .archive
            .load_root_snapshot_at(&fixture.reference, &fixture.target),
    );
    let observed = cellar::asyncrt::select! {
        result = &mut load => panic!("held install returned: {:?}", result.err()),
        result = async {
            fixture.gate.entered.notified().await;
            fixture.release_network();
            fixture.second_download().await
        } => result,
    };
    // Poll after the GET failure: the outstanding install must still own the
    // operation, and no third object may be admitted while it is being drained.
    let waiting = cellar::asyncrt::select! {
        _ = &mut load => false,
        _ = tokio::task::yield_now() => true,
    };
    let reads = fixture.remote.root_extent_reads.load(Ordering::Acquire);
    drop(release);
    fixture.release_network();
    let result = tokio::time::timeout(Duration::from_secs(10), load).await?;
    let usage = fixture.target.usage()?;
    fixture.domain.shutdown().await;
    fixture.target.retain_only(&[])?;
    fixture.target.reclaim(16)?;
    let reclaimed = fixture.target.usage()?;
    fixture.source.close().await?;
    observed?;
    ensure!(
        waiting && reads == 2,
        "failed window returned or admitted more objects"
    );
    ensure!(matches!(result, Err(ArchiveError::Storage(_))) && !usage.busy && !usage.quarantined);
    ensure!(
        reclaimed.roots == 0 && reclaimed.objects == 0,
        "failed receive leaked its pin"
    );
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cancelled_root_download_joins_its_physical_install_and_releases_receive_pins() -> Result<()>
{
    let fixture = Download::new().await?;
    let release = fixture.gate.arm(fault::HOLD_EXTENT_READ);
    let mut load = Box::pin(
        fixture
            .archive
            .load_root_snapshot_at(&fixture.reference, &fixture.target),
    );
    cellar::asyncrt::select! {
        result = &mut load => panic!("held install returned: {:?}", result.err()),
        _ = fixture.gate.entered.notified() => (),
    };
    drop(load);
    let mut shutdown = Box::pin(fixture.domain.shutdown());
    let waiting = cellar::asyncrt::select! {
        _ = &mut shutdown => false,
        _ = tokio::task::yield_now() => true,
    };
    let held = fixture.target.usage()?;
    drop(release);
    tokio::time::timeout(Duration::from_secs(10), shutdown).await?;
    let usage = fixture.target.usage()?;
    fixture.target.retain_only(&[])?;
    fixture.target.reclaim(16)?;
    let reclaimed = fixture.target.usage()?;
    fixture.source.close().await?;
    ensure!(
        waiting && held.busy,
        "cancelled receive lost its physical owner"
    );
    ensure!(!usage.busy && !usage.quarantined);
    ensure!(
        reclaimed.roots == 0 && reclaimed.objects == 0,
        "cancelled receive leaked its pin"
    );
    Ok(())
}
