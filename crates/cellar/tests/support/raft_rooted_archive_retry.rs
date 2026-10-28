// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Publication retries must verify remote bytes without resending known objects.

use super::*;
use anyhow::{ensure, Context as _};
use futures_util::TryStreamExt as _;
use object_store::ObjectStore as _;
use std::sync::atomic::Ordering;

struct Upload {
    _root: tempfile::TempDir,
    remote: Arc<archive_faults::Store>,
    archive: CellArchive,
    host: Host,
    source: Arc<RootSnapshotSource>,
    unique: usize,
}

impl Upload {
    async fn new(padding: usize) -> Result<Self> {
        let root = tempfile::tempdir()?;
        let remote = Arc::new(archive_faults::Store::default());
        let (_, birth, archive) = archive::history(remote.clone()).await?;
        let (host, source) = snapshots::source_padding(
            &root.path().join("source"),
            birth.digest(),
            vec![MemberId::new(2)?],
            padding,
            padding > 0,
        )
        .await?;
        let unique = RootSnapshotSet::for_seed(
            &birth,
            [9; 32],
            &source.snapshot(),
            source.manifest(),
            SnapshotLimits::default(),
        )?
        .extents()
        .count();
        Ok(Self {
            _root: root,
            remote,
            archive,
            host,
            source: Arc::new(source),
            unique,
        })
    }

    fn counts(&self) -> (usize, usize) {
        (
            self.remote.root_extent_reads.load(Ordering::Acquire),
            self.remote.root_extent_puts.load(Ordering::Acquire),
        )
    }

    async fn close(self) -> Result<()> {
        drop(self.source);
        self.host.close().await
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn retry_verifies_existing_extents_without_resending_them() -> Result<()> {
    let fixture = Upload::new(24 * 1024 * 1024).await?;
    let result = async {
        ensure!(fixture.unique > 1);
        // A lost successful PUT response must still be reconciled by exact bytes.
        fixture.remote.fail_after.store(true, Ordering::Release);
        let reference = fixture
            .archive
            .store_root_snapshot(fixture.source.clone())
            .await?;
        ensure!(!fixture.remote.fail_after.load(Ordering::Acquire));
        let (reads, puts) = fixture.counts();
        ensure!(puts == fixture.unique);
        ensure!(
            fixture
                .archive
                .store_root_snapshot(fixture.source.clone())
                .await?
                == reference
        );
        let (after_reads, after_puts) = fixture.counts();
        ensure!(
            after_puts == puts,
            "retry resent {} existing extents instead of verifying their bytes",
            after_puts - puts
        );
        ensure!(
            after_reads - reads == fixture.unique,
            "retry skipped fresh remote verification or downloaded an extent twice"
        );

        let objects = fixture
            .remote
            .inner
            .list(None)
            .try_collect::<Vec<_>>()
            .await?;
        ensure!(objects
            .iter()
            .all(|object| !object.location.as_ref().ends_with("/head")));
        let extent = objects
            .into_iter()
            .find(|object| object.location.as_ref().contains("/root-extents/"))
            .context("uploaded extent")?;
        // This helper has not selected a head. Missing selected history is
        // separately required to fail before publication can repair anything.
        fixture.remote.inner.delete(&extent.location).await?;
        ensure!(
            fixture
                .archive
                .store_root_snapshot(fixture.source.clone())
                .await?
                == reference
        );
        let (repaired_reads, repaired_puts) = fixture.counts();
        ensure!(
            repaired_puts - after_puts == 1,
            "retry uploaded more than its missing object"
        );
        ensure!(
            repaired_reads - after_reads == fixture.unique + 1,
            "missing object did not receive both an absence probe and upload confirmation"
        );
        Ok::<_, anyhow::Error>(())
    }
    .await;
    let closed = fixture.close().await;
    result?;
    closed
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn failed_or_corrupt_extent_probe_never_falls_back_to_upload() -> Result<()> {
    let fixture = Upload::new(0).await?;
    let result = async {
        ensure!(fixture.unique == 1);
        fixture
            .archive
            .store_root_snapshot(fixture.source.clone())
            .await?;
        let (reads, puts) = fixture.counts();
        fixture
            .remote
            .fail_root_extent
            .store(reads + 1, Ordering::Release);
        ensure!(
            matches!(
                fixture
                    .archive
                    .store_root_snapshot(fixture.source.clone())
                    .await,
                Err(ArchiveError::Storage(_))
            ),
            "failed probe became a successful archive reference"
        );
        ensure!(
            fixture.counts().1 == puts,
            "read failure caused a speculative upload"
        );
        fixture.remote.fail_root_extent.store(0, Ordering::Release);
        let extent = fixture
            .remote
            .inner
            .list(None)
            .try_collect::<Vec<_>>()
            .await?
            .into_iter()
            .find(|object| object.location.as_ref().contains("/root-extents/"))
            .context("uploaded extent")?;
        let mut corrupt = fixture
            .remote
            .inner
            .get(&extent.location)
            .await?
            .bytes()
            .await?
            .to_vec();
        corrupt[0] ^= 1;
        fixture
            .remote
            .inner
            .put(&extent.location, corrupt.clone().into())
            .await?;
        ensure!(
            matches!(
                fixture
                    .archive
                    .store_root_snapshot(fixture.source.clone())
                    .await,
                Err(ArchiveError::Corrupt(_))
            ),
            "corrupt probe became a successful archive reference"
        );
        ensure!(
            fixture.counts().1 == puts,
            "corrupt bytes caused a speculative upload"
        );
        ensure!(
            fixture
                .remote
                .inner
                .get(&extent.location)
                .await?
                .bytes()
                .await?
                .as_ref()
                == corrupt.as_slice()
        );
        Ok::<_, anyhow::Error>(())
    }
    .await;
    let closed = fixture.close().await;
    result?;
    closed
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn existing_archive_objects_do_not_make_a_closed_source_usable() -> Result<()> {
    let fixture = Upload::new(0).await?;
    let stored = fixture
        .archive
        .store_root_snapshot(fixture.source.clone())
        .await;
    let archive = fixture.archive.clone();
    let source = fixture.source.clone();
    let remote = fixture.remote.clone();
    let counts = fixture.counts();
    let closed = fixture.close().await;
    stored?;
    closed?;
    ensure!(
        matches!(
            archive.store_root_snapshot(source).await,
            Err(ArchiveError::Root(_))
        ),
        "existing remote bytes revived a closed source"
    );
    ensure!(remote.root_extent_reads.load(Ordering::Acquire) == counts.0);
    ensure!(remote.root_extent_puts.load(Ordering::Acquire) == counts.1);
    Ok(())
}
