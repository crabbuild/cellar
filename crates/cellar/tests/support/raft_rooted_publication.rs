// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Count actual immutable-object operations through native committed roots.
use super::*;
use anyhow::{ensure, Context as _};
use futures_util::TryStreamExt as _;
use object_store::ObjectStore as _;
use std::{collections::BTreeSet, sync::atomic::Ordering};

struct Publication {
    root: tempfile::TempDir,
    remote: Arc<archive_faults::Store>,
    archive: CellArchive,
    source: Host,
    verifier: ArchiveVerifier,
    before: Vec<u8>,
    history: [u8; 32],
}

impl Publication {
    async fn new() -> Result<Self> {
        let root = tempfile::tempdir()?;
        let remote = Arc::new(archive_faults::Store::default());
        let (_, birth, archive) = archive::history(remote.clone()).await?;
        let (source, captured) = snapshots::source_members(
            &root.path().join("source"),
            birth.digest(),
            vec![MemberId::new(2)?],
        )
        .await?;
        let set = RootSnapshotSet::for_seed(
            &birth,
            [9; 32],
            &captured.snapshot(),
            captured.manifest(),
            SnapshotLimits::default(),
        )?;
        let manifest = ExtentManifest::decode(captured.manifest(), set.reference().root())?;
        let mut before = Vec::new();
        for index in 0..manifest.geometry().extent_count() {
            before.extend(captured.read_extent(manifest.extent(index).context("extent")?)?);
        }
        let (verifier, _) = archive_heads::verifier(&root.path().join("unused-flat-verifier"))?;
        Ok(Self {
            root,
            remote,
            archive,
            source,
            verifier,
            before,
            history: birth.digest(),
        })
    }

    fn objects(&self) -> Result<BTreeSet<ExtentId>> {
        let image = self
            .source
            .applier
            .current()
            .context("source root")?
            .image();
        let manifest = image.manifest();
        (0..manifest.geometry().extent_count())
            .map(|index| manifest.extent(index).context("root extent"))
            .collect()
    }

    async fn advance(&mut self) -> Result<()> {
        let after = images::image(Some(&self.before), "UPDATE counter SET value=8")?;
        self.source
            .commit(&archive_heads::change(
                &self.before,
                &after,
                1,
                self.history,
            )?)
            .await?;
        self.before = after;
        Ok(())
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn confirmed_root_publication_does_not_download_its_own_extents_twice() -> Result<()> {
    let fixture = Publication::new().await?;
    let result = async {
        let unique = fixture.objects()?.len();
        ensure!(unique >= 3);
        let published = fixture
            .archive
            .publish(
                archive_heads::cut(
                    &fixture.source,
                    vec![ArchiveDependency(b"dependency".to_vec())],
                )?,
                &fixture.verifier,
            )
            .await?;
        let reads = fixture.remote.root_extent_reads.load(Ordering::Acquire);
        let hits = fixture.remote.root_extent_hits.load(Ordering::Acquire);
        let puts = fixture.remote.root_extent_puts.load(Ordering::Acquire);
        let target = NodeExtentStore::open(
            fixture.root.path().join("receiver"),
            Arc::new(DirectFileSystem),
            Default::default(),
        )?;
        let receiving = fixture.archive.clone().with_root_store(target.clone());
        let restored = receiving.recovery_set_at(&published.reference()).await?;
        ensure!(restored.reference() == published.reference());
        ensure!(restored
            .dependencies()
            .any(|(_, bytes)| bytes == b"dependency"));
        let independent_reads = fixture.remote.root_extent_reads.load(Ordering::Acquire) - reads;
        ensure!(target.usage()?.objects == unique);
        ensure!(
            puts == unique && reads == 2 * unique && hits == unique,
            "producer used {puts} PUTs, {reads} GET probes/confirmations and {hits} object downloads for {unique} extents"
        );
        ensure!(
            independent_reads == unique,
            "independent recovery skipped remote bytes"
        );
        Ok::<_, anyhow::Error>(())
    }
    .await;
    let closed = fixture.source.close().await;
    result?;
    closed
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn advancing_root_reuses_only_extents_verified_in_the_current_publication_attempt(
) -> Result<()> {
    let mut fixture = Publication::new().await?;
    let result = async {
        let old = fixture.objects()?;
        drop(fixture.archive.publish(archive_heads::cut(&fixture.source, vec![])?, &fixture.verifier).await?);
        fixture.advance().await?;
        let current = fixture.objects()?;
        let changed = current.difference(&old).count();
        ensure!(changed > 0 && changed < current.len());
        let before_reads = fixture.remote.root_extent_reads.load(Ordering::Acquire);
        let before_hits = fixture.remote.root_extent_hits.load(Ordering::Acquire);
        let before_puts = fixture.remote.root_extent_puts.load(Ordering::Acquire);
        let published = fixture.archive.publish(archive_heads::cut(&fixture.source, vec![])?, &fixture.verifier).await?;
        let reads = fixture.remote.root_extent_reads.load(Ordering::Acquire) - before_reads;
        let puts = fixture.remote.root_extent_puts.load(Ordering::Acquire) - before_puts;
        let hits = fixture.remote.root_extent_hits.load(Ordering::Acquire) - before_hits;
        ensure!(reads == old.len() + 2 * changed && hits == old.len() + changed && puts == changed,
            "advance used {reads} GET probes/confirmations, {hits} object downloads and {puts} PUTs; expected {} old checks and {changed} changed extents", old.len());
        let receiving = fixture.archive.clone().with_root_store(NodeExtentStore::open(
            fixture.root.path().join("receiver"), Arc::new(DirectFileSystem), Default::default(),
        )?);
        let restored = receiving.recovery_set_at(&published.reference()).await?;
        ensure!(restored.position() == published.position());
        ensure!(fixture.remote.root_extent_reads.load(Ordering::Acquire) - before_reads - reads == current.len());
        Ok::<_, anyhow::Error>(())
    }.await;
    let closed = fixture.source.close().await;
    result?;
    closed
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn newer_root_refuses_missing_or_corrupt_selected_extents_before_uploading() -> Result<()> {
    let mut fixture = Publication::new().await?;
    let result = async {
        let old = fixture.objects()?;
        let published = fixture
            .archive
            .publish(
                archive_heads::cut(&fixture.source, vec![])?,
                &fixture.verifier,
            )
            .await?;
        fixture.advance().await?;
        let current = fixture.objects()?;
        let shared = old
            .intersection(&current)
            .next()
            .context("shared immutable extent")?;
        let objects = fixture.remote.list(None).try_collect::<Vec<_>>().await?;
        let extent = objects
            .iter()
            .find(|object| {
                object.location.as_ref().contains("/root-extents/")
                    && object
                        .location
                        .as_ref()
                        .ends_with(&hex::encode(shared.bytes()))
            })
            .context("shared remote extent")?;
        let head = objects
            .iter()
            .find(|object| object.location.as_ref().ends_with("/head"))
            .context("head")?;
        let head_bytes = fixture
            .remote
            .inner
            .get(&head.location)
            .await?
            .bytes()
            .await?;
        let original = fixture
            .remote
            .inner
            .get(&extent.location)
            .await?
            .bytes()
            .await?;
        let puts = fixture.remote.root_extent_puts.load(Ordering::Acquire);
        let cut = archive_heads::cut(&fixture.source, vec![])?;
        for corrupt in [false, true] {
            if corrupt {
                let mut bytes = original.to_vec();
                bytes[0] ^= 1;
                fixture
                    .remote
                    .inner
                    .put(&extent.location, bytes.into())
                    .await?;
            } else {
                fixture.remote.inner.delete(&extent.location).await?;
            }
            ensure!(fixture
                .archive
                .publish(cut.clone(), &fixture.verifier)
                .await
                .is_err());
            ensure!(
                fixture.remote.root_extent_puts.load(Ordering::Acquire) == puts,
                "new uploads preceded selected-history verification"
            );
            ensure!(
                fixture
                    .remote
                    .inner
                    .get(&head.location)
                    .await?
                    .bytes()
                    .await?
                    == head_bytes
            );
            ensure!(
                fixture
                    .archive
                    .recovery_metadata()
                    .await?
                    .context("selected head")?
                    .reference()
                    == published.reference()
            );
            if corrupt {
                ensure!(
                    fixture
                        .remote
                        .inner
                        .get(&extent.location)
                        .await?
                        .bytes()
                        .await?
                        != original,
                    "publication repaired immutable corruption"
                );
            } else {
                ensure!(fixture.remote.inner.get(&extent.location).await.is_err());
            }
        }
        Ok::<_, anyhow::Error>(())
    }
    .await;
    let closed = fixture.source.close().await;
    result?;
    closed
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn successful_extent_put_requires_confirmation_before_a_root_head_is_published() -> Result<()>
{
    let fixture = Publication::new().await?;
    let result = async {
        fixture
            .remote
            .fail_root_confirmation
            .store(true, Ordering::Release);
        let cut = archive_heads::cut(&fixture.source, vec![])?;
        ensure!(fixture
            .archive
            .publish(cut.clone(), &fixture.verifier)
            .await
            .is_err());
        ensure!(fixture.remote.root_extent_puts.load(Ordering::Acquire) > 0);
        ensure!(!fixture
            .remote
            .fail_root_confirmation
            .load(Ordering::Acquire));
        ensure!(
            fixture.archive.recovery_metadata().await?.is_none(),
            "unconfirmed bytes published a head"
        );
        drop(fixture.archive.publish(cut, &fixture.verifier).await?);
        Ok::<_, anyhow::Error>(())
    }
    .await;
    let closed = fixture.source.close().await;
    result?;
    closed
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn confirmed_remote_bytes_do_not_hide_missing_or_corrupt_local_root_metadata() -> Result<()> {
    let fixture = Publication::new().await?;
    let result = async {
        let image = fixture
            .source
            .applier
            .current()
            .context("source image")?
            .image();
        let path = fixture.root.path().join("source/extents").join(format!(
            "r-{}",
            hex::encode(image.reference().root().bytes())
        ));
        let original = std::fs::read(&path)?;
        let cut = archive_heads::cut(&fixture.source, vec![])?;
        for corrupt in [false, true] {
            if corrupt {
                let mut bytes = original.clone();
                bytes[0] ^= 1;
                std::fs::write(&path, &bytes)?;
            } else {
                std::fs::remove_file(&path)?;
            }
            ensure!(
                fixture
                    .archive
                    .publish(cut.clone(), &fixture.verifier)
                    .await
                    .is_err(),
                "producer bypassed the local root seal"
            );
            ensure!(fixture.archive.recovery_metadata().await?.is_none());
            if corrupt {
                ensure!(
                    std::fs::read(&path)? != original,
                    "producer repaired immutable metadata"
                );
            } else {
                ensure!(!path.exists(), "producer recreated missing local metadata");
            }
        }
        Ok::<_, anyhow::Error>(())
    }
    .await;
    let closed = fixture.source.close().await;
    result?;
    closed
}
