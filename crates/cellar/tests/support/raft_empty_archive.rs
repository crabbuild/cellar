//! Cold archives name durable configuration without manufacturing SQL state.
use super::*;

async fn empty_cut(f: &Fixture) -> Result<CommittedArchiveCut> {
    let mut write = GroupWrite::new(&f.group);
    let index = f.group.applied()?.position.index.get() + 1;
    write.entries.push(Entry {
        index,
        term: 1,
        ..Default::default()
    });
    let mut hard = f.group.initial_state()?.hard_state;
    hard.commit = index;
    write.hard_state = Some(hard);
    write.applied = Some(DurableApply {
        position: LogPosition {
            index: LogIndex::new(index),
            term: Term::new(1),
        },
        image: None,
    });
    f.log.persist(OperationId::new(index), vec![write]).await?;
    Ok(CommittedArchiveCut::capture_empty(
        &f.group,
        [9; 32],
        ArchiveLimits::default(),
    )?)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn archive_empty_configuration_roundtrip_and_lost_reply() -> Result<()> {
    let f = Fixture::new(ArchiveLimits::default()).await?;
    ensure!(
        CommittedArchiveCut::capture_empty(&f.group, [9; 32], ArchiveLimits::default())?.position()
            == f.group.applied()?.position
    );
    let cut = empty_cut(&f).await?;
    let position = cut.position();
    f.store.fail_head_after.store(true, Ordering::Release);
    let set = f.publish(cut).await?;
    ensure!(
        set.position() == position && set.tail_entries() == 0 && set.dependencies().count() == 0
    );
    let root = tempfile::tempdir()?;
    let images = GenerationStore::open(
        root.path().to_path_buf(),
        Arc::new(DirectFileSystem),
        GenerationLimits::default(),
    )?;
    ensure!(set.restore_state(&images, images::id(93))?.is_none());
    ensure!(set.restore(&images, images::id(94)).is_err());
    ensure!(images.usage()?.generations == 0);
    let reference = set.reference();
    let next = empty_cut(&f).await?;
    let next = f.publish(next).await?;
    ensure!(next.position().index > position.index);
    let retained = f.archive.recovery_set_at(&reference).await?;
    ensure!(retained.position() == position);
    ensure!(retained.restore_state(&images, images::id(95))?.is_none());
    let objects = f.store.inner.list(None).try_collect::<Vec<_>>().await?;
    ensure!(objects
        .iter()
        .all(|o| !o.location.as_ref().contains("/snapshots/")
            && !o.location.as_ref().contains("/tails/")));
    f.close().await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn archive_empty_reference_survives_first_image_without_erasure() -> Result<()> {
    let mut f = Fixture::new(ArchiveLimits::default()).await?;
    let cut = empty_cut(&f).await?;
    let empty = f.publish(cut.clone()).await?;
    let reference = empty.reference();
    let populated = f.commit(17, vec![]).await?;
    ensure!(
        CommittedArchiveCut::capture_empty(&f.group, [9; 32], ArchiveLimits::default()).is_err(),
        "applied data cannot become an empty publication"
    );
    let set = f.publish(populated).await?;
    ensure!(
        set.tail_entries() == 0,
        "first data establishes a complete image"
    );
    restored(&set, 17)?;
    let newer = f.publish(cut).await?;
    ensure!(
        newer.reference() == set.reference(),
        "an older empty cut cannot erase newer data"
    );
    let retained = f.archive.recovery_set_at(&reference).await?;
    ensure!(retained.restore_state(&f.images, images::id(96))?.is_none());
    ensure!(
        f.images.usage()?.generations > 0,
        "empty restore must not reclaim live images"
    );
    restored(
        &f.archive.recovery_set().await?.context("current archive")?,
        17,
    )?;
    f.close().await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn archive_empty_and_mixed_batches_keep_each_history_verified() -> Result<()> {
    let store = Arc::new(faults::Store::default());
    let mut first = Fixture::new_group(ArchiveLimits::default(), store.clone(), 7).await?;
    let second = Fixture::new_group(ArchiveLimits::default(), store, 8).await?;
    let cuts = vec![
        (first.archive.clone(), empty_cut(&first).await?),
        (second.archive.clone(), empty_cut(&second).await?),
    ];
    let sets = CellArchive::publish_batch(cuts, &first.verifier).await?;
    ensure!(sets.len() == 2);
    for set in sets {
        let set = set?;
        ensure!(set.image_state()?.is_none());
    }
    let cuts = vec![
        (first.archive.clone(), first.commit(21, vec![]).await?),
        (second.archive.clone(), empty_cut(&second).await?),
    ];
    let mut sets = CellArchive::publish_batch(cuts, &first.verifier)
        .await?
        .into_iter();
    let populated = sets.next().context("first batch member")??;
    restored(&populated, 21)?;
    ensure!(sets
        .next()
        .context("second batch member")??
        .image_state()?
        .is_none());
    ensure!(sets.next().is_none());
    first.close().await?;
    second.close().await
}
