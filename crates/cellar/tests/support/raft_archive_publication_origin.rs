// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Preserve the actual producer's position, rather than comparing replica aliases.
use super::*;

async fn protocol_progress(f: &Fixture) -> Result<LogPosition> {
    let applied = f.group.applied()?;
    let index = applied.position.index.get() + 1;
    let position = LogPosition {
        index: LogIndex::new(index),
        term: applied.position.term,
    };
    let mut write = GroupWrite::new(&f.group);
    write.entries.push(Entry {
        index,
        term: position.term.get(),
        ..Default::default()
    });
    let mut hard = f.group.initial_state()?.hard_state;
    hard.commit = index;
    write.hard_state = Some(hard);
    write.applied = Some(DurableApply {
        position,
        image: applied.image,
    });
    f.log.persist(OperationId::new(index), vec![write]).await?;
    Ok(position)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn archive_preserves_publication_origin_across_protocol_progress() -> Result<()> {
    let limits = ArchiveLimits::default();
    let mut f = Fixture::new(limits).await?;
    f.commit(17, vec![]).await?;
    let applier = ImageApplier::recover(
        f.group.clone(),
        f.images.clone(),
        [9; 32],
        EnvelopeLimits::default(),
    )?;
    let image = applier.current().context("original published image")?;
    let applied = protocol_progress(&f).await?;
    ensure!(applied.index > image.position().index);
    let cut = CommittedArchiveCut::capture(&f.group, image.clone(), vec![], limits)?;
    let set = f.publish(cut).await?;
    ensure!(set.position() == applied);
    ensure!(set.image_position()? == Some(image.position()));
    let selected = f.archive.recovery_set_at(&set.reference()).await?;
    ensure!(selected.image_position()? == Some(image.position()));
    restored(&selected, 17)?;

    let recovered = ImageApplier::recover(
        f.group.clone(),
        f.images.clone(),
        [9; 32],
        EnvelopeLimits::default(),
    )?;
    let alias = recovered.current().context("recovered publication alias")?;
    ensure!(alias.position() == applied);
    ensure!(alias.pin().state() == image.pin().state());
    let later = protocol_progress(&f).await?;
    let cut = CommittedArchiveCut::capture(&f.group, alias.clone(), vec![], limits)?;
    let set = f.publish(cut).await?;
    ensure!(set.position() == later);
    ensure!(set.image_position()? == Some(alias.position()));
    ensure!(set.image_position()? != Some(set.position()));
    restored(&set, 17)?;
    f.close().await
}
