// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Root restoration through real runtime tickets and durable engine markers.

use super::*;
use anyhow::{ensure, Context as _};
use raft::Storage as _;

async fn snapshot_ticket(
    host: &mut Host,
    snapshot: raft::prelude::Snapshot,
) -> Result<ApplyTicket> {
    host.runtime
        .step(
            &scope(),
            raft::prelude::Message {
                from: 1,
                to: 2,
                term: snapshot.get_metadata().term + 1,
                msg_type: raft::prelude::MessageType::MsgSnapshot,
                snapshot: Some(snapshot).into(),
                ..Default::default()
            },
        )
        .await?;
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            match host
                .events
                .recv()
                .await
                .context("runtime events closed")?
                .kind
            {
                RuntimeEventKind::Snapshot { ticket, .. } => return Ok(ticket),
                RuntimeEventKind::Failed { error, .. } => return Err(error.into()),
                _ => (),
            }
        }
    })
    .await?
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn transferred_root_installs_with_a_runtime_ticket_and_survives_restart() -> Result<()> {
    let root = tempfile::tempdir()?;
    let learners = vec![MemberId::new(2)?];
    let (mut source, captured) =
        snapshots::source_members(&root.path().join("source"), [7; 32], learners.clone()).await?;
    let path = root.path().join("replacement");
    let mut target = Host::open_members(&path, false, 2, [9; 32], [7; 32], 2, learners).await?;
    let result = async {
        let group = target.log.group(&scope())?;
        let set = RootSnapshotSet::for_group(
            &group,
            [9; 32],
            &captured.snapshot(),
            captured.manifest(),
            SnapshotLimits::default(),
        )?;
        for id in set.extents() {
            set.install_extent(&target.store, id, &captured.read_extent(id)?)?;
        }
        let before = {
            let manifest = ExtentManifest::decode(captured.manifest(), set.reference().root())?;
            let mut bytes = Vec::new();
            for index in 0..manifest.geometry().extent_count() {
                bytes.extend(captured.read_extent(manifest.extent(index).context("extent")?)?);
            }
            bytes
        };
        let expected = set.marker().clone();
        let prepared = set.prepare(
            &group,
            [9; 32],
            &target.store,
            SnapshotLimits::default(),
            &mut target.buffer,
        )?;
        let ticket = snapshot_ticket(&mut target, captured.snapshot()).await?;
        ensure!(target.applier.current().is_none());
        let completion = prepared.completion(&ticket)?;
        ensure!(completion.applied.image.as_ref() == Some(&expected));
        target.runtime.complete_snapshot(completion.clone()).await?;
        ensure!(target.runtime.complete_snapshot(completion).await.is_err());
        tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                match target
                    .events
                    .recv()
                    .await
                    .context("runtime events closed")?
                    .kind
                {
                    RuntimeEventKind::SnapshotInstalled {
                        ticket: installed, ..
                    } => {
                        ensure!(installed == ticket);
                        return Ok::<_, anyhow::Error>(());
                    }
                    RuntimeEventKind::Failed { error, .. } => return Err(error.into()),
                    _ => (),
                }
            }
        })
        .await??;
        let restored = prepared.publish_root(&mut target.applier, &mut target.buffer)?;
        ensure!(read(&restored, &root.path().join("installed-read"))? == 7);
        // Commit a real update on the source, then deliver its ordinary log
        // entry. The restored follower uses the same apply and sync gates.
        let after = images::image(Some(&before), "UPDATE counter SET value=8")?;
        let next = source.commit(&update(&before, &after, 1)?).await?;
        let index = next.position().index.get();
        let entry = source
            .log
            .group(&scope())?
            .entries(
                index,
                index + 1,
                None,
                raft::GetEntriesContext::empty(false),
            )?
            .pop()
            .context("committed source entry")?;
        target
            .runtime
            .step(
                &scope(),
                raft::prelude::Message {
                    from: 1,
                    to: 2,
                    term: captured.snapshot().get_metadata().term + 1,
                    index: ticket.position().index.get(),
                    log_term: ticket.position().term.get(),
                    commit: index,
                    msg_type: raft::prelude::MessageType::MsgAppend,
                    entries: vec![entry].into(),
                    ..Default::default()
                },
            )
            .await?;
        let (apply, entry) = target.next_apply().await?;
        let completion = target
            .applier
            .prepare(apply.clone(), &entry, &mut target.buffer)?;
        target.runtime.complete_apply(completion).await?;
        let written = target.applied(&apply).await?;
        ensure!(read(&written, &root.path().join("new-write-read"))? == 8);
        ensure!(!path.join("materialized").exists());
        Ok::<_, anyhow::Error>(())
    }
    .await;
    let source_closed = source.close().await;
    let target_closed = target.close().await;
    result?;
    source_closed?;
    target_closed?;
    let resumed = Host::open_members(&path, true, 3, [9; 32], [7; 32], 2, vec![]).await?;
    let recovered = resumed
        .applier
        .current()
        .context("missing recovered root")?;
    ensure!(read(&recovered, &root.path().join("restarted-read"))? == 8);
    resumed.close().await
}

fn prepare(host: &mut Host, source: &RootSnapshotSource) -> Result<PreparedSnapshot> {
    Ok(RootSnapshotSet::for_group(
        &host.log.group(&scope())?,
        [9; 32],
        &source.snapshot(),
        source.manifest(),
        SnapshotLimits::default(),
    )?
    .prepare(
        &host.log.group(&scope())?,
        [9; 32],
        &host.store,
        SnapshotLimits::default(),
        &mut host.buffer,
    )?)
}

async fn installed(host: &mut Host, expected: &ApplyTicket) -> Result<()> {
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            match host
                .events
                .recv()
                .await
                .context("runtime events closed")?
                .kind
            {
                RuntimeEventKind::SnapshotInstalled { ticket, .. } => {
                    ensure!(ticket == *expected);
                    return Ok(());
                }
                RuntimeEventKind::Failed { error, .. } => return Err(error.into()),
                _ => (),
            }
        }
    })
    .await?
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn root_snapshot_waits_for_engine_sync_and_requires_the_local_inventory() -> Result<()> {
    let root = tempfile::tempdir()?;
    let learners = vec![MemberId::new(2)?];
    let (source, captured) =
        snapshots::source_members(&root.path().join("source"), [7; 32], learners.clone()).await?;
    let mut target = Host::open_members(
        &root.path().join("target"),
        false,
        2,
        [9; 32],
        [7; 32],
        2,
        learners,
    )
    .await?;
    let result = async {
        let group = target.log.group(&scope())?;
        let set = RootSnapshotSet::for_group(
            &group,
            [9; 32],
            &captured.snapshot(),
            captured.manifest(),
            SnapshotLimits::default(),
        )?;
        for id in set.extents() {
            set.install_extent(&target.store, id, &captured.read_extent(id)?)?;
        }
        let foreign_store = NodeExtentStore::open(
            root.path().join("unrelated-inventory"),
            Arc::new(DirectFileSystem),
            Default::default(),
        )?;
        let mut other = RootImageApplier::recover(
            group.clone(),
            foreign_store,
            [9; 32],
            EnvelopeLimits::default(),
            64 * 1024 * 1024,
            &mut target.buffer,
        )?;
        ensure!(prepare(&mut target, &captured)?
            .publish_root(&mut target.applier, &mut target.buffer)
            .is_err());
        ensure!(target.applier.current().is_none());
        let prepared = prepare(&mut target, &captured)?;
        let ticket = snapshot_ticket(&mut target, captured.snapshot()).await?;
        let release = target.gate.arm(fault::HOLD_SYNC);
        target
            .runtime
            .complete_snapshot(prepared.completion(&ticket)?)
            .await?;
        tokio::time::timeout(Duration::from_secs(5), target.gate.entered.notified()).await?;
        ensure!(group.applied()?.image.is_none());
        ensure!(prepare(&mut target, &captured)?
            .publish_root(&mut target.applier, &mut target.buffer)
            .is_err());
        ensure!(target.applier.current().is_none());
        while let Ok(event) = target.events.try_recv() {
            ensure!(!matches!(
                event.kind,
                RuntimeEventKind::SnapshotInstalled { .. }
            ));
        }
        drop(release);
        installed(&mut target, &ticket).await?;
        ensure!(prepare(&mut target, &captured)?
            .publish_root(&mut other, &mut target.buffer)
            .is_err());
        ensure!(
            other.current().is_none(),
            "foreign inventory became queryable"
        );
        let image = prepared.publish_root(&mut target.applier, &mut target.buffer)?;
        ensure!(read(&image, &root.path().join("read"))? == 7);
        Ok::<_, anyhow::Error>(())
    }
    .await;
    let source_closed = source.close().await;
    let target_closed = target.close().await;
    result?;
    source_closed?;
    target_closed
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn root_snapshot_completion_refuses_another_engine_with_the_same_replica_identity(
) -> Result<()> {
    let root = tempfile::tempdir()?;
    let learners = vec![MemberId::new(2)?];
    let (source, captured) =
        snapshots::source_members(&root.path().join("source"), [7; 32], learners.clone()).await?;
    let mut target = Host::open_members(
        &root.path().join("target"),
        false,
        2,
        [9; 32],
        [7; 32],
        2,
        learners.clone(),
    )
    .await?;
    let mut foreign = Host::open_members(
        &root.path().join("foreign"),
        false,
        2,
        [9; 32],
        [7; 32],
        2,
        learners,
    )
    .await?;
    let result = async {
        let set = RootSnapshotSet::for_group(
            &target.log.group(&scope())?,
            [9; 32],
            &captured.snapshot(),
            captured.manifest(),
            SnapshotLimits::default(),
        )?;
        for id in set.extents() {
            set.install_extent(&target.store, id, &captured.read_extent(id)?)?;
        }
        let prepared = prepare(&mut target, &captured)?;
        let own = snapshot_ticket(&mut target, captured.snapshot()).await?;
        let other = snapshot_ticket(&mut foreign, captured.snapshot()).await?;
        ensure!(
            own.replica() == other.replica()
                && own.position() == other.position()
                && own.entry_digest() == other.entry_digest()
        );
        ensure!(
            prepared.completion(&other).is_err(),
            "foreign engine ticket was rebound"
        );
        target
            .runtime
            .complete_snapshot(prepared.completion(&own)?)
            .await?;
        installed(&mut target, &own).await?;
        let image = prepared.publish_root(&mut target.applier, &mut target.buffer)?;
        ensure!(read(&image, &root.path().join("read"))? == 7);
        foreign.runtime.abort_snapshot(other).await?;
        Ok::<_, anyhow::Error>(())
    }
    .await;
    let source_closed = source.close().await;
    let target_closed = target.close().await;
    let foreign_closed = foreign.close().await;
    result?;
    source_closed?;
    target_closed?;
    foreign_closed
}
