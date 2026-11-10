// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Actual RawNode restore/Ready acceptance on a partitioned existing voter.
//! Transfer uses the native streaming API, not yet the snapshot RPC transport.
use super::*;

async fn prepared(
    rooted: bool,
) -> Result<(
    Cluster,
    CellGroupApplier,
    PreparedCellGroupCheckpoint,
    CellGroupCheckpointPool,
    Message,
)> {
    prepared_at(rooted, None).await
}

async fn prepared_at(
    rooted: bool,
    index: Option<u64>,
) -> Result<(
    Cluster,
    CellGroupApplier,
    PreparedCellGroupCheckpoint,
    CellGroupCheckpointPool,
    Message,
)> {
    let (mut cluster, _) = checkpoint_fixture::populated_partition(rooted).await?;
    let route = cluster.birth.route().clone();
    let group = cluster.voters[2].log.cell_group(&route.history)?;
    let before = group.group_manifest()?.context("old marker")?;
    let source_pool = CellGroupCheckpointPool::new(checkpoint_fixture::limits())?;
    let source_owner = cluster.voters[0].owner.as_ref().unwrap();
    let mut source = source_pool.capture(
        &cluster.voters[0].log.cell_group(&route.history)?,
        source_owner.manifest(),
    )?;
    let term = source_owner.status()?.term.get();
    assert!(source.marker().position().index > before.position().index);
    let config = checkpoint_receive::config(
        cluster.voters[2].images.clone(),
        cluster.voters[2].extents.clone(),
        cluster._root.path(),
    )?;
    let applier = CellGroupApplier::new(
        group,
        cluster.voters[2].pages.take().context("pages")?,
        config,
        ManifestLimits::default(),
        GroupEntryLimits::default(),
        100,
    )?;
    let pool = CellGroupCheckpointPool::new(checkpoint_fixture::limits())?;
    let mut receiver = pool.receive(
        applier.clone(),
        checkpoint_receive::scratch()?,
        cluster._root.path().into(),
    )?;
    let checkpoint = cluster
        .domain
        .blocking(move || {
            source.stream(|chunk| {
                if let Some(index) =
                    index.filter(|_| matches!(chunk.part, CellGroupCheckpointPart::Header))
                {
                    assert_eq!(chunk.offset, 0);
                    assert_eq!(chunk.length, chunk.bytes.len() as u64);
                    let mut header: serde_json::Value =
                        serde_json::from_slice(chunk.bytes).unwrap();
                    header["root"]["index"] = serde_json::json!(index);
                    let bytes = serde_json::to_vec(&header).unwrap();
                    receiver.append(CellGroupCheckpointChunk {
                        length: bytes.len() as u64,
                        bytes: &bytes,
                        ..chunk
                    })
                } else {
                    receiver.append(chunk)
                }
            })?;
            receiver.finish()
        })?
        .await??;
    let mut message = Message::default();
    message.set_msg_type(MessageType::MsgSnapshot);
    message.from = 1;
    message.to = 3;
    message.term = term;
    message.set_snapshot(checkpoint.snapshot().clone());
    Ok((cluster, applier, checkpoint, pool, message))
}

pub(crate) async fn checkpoint_owner(rooted: bool) -> Result<()> {
    let (mut cluster, applier, checkpoint, pool, message) = prepared(rooted).await?;
    let route = cluster.birth.route().clone();
    let group = cluster.voters[2].log.cell_group(&route.history)?;
    let before = group.group_manifest()?.context("old root")?;
    let cut = checkpoint.marker().clone();
    let owner = cluster.voters[2].owner.as_mut().unwrap();
    assert!(
        owner.step(message.clone()).is_err(),
        "unverified peer ingress must stay closed"
    );
    for variant in 0..4 {
        let mut bad = message.clone();
        match variant {
            0 => bad.mut_snapshot().data = b"foreign descriptor".to_vec().into(),
            1 => bad.to = 2,
            2 => bad.from = 99,
            _ => bad.term = 1,
        }
        assert!(owner.step_checkpoint(bad, checkpoint.clone()).is_err());
        assert_eq!(group.group_manifest()?.as_ref(), Some(&before));
        assert!(owner.poll()?.is_none(), "refusal must not mutate Raft");
    }
    assert_eq!(
        owner.step_checkpoint(message.clone(), checkpoint.clone())?,
        CellGroupSnapshotAdmission::Restoring
    );
    drop(checkpoint);
    assert!(matches!(owner.campaign(), Err(RuntimeError::NotReady)));
    assert!(matches!(owner.step(message), Err(RuntimeError::NotReady)));
    let Some(CellGroupEffect::Persist(work)) = owner.poll()? else {
        anyhow::bail!("snapshot Ready persistence");
    };
    let gate = cluster.voters[2].gate.clone();
    let release = gate.arm(fault::HOLD_SYNC);
    let mut pending = cluster.domain.spawn(work.run())?;
    tokio::time::timeout(Duration::from_secs(5), gate.entered.notified()).await?;
    assert_eq!(pool.usage().sources, 1);
    assert_eq!(group.group_manifest()?.as_ref(), Some(&before));
    assert_eq!(
        cluster.voters[2].owner.as_ref().unwrap().status()?.applied,
        before.position()
    );
    assert!(
        cluster.voters[2].owner.as_mut().unwrap().poll()?.is_none(),
        "snapshot acknowledgement escaped sync"
    );
    assert!(
        tokio::time::timeout(Duration::from_millis(30), &mut pending)
            .await
            .is_err()
    );
    drop(release);
    let completion = pending.await??;
    assert_eq!(group.group_manifest()?.as_ref(), Some(&cut));
    let owner = cluster.voters[2].owner.as_mut().unwrap();
    assert_eq!(
        owner.status()?.applied,
        before.position(),
        "engine receipt alone cannot advance RawNode"
    );
    owner.complete_persist(completion)?;
    let mut acknowledged = false;
    let mut installed = false;
    for _ in 0..20 {
        match owner.poll()? {
            Some(CellGroupEffect::Message(response)) => {
                assert_eq!(response.get_msg_type(), MessageType::MsgAppendResponse);
                assert_eq!(
                    (response.from, response.to, response.index),
                    (3, 1, cut.position().index.get())
                );
                acknowledged = true;
            }
            Some(CellGroupEffect::CheckpointInstalled { manifest }) => {
                assert_eq!(manifest.as_ref(), &cut);
                installed = true;
            }
            None => break,
            _ => anyhow::bail!("unexpected snapshot completion"),
        }
    }
    assert!(acknowledged && installed);
    assert_eq!(owner.manifest().marker(), &cut);
    assert_eq!(owner.manifest().pinned_images(), 3);
    assert_eq!(pool.usage().sources, 0);
    assert!(!owner.status()?.ready);
    drop(group);
    let mut fixture = RuntimeCluster::from_cluster(cluster, 256, 256, 1000).await?;
    let sql = CellGroupSqlPool::start(
        &fixture.cluster.voters[2].log,
        CellSqlLimits {
            workers: 1,
            queue_per_worker: 2,
            max_pending_cells: 2,
            group_pending_cells: 2,
            node_bytes: 512 * 1024 * 1024,
            group_bytes: 512 * 1024 * 1024,
        },
    )?;
    fixture.appliers.insert((2, route.history), (sql, applier));
    fixture.runtimes[0].campaign(route.history).await?;
    fixture.drive(None).await?;
    let later = history(&route, "after-raft-snapshot")?;
    let bytes = images::image(None, "CREATE TABLE value(n); INSERT INTO value VALUES(17);")?;
    let accepted = fixture.runtimes[0]
        .propose(
            route.history,
            sql::apply::enrollment_bytes(&route, &later, &bytes, rooted)?,
        )
        .await?;
    fixture.drive(Some(accepted.position.index)).await?;
    let group = fixture.cluster.voters[2].log.cell_group(&route.history)?;
    assert_eq!(group.checkpoint_manifest()?.as_ref(), Some(&cut));
    assert_eq!(
        group
            .group_manifest()?
            .context("later root")?
            .root()
            .cells(),
        4
    );
    drop(group);
    fixture.close().await
}

pub(crate) async fn checkpoint_shutdown() -> Result<()> {
    for rooted in [false, true] {
        let (cluster, applier, checkpoint, pool, message) = prepared(rooted).await?;
        let route = cluster.birth.route().clone();
        let group = cluster.voters[2].log.cell_group(&route.history)?;
        let before = group.group_manifest()?.context("old root")?;
        let cut = checkpoint.marker().clone();
        let gate = cluster.voters[2].gate.clone();
        let fixture = RuntimeCluster::from_cluster(cluster, 256, 256, 1000).await?;
        let release = gate.arm(fault::HOLD_SYNC);
        assert_eq!(
            fixture.runtimes[2]
                .step_checkpoint(route.history, message, checkpoint)
                .await?,
            CellGroupSnapshotAdmission::Restoring
        );
        tokio::time::timeout(Duration::from_secs(5), gate.entered.notified()).await?;
        let runtime = fixture.runtimes[2].clone();
        let mut close = fixture
            .cluster
            .domain
            .spawn(async move { runtime.close().await })?;
        assert!(tokio::time::timeout(Duration::from_millis(30), &mut close)
            .await
            .is_err());
        assert_eq!(pool.usage().sources, 1);
        assert_eq!(group.group_manifest()?.as_ref(), Some(&before));
        drop(release);
        close.await??;
        assert_eq!(group.group_manifest()?.as_ref(), Some(&cut));
        assert_eq!(group.checkpoint_manifest()?.as_ref(), Some(&cut));
        assert_eq!(pool.usage().sources, 0);
        drop(applier);
        drop(group);
        fixture.close().await?;
    }
    Ok(())
}

pub(crate) async fn checkpoint_bounds() -> Result<()> {
    for index in [u64::MAX, u64::MAX - 1, u64::MAX - 2] {
        let (mut cluster, applier, checkpoint, pool, message) =
            prepared_at(false, Some(index)).await?;
        let owner = cluster.voters[2].owner.as_mut().unwrap();
        let before = owner.manifest().marker().clone();
        assert_eq!(checkpoint.marker().position().index.get(), index);
        assert!(matches!(
            owner.step_checkpoint(message, checkpoint),
            Err(RuntimeError::Invalid(_))
        ));
        assert_eq!(owner.manifest().marker(), &before);
        assert!(owner.poll()?.is_none());
        assert_eq!(pool.usage().sources, 0);
        drop(applier);
        cluster.close().await?;
    }
    Ok(())
}

pub(crate) async fn checkpoint_matching_log() -> Result<()> {
    use raft::{GetEntriesContext, Storage};
    for rooted in [false, true] {
        let (mut cluster, applier, checkpoint, pool, message) = prepared(rooted).await?;
        let route = cluster.birth.route().clone();
        let group = cluster.voters[2].log.cell_group(&route.history)?;
        let before = group.group_manifest()?.context("old root")?;
        let cut = checkpoint.marker().clone();
        // Store the leader's actual entries, but withhold its commit advance.
        // Raft must recognize this log and replay it, not install the image.
        let mut append = Message::default();
        append.set_msg_type(MessageType::MsgAppend);
        append.from = 1;
        append.to = 3;
        append.term = message.term;
        append.index = before.position().index.get();
        append.log_term = before.position().term.get();
        append.commit = before.position().index.get();
        append.entries = cluster.voters[0]
            .log
            .cell_group(&route.history)?
            .entries(
                append.index + 1,
                cut.position().index.get() + 1,
                None,
                GetEntriesContext::empty(false),
            )?
            .into();
        let owner = cluster.voters[2].owner.as_mut().unwrap();
        owner.step(append)?;
        loop {
            match owner.poll()? {
                Some(CellGroupEffect::Persist(work)) => {
                    owner.complete_persist(work.run().await?)?
                }
                Some(CellGroupEffect::Message(_)) => (),
                None => break,
                _ => anyhow::bail!("uncommitted entries must not apply"),
            }
        }
        assert_eq!(
            group.initial_state()?.hard_state.commit,
            before.position().index.get()
        );
        assert_eq!(
            owner.step_checkpoint(message, checkpoint)?,
            CellGroupSnapshotAdmission::Ignored
        );
        assert_eq!(pool.usage().sources, 0);
        let Some(CellGroupEffect::Persist(work)) = owner.poll()? else {
            anyhow::bail!("commit must persist before acknowledgement");
        };
        assert!(owner.poll()?.is_none());
        owner.complete_persist(work.run().await?)?;
        let Some(CellGroupEffect::Message(response)) = owner.poll()? else {
            anyhow::bail!("ignored snapshot response");
        };
        assert_eq!(response.index, cut.position().index.get());
        assert_eq!(owner.manifest().marker(), &before);
        assert_eq!(group.checkpoint_manifest()?, None);
        assert_eq!(
            group.initial_state()?.hard_state.commit,
            cut.position().index.get()
        );
        // Restart RawNode ownership over the actual durable committed log and
        // replay through the production image adapter on the shared workers.
        drop(group);
        let mut fixture = RuntimeCluster::from_cluster(cluster, 256, 256, 1000).await?;
        let sql = CellGroupSqlPool::start(
            &fixture.cluster.voters[2].log,
            CellSqlLimits {
                workers: 1,
                queue_per_worker: 2,
                max_pending_cells: 2,
                group_pending_cells: 2,
                node_bytes: 512 * 1024 * 1024,
                group_bytes: 512 * 1024 * 1024,
            },
        )?;
        fixture.appliers.insert((2, route.history), (sql, applier));
        fixture.runtimes[0].campaign(route.history).await?;
        fixture.drive(None).await?;
        let group = fixture.cluster.voters[2].log.cell_group(&route.history)?;
        assert_eq!(group.checkpoint_manifest()?, None);
        assert_eq!(
            group
                .group_manifest()?
                .context("replayed root")?
                .root()
                .cells(),
            3
        );
        assert_eq!(
            group.group_manifest()?.context("replayed root")?.root(),
            cut.root()
        );
        drop(group);
        fixture.close().await?;
    }
    Ok(())
}
