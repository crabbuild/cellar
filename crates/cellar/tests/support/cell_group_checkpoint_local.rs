// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Local publication, retained suffixes and real Raft-generated catch-up over
//! native streaming and authenticated TLS, followed by cold source recovery.
use super::*;
use raft::{GetEntriesContext, Storage};

impl RuntimeCluster {
    pub(super) async fn transfer_checkpoint(
        &mut self,
        history: GroupHistory,
        message: Message,
    ) -> Result<()> {
        let from = message.from as usize - 1;
        let to = message.to as usize - 1;
        let root = self
            .snapshot_sources
            .get(&(from, history))
            .context("snapshot source not enabled")?;
        let group = self.cluster.voters[from].log.cell_group(&history)?;
        let mut limits = checkpoint_fixture::limits();
        limits.max_sources = 2;
        limits.max_retained_bytes *= 2;
        let pool = CellGroupCheckpointPool::new(limits)?;
        let mut source = pool.capture_snapshot(&group, root, message.get_snapshot())?;
        let (_, applier) = self
            .appliers
            .get(&(to, history))
            .context("snapshot destination not enabled")?;
        let mut receiver = pool.receive(
            applier.clone(),
            checkpoint_receive::scratch()?,
            self.cluster._root.path().into(),
        )?;
        let checkpoint = self
            .cluster
            .domain
            .blocking(move || {
                source.stream(|chunk| receiver.append(chunk))?;
                receiver.finish()
            })?
            .await??;
        match self.runtimes[to]
            .step_checkpoint(history, message.clone(), checkpoint)
            .await
        {
            Ok(CellGroupSnapshotAdmission::Restoring) => self.snapshot_transfers += 1,
            Ok(CellGroupSnapshotAdmission::Ignored) => (),
            Err(RuntimeError::NotReady | RuntimeError::Overloaded) => {
                self.network.push_back((history, message))
            }
            Err(error) => return Err(error.into()),
        }
        assert!(
            pool.usage().sources <= 1,
            "only the accepted receiver may retain admission"
        );
        Ok(())
    }
}

pub(crate) async fn local_publication(rooted: bool) -> Result<()> {
    publication(rooted, Transfer::Native).await
}

pub(crate) async fn tcp_publication(rooted: bool) -> Result<()> {
    publication(rooted, Transfer::Tcp).await
}

pub(crate) async fn tcp_protocol() -> Result<()> {
    publication(false, Transfer::Probes).await
}

pub(crate) async fn tcp_cancelled_delivery() -> Result<()> {
    publication(false, Transfer::Cancelled).await
}

pub(crate) async fn tcp_retry() -> Result<()> {
    publication(false, Transfer::Retry).await
}

enum Transfer {
    Native,
    Tcp,
    Probes,
    Cancelled,
    Retry,
}

async fn publication(rooted: bool, transfer: Transfer) -> Result<()> {
    let tcp = !matches!(transfer, Transfer::Native);
    let probes = matches!(transfer, Transfer::Probes);
    let (mut cluster, _) = checkpoint_fixture::populated_partition(rooted).await?;
    let route = cluster.birth.route().clone();
    let group = cluster.voters[0].log.cell_group(&route.history)?;
    let root = cluster.voters[0].owner.as_ref().unwrap().manifest().clone();
    let cut = root.marker().clone();
    let pool = CellGroupCheckpointPool::new(checkpoint_fixture::limits())?;
    let checkpoint = pool.capture(&group, &root)?.prepare_publication()?;
    let snapshot = checkpoint.snapshot().clone();
    assert!(pool.capture_snapshot(&group, &root, &snapshot).is_err());
    let cell = history(&route, "suffix")?;
    let bytes = images::image(None, "CREATE TABLE value(n); INSERT INTO value VALUES(41);")?;
    let proposal = cluster.voters[0]
        .owner
        .as_mut()
        .unwrap()
        .propose(sql::apply::enrollment_bytes(&route, &cell, &bytes, rooted)?)?;
    // Persist a real suffix but withhold all peer replies, so the cut remains
    // applied while the leader's next proposal remains uncommitted.
    loop {
        match cluster.voters[0].owner.as_mut().unwrap().poll()? {
            Some(CellGroupEffect::Persist(work)) => {
                cluster.voters[0]
                    .owner
                    .as_mut()
                    .unwrap()
                    .complete_persist(work.run().await?)?;
            }
            Some(CellGroupEffect::Message(message)) => cluster.network.push_back(message),
            None => break,
            _ => anyhow::bail!("isolated suffix must not apply"),
        }
    }
    let hard = group.initial_state()?.hard_state;
    assert_eq!(hard.commit, cut.position().index.get());
    assert_eq!(group.last_index()?, proposal.position.index.get());
    let suffix = group.entries(
        hard.commit + 1,
        group.last_index()? + 1,
        None,
        GetEntriesContext::empty(false),
    )?;
    // A local publication cannot bundle even an identical hard state, and a
    // captured closure from another engine cannot publish on this replica.
    let mut mixed = GroupWrite::new(&group);
    mixed.snapshot = Some(SnapshotChange::CellGroupCheckpoint(checkpoint.clone()));
    mixed.hard_state = Some(hard.clone());
    assert!(cluster.voters[0]
        .log
        .persist_cells(cellar_logic::consensus::OperationId::new(973), vec![mixed])
        .await
        .is_err());
    let foreign_group = cluster.voters[1].log.cell_group(&route.history)?;
    let mut foreign = GroupWrite::new(&foreign_group);
    foreign.snapshot = Some(SnapshotChange::CellGroupCheckpoint(checkpoint.clone()));
    assert!(cluster.voters[1]
        .log
        .persist_cells(
            cellar_logic::consensus::OperationId::new(974),
            vec![foreign]
        )
        .await
        .is_err());
    assert_eq!(group.checkpoint_manifest()?, None);
    drop(foreign_group);
    let stale = checkpoint.clone();
    cluster.voters[0]
        .owner
        .as_mut()
        .unwrap()
        .publish_checkpoint(checkpoint)?;
    let Some(CellGroupEffect::Persist(work)) = cluster.voters[0].owner.as_mut().unwrap().poll()?
    else {
        anyhow::bail!("local checkpoint persistence");
    };
    let release = cluster.voters[0].gate.arm(fault::HOLD_SYNC);
    let mut pending = cluster.domain.spawn(work.run())?;
    tokio::time::timeout(
        Duration::from_secs(5),
        cluster.voters[0].gate.entered.notified(),
    )
    .await?;
    assert_eq!(group.checkpoint_manifest()?, None);
    assert_eq!(pool.usage().sources, 1);
    assert!(cluster.voters[0].owner.as_mut().unwrap().poll()?.is_none());
    assert!(
        tokio::time::timeout(Duration::from_millis(30), &mut pending)
            .await
            .is_err()
    );
    drop(release);
    let completion = pending.await??;
    assert_eq!(group.snapshot(0, 3)?, snapshot);
    assert_eq!(group.first_index()?, cut.position().index.get() + 1);
    assert_eq!(group.initial_state()?.hard_state, hard);
    assert_eq!(group.group_manifest()?.as_ref(), Some(&cut));
    assert_eq!(
        group.entries(
            hard.commit + 1,
            group.last_index()? + 1,
            None,
            GetEntriesContext::empty(false)
        )?,
        suffix
    );
    cluster.voters[0]
        .owner
        .as_mut()
        .unwrap()
        .complete_persist(completion)?;
    let Some(CellGroupEffect::CheckpointPublished { manifest }) =
        cluster.voters[0].owner.as_mut().unwrap().poll()?
    else {
        anyhow::bail!("local publication completion");
    };
    assert_eq!(manifest.as_ref(), &cut);
    cluster.drive_partition(false, Some(2)).await?;
    assert_eq!(
        cluster.voters[0].owner.as_ref().unwrap().status()?.applied,
        proposal.position
    );
    assert!(matches!(
        cluster.voters[0]
            .owner
            .as_mut()
            .unwrap()
            .publish_checkpoint(stale),
        Err(RuntimeError::Invalid(_))
    ));
    assert_eq!(pool.usage().sources, 0);
    assert!(pool.capture(&group, &root).is_err());
    let retained = pool.capture_snapshot(
        &group,
        cluster.voters[0].owner.as_ref().unwrap().manifest(),
        &snapshot,
    )?;
    assert_eq!(retained.marker(), &cut);
    drop(retained);
    let anchor = cluster.voters[0].owner.as_ref().unwrap().manifest().clone();
    let destination = cluster.voters[2].log.cell_group(&route.history)?;
    let applier = CellGroupApplier::new(
        destination,
        cluster.voters[2]
            .pages
            .take()
            .context("destination pages")?,
        checkpoint_receive::config(
            cluster.voters[2].images.clone(),
            cluster.voters[2].extents.clone(),
            cluster._root.path(),
        )?,
        ManifestLimits::default(),
        GroupEntryLimits::default(),
        100,
    )?;
    let source_applier = if tcp {
        Some(CellGroupApplier::new(
            group.clone(),
            cluster.voters[0].pages.take().context("source pages")?,
            checkpoint_receive::config(
                cluster.voters[0].images.clone(),
                cluster.voters[0].extents.clone(),
                cluster._root.path(),
            )?,
            ManifestLimits::default(),
            GroupEntryLimits::default(),
            100,
        )?)
    } else {
        None
    };
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
    fixture.snapshot_sources.insert((0, route.history), anchor);
    let mut registrations = Vec::new();
    let mut transport_pools = Vec::new();
    if let Some(source_applier) = source_applier {
        fixture.appliers.insert(
            (0, route.history),
            (
                CellGroupSqlPool::start(
                    &fixture.cluster.voters[0].log,
                    CellSqlLimits {
                        workers: 1,
                        queue_per_worker: 2,
                        max_pending_cells: 2,
                        group_pending_cells: 2,
                        node_bytes: 512 * 1024 * 1024,
                        group_bytes: 512 * 1024 * 1024,
                    },
                )?,
                source_applier,
            ),
        );
        let other = if probes {
            Some(fixture.add_group(91).await?)
        } else {
            None
        };
        fixture.connect_tcp().await?;
        for n in [0, 2] {
            let pool = CellGroupCheckpointPool::new(checkpoint_fixture::limits())?;
            let transport = &fixture.transports[n];
            transport.enable_group_checkpoints(pool.clone())?;
            assert!(matches!(
                transport.enable_group_checkpoints(pool.clone()),
                Err(TransportError::Identity)
            ));
            let applier = fixture.appliers[&(n, route.history)].1.clone();
            registrations.push(transport.register_group_checkpoint(applier.clone())?);
            assert!(matches!(
                transport.register_group_checkpoint(applier),
                Err(TransportError::Identity)
            ));
            transport_pools.push(pool);
        }
        assert!(matches!(
            fixture.transports[1]
                .register_group_checkpoint(fixture.appliers[&(0, route.history)].1.clone()),
            Err(TransportError::Identity)
        ));
        if let Some(other) = other {
            network_probes::probe(
                &mut fixture,
                route.history,
                &snapshot,
                &transport_pools[1],
                other,
                &mut registrations,
            )
            .await?;
        }
    }
    fixture.retry_snapshot = matches!(transfer, Transfer::Retry);
    if fixture.retry_snapshot {
        drop(registrations.pop().context("receiver registration")?);
    }
    fixture.drop_snapshot_delivery = matches!(transfer, Transfer::Cancelled);
    fixture.runtimes[0].campaign(route.history).await?;
    fixture.drive(None).await?;
    assert!(
        fixture.snapshot_transfers >= 1,
        "Raft must generate a snapshot after compaction"
    );
    if fixture.retry_snapshot {
        assert!(fixture.failed_snapshot);
        assert!(fixture.transports[0].usage().failed >= 1);
    }
    let restored = fixture.cluster.voters[2].log.cell_group(&route.history)?;
    assert_eq!(restored.checkpoint_manifest()?.as_ref(), Some(&cut));
    let source = group.group_manifest()?.context("source root")?;
    assert_eq!(restored.group_manifest()?.as_ref(), Some(&source));
    assert_eq!(restored.group_manifest()?.unwrap().root().cells(), 4);
    let tail = group.entries(
        group.first_index()?,
        group.last_index()? + 1,
        None,
        GetEntriesContext::empty(false),
    )?;
    assert_eq!(tail.first(), suffix.first());
    let disk = std::mem::replace(&mut fixture.cluster._root, tempfile::tempdir()?);
    drop(restored);
    drop(root);
    drop(group);
    drop(registrations);
    fixture.close().await?;
    for pool in transport_pools {
        assert_eq!(pool.usage().sources, 0);
    }
    // Reopen the actual leader engine and physical closures. Cache-only suffix
    // preservation cannot pass this check if publication cleaned durable entries.
    let domain = ExecutionDomain::new(tokio::runtime::Handle::current());
    let base = disk.path().join("node-1");
    let log = domain
        .run(ReplicaLog::open(
            ReplicaLogOptions::new(
                base.join("log"),
                NodeId::new([1; 16])?,
                DiskLineage::new([11; 16])?,
                ProcessGeneration::new([67; 16])?,
                LogOpenMode::Resume,
            ),
            Arc::new(DirectFileSystem),
        ))
        .await??;
    let group = log.cell_group(&route.history)?;
    assert_eq!(group.snapshot(0, 3)?, snapshot);
    assert_eq!(group.group_manifest()?.as_ref(), Some(&source));
    assert_eq!(
        group.entries(
            group.first_index()?,
            group.last_index()? + 1,
            None,
            GetEntriesContext::empty(false)
        )?,
        tail
    );
    let pages = ManifestPageStore::open(
        &base.join("pages"),
        Arc::new(DirectFileSystem),
        1024,
        16 * 1024 * 1024,
        16,
    )?;
    let config = checkpoint_receive::config(
        GenerationStore::open(
            base.join("images"),
            Arc::new(DirectFileSystem),
            Default::default(),
        )?,
        if rooted {
            Some(cellar_ltx::internal::NodeExtentStore::open(
                disk.path().join("extents-0"),
                Arc::new(DirectFileSystem),
                Default::default(),
            )?)
        } else {
            None
        },
        disk.path(),
    )?;
    let recovering = group.clone();
    let (applier, recovered) = domain
        .blocking(move || recover_source(recovering, pages, config, rooted))?
        .await??;
    assert_eq!(recovered.marker(), &source);
    assert_eq!(recovered.pinned_images(), 4);
    let (empty, empty_image) = recovered
        .pin_cell(&CellId::new("empty")?)
        .context("retained empty enrollment")?;
    assert_eq!(empty.version.sequence.get(), 1);
    assert_eq!(empty.version.image.txid, 0);
    assert!(matches!(empty_image, CellImagePin::Empty));
    for name in ["alpha", "beta", "suffix"] {
        let (leaf, image) = recovered
            .pin_cell(&CellId::new(name)?)
            .context("retained populated cell")?;
        assert_eq!(leaf.version.sequence.get(), 1);
        assert!(!matches!(image, CellImagePin::Empty));
    }

    let mut retained = pool.capture_snapshot(&group, &recovered, &snapshot)?;
    assert_eq!(retained.marker(), &cut);
    domain
        .blocking(move || retained.stream(|_| Ok(())))?
        .await??;
    assert_eq!(pool.usage().sources, 0);
    drop(recovered);
    drop(applier);
    drop(group);
    log.close().await?;
    drop(log);
    domain.shutdown().await;
    Ok(())
}

pub(crate) async fn local_shutdown() -> Result<()> {
    for rooted in [false, true] {
        let (cluster, _) = checkpoint_fixture::populated_with_lagging(rooted, false).await?;
        let history = cluster.birth.route().history;
        let group = cluster.voters[0].log.cell_group(&history)?;
        let pool = CellGroupCheckpointPool::new(checkpoint_fixture::limits())?;
        let checkpoint = pool
            .capture(&group, cluster.voters[0].owner.as_ref().unwrap().manifest())?
            .prepare_publication()?;
        let cut = checkpoint.marker().clone();
        let gate = cluster.voters[0].gate.clone();
        let fixture = RuntimeCluster::from_cluster(cluster, 256, 256, 1000).await?;
        let release = gate.arm(fault::HOLD_SYNC);
        fixture.runtimes[0]
            .publish_checkpoint(history, checkpoint)
            .await?;
        tokio::time::timeout(Duration::from_secs(5), gate.entered.notified()).await?;
        let runtime = fixture.runtimes[0].clone();
        let mut close = fixture
            .cluster
            .domain
            .spawn(async move { runtime.close().await })?;
        assert!(tokio::time::timeout(Duration::from_millis(30), &mut close)
            .await
            .is_err());
        assert_eq!(pool.usage().sources, 1);
        assert_eq!(group.checkpoint_manifest()?, None);
        drop(release);
        close.await??;
        assert_eq!(pool.usage().sources, 0);
        assert_eq!(group.checkpoint_manifest()?.as_ref(), Some(&cut));
        drop(group);
        fixture.close().await?;
    }
    Ok(())
}

// The source fixture deliberately uses local generation IDs distinct from the
// production SQL adapter. Restore its materialized pins with that same explicit
// resolver; logical roots exclude local filenames. Extents use the production
// recovery adapter. Both paths verify current and retained checkpoint closures.
fn recover_source(
    group: GroupLog<GroupHistory>,
    mut pages: ManifestPageStore,
    config: CellGroupApplyImages,
    rooted: bool,
) -> Result<(Option<CellGroupApplier>, PreparedGroupManifest)> {
    if rooted {
        let (applier, root) = CellGroupApplier::recover(
            group,
            pages,
            config,
            ManifestLimits::default(),
            GroupEntryLimits::default(),
            100,
        )?;
        return Ok((Some(applier), root));
    }
    let CellGroupApplyImages::Materialized { images, .. } = config else {
        anyhow::bail!("materialized fixture resolver");
    };
    let current = group.group_manifest()?.context("current marker")?;
    let checkpoint = group.checkpoint_manifest()?.context("checkpoint marker")?;
    let tree = ManifestTree::new(current.route().clone(), ManifestLimits::default())?;
    let mut recovered = None;
    for marker in [&current, &checkpoint] {
        let root = tree.prepare_complete(
            &group,
            &mut pages,
            marker.root(),
            marker.position(),
            marker.membership_revision(),
            |leaf| {
                if leaf.version.image.txid == 0 {
                    return Ok(CellImagePin::Empty);
                }
                let CellImageCommitment::Materialized(digest) = leaf.version.image.commitment
                else {
                    return Err(ManifestError::Invalid("fixture materialized commitment"));
                };
                let id = image_id(&leaf.version.history, leaf.version.sequence.get())
                    .map_err(|error| ManifestError::Io(std::io::Error::other(error)))?;
                images
                    .adopt(
                        id,
                        ImageState {
                            epoch: leaf.version.image.epoch,
                            txid: TXID(leaf.version.image.txid),
                            checksum: leaf.version.image.checksum,
                            digest,
                            length: leaf.geometry.length,
                            page_size: leaf.geometry.page_size,
                        },
                    )
                    .map(CellImagePin::Materialized)
                    .map_err(|error| ManifestError::Io(std::io::Error::other(error)))
            },
        )?;
        if marker == &current {
            recovered = Some(root);
        }
    }
    Ok((None, recovered.context("current closure")?))
}
