// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Receiver/storage qualification with a lagging member of the real quorum.
//! Snapshot transport and Raft Ready acceptance are deliberately separate gates.
use super::*;
use cellar_logic::consensus::OperationId;
use cellar_ltx::internal::{CacheLimits, NodeExtentStore, EXTENT_BYTES};
use cellar_ltx::stream_snapshot::{ScratchLimits, ScratchPool};
use raft::Storage;
use std::path::Path;

fn scratch() -> Result<ScratchPool> {
    Ok(ScratchPool::new(ScratchLimits {
        max_bytes: 32 * 1024 * 1024,
        max_files: 1,
    })?)
}

fn config(
    images: GenerationStore,
    extents: Option<NodeExtentStore>,
    directory: &Path,
) -> Result<CellGroupApplyImages> {
    let services = cellar_ltx::generation::ReadServices {
        now_unix_millis: Arc::new(|| 10),
        fill_random: Arc::new(|bytes| bytes.fill(17)),
    };
    Ok(match extents {
        Some(images) => CellGroupApplyImages::Extents {
            images,
            services,
            limits: Default::default(),
            cache_directory: directory.join("cache"),
            cache_pool: scratch()?,
            cache: CacheLimits {
                max_cached_bytes: EXTENT_BYTES,
                max_cached_extents: 1,
            },
        },
        None => CellGroupApplyImages::Materialized { images, services },
    })
}

fn applier(cluster: &mut Cluster) -> Result<CellGroupApplier> {
    let route = cluster.birth.route();
    let voter = &mut cluster.voters[2];
    drop(voter.owner.take());
    drop(voter.held.take());
    voter.paused = false;
    Ok(CellGroupApplier::new(
        voter.log.cell_group(&route.history)?,
        voter.pages.take().context("pages")?,
        config(
            voter.images.clone(),
            voter.extents.clone(),
            cluster._root.path(),
        )?,
        ManifestLimits::default(),
        GroupEntryLimits::default(),
        100,
    )?)
}

pub(crate) async fn install_checkpoint(rooted: bool) -> Result<()> {
    let (mut cluster, expected) = checkpoint_fixture::populated_with_lagging(rooted, true).await?;
    let route = cluster.birth.route().clone();
    let group = cluster.voters[2].log.cell_group(&route.history)?;
    let before = group.group_manifest()?.context("old marker")?;
    let source_pool = CellGroupCheckpointPool::new(checkpoint_fixture::limits())?;
    let mut source = source_pool.capture(
        &cluster.voters[0].log.cell_group(&route.history)?,
        cluster.voters[0].owner.as_ref().unwrap().manifest(),
    )?;
    let cut = source.marker().clone();
    assert!(cut.position().index > before.position().index);
    let applier = applier(&mut cluster)?;
    let pool = CellGroupCheckpointPool::new(checkpoint_fixture::limits())?;
    let scratch = scratch()?;
    let mut receiver = pool.receive(
        applier.clone(),
        scratch.clone(),
        cluster._root.path().into(),
    )?;
    let prepared = cluster
        .domain
        .blocking(move || {
            source.stream(|chunk| receiver.append(chunk))?;
            receiver.finish()
        })?
        .await??;
    assert_eq!(scratch.usage()?.reserved_bytes, 0);
    assert_eq!(prepared.marker(), &cut);
    assert_eq!(group.group_manifest()?.as_ref(), Some(&before));
    assert_eq!(group.checkpoint_manifest()?, None);
    assert_eq!(pool.usage().sources, 1);
    // A receiver proof cannot grant serving admission before atomic publication.
    assert!(cluster
        .domain
        .run(CellGroupOwner::start(
            cluster.permit(2).await?,
            prepared.manifest().clone(),
            cluster.options.clone(),
            [91; 32]
        ))
        .await?
        .is_err());
    let mut write = GroupWrite::new(&group);
    write.snapshot = Some(SnapshotChange::CellGroup(prepared.clone()));
    let mut hard = group.initial_state()?.hard_state;
    hard.commit = cut.position().index.get();
    write.hard_state = Some(hard);
    // Reject mixed contracts without releasing the receiver's physical owners.
    let mut mixed = write.clone();
    mixed.group_manifest = Some(prepared.manifest().clone());
    assert!(cluster.voters[2]
        .log
        .persist_cells(OperationId::new(910), vec![mixed])
        .await
        .is_err());
    assert_eq!(group.group_manifest()?.as_ref(), Some(&before));
    let installed = prepared.manifest().clone();
    drop(prepared);
    let gate = cluster.voters[2].gate.clone();
    let release = gate.arm(fault::HOLD_SYNC);
    let writer = cluster.voters[2].log.clone();
    let mut pending = cluster.domain.spawn(async move {
        writer
            .persist_cells(OperationId::new(911), vec![write])
            .await
    })?;
    tokio::time::timeout(Duration::from_secs(5), gate.entered.notified()).await?;
    assert_eq!(group.group_manifest()?.as_ref(), Some(&before));
    assert_eq!(group.checkpoint_manifest()?, None);
    assert_eq!(
        pool.usage().sources,
        1,
        "queued physical work owns receive admission"
    );
    assert!(
        tokio::time::timeout(Duration::from_millis(30), &mut pending)
            .await
            .is_err()
    );
    cluster.voters[2].images.retain_only(&[])?;
    cluster.voters[2].images.reclaim(1024)?;
    if let Some(extents) = &cluster.voters[2].extents {
        extents.retain_only(&[])?;
        extents.reclaim(1024)?;
    }
    drop(release);
    pending.await??;
    assert_eq!(pool.usage().sources, 0);
    assert_eq!(group.group_manifest()?.as_ref(), Some(&cut));
    assert_eq!(group.checkpoint_manifest()?.as_ref(), Some(&cut));
    // Stream the receiver's independently sealed closure and compare every byte
    // against the SQL fixtures, including the canonical empty cell.
    let mut received = checkpoint_fixture::Received::default();
    let mut local = source_pool.capture(&group, &installed)?;
    local.stream(|chunk| received.append(chunk))?;
    let leaves = expected
        .keys()
        .map(|name| {
            Ok((
                name.clone(),
                installed
                    .pin_cell(&CellId::new(name)?)
                    .context("received leaf")?
                    .0
                    .clone(),
            ))
        })
        .collect::<Result<BTreeMap<_, _>>>()?;
    received.verify(&cut, &leaves, &expected, rooted)?;
    drop(local);
    cluster.voters[2].owner = Some(
        cluster
            .domain
            .run(CellGroupOwner::start(
                cluster.permit(2).await?,
                installed.clone(),
                cluster.options.clone(),
                [92; 32],
            ))
            .await??,
    );
    assert!(!cluster.voters[2].owner.as_ref().unwrap().status()?.ready);
    drop(installed);
    assert_eq!(pool.usage().sources, 0);
    drop(group);
    // Resume real quorum replication, now through the production apply pool on
    // the receiver. A newer root must not erase the installed Raft checkpoint.
    cluster.network.clear();
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
    let later = history(&route, "after-checkpoint")?;
    let bytes = images::image(None, "CREATE TABLE value(n); INSERT INTO value VALUES(42);")?;
    let accepted = fixture.runtimes[0]
        .propose(
            route.history,
            sql::apply::enrollment_bytes(&route, &later, &bytes, rooted)?,
        )
        .await?;
    fixture.drive(Some(accepted.position.index)).await?;
    let group = fixture.cluster.voters[2].log.cell_group(&route.history)?;
    let current = group.group_manifest()?.context("later marker")?;
    assert!(current.position().index > cut.position().index);
    assert_ne!(current.root(), cut.root());
    assert_eq!(group.checkpoint_manifest()?.as_ref(), Some(&cut));
    drop(group);
    let directory = fixture.cluster.directory.clone();
    let birth = fixture.cluster.birth.clone();
    let disk = std::mem::replace(&mut fixture.cluster._root, tempfile::tempdir()?);
    fixture.close().await?;
    // Every original engine, runtime, image/page pin and receiver is now gone.
    // Remove the old checkpoint root page, while the later root stays on disk.
    let checkpoint_path = disk.path().join("node-3/pages").join(format!(
        "{}.page",
        hex::encode(cut.root().digest().unwrap())
    ));
    // Independently traverse the page references to prove this fault removes
    // only the old checkpoint, not any dependency of the current applied tree.
    let mut pending_pages = vec![current.root().digest().unwrap()];
    while let Some(digest) = pending_pages.pop() {
        assert_ne!(digest, cut.root().digest().unwrap());
        let bytes = std::fs::read(
            disk.path()
                .join("node-3/pages")
                .join(format!("{}.page", hex::encode(digest))),
        )?;
        let page: serde_json::Value = serde_json::from_slice(&bytes[8..])?;
        for child in ["left", "right"] {
            if !page[child].is_null() {
                pending_pages.push(serde_json::from_value::<[u8; 32]>(
                    page[child]["digest"].clone(),
                )?);
            }
        }
    }
    let checkpoint_bytes = std::fs::read(&checkpoint_path)?;
    std::fs::remove_file(&checkpoint_path)?;
    for missing in [true, false] {
        if !missing {
            std::fs::write(&checkpoint_path, &checkpoint_bytes)?;
        }
        let domain = ExecutionDomain::new(tokio::runtime::Handle::current());
        let base = disk.path().join("node-3");
        let log = domain
            .run(ReplicaLog::open(
                ReplicaLogOptions::new(
                    base.join("log"),
                    NodeId::new([3; 16])?,
                    DiskLineage::new([13; 16])?,
                    ProcessGeneration::new([if missing { 41 } else { 42 }; 16])?,
                    LogOpenMode::Resume,
                ),
                Arc::new(fault::FaultFileSystem {
                    gate: fault::Gate::new(None),
                }),
            ))
            .await??;
        let group = log.cell_group(&route.history)?;
        assert_eq!(group.group_manifest()?.as_ref(), Some(&current));
        assert_eq!(group.checkpoint_manifest()?.as_ref(), Some(&cut));
        let pages = ManifestPageStore::open(
            &base.join("pages"),
            Arc::new(DirectFileSystem),
            1024,
            16 * 1024 * 1024,
            16,
        )?;
        let config = config(
            GenerationStore::open(
                base.join("images"),
                Arc::new(DirectFileSystem),
                Default::default(),
            )?,
            if rooted {
                Some(NodeExtentStore::open(
                    disk.path().join("extents-2"),
                    Arc::new(DirectFileSystem),
                    Default::default(),
                )?)
            } else {
                None
            },
            disk.path(),
        )?;
        let recovering = group.clone();
        let recovered = domain
            .blocking(move || {
                CellGroupApplier::recover(
                    recovering,
                    pages,
                    config,
                    ManifestLimits::default(),
                    GroupEntryLimits::default(),
                    100,
                )
            })?
            .await?;
        if missing {
            assert!(
                recovered.is_err(),
                "missing installed checkpoint was admitted"
            );
        } else {
            let (applier, root) = recovered?;
            assert_eq!(root.marker(), &current);
            assert_eq!(root.pinned_images(), 4);
            assert!(root.pin_cell(&later.cell).is_some());
            let permit = domain
                .run(directory.bootstrap_group_member(&birth, &log))
                .await??;
            let owner = domain
                .run(CellGroupOwner::start(
                    permit,
                    root,
                    RaftRuntimeOptions::default(),
                    [93; 32],
                ))
                .await??;
            assert!(
                !owner.status()?.ready,
                "storage recovery cannot renew quorum authority"
            );
            drop(owner);
            drop(applier);
        }
        drop(group);
        log.close().await?;
        drop(log);
        domain.shutdown().await;
    }
    Ok(())
}

pub(crate) async fn refuse_checkpoint() -> Result<()> {
    for rooted in [false, true] {
        let (mut cluster, _) = checkpoint_fixture::populated_with_lagging(rooted, true).await?;
        let route = cluster.birth.route().clone();
        let group = cluster.voters[2].log.cell_group(&route.history)?;
        let before = group.group_manifest()?.context("old marker")?;
        let applier = applier(&mut cluster)?;
        let pool = CellGroupCheckpointPool::new(checkpoint_fixture::limits())?;
        let source_pool = CellGroupCheckpointPool::new(checkpoint_fixture::limits())?;
        for failure in 0..6 {
            let scratch = if failure == 3 {
                ScratchPool::new(ScratchLimits {
                    max_bytes: 1,
                    max_files: 1,
                })?
            } else {
                scratch()?
            };
            let mut receiver = pool.receive(
                applier.clone(),
                scratch.clone(),
                cluster._root.path().into(),
            )?;
            assert!(matches!(
                pool.receive(
                    applier.clone(),
                    scratch.clone(),
                    cluster._root.path().into()
                ),
                Err(CellGroupCheckpointError::Busy)
            ));
            let mut source = source_pool.capture(
                &cluster.voters[0].log.cell_group(&route.history)?,
                cluster.voters[0].owner.as_ref().unwrap().manifest(),
            )?;
            let mut changed = false;
            let result = source.stream(|chunk| {
                if failure >= 4 && matches!(chunk.part, CellGroupCheckpointPart::Header) {
                    changed = true;
                    if failure == 5 {
                        return receiver.append(CellGroupCheckpointChunk {
                            length: 65537,
                            ..chunk
                        });
                    }
                    assert_eq!(chunk.offset, 0);
                    assert_eq!(chunk.length, chunk.bytes.len() as u64);
                    let mut header: serde_json::Value =
                        serde_json::from_slice(chunk.bytes).unwrap();
                    header["root"]["incarnation"] = serde_json::to_value([99u8; 16]).unwrap();
                    let foreign = serde_json::to_vec(&header).unwrap();
                    return receiver.append(CellGroupCheckpointChunk {
                        length: foreign.len() as u64,
                        bytes: &foreign,
                        ..chunk
                    });
                }
                if !changed
                    && (matches!(chunk.part, CellGroupCheckpointPart::Materialized(_))
                        || matches!(chunk.part, CellGroupCheckpointPart::Extent { .. }))
                {
                    changed = true;
                    if failure == 0 {
                        return Err(CellGroupCheckpointError::Invalid("test truncation"));
                    }
                    if failure == 1 {
                        return receiver.append(CellGroupCheckpointChunk {
                            offset: chunk.offset + 1,
                            ..chunk
                        });
                    }
                    if failure == 2 {
                        let mut corrupt = chunk.bytes.to_vec();
                        corrupt[0] ^= 0xff;
                        return receiver.append(CellGroupCheckpointChunk {
                            bytes: &corrupt,
                            ..chunk
                        });
                    }
                }
                receiver.append(chunk)
            });
            if failure != 3 || !rooted {
                assert!(changed);
                assert!(result.is_err());
                assert!(receiver.finish().is_err());
            } else {
                // Extent transfer consumes bounded memory, no flat-image spool.
                result?;
                drop(receiver.finish()?);
            }
            assert_eq!(group.group_manifest()?.as_ref(), Some(&before));
            assert_eq!(group.checkpoint_manifest()?, None);
            assert_eq!(scratch.usage()?.reserved_files, 0);
            assert_eq!(pool.usage().sources, 0);
        }
        if !rooted {
            // Keep the existing image seal cached while corrupting only this
            // disposable, unselected destination. Fresh incoming bytes must not
            // make the stale cached seal an install proof.
            let mut source = source_pool.capture(
                &cluster.voters[0].log.cell_group(&route.history)?,
                cluster.voters[0].owner.as_ref().unwrap().manifest(),
            )?;
            let mut receiver =
                pool.receive(applier.clone(), scratch()?, cluster._root.path().into())?;
            source.stream(|chunk| receiver.append(chunk))?;
            drop(source);
            let prepared = receiver.finish()?;
            let (_, CellImagePin::Materialized(pin)) = prepared
                .manifest()
                .pin_cell(&CellId::new("alpha")?)
                .context("alpha image")?
            else {
                anyhow::bail!("materialized image required");
            };
            drop(prepared);
            let path = cluster
                ._root
                .path()
                .join("node-3/images")
                .join(format!("{}.sqlite", hex::encode(pin.id().bytes())));
            let mut file = std::fs::OpenOptions::new().write(true).open(path)?;
            std::io::Write::write_all(&mut file, b"corrupt")?;
            file.sync_all()?;
            drop(file);
            let mut source = source_pool.capture(
                &cluster.voters[0].log.cell_group(&route.history)?,
                cluster.voters[0].owner.as_ref().unwrap().manifest(),
            )?;
            let mut receiver =
                pool.receive(applier.clone(), scratch()?, cluster._root.path().into())?;
            assert!(
                source.stream(|chunk| receiver.append(chunk)).is_err(),
                "cached destination corruption must refuse transfer"
            );
            assert!(receiver.finish().is_err());
            assert_eq!(group.group_manifest()?.as_ref(), Some(&before));
            assert_eq!(pool.usage().sources, 0);
            drop(pin);
        }
        drop(applier);
        drop(group);
        cluster.close().await?;
    }
    Ok(())
}
