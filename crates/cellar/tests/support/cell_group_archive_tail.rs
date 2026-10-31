// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

use super::*;
use raft::prelude::{ConfChangeSingle, ConfChangeTransition, ConfChangeType, ConfChangeV2};

pub(super) async fn tail_index(
    store: &dyn ObjectStore,
    reference: &CellGroupArchiveTailReference,
) -> Result<(Path, serde_json::Value)> {
    let suffix = format!(
        "/tails/{}-{}",
        reference.length(),
        hex::encode(reference.digest())
    );
    let objects: Vec<_> = store.list(None).try_collect().await?;
    let path = objects
        .into_iter()
        .find(|m| {
            m.location.as_ref().ends_with(&suffix)
                && generation_prefix(&m.location)
                    .is_ok_and(|(_, generation)| generation == reference.generation())
                && registry_id(&m.location).is_ok_and(|id| id == reference.registry())
        })
        .context("tail index")?
        .location;
    let body = store.get(&path).await?.bytes().await?;
    Ok((path, serde_json::from_slice(&body)?))
}

fn entry_path(path: &Path, entry: &serde_json::Value) -> Result<Path> {
    let digest: Vec<u8> = serde_json::from_value(entry["object"]["digest"].clone())?;
    let (prefix, _) = generation_prefix(path)?;
    Ok(Path::from(format!(
        "{prefix}/generations/{}/entries/{}-{}",
        entry["object"]["generation"],
        entry["object"]["length"],
        hex::encode(digest)
    )))
}

async fn write_tail(
    store: &dyn ObjectStore,
    original: &Path,
    tail: &serde_json::Value,
) -> Result<CellGroupArchiveTailReference> {
    let bytes = serde_json::to_vec(tail)?;
    let digest: [u8; 32] = Sha256::digest(&bytes).into();
    let prefix = original
        .as_ref()
        .split("/tails/")
        .next()
        .context("tail prefix")?;
    store
        .put(
            &Path::from(format!(
                "{prefix}/tails/{}-{}",
                bytes.len(),
                hex::encode(digest)
            )),
            bytes.clone().into(),
        )
        .await?;
    Ok(serde_json::from_value(
        serde_json::json!({"registry": registry_id(original)?, "generation": generation_prefix(original)?.1, "digest": digest, "length": bytes.len()}),
    )?)
}

pub(super) async fn check_images(
    cluster: &Cluster,
    root: &PreparedGroupManifest,
    expected: &std::collections::BTreeMap<String, Vec<u8>>,
) -> Result<()> {
    assert_eq!(root.pinned_images(), expected.len());
    for (name, wanted) in expected {
        let (_, pin) = root
            .pin_cell(&CellId::new(name)?)
            .context("archived cell")?;
        let bytes = cluster
            .domain
            .blocking(move || -> Result<Vec<u8>> {
                Ok(match pin {
                    CellImagePin::Empty => Vec::new(),
                    CellImagePin::Materialized(pin) => pin
                        .open_reader()?
                        .read_exact_at(0, pin.state().length as usize)?,
                    CellImagePin::Extents(pin) => {
                        let mut bytes = Vec::new();
                        for n in 0..pin.manifest().geometry().extent_count() {
                            bytes.extend(
                                pin.view()
                                    .read_extent(pin.manifest().extent(n).context("extent")?)?,
                            );
                        }
                        bytes
                    }
                })
            })?
            .await??;
        assert_eq!(&bytes, wanted);
    }
    Ok(())
}

pub(super) async fn enrollment_controls(rooted: bool) -> Result<()> {
    let mut cluster = Cluster::new().await?;
    cluster.elect().await?;
    cluster.voters[2].paused = true;
    let store = archive_store::Store::new();
    let archive = new_archive(
        &cluster,
        store.clone(),
        Path::from("tail-qualification"),
        Default::default(),
    )
    .await?;
    let sources = CellGroupCheckpointPool::new(checkpoint_fixture::limits())?;
    let base = archive
        .upload_checkpoint(&cluster.domain, source(&sources, &cluster)?)
        .await?;
    let mut expected =
        checkpoint_fixture::enroll_images(&mut cluster, rooted, false, 1, 8192).await?;
    for name in ["alpha", "beta"] {
        checkpoint_fixture::advance_partition(&mut cluster, name, &expected[name], rooted, Some(2))
            .await?;
        expected.insert(
            name.into(),
            images::image(Some(&expected[name]), "UPDATE value SET n=n+1")?,
        );
    }
    let group = cluster.voters[0]
        .log
        .cell_group(&cluster.birth.route().history)?;
    // A genuine explicit joint demotion and leave preserve initial member 3 as
    // a learner. Both changes commit on the two healthy voters, with no fake cell.
    let mut joint = ConfChangeV2::default();
    joint.set_transition(ConfChangeTransition::Explicit);
    joint.changes.push(ConfChangeSingle {
        node_id: 3,
        change_type: ConfChangeType::AddLearnerNode,
        ..Default::default()
    });
    let change =
        CellGroupMembershipChange::new(cluster.birth.route(), &group.membership()?, joint, &[])?;
    cluster.voters[0]
        .owner
        .as_mut()
        .unwrap()
        .configure(change)?;
    cluster
        .drive(false)
        .await
        .context("explicit joint configuration apply")?;
    let leave = CellGroupMembershipChange::new(
        cluster.birth.route(),
        &group.membership()?,
        ConfChangeV2::default(),
        &[],
    )?;
    cluster.voters[0].owner.as_mut().unwrap().configure(leave)?;
    cluster.drive(false).await?;
    // Real leadership turnover appends the term's genuine Raft no-op after all
    // data/control entries. The original leader applies it as a follower.
    cluster.voters[0]
        .owner
        .as_mut()
        .unwrap()
        .transfer_leader(2)?;
    cluster.drive(false).await?;
    assert!(cluster.voters[1].owner.as_ref().unwrap().status()?.ready);
    let current = cluster.voters[0].owner.as_ref().unwrap().manifest().clone();
    let expected_membership = group.membership()?;
    let tails = CellGroupArchiveTailPool::new(Default::default())?;
    let reference = archive
        .upload_tail(&cluster.domain, &tails, &base, &group, &current)
        .await?;
    let (_, encoded) = tail_index(store.as_ref(), &reference).await?;
    assert_eq!(encoded["entries"].as_array().unwrap().len(), 9);
    assert_eq!(tails.usage().operations, 0);
    let applier = destination(&cluster).await?;
    let pool = CellGroupCheckpointPool::new(checkpoint_fixture::limits())?;
    let receiver_group = cluster.voters[2]
        .log
        .cell_group(&cluster.birth.route().history)?;
    let selected = receiver_group.group_manifest()?;
    for voter in &mut cluster.voters[..2] {
        drop(voter.owner.take());
        voter.log.close().await?;
    }
    let recovered = archive
        .verify_tail(
            &cluster.domain,
            &tails,
            &reference,
            receiver(&pool, &applier, &cluster)?,
        )
        .await?;
    assert_eq!(recovered.manifest().marker(), current.marker());
    assert_eq!(recovered.reference(), reference.reference());
    assert!(recovered.configuration().voters_outgoing.is_empty());
    assert_eq!(recovered.configuration().learners, vec![3]);
    assert_eq!(recovered.membership(), &expected_membership);
    assert_eq!(recovered.manifest().marker().root().cells(), 4);
    for name in ["alpha", "beta"] {
        assert_eq!(
            recovered
                .manifest()
                .pin_cell(&CellId::new(name)?)
                .unwrap()
                .0
                .version
                .sequence
                .get(),
            2
        );
    }
    assert_eq!(receiver_group.group_manifest()?, selected);
    assert_eq!(receiver_group.checkpoint_manifest()?, None);
    assert_eq!(
        tails.usage().operations,
        1,
        "returned closure retains replay admission"
    );
    assert_eq!(pool.usage().sources, 1);
    check_images(&cluster, recovered.manifest(), &expected).await?;
    drop(recovered);
    assert_eq!(tails.usage().operations, 0);
    assert_eq!(pool.usage().sources, 0);
    cluster.close().await?;
    Ok(())
}

pub(super) async fn dependencies(rooted: bool) -> Result<()> {
    let (mut cluster, mut expected) =
        checkpoint_fixture::populate_images(rooted, true, false, 1, 8192).await?;
    let store = archive_store::Store::new();
    let archive = new_archive(
        &cluster,
        store.clone(),
        Path::from("tail-qualification"),
        Default::default(),
    )
    .await?;
    let sources = CellGroupCheckpointPool::new(checkpoint_fixture::limits())?;
    let base = archive
        .upload_checkpoint(&cluster.domain, source(&sources, &cluster)?)
        .await?;
    checkpoint_fixture::advance_partition(
        &mut cluster,
        "alpha",
        &expected["alpha"],
        rooted,
        Some(2),
    )
    .await?;
    expected.insert(
        "alpha".into(),
        images::image(Some(&expected["alpha"]), "UPDATE value SET n=n+1")?,
    );
    let group = cluster.voters[0]
        .log
        .cell_group(&cluster.birth.route().history)?;
    let current = cluster.voters[0].owner.as_ref().unwrap().manifest().clone();
    let tails = CellGroupArchiveTailPool::new(Default::default())?;
    let reference = archive
        .upload_tail(&cluster.domain, &tails, &base, &group, &current)
        .await?;
    let (path, encoded) = tail_index(store.as_ref(), &reference).await?;
    let entry = entry_path(&path, &encoded["entries"][0])?;
    let entry_bytes = store.get(&entry).await?.bytes().await?;
    let (base_path, base_index) = index(store.as_ref(), &base).await?;
    let dependency = cell_objects(&base_path, &base_index, "beta")
        .into_iter()
        .last()
        .context("base beta")?;
    let dependency_bytes = store.get(&dependency).await?.bytes().await?;
    let applier = destination(&cluster).await?;
    let pool = CellGroupCheckpointPool::new(checkpoint_fixture::limits())?;
    for missing in [&dependency, &entry] {
        store.inner.delete(missing).await?;
        assert!(matches!(
            archive
                .verify_tail(
                    &cluster.domain,
                    &tails,
                    &reference,
                    receiver(&pool, &applier, &cluster)?
                )
                .await,
            Err(CellGroupArchiveError::Invalid("missing archive object"))
        ));
        let bytes = if missing == &dependency {
            dependency_bytes.clone()
        } else {
            entry_bytes.clone()
        };
        store.inner.put(missing, bytes.into()).await?;
        assert_eq!(tails.usage().operations, 0);
        assert_eq!(pool.usage().sources, 0);
    }
    let mut corrupt = entry_bytes.to_vec();
    corrupt[0] ^= 1;
    store.inner.put(&entry, corrupt.into()).await?;
    assert!(matches!(
        archive
            .verify_tail(
                &cluster.domain,
                &tails,
                &reference,
                receiver(&pool, &applier, &cluster)?
            )
            .await,
        Err(CellGroupArchiveError::Invalid("object digest/length"))
    ));
    store.inner.put(&entry, entry_bytes.into()).await?;
    let recovered = archive
        .verify_tail(
            &cluster.domain,
            &tails,
            &reference,
            receiver(&pool, &applier, &cluster)?,
        )
        .await?;
    check_images(&cluster, recovered.manifest(), &expected).await?;
    assert_eq!(recovered.manifest().marker(), current.marker());
    drop(recovered);
    for attack in 0..3 {
        let mut forged = encoded.clone();
        match attack {
            0 => forged["entries"] = serde_json::json!([]),
            1 => {
                forged["entries"][0]["index"] =
                    serde_json::json!(current.marker().position().index.get() + 1)
            }
            2 => forged["cut"]["marker"]["root"] = base_index["marker"]["root"].clone(),
            _ => unreachable!(),
        }
        let forged = write_tail(store.as_ref(), &path, &forged).await?;
        let result = archive
            .verify_tail(
                &cluster.domain,
                &tails,
                &forged,
                receiver(&pool, &applier, &cluster)?,
            )
            .await;
        match attack {
            0 => assert!(matches!(
                result,
                Err(CellGroupArchiveError::Invalid("tail domain/cuts/limits"))
            )),
            1 => assert!(matches!(
                result,
                Err(CellGroupArchiveError::Invalid("tail continuity"))
            )),
            2 => assert!(matches!(
                result,
                Err(CellGroupArchiveError::Invalid("replayed complete cut"))
            )),
            _ => unreachable!(),
        }
        assert_eq!(tails.usage().operations, 0);
        assert_eq!(pool.usage().sources, 0);
    }
    cluster.close().await?;
    Ok(())
}

pub(super) async fn warm_replay(rooted: bool) -> Result<()> {
    let (mut cluster, before) =
        checkpoint_fixture::populate_images(rooted, true, false, 1, 8192).await?;
    let store = archive_store::Store::new();
    let archive = new_archive(
        &cluster,
        store.clone(),
        Path::from("tail-warm"),
        Default::default(),
    )
    .await?;
    let sources = CellGroupCheckpointPool::new(checkpoint_fixture::limits())?;
    let base = archive
        .upload_checkpoint(&cluster.domain, source(&sources, &cluster)?)
        .await?;
    checkpoint_fixture::advance_partition(&mut cluster, "alpha", &before["alpha"], rooted, Some(2))
        .await?;
    let group = cluster.voters[0]
        .log
        .cell_group(&cluster.birth.route().history)?;
    let current = cluster.voters[0].owner.as_ref().unwrap().manifest().clone();
    let tails = CellGroupArchiveTailPool::new(Default::default())?;
    let reference = archive
        .upload_tail(&cluster.domain, &tails, &base, &group, &current)
        .await?;
    let (path, original) = tail_index(store.as_ref(), &reference).await?;
    let entry_location = entry_path(&path, &original["entries"][0])?;
    let entry = store.get(&entry_location).await?.bytes().await?;
    let applier = destination(&cluster).await?;
    let pool = CellGroupCheckpointPool::new(checkpoint_fixture::limits())?;
    let recovered = archive
        .verify_tail(
            &cluster.domain,
            &tails,
            &reference,
            receiver(&pool, &applier, &cluster)?,
        )
        .await?;
    let (_, final_pin) = recovered
        .manifest()
        .pin_cell(&CellId::new("alpha")?)
        .context("warm result")?;
    let (_, base_pin) = recovered
        .checkpoint()
        .checkpoint()
        .manifest()
        .pin_cell(&CellId::new("alpha")?)
        .context("warm base")?;
    drop(recovered);
    let wrong_image = images::image(Some(&before["alpha"]), "UPDATE value SET n=n+17")?;
    let wrong = images::Cut::new(&before["alpha"], &wrong_image, 1, true, false);
    if let (CellImagePin::Materialized(base_pin), CellImagePin::Materialized(final_pin)) =
        (&base_pin, &final_pin)
    {
        let store = applier.images.clone();
        let base_pin = base_pin.clone();
        let final_pin = final_pin.clone();
        let wrong = wrong.bytes.clone();
        let before = before["alpha"].clone();
        cluster
            .domain
            .blocking(move || -> Result<()> {
                let mut cut = images::Cut::new(&before, &wrong_image, 1, true, false);
                cut.bytes = wrong;
                let mut update = cut.update();
                update.base = base_pin.state();
                update.result = final_pin.state();
                assert!(
                    matches!(
                        store.prepare(final_pin.id(), Some(&base_pin), &update),
                        Err(cellar_ltx::generation::GenerationError::Exists)
                    ),
                    "ordinary cached preparation does not replay the supplied LTX"
                );
                assert!(store
                    .pin_prepared(final_pin.id(), final_pin.state())
                    .is_ok());
                Ok(())
            })?
            .await??;
    }
    // Re-encode a valid group command carrying a different valid SQLite delta,
    // while retaining the original declared result. Hash the forged entry/index
    // correctly so only actual LTX replay can reject it, including on warm pins.
    let fixed = b"cellar-cell-group-archive-entry-1\0".len();
    let context = u32::from_be_bytes(entry[fixed + 18..fixed + 22].try_into()?) as usize;
    let data = fixed + 26 + context;
    let mut decoded = CellGroupEntry::decode(
        &entry[data..],
        cluster.birth.route(),
        GroupEntryLimits::default(),
    )?;
    let GroupCommand::Mutation(mutation) = &mut decoded.command else {
        anyhow::bail!("expected mutation tail")
    };
    let bad = wrong.update();
    mutation.files = vec![GroupLtx {
        min_txid: bad.segments[0].min_txid.0,
        max_txid: bad.segments[0].max_txid.0,
        page_count: bad.segments[0].page_count as u64,
        decoded_bytes: bad.segments[0].decoded_bytes,
        bytes: &wrong.bytes,
    }];
    let command = decoded.encode(cluster.birth.route(), GroupEntryLimits::default())?;
    let mut body = entry[..data].to_vec();
    body[fixed + 22..fixed + 26].copy_from_slice(&(command.len() as u32).to_be_bytes());
    body.extend(command);
    let digest: [u8; 32] = Sha256::digest(&body).into();
    let mut forged = original.clone();
    forged["entries"][0]["object"] = serde_json::json!({"registry": original["entries"][0]["object"]["registry"], "generation": original["entries"][0]["object"]["generation"], "digest": digest, "length": body.len()});
    forged["bytes"] = serde_json::json!(body.len());
    let object = entry_path(&path, &forged["entries"][0])?;
    store.inner.put(&object, body.into()).await?;
    let forged = write_tail(store.as_ref(), &path, &forged).await?;
    assert!(matches!(
        archive
            .verify_tail(
                &cluster.domain,
                &tails,
                &forged,
                receiver(&pool, &applier, &cluster)?
            )
            .await,
        Err(CellGroupArchiveError::Replay(_))
    ));
    assert_eq!(tails.usage().operations, 0);
    // A cached final destination with damaged bytes must also refuse. Repair it
    // only in this owned fault fixture; verification must independently resync it.
    let physical = match &final_pin {
        CellImagePin::Materialized(pin) => cluster
            ._root
            .path()
            .join("archive-data/images")
            .join(format!("{}.sqlite", hex::encode(pin.id().bytes()))),
        CellImagePin::Extents(pin) => {
            let id = pin.manifest().extent(0).context("result extent")?;
            cluster
                ._root
                .path()
                .join("archive-data/extents")
                .join(format!("e-{:08x}-{}", id.length(), hex::encode(id.bytes())))
        }
        CellImagePin::Empty => anyhow::bail!("expected populated result"),
    };
    let bytes = std::fs::read(&physical)?;
    let mut corrupt = bytes.clone();
    corrupt[100] ^= 0x80;
    std::fs::write(&physical, corrupt)?;
    assert!(archive
        .verify_tail(
            &cluster.domain,
            &tails,
            &reference,
            receiver(&pool, &applier, &cluster)?
        )
        .await
        .is_err());
    assert_eq!(tails.usage().operations, 0);
    std::fs::write(physical, bytes)?;
    let recovered = archive
        .verify_tail(
            &cluster.domain,
            &tails,
            &reference,
            receiver(&pool, &applier, &cluster)?,
        )
        .await?;
    assert_eq!(recovered.manifest().marker(), current.marker());
    drop(recovered);
    cluster.close().await?;
    Ok(())
}

pub(super) async fn physical_cancellation() -> Result<()> {
    let gate = fault::Gate::new(None);
    let mut cluster = Cluster::with_storage(
        Default::default(),
        tempfile::tempdir()?,
        Arc::new(fault::FaultFileSystem { gate: gate.clone() }),
    )
    .await?;
    cluster.elect().await?;
    cluster.voters[2].paused = true;
    let expected = checkpoint_fixture::enroll_images(&mut cluster, false, false, 1, 8192).await?;
    let store = archive_store::Store::new();
    let archive = new_archive(
        &cluster,
        store,
        Path::from("tail-cancellation"),
        Default::default(),
    )
    .await?;
    let sources = CellGroupCheckpointPool::new(checkpoint_fixture::limits())?;
    let base = archive
        .upload_checkpoint(&cluster.domain, source(&sources, &cluster)?)
        .await?;
    checkpoint_fixture::advance_partition(
        &mut cluster,
        "alpha",
        &expected["alpha"],
        false,
        Some(2),
    )
    .await?;
    let group = cluster.voters[0]
        .log
        .cell_group(&cluster.birth.route().history)?;
    let current = cluster.voters[0].owner.as_ref().unwrap().manifest().clone();
    let tails = CellGroupArchiveTailPool::new(Default::default())?;
    let reference = archive
        .upload_tail(&cluster.domain, &tails, &base, &group, &current)
        .await?;
    let applier = destination_with_filesystem(
        &cluster,
        Arc::new(fault::FaultFileSystem { gate: gate.clone() }),
    )
    .await?;
    let pool = CellGroupCheckpointPool::new(checkpoint_fixture::limits())?;
    let recovered = archive
        .verify_tail(
            &cluster.domain,
            &tails,
            &reference,
            receiver(&pool, &applier, &cluster)?,
        )
        .await?;
    let (_, pin) = recovered
        .manifest()
        .pin_cell(&CellId::new("alpha")?)
        .context("physical replay result")?;
    let CellImagePin::Materialized(pin) = pin else {
        anyhow::bail!("expected materialized fixture")
    };
    let path = cluster
        ._root
        .path()
        .join("archive-data/images")
        .join(format!("{}.sqlite", hex::encode(pin.id().bytes())));
    drop(recovered);
    // Only the replayed result is selected; checkpoint/base reads cannot enter
    // this gate. The warm path first recomputes incoming LTX into private scratch.
    gate.image_read_paths.lock().unwrap().push(path);
    let release = gate.arm(fault::HOLD_IMAGE_READ);
    let mut pending = Box::pin(archive.verify_tail(
        &cluster.domain,
        &tails,
        &reference,
        receiver(&pool, &applier, &cluster)?,
    ));
    let entered = Box::pin(tokio::time::timeout(
        Duration::from_secs(5),
        gate.entered.notified(),
    ));
    match futures_util::future::select(entered, pending.as_mut()).await {
        futures_util::future::Either::Left((result, _)) => result?,
        futures_util::future::Either::Right(_) => {
            anyhow::bail!("replay ended before selected physical read")
        }
    }
    assert_eq!(archive.usage().operations, 1);
    assert_eq!(tails.usage().operations, 1);
    assert_eq!(pool.usage().sources, 1);
    drop(pending); // Stop the waiter while its physical job remains entered.
    assert_eq!(archive.usage().operations, 1);
    assert_eq!(tails.usage().operations, 1);
    assert_eq!(pool.usage().sources, 1);
    checkpoint_fixture::advance_partition(&mut cluster, "beta", &expected["beta"], false, Some(2))
        .await?;
    assert!(
        cluster.voters[0]
            .owner
            .as_ref()
            .unwrap()
            .manifest()
            .marker()
            .position()
            .index
            > current.marker().position().index
    );
    for voter in &mut cluster.voters {
        drop(voter.owner.take());
        drop(voter.held.take());
        voter.log.close().await?;
    }
    let mut shutdown = Box::pin(cluster.domain.shutdown());
    assert!(
        tokio::time::timeout(Duration::from_millis(30), &mut shutdown)
            .await
            .is_err(),
        "domain shutdown must join the held physical replay"
    );
    assert_eq!(tails.usage().operations, 1);
    drop(release);
    tokio::time::timeout(Duration::from_secs(5), &mut shutdown).await?;
    drop(shutdown);
    assert_eq!(archive.usage().retained_bytes, 0);
    assert_eq!(tails.usage().retained_bytes, 0);
    assert_eq!(pool.usage().sources, 0);
    cluster.close().await?;
    Ok(())
}

pub(super) async fn extend_compacted(rooted: bool) -> Result<()> {
    let (mut cluster, mut expected) =
        checkpoint_fixture::populate_images(rooted, true, false, 1, 8192).await?;
    let store = archive_store::Store::new();
    let archive = new_archive(
        &cluster,
        store.clone(),
        Path::from("tail-extension"),
        Default::default(),
    )
    .await?;
    let sources = CellGroupCheckpointPool::new(checkpoint_fixture::limits())?;
    let base = archive
        .upload_checkpoint(&cluster.domain, source(&sources, &cluster)?)
        .await?;
    checkpoint_fixture::advance_partition(
        &mut cluster,
        "alpha",
        &expected["alpha"],
        rooted,
        Some(2),
    )
    .await?;
    expected.insert(
        "alpha".into(),
        images::image(Some(&expected["alpha"]), "UPDATE value SET n=n+1")?,
    );
    let group = cluster.voters[0]
        .log
        .cell_group(&cluster.birth.route().history)?;
    let first = cluster.voters[0].owner.as_ref().unwrap().manifest().clone();
    let tails = CellGroupArchiveTailPool::new(Default::default())?;
    let prefix = archive
        .upload_tail(&cluster.domain, &tails, &base, &group, &first)
        .await?;
    let (prefix_path, prefix_index) = tail_index(store.as_ref(), &prefix).await?;
    let checkpoint = source(&sources, &cluster)?.prepare_publication()?;
    cluster.voters[0]
        .owner
        .as_mut()
        .unwrap()
        .publish_checkpoint(checkpoint)?;
    cluster.drive_partition(false, Some(2)).await?;
    assert_eq!(group.checkpoint_manifest()?.as_ref(), Some(first.marker()));
    assert_eq!(
        raft::Storage::first_index(&group)?,
        first.marker().position().index.get() + 1
    );
    checkpoint_fixture::advance_partition(&mut cluster, "beta", &expected["beta"], rooted, Some(2))
        .await?;
    expected.insert(
        "beta".into(),
        images::image(Some(&expected["beta"]), "UPDATE value SET n=n+1")?,
    );
    let latest = cluster.voters[0].owner.as_ref().unwrap().manifest().clone();
    assert!(
        matches!(
            archive
                .upload_tail(&cluster.domain, &tails, &base, &group, &latest)
                .await,
            Err(CellGroupArchiveError::Invalid("tail source committed cut"))
        ),
        "the original local prefix is genuinely unavailable"
    );
    let count = store.puts.load(Ordering::Acquire);
    let reference = archive
        .extend_tail(&cluster.domain, &tails, &prefix, &group, &latest)
        .await?;
    assert_eq!(
        store.puts.load(Ordering::Acquire),
        count + 2,
        "only the new entry and complete index require PUT"
    );
    let (_, index) = tail_index(store.as_ref(), &reference).await?;
    assert_eq!(index["entries"].as_array().unwrap().len(), 2);
    assert_eq!(index["entries"][0], prefix_index["entries"][0]);
    assert_eq!(index["checkpoint"], prefix_index["checkpoint"]);
    // The flattened extension names the original checkpoint and all immutable
    // entry objects directly; the old tail index is not a recovery dependency.
    store.inner.delete(&prefix_path).await?;
    let applier = destination(&cluster).await?;
    let pool = CellGroupCheckpointPool::new(checkpoint_fixture::limits())?;
    for voter in &mut cluster.voters[..2] {
        drop(voter.owner.take());
        voter.log.close().await?;
    }
    let recovered = archive
        .verify_tail(
            &cluster.domain,
            &tails,
            &reference,
            receiver(&pool, &applier, &cluster)?,
        )
        .await?;
    assert_eq!(recovered.manifest().marker(), latest.marker());
    check_images(&cluster, recovered.manifest(), &expected).await?;
    drop(recovered);
    assert_eq!(tails.usage().retained_bytes, 0);
    cluster.close().await?;
    Ok(())
}
