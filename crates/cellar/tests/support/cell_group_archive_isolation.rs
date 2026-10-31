// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Data recovery deliberately has neither an engine nor serving authority.
use super::*;
use cellar_logic::consensus::OperationId;

pub(super) async fn erased_voters(rooted: bool) -> Result<()> {
    let (mut cluster, mut expected) =
        checkpoint_fixture::populate_images(rooted, true, false, 1, 8192).await?;
    let archive_pool = CellGroupArchivePool::new(Default::default())?;
    let binding = cluster
        .directory
        .prepare_group_archive_binding(&cluster.domain, &archive_pool, &cluster.birth)
        .await?;
    let saved = persist_archive_intent(&cluster, &binding.encode().to_vec()).await?;
    assert_eq!(saved, binding.encode());
    let registry = binding.commit(&cluster.domain).await?;
    let owners = CellGroupArchiveUploadPool::new(Default::default())?;
    let tails = CellGroupArchiveTailPool::new(Default::default())?;
    let intent = registry.prepare_upload(&cluster.domain).await?;
    let saved = persist_archive_intent(&cluster, &intent).await?;
    let ticket = registry.reserve(&cluster.domain, &saved).await?;
    let archive = registry
        .open_upload(&cluster.domain, &owners, &ticket, &tails)
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
    let marker = current.marker().clone();
    let membership = group.membership()?;
    let reference = archive
        .upload_tail(&cluster.domain, &tails, &base, &group, &current)
        .await?
        .into_reference();
    // No source physical pin, open GroupLog or voter survives the disk erasure.
    drop(current);
    drop(group);
    drop(archive);
    drop(registry);
    drop(owners);
    for voter in &mut cluster.voters {
        drop(voter.owner.take());
        drop(voter.held.take());
        voter.log.close().await?;
    }
    drop(std::mem::take(&mut cluster.voters));
    cluster.network.clear();
    let directory = cluster._root.path().to_owned();
    cluster
        .domain
        .blocking(move || -> Result<()> {
            for n in 1..=3 {
                let path = directory.join(format!("node-{n}"));
                assert!(path.join("log").exists());
                assert!(path.join("images").exists());
                std::fs::remove_dir_all(&path)?;
                assert!(!path.exists());
                let extents = directory.join(format!("extents-{}", n - 1));
                if rooted {
                    assert!(extents.exists());
                    std::fs::remove_dir_all(&extents)?;
                    assert!(!extents.exists());
                }
            }
            assert!(!directory.join("archive-data").exists());
            std::fs::remove_dir_all(directory.join("archive-intents"))?;
            Ok(())
        })?
        .await??;
    // Reconstruct the expected registry identity with a fresh catalog owner,
    // using only remote metadata after all local intent journals were erased.
    let (directory, birth) = binding_fixture::reopen_directory(&cluster).await?;
    let registry = directory
        .open_group_archive(&cluster.domain, &archive_pool, &birth)
        .await?;
    let archive = registry.archive();
    // Open only physical data stores, after erasure. No bootstrap permit, local
    // member/disk identity, Raft metadata or replacement genesis is fabricated.
    let destination = destination_mode(&cluster, rooted, Arc::new(DirectFileSystem)).await?;
    let receivers = CellGroupCheckpointPool::new(checkpoint_fixture::limits())?;
    let recovered = archive
        .verify_tail(
            &cluster.domain,
            &tails,
            &reference,
            receiver(&receivers, &destination, &cluster)?,
        )
        .await?;
    assert_eq!(recovered.manifest().marker(), &marker);
    assert_eq!(recovered.membership(), &membership);
    tail_fixture::check_images(&cluster, recovered.manifest(), &expected).await?;
    assert!(cluster.voters.is_empty());
    // Pins protect all currently returned materialized images; releasing the
    // proof must also release private image retention and both admission pools.
    if !rooted {
        let images = destination.images.clone();
        assert_eq!(
            cluster
                .domain
                .blocking(move || images.reclaim(4096))?
                .await??
                .files,
            0
        );
    }
    drop(recovered);
    assert_eq!(receivers.usage().sources, 0);
    assert_eq!(tails.usage().operations, 0);
    if !rooted {
        let images = destination.images.clone();
        assert!(
            cluster
                .domain
                .blocking(move || images.reclaim(4096))?
                .await??
                .files
                > 0
        );
        // Re-fetching the whole closure also works after private cache eviction.
        let recovered = archive
            .verify_tail(
                &cluster.domain,
                &tails,
                &reference,
                receiver(&receivers, &destination, &cluster)?,
            )
            .await?;
        tail_fixture::check_images(&cluster, recovered.manifest(), &expected).await?;
    }
    let pending = receiver(&receivers, &destination, &cluster)?;
    destination.store.close();
    assert!(archive
        .verify_checkpoint(&cluster.domain, &base, pending)
        .await
        .is_err());
    assert!(receiver(&receivers, &destination, &cluster).is_err());
    assert_eq!(receivers.usage().sources, 0);
    cluster.close().await?;
    Ok(())
}

pub(super) async fn moving_cut(rooted: bool) -> Result<()> {
    let (mut cluster, expected) =
        checkpoint_fixture::populate_images(rooted, true, false, 1, 8192).await?;
    let remote = archive_store::Store::new();
    let archive = new_archive(
        &cluster,
        remote.clone(),
        Path::from("moving-cut"),
        Default::default(),
    )
    .await?;
    let sources = CellGroupCheckpointPool::new(checkpoint_fixture::limits())?;
    let original = source(&sources, &cluster)?;
    let marker = original.marker().clone();
    let reference = archive.upload_checkpoint(&cluster.domain, original).await?;
    let (path, index) = index(remote.as_ref(), &reference).await?;
    let gate = remote.hold(cell_objects(&path, &index, "beta")[0].clone());
    let destination = destination(&cluster).await?;
    let receivers = CellGroupCheckpointPool::new(checkpoint_fixture::limits())?;
    let archive_domain = ExecutionDomain::new(tokio::runtime::Handle::current());
    let mut pending = Box::pin(archive.verify_checkpoint(
        &archive_domain,
        &reference,
        receiver(&receivers, &destination, &cluster)?,
    ));
    let entered = Box::pin(tokio::time::timeout(
        Duration::from_secs(5),
        gate.entered.notified(),
    ));
    match futures_util::future::select(entered, pending.as_mut()).await {
        futures_util::future::Either::Left((result, _)) => result?,
        futures_util::future::Either::Right(_) => {
            anyhow::bail!("archive ended before held dependency")
        }
    }
    checkpoint_fixture::advance_partition(
        &mut cluster,
        "alpha",
        &expected["alpha"],
        rooted,
        Some(2),
    )
    .await?;
    let group = cluster.voters[0]
        .log
        .cell_group(&cluster.birth.route().history)?;
    let advanced = group.group_manifest()?.context("advanced serving cut")?;
    assert!(advanced.position().index > marker.position().index);
    remote.clear();
    gate.release.notify_one();
    let recovered = pending.await?;
    assert_eq!(recovered.checkpoint().marker(), &marker);
    tail_fixture::check_images(&cluster, recovered.checkpoint().manifest(), &expected).await?;
    assert_eq!(group.group_manifest()?.as_ref(), Some(&advanced));
    assert!(matches!(
        sources.capture(&group, recovered.checkpoint().manifest()),
        Err(CellGroupCheckpointError::Invalid("source root/owner"))
    ));
    drop(recovered);
    // Verifying a newer checkpoint does not prevent selecting an older archive
    // cut later, and neither result may mutate or publish a live engine root.
    let latest = archive
        .upload_checkpoint(&cluster.domain, source(&sources, &cluster)?)
        .await?;
    let newer = archive
        .verify_checkpoint(
            &cluster.domain,
            &latest,
            receiver(&receivers, &destination, &cluster)?,
        )
        .await?;
    assert_eq!(newer.checkpoint().marker(), &advanced);
    drop(cluster.voters[0].owner.take());
    let mut write = GroupWrite::new(&group);
    write.group_manifest = Some(newer.checkpoint().manifest().clone());
    assert!(
        matches!(cluster.voters[0].log.persist_cells(OperationId::new(777), vec![write]).await,
        Err(LogError::Invalid(message)) if message == "invalid cell-group apply binding or prefix")
    );
    assert_eq!(group.group_manifest()?.as_ref(), Some(&advanced));
    drop(newer);
    let older = archive
        .verify_checkpoint(
            &cluster.domain,
            &reference,
            receiver(&receivers, &destination, &cluster)?,
        )
        .await?;
    assert_eq!(older.checkpoint().marker(), &marker);
    tail_fixture::check_images(&cluster, older.checkpoint().manifest(), &expected).await?;
    drop(older);
    assert_eq!(receivers.usage().sources, 0);
    archive_domain.shutdown().await;
    cluster.close().await?;
    Ok(())
}
