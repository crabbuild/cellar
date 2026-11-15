// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Receiver-owned external-byte verification and actual archive-root collection.
//! The fixture seeds a value row directly; ordinary SDK admission has its own gate.

use super::*;
use anyhow::ensure;

async fn publish(
    cluster: &Cluster,
    registry: &CellGroupArchiveRegistry,
    rooted: bool,
    label: &str,
) -> Result<CellGroupArchiveCoverage> {
    let owners = CellGroupArchiveUploadPool::new(Default::default())?;
    let tails = CellGroupArchiveTailPool::new(Default::default())?;
    let sources = CellGroupCheckpointPool::new(checkpoint_fixture::limits())?;
    let intent = registry.prepare_upload(&cluster.domain).await?;
    let ticket = registry.reserve(&cluster.domain, &intent).await?;
    let upload = registry
        .open_upload(&cluster.domain, &owners, &ticket, &tails)
        .await?;
    let checkpoint = upload
        .upload_checkpoint(&cluster.domain, source(&sources, cluster)?)
        .await?;
    let group = cluster.voters[0]
        .log
        .cell_group(&cluster.birth.route().history)?;
    let tail = upload
        .upload_tail(
            &cluster.domain,
            &tails,
            &checkpoint,
            &group,
            cluster.voters[0]
                .owner
                .as_ref()
                .context("source owner")?
                .manifest(),
        )
        .await?;
    let destination = destination_at(
        cluster,
        rooted,
        Arc::new(DirectFileSystem),
        cluster._root.path().join(label),
        cluster.birth.route().clone(),
    )
    .await?;
    let receivers = CellGroupCheckpointPool::new(checkpoint_fixture::limits())?;
    Ok(upload
        .prepare_publication(
            &cluster.domain,
            &tails,
            &tail,
            receiver(&receivers, &destination, cluster)?,
        )
        .await?
        .commit(&cluster.domain)
        .await?)
}

async fn recover(
    cluster: &Cluster,
    registry: &CellGroupArchiveRegistry,
    hold: &CellGroupArchiveHold,
    rooted: bool,
    label: &str,
) -> Result<CellGroupArchiveCoverage> {
    let tails = CellGroupArchiveTailPool::new(Default::default())?;
    let receivers = CellGroupCheckpointPool::new(checkpoint_fixture::limits())?;
    let destination = destination_at(
        cluster,
        rooted,
        Arc::new(DirectFileSystem),
        cluster._root.path().join(label),
        cluster.birth.route().clone(),
    )
    .await?;
    Ok(registry
        .verify_hold(
            &cluster.domain,
            hold,
            &tails,
            receiver(&receivers, &destination, cluster)?,
        )
        .await?)
}

pub(super) async fn qualify(rooted: bool, backend: Arc<dyn ObjectStore>) -> Result<()> {
    let remote = archive_store::Store::around(backend);
    let mut cluster = Cluster::with_store_birth(
        Default::default(),
        tempfile::tempdir()?,
        Arc::new(DirectFileSystem),
        [6; 32],
        remote.clone(),
        ClusterBirthOptions {
            group: GroupId::new([3; 16])?,
            image_storage: if rooted {
                RaftImageStorage::Extents
            } else {
                RaftImageStorage::Materialized
            },
            minimum: Durability::Quorum,
            log: Default::default(),
        },
    )
    .await?;
    cluster.elect().await?;
    let catalog = Catalog::new(remote.clone(), Default::default())?;
    let namespace = cluster
        .domain
        .run(catalog.existing_namespace(cluster.directory.namespace_config().clone()))
        .await??;
    let payload_pool = CellGroupPayloadPool::new(Default::default())?;
    let payloads = namespace.cell_payloads(&payload_pool)?;
    let history = history(cluster.birth.route(), "alpha")?;
    let body = bytes::Bytes::from(vec![93; 2 * 1024 * 1024]);
    let published = cluster
        .domain
        .run(payloads.publish(&history, body.clone(), Duration::from_secs(3)))
        .await??;
    let reference = published.reference().clone();
    let expected = checkpoint_fixture::enroll_images_with_payload(
        &mut cluster,
        rooted,
        false,
        0,
        8192,
        Some(&reference),
    )
    .await?;
    let archive_pool = CellGroupArchivePool::new(Default::default())?.with_payloads(payload_pool);
    let registry = cluster
        .directory
        .prepare_group_archive_binding(&cluster.domain, &archive_pool, &cluster.birth)
        .await?
        .commit(&cluster.domain)
        .await?;
    let old = Box::pin(publish(&cluster, &registry, rooted, "payload-initial")).await?;
    let old_hold = old.hold().clone();
    drop(old);
    let objects: Vec<_> = remote.inner.list(None).try_collect().await?;
    let payload_path = objects
        .iter()
        .find(|object| {
            object.location.as_ref().contains("/cell-payloads-1/")
                && !object.location.as_ref().contains("/ownership/")
                && object
                    .location
                    .filename()
                    .is_some_and(|name| name.ends_with(&hex::encode(reference.digest())))
        })
        .context("actual external object")?
        .location
        .clone();
    remote
        .inner
        .put(
            &payload_path,
            bytes::Bytes::from(vec![0; body.len()]).into(),
        )
        .await?;
    let corrupt = Box::pin(recover(
        &cluster,
        &registry,
        &old_hold,
        rooted,
        "payload-corrupt",
    ))
    .await;
    ensure!(
        matches!(corrupt, Err(error) if matches!(error.downcast_ref::<CellGroupArchiveError>(), Some(CellGroupArchiveError::Payload(CellGroupPayloadError::Invalid(_))))),
        "corrupt external bytes entered archive coverage"
    );
    remote.inner.delete(&payload_path).await?;
    let missing = Box::pin(recover(
        &cluster,
        &registry,
        &old_hold,
        rooted,
        "payload-missing",
    ))
    .await;
    ensure!(
        matches!(missing, Err(error) if matches!(error.downcast_ref::<CellGroupArchiveError>(), Some(CellGroupArchiveError::Payload(CellGroupPayloadError::Missing)))),
        "missing external bytes entered archive coverage"
    );
    remote.inner.put(&payload_path, body.clone().into()).await?;
    let after = images::image(
        Some(&expected["alpha"]),
        "UPDATE value SET n=n+1; DELETE FROM __sys_kv;",
    )?;
    checkpoint_fixture::advance_image(
        &mut cluster,
        "alpha",
        &expected["alpha"],
        &after,
        rooted,
        None,
    )
    .await?;
    let current = Box::pin(publish(&cluster, &registry, rooted, "payload-current")).await?;
    let current_hold = current.hold().clone();
    drop(current);
    let kept = registry
        .collect_payloads(&cluster.domain, Default::default())
        .await?;
    ensure!(
        kept.deleted_objects == 0 && kept.protected_objects == 1,
        "newer value deletion collected a retained recovery dependency"
    );
    let recovered = Box::pin(recover(
        &cluster,
        &registry,
        &old_hold,
        rooted,
        "payload-old-cut",
    ))
    .await?;
    tail_fixture::check_images(&cluster, recovered.recovery().manifest(), &expected).await?;
    ensure!(
        cluster
            .domain
            .run(payloads.get(&history, &reference, Duration::from_secs(3)))
            .await??
            .as_ref()
            == body.as_ref()
    );
    drop(recovered);
    let gate = remote.hold(payload_path.clone());
    cluster
        .domain
        .run(async {
            let mut reader = Box::pin(payloads.get(&history, &reference, Duration::from_secs(3)));
            match futures_util::future::select(Box::pin(gate.entered.notified()), reader.as_mut())
                .await
            {
                futures_util::future::Either::Left(_) => (),
                futures_util::future::Either::Right(_) => {
                    anyhow::bail!("external reader never reached the held provider")
                }
            }
            registry.release(&cluster.domain, &old_hold).await?;
            ensure!(
                registry
                    .collect_payloads(&cluster.domain, Default::default())
                    .await?
                    .deleted_objects
                    == 0,
                "retiring a cut deleted an entered reader"
            );
            remote.clear();
            gate.release.notify_waiters();
            let local = reader.await?;
            ensure!(local.as_ref() == body.as_ref());
            let collected = registry
                .collect_payloads(&cluster.domain, Default::default())
                .await?;
            ensure!(collected.deleted_objects == 1 && collected.deleted_bytes == body.len() as u64);
            ensure!(
                local.as_ref() == body.as_ref(),
                "retained local body changed after collection"
            );
            Ok::<_, anyhow::Error>(())
        })
        .await??;
    ensure!(
        matches!(
            remote.inner.get(&payload_path).await,
            Err(object_store::Error::NotFound { .. })
        ),
        "payload reclamation did not delete actual bytes"
    );
    let restored_current = Box::pin(recover(
        &cluster,
        &registry,
        &current_hold,
        rooted,
        "payload-after-collection",
    ))
    .await?;
    let mut current_expected = expected;
    current_expected.insert("alpha".into(), after);
    tail_fixture::check_images(
        &cluster,
        restored_current.recovery().manifest(),
        &current_expected,
    )
    .await?;
    drop(restored_current);
    registry.release(&cluster.domain, &current_hold).await?;
    cluster.close().await
}
