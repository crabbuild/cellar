// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! A transient dependency GET must not permanently stop the archive owner.
use super::*;

pub(crate) async fn archive_controller_payload_outage(
    rooted: bool,
    backend: Arc<dyn ObjectStore>,
) -> Result<()> {
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
    let payload_pool = CellGroupPayloadPool::new(Default::default())?;
    let catalog = Catalog::new(remote.clone(), Default::default())?;
    let namespace = cluster
        .domain
        .run(catalog.existing_namespace(cluster.directory.namespace_config().clone()))
        .await??;
    let payloads = namespace.cell_payloads(&payload_pool)?;
    let cell = history(cluster.birth.route(), "alpha")?;
    let body = bytes::Bytes::from(vec![93; 2 * 1024 * 1024]);
    let allocation = cluster
        .domain
        .run(payloads.publish(&cell, body.clone(), Duration::from_secs(3)))
        .await??;
    let reference = allocation.reference().clone();
    let expected = checkpoint_fixture::enroll_images_with_payload(
        &mut cluster,
        rooted,
        false,
        0,
        8192,
        Some(&reference),
    )
    .await?;
    let route = cluster.birth.route().clone();
    let mut fixture = RuntimeCluster::from_cluster(cluster, 256, 256, 1_000_000).await?;
    for node in 0..3 {
        apply::install(&mut fixture, &cell, node).await?;
    }
    fixture.runtimes[0].campaign(route.history).await?;
    fixture.drive(None).await?;
    let (mut config, _) = registration(&fixture, &route, rooted).await?;
    config.io = config.io.with_payloads(payload_pool);
    let index = fixture.runtimes[0]
        .status(route.history)
        .await?
        .applied
        .index;
    let controllers = controller::start_with_limits(&mut fixture, controller::limits()).await?;
    remote.deny_payload_reads.store(true, Ordering::Release);
    let result = futures_util::FutureExt::catch_unwind(std::panic::AssertUnwindSafe(async {
        // Dropping a response never cancels the admitted archive owner.
        drop(controllers[0].archive(config.clone())?);
        controller::wait_for(async || {
            let Some(status) = controllers[0].archive_status(route.history)? else {
                return Ok(false);
            };
            match status.error.as_deref() {
                Some(CellGroupArchiveControlError::Archive(CellGroupArchiveError::Payload(
                    CellGroupPayloadError::Storage(_),
                ))) => {
                    anyhow::ensure!(remote.denied_payload_reads.load(Ordering::Acquire) > 0);
                    anyhow::ensure!(
                        !status.stopped,
                        "transient payload GET permanently stopped archiving"
                    );
                    anyhow::ensure!(
                        status.published.is_none(),
                        "unread payload entered published coverage"
                    );
                    Ok(true)
                }
                Some(error) if status.stopped => anyhow::bail!("unexpected archive stop: {error}"),
                _ => Ok(false),
            }
        })
        .await?;
        let reads = remote.payload_gets.load(Ordering::Acquire);
        remote.deny_payload_reads.store(false, Ordering::Release);
        // No application request, wake, re-registration or new journal intent.
        published(&controllers[0], route.history, index).await?;
        anyhow::ensure!(remote.payload_gets.load(Ordering::Acquire) > reads);
        let coverage = verified_current(&fixture, &config, rooted, "payload-outage-audit").await?;
        anyhow::ensure!(coverage.marker().position().index >= index);
        for (name, bytes) in &expected {
            let (_, image) = coverage
                .recovery()
                .manifest()
                .pin_cell(&CellId::new(name)?)
                .context("recovered cell")?;
            let expected = bytes.clone();
            fixture
                .cluster
                .domain
                .blocking(move || -> Result<()> {
                    let actual = match image {
                        CellImagePin::Empty => Vec::new(),
                        CellImagePin::Materialized(pin) => pin
                            .open_reader()?
                            .read_exact_at(0, pin.state().length as usize)?,
                        CellImagePin::Extents(pin) => {
                            let mut bytes = Vec::new();
                            for n in 0..pin.manifest().geometry().extent_count() {
                                bytes
                                    .extend(pin.view().read_extent(
                                        pin.manifest().extent(n).context("extent")?,
                                    )?);
                            }
                            bytes
                        }
                    };
                    anyhow::ensure!(actual == expected, "recovered image bytes differ");
                    Ok(())
                })?
                .await??;
        }
        anyhow::ensure!(
            fixture
                .cluster
                .domain
                .run(payloads.get(&cell, &reference, Duration::from_secs(3)))
                .await??
                .as_ref()
                == body.as_ref()
        );
        Ok::<_, anyhow::Error>(())
    }))
    .await;
    remote.deny_payload_reads.store(false, Ordering::Release);
    let mut cleanup: Result<()> = Ok(());
    for controller in &controllers {
        let closed = controller.close().await.map_err(anyhow::Error::from);
        cleanup = cleanup.and(closed);
    }
    drop(controllers);
    drop(config);
    let root = std::mem::replace(&mut fixture.cluster._root, tempfile::tempdir()?);
    let closed = fixture.close().await;
    if !matches!(&result, Ok(Ok(()))) || cleanup.is_err() || closed.is_err() {
        let root = root.keep();
        for voter in 1..=3 {
            eprintln!(
                "retained payload archive retry disk: {}",
                root.join(format!("node-{voter}")).display()
            );
        }
    }
    match result {
        Ok(result) => result?,
        Err(panic) => std::panic::resume_unwind(panic),
    }
    cleanup?;
    closed
}
