// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Retry actual failed uploads from their durable reservation intent. Publication
//! still requires its own persisted intent and complete physical verification.

use super::*;
use anyhow::ensure;

async fn journal(fixture: &RuntimeCluster) -> Result<serde_json::Value> {
    let directory = fixture
        .cluster
        ._root
        .path()
        .join("node-1/log/cell-group-archive-journals");
    fixture
        .cluster
        .domain
        .blocking(move || -> Result<_> {
            let mut records = Vec::new();
            for entry in std::fs::read_dir(directory)? {
                let bytes = std::fs::read(entry?.path().join("state"))?;
                ensure!(bytes.len() <= 32 * 1024, "archive journal bound");
                records.push(serde_json::from_slice::<serde_json::Value>(&bytes)?);
            }
            ensure!(records.len() == 1, "expected one archive journal");
            Ok(records.remove(0))
        })?
        .await?
}

/// A confirmed CAS loss consumes the old allocation slot. It must not park a
/// fresh cut for the unrelated outage retry interval or replay that old intent.
pub(crate) async fn archive_reservation_renews_known_superseded_intent(
    rooted: bool,
    store: Arc<dyn ObjectStore>,
) -> Result<()> {
    let (mut fixture, route) = setup_store(rooted, store).await?;
    fixture.cluster._root.disable_cleanup(true);
    eprintln!(
        "retained reservation-contention disk: {}",
        fixture.cluster._root.path().display()
    );
    let (config, remote) = registration(&fixture, &route, rooted).await?;
    let mut limits = controller::limits();
    // This is the declared cadence for failed/background I/O. A single proven
    // allocation loss is not a backend outage and can finish one bounded turn.
    limits.retry_interval_ms = 10_000;
    let mut controllers = controller::start_with_limits(&mut fixture, limits).await?;
    drop(controllers[0].archive(config.clone())?);
    let initial = fixture.runtimes[0]
        .status(route.history)
        .await?
        .applied
        .index;
    let mut competing = None;
    let mut held = None;
    let work = async {
        published(&controllers[0], route.history, initial).await?;
        let registry = config
            .directory
            .open_group_archive(&fixture.cluster.domain, &config.io, &config.birth)
            .await?;
        let gate = remote.hold_publication();
        held = Some(gate.clone());
        let next = add(&fixture, &route, rooted, 47).await?;
        tokio::time::timeout(Duration::from_secs(5), gate.entered.notified())
            .await
            .context("new reservation missed actual registry CAS gate")?;
        let before = journal(&fixture).await?;
        let original = before["record"]["pending"]
            .get("Reserve")
            .context("gate did not hold a prepared reservation")?;
        let original: CellGroupArchiveIntent = serde_json::from_value(original.clone())?;
        let acquire = registry.prepare_acquire(&fixture.cluster.domain).await?;
        let hold = registry.acquire(&fixture.cluster.domain, &acquire).await?;
        competing = Some((registry.clone(), hold));
        // This genuine unrelated hold wins the version, then the actual old
        // reservation CAS loses. No synthetic success or changed data bytes.
        gate.release.notify_one();
        tokio::time::timeout(
            Duration::from_secs(5),
            published(&controllers[0], route.history, next),
        )
        .await
        .context("known superseded reservation waited for outage retry")??;
        ensure!(
            matches!(
                registry.reserve(&fixture.cluster.domain, &original).await,
                Err(CellGroupArchiveError::Expired)
            ),
            "superseded original intent became allocatable again"
        );
        // Join publication/retirement and release the controller's physical
        // workspace before an independent owner audits the selected bytes.
        let owner = controllers.remove(0);
        owner.close().await?;
        drop(owner);
        let coverage =
            verified_current(&fixture, &config, rooted, "reservation-contention-proof").await?;
        ensure!(
            coverage.marker().position().index >= next,
            "reservation retry did not independently recover the new cut"
        );
        ensure!(
            coverage
                .recovery()
                .manifest()
                .pin_cell(&CellId::new("later-47")?)
                .is_some(),
            "reservation retry lost new cell data"
        );
        Ok::<_, anyhow::Error>(())
    }
    .await;
    if let Some(gate) = held {
        gate.release.notify_one();
    }
    let mut cleanup = Ok(());
    if let Some((registry, hold)) = competing {
        cleanup = registry
            .release(&fixture.cluster.domain, &hold)
            .await
            .map_err(anyhow::Error::from);
    }
    for owner in &controllers {
        cleanup = cleanup.and(owner.close().await.map_err(anyhow::Error::from));
    }
    drop(controllers);
    drop(config);
    let closed = fixture.close().await;
    work.and(cleanup).and(closed)
}

pub(crate) async fn archive_reservation_retries_from_durable_intent(
    rooted: bool,
    store: Arc<dyn ObjectStore>,
) -> Result<()> {
    let (mut fixture, route) = setup_store(rooted, store).await?;
    fixture.cluster._root.disable_cleanup(true);
    eprintln!(
        "retained reservation-recovery disk: {}",
        fixture.cluster._root.path().display()
    );
    let (config, remote) = registration(&fixture, &route, rooted).await?;
    let initial = fixture.runtimes[0]
        .status(route.history)
        .await?
        .applied
        .index;
    remote.deny_chunk_creates.store(true, Ordering::Release);
    let controllers = controller::start_with_limits(&mut fixture, controller::limits()).await?;
    drop(controllers[0].archive(config.clone())?);
    let work = async {
        controller::wait_for(async || {
            Ok(remote.denied_chunk_creates.load(Ordering::Acquire) > 0
                && controllers[0]
                    .archive_status(route.history)?
                    .is_some_and(|status| status.error.is_some()))
        })
        .await?;
        ensure!(
            controllers[0]
                .archive_status(route.history)?
                .context("archive status")?
                .published
                .is_none(),
            "failed data writes produced coverage"
        );
        let before = journal(&fixture).await?;
        let saved = before["record"]["pending"].get("Reserve").context(
            "chunk upload replaced its durable reservation with a redundant ticket checkpoint",
        )?;
        let intent: CellGroupArchiveIntent = serde_json::from_value(saved.clone())?;
        let registry = config
            .directory
            .open_group_archive(&fixture.cluster.domain, &config.io, &config.birth)
            .await?;
        // The remote allocation is already committed. Replaying this exact
        // persisted intent must return its ticket, without allocating another.
        let ticket = registry.reserve(&fixture.cluster.domain, &intent).await?;
        let again = registry.reserve(&fixture.cluster.domain, &intent).await?;
        ensure!(
            ticket == again,
            "reservation replay changed the upload identity"
        );
        let captured = fixture
            .cluster
            ._root
            .path()
            .join("failed-upload-reservation.json");
        let bytes = serde_json::to_vec(&before)?;
        fixture
            .cluster
            .domain
            .blocking(move || std::fs::write(captured, bytes))?
            .await??;

        // Each failed drive has discarded its local upload owner. The next
        // drive must reload the real journal, reconcile the same reservation,
        // upload actual bytes and independently verify the complete cut.
        remote.deny_chunk_creates.store(false, Ordering::Release);
        published(&controllers[0], route.history, initial).await?;
        let selected = journal(&fixture).await?;
        ensure!(
            selected["record"]["head"]["root"]["object"]["generation"].as_u64()
                == Some(ticket.generation()),
            "retry allocated a replacement generation"
        );
        let later = add(&fixture, &route, rooted, 1).await?;
        published(&controllers[0], route.history, later).await?;
        Ok::<_, anyhow::Error>(())
    }
    .await;
    let mut cleanup = Vec::new();
    for controller in controllers {
        if let Err(error) = controller.close().await {
            cleanup.push(error.to_string());
        }
    }
    let audited = if work.is_ok() && cleanup.is_empty() {
        audit(&fixture, &config, rooted, 1).await
    } else {
        Ok(())
    };
    drop(config);
    let joined = fixture.close().await;
    work?;
    ensure!(
        cleanup.is_empty(),
        "reservation recovery cleanup: {cleanup:?}"
    );
    audited?;
    joined
}
