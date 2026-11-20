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
