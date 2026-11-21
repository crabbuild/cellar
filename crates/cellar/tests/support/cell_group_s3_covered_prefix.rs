// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! A cell's archived state does not depend on its siblings' later Quorum writes.
use super::*;

pub(crate) async fn s3_evicted_receipt_ignores_unarchived_sibling(
    rooted: bool,
    store: Arc<dyn ObjectStore>,
) -> Result<()> {
    let mut h = Harness::configured_resources(
        rooted,
        store,
        2,
        Durability::Quorum,
        None,
        None,
        Some(Resources::DeferredBackground),
    )
    .await?;
    let work = async {
        let written = h
            .command(1, b"archived-cell", Durability::S3, Duration::from_secs(5))
            .await
            .context("initial S3 command admission")?
            .response()
            .await
            .context("initial S3 command output")?;
        let version = written.observation().version;
        let archived = written
            .local_receipt()
            .context("archived receipt")?
            .position
            .index;
        drop(written);
        controller::wait_for(async || Ok(h.controllers[0].usage().retained_s3 == 0)).await?;

        // Use the actual shared bank to evict idle custody. No cached physical
        // proof or output hold may make the subsequent lookup pass.
        controller::wait_for(async || {
            match h.archive.checkpoints.receive_archive(
                h.archive.store.clone(),
                h.archive.scratch.clone(),
                h.archive.scratch_directory.clone(),
            ) {
                Ok(receiver) => {
                    drop(receiver);
                    Ok(h.archive.checkpoints.usage().sources == 0)
                }
                Err(CellGroupCheckpointError::Busy) => Ok(false),
                Err(error) => Err(error.into()),
            }
        })
        .await?;
        anyhow::ensure!(
            h.archive.tails.usage().operations == 0,
            "physical proof survived eviction"
        );
        h.remote
            .deny_generation_writes
            .store(true, Ordering::Release);

        let sibling = history(&h.route, "registered-b")?;
        let sibling_call = h
            .dispatcher
            .command(
                sibling.clone(),
                CellCommandRequest {
                    id: CommandId::new(ClientId::new([189; 16])?, 1)?,
                    contract: h.class.contract("Add").context("sibling Add")?,
                    input: b"unarchived-sibling".to_vec(),
                    durability: Durability::Quorum,
                    retry_until_ms: h.retry_until,
                },
                CellRoutingLimits {
                    timeout: Duration::from_secs(5),
                    max_attempts: 4,
                },
                None,
            )
            .await
            .context("sibling command admission")?;
        let sibling_result = h.controllers[0]
            .command(sibling_call, h.fixture.cell_options(&sibling).await?)?
            .response()
            .await
            .context("sibling Quorum command")?;
        anyhow::ensure!(sibling_result.durability() == Durability::Quorum);
        anyhow::ensure!(
            sibling_result
                .local_receipt()
                .context("sibling receipt")?
                .position
                .index
                > archived
        );
        drop(sibling_result);
        anyhow::ensure!(
            h.controllers[0]
                .archive_status(h.route.history)?
                .context("archive status")?
                .published
                .context("published prefix")?
                .position()
                .index
                == archived,
            "sibling cut was archived before the fault"
        );
        let denied = h.remote.denied_generation_writes.load(Ordering::Acquire);
        let mut lookup = h
            .lookup(
                h.receipt_request(1, b"archived-cell", Durability::S3)?,
                Duration::from_secs(5),
            )
            .await?;
        let response = lookup
            .execute(
                &h.fixture.runtimes[0],
                &h.fixture.appliers[&(0, h.route.history)].0,
                h.fixture.cell_options(&h.cell).await?,
            )
            .await
            .context("covered S3 receipt depended on an unarchived sibling")?;
        anyhow::ensure!(response.durability() == Durability::S3);
        anyhow::ensure!(response.observation().version == version);
        anyhow::ensure!(
            response.receipt().context("retained result")?.result() == b"archived-cell"
        );
        h.token(&response).await?;
        anyhow::ensure!(
            h.controllers[0]
                .archive_status(h.route.history)?
                .context("lookup archive status")?
                .published
                .context("lookup archive prefix")?
                .position()
                .index
                == archived,
            "covered lookup published a newer group cut"
        );
        anyhow::ensure!(
            h.remote.denied_generation_writes.load(Ordering::Acquire) == denied,
            "covered lookup attempted a new archive upload"
        );
        anyhow::ensure!(
            h.calls.load(Ordering::Acquire) == 2,
            "receipt lookup replayed business work"
        );
        drop((response, lookup));

        // A newer mutation of the requested cell really does require a new
        // archived prefix. It cannot borrow the sibling-independent old proof.
        let newer = h
            .command(
                2,
                b"unarchived-cell",
                Durability::Quorum,
                Duration::from_secs(5),
            )
            .await?
            .response()
            .await?;
        anyhow::ensure!(newer.observation().version.sequence > version.sequence);
        drop(newer);
        let request = h.receipt_request(2, b"unarchived-cell", Durability::S3)?;
        let mut unavailable = h.lookup(request.clone(), Duration::from_secs(5)).await?;
        let result = unavailable
            .execute(
                &h.fixture.runtimes[0],
                &h.fixture.appliers[&(0, h.route.history)].0,
                h.fixture.cell_options(&h.cell).await?,
            )
            .await;
        anyhow::ensure!(
            result.is_err(),
            "unarchived target escaped the S3 proof gate"
        );
        anyhow::ensure!(
            h.remote.denied_generation_writes.load(Ordering::Acquire) > denied,
            "uncovered target never reached the actual archive-write refusal"
        );
        drop(unavailable);
        h.remote
            .deny_generation_writes
            .store(false, Ordering::Release);
        let mut recovered = h
            .lookup(request, Duration::from_secs(5))
            .await
            .context("healed receipt discovery")?;
        let response = recovered
            .execute(
                &h.fixture.runtimes[0],
                &h.fixture.appliers[&(0, h.route.history)].0,
                h.fixture.cell_options(&h.cell).await?,
            )
            .await
            .context("healed target after archive-write refusal")?;
        anyhow::ensure!(response.durability() == Durability::S3);
        anyhow::ensure!(
            response.receipt().context("healed result")?.result() == b"unarchived-cell"
        );
        h.token(&response).await?;
        anyhow::ensure!(
            h.calls.load(Ordering::Acquire) == 3,
            "recovery replayed business work"
        );
        drop((response, recovered));
        eprintln!(
            "s3_covered_prefix_observations {}",
            serde_json::json!({
                "extents":rooted,"evicted_physical_proof":true,"business_calls":3,
                "original_request_budget_ms":5000,"covered_lookup_uploaded":false,
                "uncovered_lookup_refused":true
            })
        );
        Ok::<_, anyhow::Error>(())
    }
    .await;
    h.remote
        .deny_generation_writes
        .store(false, Ordering::Release);
    if work.is_err() {
        h.fixture.cluster._root.disable_cleanup(true);
        eprintln!(
            "retained covered-prefix disk: {}",
            h.fixture.cluster._root.path().display()
        );
    }
    let closed = h.close().await;
    work?;
    closed
}
