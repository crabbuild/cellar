// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Shutdown must finish an admitted ensemble publication before sealing it.
use super::*;
use futures_util::FutureExt;
use tokio::sync::Semaphore;

pub(super) struct PublicationGate {
    pub armed: std::sync::atomic::AtomicBool,
    pub requested: std::sync::atomic::AtomicBool,
    pub entered: Semaphore,
    pub release: Semaphore,
}

#[tokio::test]
async fn shutdown_during_ensemble_publication_seals_the_published_epoch() -> Result<()> {
    run(true, false).await
}

#[tokio::test]
async fn shutdown_during_first_ensemble_publication_seals_the_published_epoch() -> Result<()> {
    run(false, false).await
}

#[tokio::test]
async fn shutdown_deadline_during_publication_refuses_complete_drain() -> Result<()> {
    run(true, true).await
}

#[tokio::test]
async fn shutdown_waits_for_cancelled_shipping_round_before_sealing() -> Result<()> {
    shipping_round(false).await
}

#[tokio::test]
async fn shutdown_deadline_with_outstanding_shipping_round_refuses_complete_drain() -> Result<()> {
    shipping_round(true).await
}

async fn shipping_round(deadline: bool) -> Result<()> {
    let directory = tempfile::tempdir()?;
    let mut pair = pair(
        &directory.path().join("fleet.sqlite"),
        directory.path(),
        "closing-round",
        LogOptions::default(),
    )
    .await?;
    let domain = pair.domain.clone();
    let result = domain
        .run(async {
            // The real synchronous submission reserves a round before returning
            // its future. Never poll or apply this round: it cannot authorize a
            // fleet acknowledgement, and cancellation must release its reservation.
            let round = pair.log.ship(
                vec![cellar::ltx_repl::ShipEntry {
                    cell: CELL.into(),
                    epoch: 1,
                    txid: 1,
                    bytes: Vec::new(),
                }],
                0,
            );
            if deadline {
                ensure!(
                    !pair
                        .owner
                        .quiesce_and_seal_within(Duration::from_millis(50))
                        .await,
                    "an outstanding shipping round fabricated a complete drain"
                );
                drop(round);
                pair.log.maintain().await?;
                ensure!(
                    !pair.log.healthy() && pair.log.ensemble_width() == 0,
                    "cancelled shipping round resurrected the stopped shipper"
                );
            } else {
                let mut close = Box::pin(pair.owner.quiesce_and_seal());
                let early = tokio::time::timeout(Duration::from_millis(100), &mut close).await;
                ensure!(
                    early.is_err(),
                    "shutdown skipped an outstanding shipping round"
                );
                drop(round);
                tokio::time::timeout(Duration::from_secs(3), close).await?;
            }
            let (bytes, _) = pair
                .bucket
                .get("nodes/leader-closing-round.json")
                .await?
                .context("leader lease missing")?;
            let wire: serde_json::Value = serde_json::from_slice(&bytes)?;
            ensure!(
                wire["log"]["epoch"] == 1 && wire["log"]["may_have_fleet_acks"] == false,
                "unpolled shipping round fabricated a fleet proof: {wire}"
            );
            ensure!(
                wire["log"]["state"] == if deadline { "open" } else { "sealed" },
                "shipping reservation produced an incorrect shutdown state: {wire}"
            );
            Ok::<_, anyhow::Error>(())
        })
        .await;
    domain.run(pair.owner.shutdown_local()).await?;
    domain.shutdown().await;
    pair.transport.follower_domain.shutdown().await;
    result?
}

async fn run(recruit: bool, deadline: bool) -> Result<()> {
    let directory = tempfile::tempdir()?;
    let gate = Arc::new(PublicationGate {
        armed: false.into(),
        requested: false.into(),
        entered: Semaphore::new(0),
        release: Semaphore::new(0),
    });
    let mut pair = pair_with_publication_gate(
        &directory.path().join("fleet.sqlite"),
        directory.path(),
        "closing",
        LogOptions::default(),
        None,
        recruit,
        Some(gate.clone()),
    )
    .await?;
    let domain = pair.domain.clone();
    let result = domain.run(async {
        // A real second peer causes maintenance to replace the initial
        // one-member ensemble. Hold its actual lease publication, after the
        // desired epoch has changed but before it has been acknowledged.
        let peer = Arc::new(BucketOwnership::new(
            (*pair.bucket).clone(), (*pair.bucket).clone(),
            "second-follower".into(), "second-generation".into(),
        ).with_lease_ttl_ms(TTL));
        let _peer_lease = lease_writer(&domain, peer, "second-follower:1234").await?;
        let log = pair.log.clone();
        gate.armed.store(true, Ordering::SeqCst);
        let mut maintenance = Box::pin(log.maintain());
        cellar::asyncrt::select! {
            entered = gate.entered.acquire() => entered?.forget(),
            result = &mut maintenance => anyhow::bail!("maintenance bypassed publication gate: {result:?}"),
            _ = tokio::time::sleep(Duration::from_secs(3)) => anyhow::bail!("publication gate was not reached"),
        }
        if deadline {
            ensure!(!pair.owner.quiesce_and_seal_within(Duration::from_millis(20)).await,
                "an outstanding publication fabricated a complete drain");
            gate.release.add_permits(1);
            tokio::time::timeout(Duration::from_secs(3), maintenance).await??;
            // A late lease completion cannot recreate a shipper after local
            // fallback, even when another maintenance pass is requested.
            log.maintain().await?;
            ensure!(!log.healthy() && log.ensemble_width() == 0,
                "late publication resurrected the stopped shipper");
        } else {
            let mut close = Box::pin(pair.owner.quiesce_and_seal());
            ensure!(close.as_mut().now_or_never().is_none(), "close bypassed admitted publication");
            gate.release.add_permits(1);
            let (maintained, ()) = tokio::time::timeout(Duration::from_secs(5), async {
                tokio::join!(maintenance, close)
            }).await?;
            maintained?;
        }
        let (bytes, _) = pair.bucket.get("nodes/leader-closing.json").await?.context("leader lease missing")?;
        let wire: serde_json::Value = serde_json::from_slice(&bytes)?;
        ensure!(wire["log"]["epoch"] == if recruit { 2 } else { 1 }, "replacement epoch was not published: {wire}");
        ensure!(wire["log"]["state"] == if deadline { "open" } else { "sealed" }, "shutdown stranded a published open epoch: {wire}");
        Ok::<_, anyhow::Error>(())
    }).await;
    // Also release on failure so teardown cannot retain the artificial wait.
    gate.release.add_permits(1);
    let stopped = domain.run(pair.owner.shutdown_local()).await;
    domain.shutdown().await;
    pair.transport.follower_domain.shutdown().await;
    stopped?;
    result?
}
