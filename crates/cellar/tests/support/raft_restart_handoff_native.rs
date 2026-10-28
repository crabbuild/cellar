// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Owned handoff keeps the history pending until a surviving leader is observed.
use super::*;
use cellar::application::consensus::verification::FleetRestartRuntimeProgress;
use cellar::application::consensus::InspectionError;
#[path = "raft_restart_readiness_native.rs"]
mod readiness;
#[path = "raft_restart_stop_native.rs"]
mod stop;

async fn consume(
    s: &Setup,
    walk: &mut FleetRestartVerification,
    expected: &GroupScope,
) -> Result<usize> {
    s.f.domain
        .run(cellar::asyncrt::timeout(TIMEOUT, async {
            let mut requests = 0;
            loop {
                match walk.reconcile_next(TIMEOUT).await {
                    Ok(Some(FleetRestartRuntimeProgress::HandoffRequested {
                        group,
                        from,
                        to,
                        term,
                    })) => {
                        ensure!(group == *expected && from != to && term.get() > 0);
                        requests += 1;
                        ensure!(walk
                            .next_history(TIMEOUT)
                            .await?
                            .as_ref()
                            .is_some_and(|h| h.binding().scope().is_ok_and(|g| g == *expected)));
                    }
                    Ok(Some(FleetRestartRuntimeProgress::Verified(
                        FleetRestartHistoryProgress::Voting { group, .. },
                    ))) => {
                        ensure!(group == *expected);
                        return Ok::<_, anyhow::Error>(requests);
                    }
                    Err(FleetCatalogError::Unconfirmed | FleetCatalogError::Overloaded)
                    | Err(FleetCatalogError::Observation(
                        InspectionError::Unavailable | InspectionError::Overloaded,
                    )) => {
                        cellar::asyncrt::sleep(Duration::from_millis(10)).await;
                    }
                    other => anyhow::bail!("handoff did not retain its voting history: {other:?}"),
                }
            }
        }))
        .await??
}

async fn survivor_leader(s: &Setup, excluded: usize) -> Result<usize> {
    s.f.domain
        .run(cellar::asyncrt::timeout(TIMEOUT, async {
            loop {
                for (i, cell) in s.cells.iter().enumerate() {
                    if i == excluded {
                        continue;
                    }
                    // A requested transfer can leave the serialized snapshot
                    // temporarily unready. Keep the original polling deadline
                    // and require an actual survivor leader, never backpressure.
                    let observed = match cell.inspect().await {
                        Ok(observed) => observed,
                        Err(CommandFailure::Overloaded) => continue,
                        Err(error) => return Err(error.into()),
                    };
                    if observed.member.as_ref().is_some_and(|m| m.is_leader) {
                        return Ok::<_, anyhow::Error>(i);
                    }
                }
                cellar::asyncrt::sleep(Duration::from_millis(10)).await;
            }
        }))
        .await??
}

async fn finish(s: &Setup, walk: FleetRestartVerification, p: usize) -> Result<()> {
    let candidate = walk.finish(TIMEOUT).await?;
    ensure!(
        candidate.histories() == 1
            && candidate.voting_histories() == 1
            && candidate.matched_histories() == 1
            && candidate.physical_histories() == 1
    );
    let root = s.f.catalog.read(TIMEOUT).await?;
    ensure!(root.revision() == candidate.inventory_revision());
    ensure!(
        s.f.catalog
            .authorize_host(&root, s.nodes[p].spec.node, TIMEOUT)
            .await?
            .state()
            == FleetNodeState::RestartPending
    );
    ensure!(s
        .f
        .catalog
        .authorize_capacity(&root, s.nodes[p].spec.node, TIMEOUT)
        .await
        .is_err());
    let q = survivor_leader(s, p).await?;
    let r = (0..3)
        .find(|i| *i != p && *i != q)
        .context("second survivor")?;
    write_read(&s.f, &[s.cells[q].clone(), s.cells[r].clone()], 2, 5, 12).await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn fleet_restart_handoff_observes_primary_and_preserves_sql() -> Result<()> {
    let s = Box::pin(Setup::new()).await?;
    let p = primary(&s.f, &s.cells).await?;
    let (_, mut walk) = prepare(&s, p).await?;
    ensure!(consume(&s, &mut walk, &s.proposal.scope).await? == 1);
    ensure!(walk.next_history(TIMEOUT).await?.is_none());
    finish(&s, walk, p).await?;
    s.close().await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn fleet_restart_handoff_verifies_survivor_without_transfer() -> Result<()> {
    let s = Box::pin(Setup::new()).await?;
    let p = (primary(&s.f, &s.cells).await? + 1) % 3;
    let (_, mut walk) = prepare(&s, p).await?;
    ensure!(consume(&s, &mut walk, &s.proposal.scope).await? == 0);
    ensure!(walk.next_history(TIMEOUT).await?.is_none());
    finish(&s, walk, p).await?;
    s.close().await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn fleet_restart_handoff_reopens_after_request_and_observes_actual_leader() -> Result<()> {
    let s = Box::pin(Setup::new()).await?;
    let p = primary(&s.f, &s.cells).await?;
    let (gate, mut walk) = prepare(&s, p).await?;
    s.f.domain
        .run(cellar::asyncrt::timeout(TIMEOUT, async {
            loop {
                match walk.reconcile_next(TIMEOUT).await {
                    Ok(Some(FleetRestartRuntimeProgress::HandoffRequested {
                        from, to, ..
                    })) => {
                        ensure!(from == MemberId::new(p as u64 + 1)? && to != from);
                        return Ok::<_, anyhow::Error>(());
                    }
                    Err(FleetCatalogError::Unconfirmed | FleetCatalogError::Overloaded)
                    | Err(FleetCatalogError::Observation(
                        InspectionError::Unavailable | InspectionError::Overloaded,
                    )) => {
                        cellar::asyncrt::sleep(Duration::from_millis(10)).await;
                    }
                    other => anyhow::bail!("handoff was not requested: {other:?}"),
                }
            }
        }))
        .await???;
    drop(walk);
    survivor_leader(&s, p).await?;
    let catalog = Fixture::open_catalog(&s.f.domain, s.f.store.clone())?;
    let root = catalog.read(TIMEOUT).await?;
    let mut walk = catalog
        .prepare_restart_verification(
            &root,
            ProposalId::new(gate.identity.operation)?,
            std::slice::from_ref(&s.namespace),
            &s.spare,
            TIMEOUT,
        )
        .await?;
    ensure!(consume(&s, &mut walk, &s.proposal.scope).await? == 0);
    finish(&s, walk, p).await?;
    s.close().await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn fleet_restart_handoff_missing_second_survivor_keeps_obligation() -> Result<()> {
    let mut s = Box::pin(Setup::new()).await?;
    let p = primary(&s.f, &s.cells).await?;
    let (_, mut walk) = prepare(&s, p).await?;
    let pending = walk.next_history(TIMEOUT).await?.context("history")?;
    let victim = s.nodes.remove((p + 1) % 3);
    stopped(victim.node).await?;
    ensure!(walk.reconcile_next(TIMEOUT).await.is_err());
    ensure!(walk.next_history(TIMEOUT).await?.as_ref() == Some(&pending));
    ensure!(matches!(
        walk.finish(TIMEOUT).await,
        Err(FleetCatalogError::InventoryIncomplete)
    ));
    s.close().await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn fleet_restart_handoff_observes_survivor_while_archive_write_waits() -> Result<()> {
    Box::pin(archive_wait_case()).await
}

async fn archive_wait_case() -> Result<()> {
    let s = Box::pin(Setup::new()).await?;
    let leader = primary(&s.f, &s.cells).await?;
    let p = (leader + 1) % 3;
    let (_, mut walk) = prepare(&s, p).await?;
    let before = s.cells[leader].replication_status().await?.committed;
    s.f.store.archive_hang.store(true, Ordering::Release);
    let cell = s.cells[leader].clone();
    let operation = s.f.definition.increment.clone();
    let mut options = public_node::archived_options(1)?;
    options.id = CommandId::new(cellar_logic::consensus::ClientId::new([190; 16])?, 1)?;
    let completed = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
    let finished = completed.clone();
    let write = s.f.domain.spawn(async move {
        let result = cell
            .command(operation, 0_i64.to_be_bytes().to_vec(), options)
            .await;
        finished.store(true, Ordering::Release);
        result
    })?;
    let observed =
        s.f.domain
            .run(cellar::asyncrt::timeout(TIMEOUT, async {
                while s.cells[leader].replication_status().await?.committed <= before {
                    cellar::asyncrt::sleep(Duration::from_millis(10)).await;
                }
                ensure!(
                    !completed.load(Ordering::Acquire),
                    "archive waiter unexpectedly completed"
                );
                let requested = Box::pin(consume(&s, &mut walk, &s.proposal.scope)).await?;
                ensure!(
                    requested == 0 && !completed.load(Ordering::Acquire),
                    "observation waited for the archive write"
                );
                Ok::<_, anyhow::Error>(())
            }))
            .await;
    s.f.store.archive_hang.store(false, Ordering::Release);
    s.f.store.archive_resume.notify_waiters();
    let joined = write.await;
    let outcome = async {
        let receipt = joined??;
        observed???;
        ensure!(receipt.durability == Durability::S3);
        finish(&s, walk, p).await
    }
    .await;
    let closed = s.close().await;
    outcome?;
    closed
}
