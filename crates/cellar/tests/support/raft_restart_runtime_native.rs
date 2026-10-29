// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Actual bounded runtime walk; both survivors, not just quorum availability.
use super::*;
#[path = "raft_restart_genesis_native.rs"]
mod genesis;
#[path = "raft_restart_handoff_native.rs"]
mod handoff;
use cellar::application::consensus::verification::{
    FleetRestartHistoryProgress, FleetRestartVerification,
};

async fn prepare(
    s: &Setup,
    p: usize,
) -> Result<(FleetRestartGateRequest, FleetRestartVerification)> {
    let (_, gate, _) = admit(s, p).await?;
    discover(
        &s.f,
        &[
            &s.nodes[0].host,
            &s.nodes[1].host,
            &s.nodes[2].host,
            &s.spare,
        ],
    )
    .await?;
    let root = s.f.catalog.read(TIMEOUT).await?;
    let walk =
        s.f.catalog
            .prepare_restart_verification(
                &root,
                ProposalId::new(gate.identity.operation)?,
                std::slice::from_ref(&s.namespace),
                &s.spare,
                TIMEOUT,
            )
            .await?;
    Ok((gate, walk))
}

async fn verified(primary_target: bool) -> Result<()> {
    let s = Box::pin(Setup::new()).await?;
    let primary = primary(&s.f, &s.cells).await?;
    let p = if primary_target {
        primary
    } else {
        (primary + 1) % 3
    };
    let (gate, mut walk) = prepare(&s, p).await?;
    let progress = walk
        .verify_next(TIMEOUT)
        .await?
        .context("missing history")?;
    let FleetRestartHistoryProgress::Voting {
        group,
        member,
        survivors,
    } = progress
    else {
        anyhow::bail!("live target was not verified as a voter");
    };
    ensure!(group == s.proposal.scope && member == MemberId::new(p as u64 + 1)?);
    ensure!(survivors[0] != survivors[1] && !survivors.contains(&s.nodes[p].spec.node));
    ensure!(walk.verify_next(TIMEOUT).await?.is_none());
    let candidate = walk.finish(TIMEOUT).await?;
    ensure!(
        candidate.histories() == 1
            && candidate.physical_histories() == 1
            && candidate.voting_histories() == 1
            && candidate.retained_histories() == 0
    );
    ensure!(candidate.operation() == gate.identity.operation && candidate.digest() != [0; 32]);
    let root = s.f.catalog.read(TIMEOUT).await?;
    ensure!(root.revision() == candidate.inventory_revision());
    ensure!(
        s.f.catalog
            .authorize_host(&root, gate.node, TIMEOUT)
            .await?
            .state()
            == FleetNodeState::RestartPending
    );
    // A candidate leaves consensus alive and supplies no stop/capacity grant.
    ensure!(s
        .f
        .catalog
        .authorize_capacity(&root, gate.node, TIMEOUT)
        .await
        .is_err());
    ensure!(matches!(
        s.cells[p]
            .command(
                s.f.definition.increment.clone(),
                1i64.to_be_bytes().to_vec(),
                public_node::options(9)?
            )
            .await,
        Err(CommandFailure::Unavailable)
    ));
    let q = (p + 1) % 3;
    if primary_target {
        // This is an explicit fixture transfer through the existing control
        // API. The runtime candidate itself still owns no handoff or readiness.
        let state = s.cells[p]
            .inspect()
            .await?
            .member
            .context("primary control progress")?;
        s.cells[p]
            .transfer_leadership(
                LeadershipTransfer {
                    expected_term: state.term,
                    expected_revision: state.membership_revision,
                    target: MemberId::new(q as u64 + 1)?,
                },
                TIMEOUT,
            )
            .await?;
        s.f.domain
            .run(cellar::asyncrt::timeout(TIMEOUT, async {
                loop {
                    if s.cells[q]
                        .status()
                        .await
                        .is_ok_and(|state| state.command_ready)
                    {
                        break;
                    }
                    cellar::asyncrt::sleep(Duration::from_millis(5)).await;
                }
            }))
            .await??;
    }
    write_read(
        &s.f,
        &[s.cells[q].clone(), s.cells[(p + 2) % 3].clone()],
        2,
        5,
        12,
    )
    .await
    .context("survivor SQL after explicit fixture handoff")?;
    s.close().await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn fleet_restart_runtime_verifies_primary_and_both_survivors() -> Result<()> {
    verified(true).await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn fleet_restart_runtime_verifies_follower_and_both_survivors() -> Result<()> {
    verified(false).await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn fleet_restart_runtime_missing_second_survivor_keeps_history_pending() -> Result<()> {
    let mut s = Box::pin(Setup::new()).await?;
    let leader = primary(&s.f, &s.cells).await?;
    let p = (leader + 1) % 3;
    let (_, mut walk) = prepare(&s, p).await?;
    let pending = walk.next_history(TIMEOUT).await?.context("history")?;
    let victim = s.nodes.remove((p + 1) % 3);
    stopped(victim.node).await?;
    ensure!(walk.verify_next(TIMEOUT).await.is_err());
    ensure!(walk.next_history(TIMEOUT).await?.as_ref() == Some(&pending));
    ensure!(matches!(
        walk.finish(TIMEOUT).await,
        Err(FleetCatalogError::InventoryIncomplete)
    ));
    // The two remaining voters can still serve writes: quorum alone was not
    // accepted as proof that both post-stop survivors are ready.
    write_read(
        &s.f,
        &[s.cells[leader].clone(), s.cells[p].clone()],
        2,
        5,
        12,
    )
    .await?;
    s.close().await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn fleet_restart_runtime_root_churn_invalidates_walk() -> Result<()> {
    let s = Box::pin(Setup::new()).await?;
    let p = primary(&s.f, &s.cells).await?;
    let (_, mut walk) = prepare(&s, p).await?;
    s.declare_extra(&s.f.catalog).await?;
    ensure!(matches!(
        walk.verify_next(TIMEOUT).await,
        Err(FleetCatalogError::Conflict)
    ));
    ensure!(matches!(
        walk.finish(TIMEOUT).await,
        Err(FleetCatalogError::Conflict)
    ));
    s.close().await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn fleet_restart_runtime_unmaterialized_genesis_is_never_absent() -> Result<()> {
    let s = Box::pin(Setup::new()).await?;
    let mut proposal = s.proposal.clone();
    proposal.scope.group = GroupId::new([185; 16])?;
    proposal.scope.cell = "Counter:restart-unmaterialized".into();
    let root = s.f.catalog.read(TIMEOUT).await?;
    let reserved =
        s.f.catalog
            .reserve_birth(
                &root,
                &request(185, root.revision()),
                &s.namespace,
                proposal.clone(),
                TIMEOUT,
            )
            .await?;
    s.f.domain
        .run(
            s.namespace
                .resolve_managed_birth(&reserved.authorization, TIMEOUT),
        )
        .await??;
    let root = s.f.catalog.read(TIMEOUT).await?;
    s.f.catalog
        .confirm_birth(
            &root,
            &request(187, root.revision()),
            &s.namespace,
            &reserved.authorization,
            TIMEOUT,
        )
        .await?;
    let p = primary(&s.f, &s.cells).await?;
    let (_, mut walk) = prepare(&s, p).await?;
    loop {
        let history = walk
            .next_history(TIMEOUT)
            .await?
            .context("missing genesis skipped")?;
        if history.binding().scope()? == proposal.scope {
            ensure!(matches!(
                walk.verify_next(TIMEOUT).await,
                Err(FleetCatalogError::InventoryIncomplete)
            ));
            ensure!(walk.next_history(TIMEOUT).await?.as_ref() == Some(&history));
            break;
        }
        ensure!(walk.verify_next(TIMEOUT).await?.is_some());
    }
    ensure!(walk.finish(TIMEOUT).await.is_err());
    s.close().await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn fleet_restart_runtime_empty_target_walks_unrelated_histories() -> Result<()> {
    let s = Box::pin(Setup::new()).await?;
    let boot = s
        .spare
        .observe_node(TIMEOUT)
        .await?
        .fleet_boot
        .context("empty boot")?;
    let node = NodeId::new(boot.node)?;
    let root = s.f.catalog.read(TIMEOUT).await?;
    let preparing = FleetRestartRequest {
        identity: request(210, root.revision()),
        node,
        expected_process: ProcessGeneration::new(boot.process)?,
        grace_seconds: 120,
    };
    let committed =
        s.f.catalog
            .begin_restart_maintenance(
                &root,
                &preparing,
                std::slice::from_ref(&s.namespace),
                TIMEOUT,
            )
            .await?;
    let mut identity = request(212, committed.snapshot.revision());
    identity.operation = preparing.identity.operation;
    let gate = FleetRestartGateRequest {
        identity,
        node,
        expected_process: preparing.expected_process,
        gate: ProposalId::new([213; 16])?,
    };
    s.f.catalog
        .admit_restart_gate(&committed.snapshot, &gate, TIMEOUT)
        .await?;
    s.f.remote(
        &s.nodes[0].host,
        node,
        s.f.catalog.read(TIMEOUT).await?.revision(),
    )
    .await?;
    let root = s.f.catalog.read(TIMEOUT).await?;
    let mut walk =
        s.f.catalog
            .prepare_restart_verification(
                &root,
                ProposalId::new(preparing.identity.operation)?,
                std::slice::from_ref(&s.namespace),
                &s.nodes[0].host,
                TIMEOUT,
            )
            .await?;
    ensure!(matches!(
        walk.verify_next(TIMEOUT).await?,
        Some(FleetRestartHistoryProgress::Unrelated { .. })
    ));
    let candidate = walk.finish(TIMEOUT).await?;
    ensure!(
        candidate.histories() == 1
            && candidate.physical_histories() == 0
            && candidate.voting_histories() == 0
            && candidate.retained_histories() == 0
    );
    s.close().await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn fleet_restart_runtime_synced_fence_defeats_candidate_with_unchanged_root() -> Result<()> {
    let s = Box::pin(Setup::new()).await?;
    let p = primary(&s.f, &s.cells).await?;
    let (gate, mut walk) = prepare(&s, p).await?;
    ensure!(walk.verify_next(TIMEOUT).await?.is_some());
    let root = s.f.catalog.read(TIMEOUT).await?;
    let intent = cancel(&gate, root.revision(), 220)?;
    s.f.store.fleet_cas_hang.store(true, Ordering::Release);
    s.f.store.hold_next_fleet_cas.store(true, Ordering::Release);
    let catalog = s.f.catalog.clone();
    let cut = root.clone();
    let request = intent.clone();
    let observer = s.spare.clone();
    let pending = s.f.domain.spawn(async move {
        catalog
            .cancel_restart_gate(&cut, &request, Some(&observer), TIMEOUT)
            .await
    })?;
    // The local fence is synced before the held root CAS. Its timeout leaves
    // Root RestartPending, so a root-only verifier would miss cancellation.
    s.waiting().await?;
    ensure!(matches!(
        pending.await?,
        Err(FleetCatalogError::Unconfirmed)
    ));
    s.f.store.fleet_cas_hang.store(false, Ordering::Release);
    s.f.store.fleet_cas_resume.notify_waiters();
    ensure!(s.f.catalog.read(TIMEOUT).await?.revision() == root.revision());
    ensure!(walk.finish(TIMEOUT).await.is_err());
    s.f.catalog
        .cancel_restart_gate(&root, &intent, Some(&s.spare), TIMEOUT)
        .await?;
    s.close().await
}
