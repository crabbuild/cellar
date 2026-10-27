// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Actual remote genesis, bounded reconciliation and controller loss.
use super::*;
use cellar::application::consensus::verification::FleetRestartRuntimeProgress;
use cellar::application::consensus::InspectionError;

async fn missing(s: &Setup) -> Result<GroupScope> {
    let mut proposal = s.proposal.clone();
    proposal.scope.group = GroupId::new([180; 16])?;
    proposal.scope.cell = "Counter:restart-genesis".into();
    let root = s.f.catalog.read(TIMEOUT).await?;
    let reserved =
        s.f.catalog
            .reserve_birth(
                &root,
                &request(180, root.revision()),
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
            &request(182, root.revision()),
            &s.namespace,
            &reserved.authorization,
            TIMEOUT,
        )
        .await?;
    Ok(proposal.scope)
}

async fn focus(walk: &mut FleetRestartVerification, scope: &GroupScope) -> Result<()> {
    loop {
        let history = walk
            .next_history(TIMEOUT)
            .await?
            .context("missing pending genesis")?;
        if history.binding().scope()? == *scope {
            return Ok(());
        }
        ensure!(walk.verify_next(TIMEOUT).await?.is_some());
    }
}

async fn converge(
    s: &Setup,
    walk: &mut FleetRestartVerification,
    scope: &GroupScope,
) -> Result<Vec<(NodeId, MemberId, bool)>> {
    s.f.domain
        .run(cellar::asyncrt::timeout(TIMEOUT, async {
            let mut actions = vec![];
            loop {
                match walk.reconcile_next(TIMEOUT).await {
                    Ok(Some(FleetRestartRuntimeProgress::Materialized {
                        group,
                        node,
                        member,
                        previously_present,
                    })) => {
                        ensure!(group == *scope);
                        actions.push((node, member, previously_present));
                    }
                    Ok(Some(FleetRestartRuntimeProgress::Verified(
                        FleetRestartHistoryProgress::Voting { group, .. },
                    ))) => {
                        ensure!(group == *scope);
                        return Ok::<_, anyhow::Error>(actions);
                    }
                    Err(
                        FleetCatalogError::Unconfirmed
                        | FleetCatalogError::Overloaded
                        | FleetCatalogError::InventoryIncomplete,
                    )
                    | Err(FleetCatalogError::Observation(
                        InspectionError::Unavailable | InspectionError::Overloaded,
                    )) => {
                        cellar::asyncrt::sleep(Duration::from_millis(10)).await;
                    }
                    Err(error) => return Err(error.into()),
                    Ok(_) => anyhow::bail!("genesis skipped its pending history"),
                }
            }
        }))
        .await??
}

async fn finish_and_write(
    s: &Setup,
    mut walk: FleetRestartVerification,
    scope: &GroupScope,
    target: usize,
) -> Result<()> {
    while walk.next_history(TIMEOUT).await?.is_some() {
        ensure!(walk.verify_next(TIMEOUT).await?.is_some());
    }
    let candidate = walk.finish(TIMEOUT).await?;
    ensure!(
        candidate.histories() == 2
            && candidate.physical_histories() == 2
            && candidate.voting_histories() == 2
    );
    let root = s.f.catalog.read(TIMEOUT).await?;
    ensure!(root.revision() == candidate.inventory_revision());
    let cells = s
        .nodes
        .iter()
        .map(|n| n.host.cell(scope.namespace, &scope.cell))
        .collect::<std::result::Result<Vec<_>, _>>()?;
    let p = primary(&s.f, &cells).await?;
    if p == target {
        // Fixture-only transfer; owned maintenance handoff remains M4 work.
        let state = cells[p].inspect().await?.member.context("genesis leader")?;
        let q = (p + 1) % 3;
        cells[p]
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
                while !cells[q]
                    .status()
                    .await
                    .is_ok_and(|state| state.command_ready)
                {
                    cellar::asyncrt::sleep(Duration::from_millis(5)).await;
                }
            }))
            .await??;
    }
    let serving = cells
        .iter()
        .enumerate()
        .filter(|(i, _)| *i != target)
        .map(|(_, c)| c.clone())
        .collect::<Vec<_>>();
    write_read(&s.f, &serving, 1, 9, 9).await?;
    let original = s
        .cells
        .iter()
        .enumerate()
        .filter(|(i, _)| *i != target)
        .map(|(_, c)| c.clone())
        .collect::<Vec<_>>();
    write_read(&s.f, &original, 2, 5, 12).await?;
    for node in &s.nodes {
        let current =
            s.f.remote(&s.spare, node.spec.node, root.revision())
                .await?;
        ensure!(current.view.node.groups == 2 && current.view.unattached_engine_histories == 0);
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn fleet_restart_genesis_materializes_all_voters_and_preserves_sql() -> Result<()> {
    let s = Box::pin(Setup::new()).await?;
    let scope = missing(&s).await?;
    let target = (primary(&s.f, &s.cells).await? + 1) % 3;
    let (_, mut walk) = prepare(&s, target).await?;
    focus(&mut walk, &scope).await?;
    ensure!(matches!(
        walk.verify_next(TIMEOUT).await,
        Err(FleetCatalogError::InventoryIncomplete)
    ));
    let actions = converge(&s, &mut walk, &scope).await?;
    ensure!(actions.len() == 3 && actions.iter().all(|(_, _, present)| !present));
    let nodes = actions
        .iter()
        .map(|(n, _, _)| *n)
        .collect::<std::collections::BTreeSet<_>>();
    ensure!(nodes == s.nodes.iter().map(|n| n.spec.node).collect());
    finish_and_write(&s, walk, &scope, target).await?;
    s.close().await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn fleet_restart_genesis_reopens_after_interrupted_claim() -> Result<()> {
    let s = Box::pin(Setup::new()).await?;
    let scope = missing(&s).await?;
    let target = (primary(&s.f, &s.cells).await? + 1) % 3;
    let (gate, mut walk) = prepare(&s, target).await?;
    focus(&mut walk, &scope).await?;
    s.f.store.claim_hang.store(true, Ordering::Release);
    s.f.store.hold_nth_claim.store(1, Ordering::Release);
    let pending =
        s.f.domain
            .spawn(async move { walk.reconcile_next(TIMEOUT).await })?;
    s.f.domain
        .run(cellar::asyncrt::timeout(
            TIMEOUT,
            s.f.store.claim_waiting.notified(),
        ))
        .await??;
    let during = s.nodes[target].host.observe_node(TIMEOUT).await?;
    ensure!(
        during.node.groups == 2
            && during.unattached_engine_histories == 1
            && during.dormant_histories == 1
    );
    pending.abort();
    ensure!(pending.await.is_err());
    s.f.store.claim_hang.store(false, Ordering::Release);
    s.f.store.claim_resume.notify_waiters();
    s.f.domain
        .run(cellar::asyncrt::timeout(TIMEOUT, async {
            loop {
                let observed = s.nodes[target]
                    .host
                    .inspect_peer_physical_inventory(
                        s.nodes[target].spec.node,
                        s.f.catalog.read(TIMEOUT).await?.revision(),
                        TIMEOUT,
                    )
                    .await?;
                if observed
                    .inventory
                    .as_ref()
                    .context("inventory")?
                    .initializing()
                    .is_none()
                    && observed.view.pending_history_registrations == 0
                    && observed.view.seed_workers == 0
                {
                    return Ok::<_, anyhow::Error>(());
                }
                cellar::asyncrt::sleep(Duration::from_millis(10)).await;
            }
        }))
        .await???;
    let recreated = Fixture::open_catalog(&s.f.domain, s.f.store.clone())?;
    let root = recreated.read(TIMEOUT).await?;
    let mut walk = recreated
        .prepare_restart_verification(
            &root,
            ProposalId::new(gate.identity.operation)?,
            std::slice::from_ref(&s.namespace),
            &s.spare,
            TIMEOUT,
        )
        .await?;
    focus(&mut walk, &scope).await?;
    converge(&s, &mut walk, &scope).await?;
    finish_and_write(&s, walk, &scope, target).await?;
    s.close().await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn fleet_restart_genesis_changed_basis_refuses_physical_work() -> Result<()> {
    let s = Box::pin(Setup::new()).await?;
    let scope = missing(&s).await?;
    let target = primary(&s.f, &s.cells).await?;
    let (_, mut walk) = prepare(&s, target).await?;
    focus(&mut walk, &scope).await?;
    s.declare_extra(&s.f.catalog).await?;
    ensure!(matches!(
        walk.reconcile_next(TIMEOUT).await,
        Err(FleetCatalogError::Conflict)
    ));
    for node in &s.nodes {
        ensure!(node.host.observe_node(TIMEOUT).await?.node.groups == 1);
    }
    s.close().await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn fleet_restart_genesis_synced_fence_prevents_dispatch() -> Result<()> {
    let s = Box::pin(Setup::new()).await?;
    let scope = missing(&s).await?;
    let target = primary(&s.f, &s.cells).await?;
    let (gate, mut walk) = prepare(&s, target).await?;
    focus(&mut walk, &scope).await?;
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
    s.waiting().await?;
    ensure!(matches!(
        pending.await?,
        Err(FleetCatalogError::Unconfirmed)
    ));
    s.f.store.fleet_cas_hang.store(false, Ordering::Release);
    s.f.store.fleet_cas_resume.notify_waiters();
    ensure!(s.f.catalog.read(TIMEOUT).await?.revision() == root.revision());
    ensure!(walk.reconcile_next(TIMEOUT).await.is_err());
    for node in &s.nodes {
        ensure!(node.host.observe_node(TIMEOUT).await?.node.groups == 1);
    }
    s.f.catalog
        .cancel_restart_gate(&root, &intent, Some(&s.spare), TIMEOUT)
        .await?;
    s.close().await
}
