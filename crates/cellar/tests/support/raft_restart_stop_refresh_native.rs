// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Refreshed stop consent keeps the physical selector and signed closure stable.
use super::*;
use cellar::application::consensus::verification::{
    FleetRestartStopRefreshRequest, FleetRestartVerification,
};

fn identity(id: u8, revision: u64, operation: [u8; 16]) -> FleetRequestIdentity {
    let mut identity = request_identity(id, revision, operation);
    identity.id[15] = 238;
    identity
}

async fn churn(s: &Setup, catalog: &FleetCatalog, id: u8) -> Result<()> {
    let root = catalog.read(TIMEOUT).await?;
    let request = identity(id, root.revision(), [id; 16]);
    catalog
        .commit(
            &root,
            &FleetMutation {
                id: request.id,
                operation: request.operation,
                principal: request.principal,
                expected_revision: root.revision(),
                changes: vec![FleetChange {
                    table: FleetTable::Policy,
                    key: "stop-refresh-input".into(),
                    value: Some(vec![id]),
                }],
            },
            TIMEOUT,
        )
        .await?;
    trust(s).await
}

async fn fresh(
    s: &Setup,
    old: &FleetRestartStopRequest,
    id: u8,
) -> Result<FleetRestartStopRefreshRequest> {
    trust(s).await?;
    let root = s.f.catalog.read(TIMEOUT).await?;
    let mut walk: FleetRestartVerification =
        s.f.catalog
            .prepare_restart_verification(
                &root,
                ProposalId::new(old.identity.operation)?,
                std::slice::from_ref(&s.namespace),
                &s.spare,
                TIMEOUT,
            )
            .await?;
    consume(s, &mut walk, &s.proposal.scope).await?;
    let ready = FleetRestartReadyRequest {
        identity: identity(id, root.revision(), old.identity.operation),
        node: old.node,
        expected_process: old.expected_process,
    };
    walk.publish_ready(&ready, TIMEOUT).await?;
    let root = s.f.catalog.read(TIMEOUT).await?;
    let mut next = old.clone();
    next.identity = identity(id + 1, root.revision(), old.identity.operation);
    next.ready = ready.identity;
    Ok(FleetRestartStopRefreshRequest {
        previous: old.clone(),
        request: next,
    })
}

async fn admitted(
    s: &Setup,
    p: usize,
) -> Result<(FleetRestartGateRequest, FleetRestartStopRequest)> {
    let (gate, stop, root) = consent(s, p).await?;
    s.f.catalog
        .admit_restart_stop(&root, &stop, TIMEOUT)
        .await?;
    Ok((gate, stop))
}

async fn complete_return(
    s: &mut Setup,
    p: usize,
    mut target: Running,
    offline: FleetRequestIdentity,
) -> Result<()> {
    let root = s.f.catalog.read(TIMEOUT).await?;
    let predecessor =
        s.f.catalog
            .restart_maintenance_admission(&root, ProposalId::new(offline.operation)?, TIMEOUT)
            .await?
            .boot()?;
    let returning = FleetRestartReturnRequest {
        identity: identity(180, root.revision(), offline.operation),
        offline,
        node: target.spec.node,
        expected_process: ProcessGeneration::new(predecessor.process)?,
        next_process: ProcessGeneration::new([184; 16])?,
    };
    s.f.catalog
        .admit_restart_return(&root, &returning, TIMEOUT)
        .await?;
    let node = public_node::node_named(
        target.root.path(),
        s.f.definition.application.clone(),
        &target.spec.label,
    )
    .await?;
    let (config, services) = configuration(s, &target, &node, &returning).await?;
    let host = node.start_raft_cells(config, services).await?;
    s.cells[p] = host.cell(s.proposal.scope.namespace, &s.proposal.scope.cell)?;
    target.node = node;
    target.host = host;
    s.nodes.insert(p, target);
    trust(s).await?;
    let mut attestation =
        crate::hosts::capacity_tests::attestation(&s.f, &s.spare, &s.nodes[p].host).await?;
    let mut profile = s.nodes[p].host.observe_node(TIMEOUT).await?.node.profile;
    ensure!(profile.draining);
    profile.draining = false;
    attestation.profile_digest = fleet_capacity_profile_digest(&profile)?;
    let root = s.f.catalog.read(TIMEOUT).await?;
    let resume = FleetRestartResumeRequest {
        identity: identity(190, root.revision(), returning.identity.operation),
        returning: returning.identity,
        attestation,
        resume: ProposalId::new([192; 16])?,
    };
    let consent = consent_resume(s, &resume).await?;
    trust(s).await?;
    s.f.catalog
        .resume_restart_maintenance(&consent, &s.spare, TIMEOUT)
        .await?;
    let mut walk = prepare_resume(s, &resume).await?;
    ensure!(walk.phase() == FleetRestartResumePhase::Resumed);
    consume_resume(s, &mut walk).await?;
    let root = s.f.catalog.read(TIMEOUT).await?;
    let completion = FleetRestartResumeCompleteRequest {
        identity: identity(194, root.revision(), resume.identity.operation),
        resume: resume.identity,
    };
    walk.complete_resume(&completion, TIMEOUT).await?;
    let root = s.f.catalog.read(TIMEOUT).await?;
    ensure!(
        s.f.catalog
            .authorize_host(&root, s.nodes[p].spec.node, TIMEOUT)
            .await?
            .state()
            == FleetNodeState::Active
    );
    s.f.catalog
        .authorize_capacity(&root, s.nodes[p].spec.node, TIMEOUT)
        .await?;
    ensure!(s
        .f
        .catalog
        .get(&root, FleetTable::Policy, "active-disruption", TIMEOUT)
        .await?
        .is_none());
    write_read(&s.f, &s.cells, 3, 2, 14).await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn fleet_restart_stop_refresh_lost_confirmation_preserves_closure_through_resume(
) -> Result<()> {
    let mut s = Box::pin(Setup::new()).await?;
    let p = (primary(&s.f, &s.cells).await? + 1) % 3;
    let (_, old) = admitted(&s, p).await?;
    let root = s.f.catalog.read(TIMEOUT).await?;
    let original =
        s.f.catalog
            .restart_stop_consent(&root, &old, TIMEOUT)
            .await?
            .context("original consent")?
            .stop_request()?;
    churn(&s, &s.f.catalog, 150).await?;
    let root = s.f.catalog.read(TIMEOUT).await?;
    ensure!(s
        .f
        .catalog
        .stop_restart_maintenance(&root, &old, &s.spare, TIMEOUT)
        .await
        .is_err());
    ensure!(!s.nodes[p].node.is_closed());
    let refresh = fresh(&s, &old, 160).await?;
    let root = s.f.catalog.read(TIMEOUT).await?;
    s.f.store
        .fail_fleet_confirmation
        .store(true, Ordering::Release);
    ensure!(s
        .f
        .catalog
        .refresh_restart_stop(&root, &refresh, TIMEOUT)
        .await
        .is_err());
    ensure!(!s.f.store.fail_fleet_confirmation.load(Ordering::Acquire));
    let catalog = Fixture::open_catalog(&s.f.domain, s.f.store.clone())?;
    let root = catalog.read(TIMEOUT).await?;
    let recovered = catalog
        .refresh_restart_stop(&root, &refresh, TIMEOUT)
        .await?;
    ensure!(serde_json::to_value(recovered.stop_request()?)? == serde_json::to_value(&original)?);
    ensure!(
        catalog
            .restart_stop_consent(&root, &refresh.request, TIMEOUT)
            .await?
            .context("refreshed read")?
            .applied_revision()
            == recovered.applied_revision()
    );
    ensure!(!s.nodes[p].node.is_closed());
    let survivors = s
        .cells
        .iter()
        .enumerate()
        .filter(|(i, _)| *i != p)
        .map(|(_, c)| c.clone())
        .collect::<Vec<_>>();
    write_read(&s.f, &survivors, 2, 5, 12).await?;
    trust(&s).await?;
    // A queued original selector now consumes the current refreshed consent.
    let result = catalog
        .stop_restart_maintenance(&root, &old, &s.spare, TIMEOUT)
        .await;
    ensure!(
        result.is_ok()
            || matches!(
                result,
                Err(FleetCatalogError::Closed | FleetCatalogError::Unconfirmed)
            )
    );
    let mut target = s.nodes.remove(p);
    joined(&mut target.node, &s.f).await?;
    let a = catalog.restart_stop_closure(&old, TIMEOUT).await?;
    let b = catalog
        .restart_stop_closure(&refresh.request, TIMEOUT)
        .await?;
    ensure!(a.digest() == b.digest());
    let root = catalog.read(TIMEOUT).await?;
    let offline = FleetRestartOfflineRequest {
        identity: identity(170, root.revision(), old.identity.operation),
        stop: refresh.request.clone(),
    };
    catalog
        .confirm_restart_offline(&root, &offline, TIMEOUT)
        .await?;
    let root = catalog.read(TIMEOUT).await?;
    ensure!(
        catalog
            .refresh_restart_stop(&root, &refresh, TIMEOUT)
            .await?
            .applied_revision()
            == recovered.applied_revision()
    );
    Box::pin(complete_return(&mut s, p, target, offline.identity)).await?;
    s.close().await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn fleet_restart_stop_refresh_independent_writers_race_one_projection() -> Result<()> {
    let s = Box::pin(Setup::new()).await?;
    let p = (primary(&s.f, &s.cells).await? + 1) % 3;
    let (_, old) = admitted(&s, p).await?;
    let a = fresh(&s, &old, 160).await?;
    let mut b = a.clone();
    b.request.identity.id[0] = 162;
    let second = Fixture::open_catalog(&s.f.domain, s.f.store.clone())?;
    let ra = s.f.catalog.read(TIMEOUT).await?;
    let rb = second.read(TIMEOUT).await?;
    let (a, b) = tokio::join!(
        s.f.catalog.refresh_restart_stop(&ra, &a, TIMEOUT),
        second.refresh_restart_stop(&rb, &b, TIMEOUT)
    );
    ensure!(a.is_ok() != b.is_ok());
    let root = s.f.catalog.read(TIMEOUT).await?;
    ensure!(root.revision() == ra.revision() + 1);
    ensure!(!s.nodes[p].node.is_closed());
    s.close().await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn fleet_restart_stop_refresh_rejects_rebound_scope_and_changed_replay_predecessor(
) -> Result<()> {
    let s = Box::pin(Setup::new()).await?;
    let p = (primary(&s.f, &s.cells).await? + 1) % 3;
    let (_, old) = admitted(&s, p).await?;
    let refresh = fresh(&s, &old, 160).await?;
    let root = s.f.catalog.read(TIMEOUT).await?;
    for n in 0..4 {
        let mut bad = refresh.clone();
        match n {
            0 => bad.request.stop = ProposalId::new([165; 16])?,
            1 => bad.request.expected_process = ProcessGeneration::new([165; 16])?,
            2 => bad.request.node = NodeId::new([165; 16])?,
            _ => bad.previous.identity.principal.push_str("-forged"),
        }
        ensure!(matches!(
            s.f.catalog.refresh_restart_stop(&root, &bad, TIMEOUT).await,
            Err(FleetCatalogError::PayloadConflict)
        ));
    }
    ensure!(s.f.catalog.read(TIMEOUT).await?.revision() == root.revision());
    s.f.catalog
        .refresh_restart_stop(&root, &refresh, TIMEOUT)
        .await?;
    let root = s.f.catalog.read(TIMEOUT).await?;
    let mut bad = refresh.clone();
    bad.previous.ready.principal.push_str("-forged");
    ensure!(matches!(
        s.f.catalog.refresh_restart_stop(&root, &bad, TIMEOUT).await,
        Err(FleetCatalogError::PayloadConflict)
    ));
    ensure!(!s.nodes[p].node.is_closed());
    s.close().await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn fleet_restart_stop_refresh_requires_current_ready_inputs() -> Result<()> {
    let s = Box::pin(Setup::new()).await?;
    let p = (primary(&s.f, &s.cells).await? + 1) % 3;
    let (_, old) = admitted(&s, p).await?;
    let mut refresh = fresh(&s, &old, 160).await?;
    churn(&s, &s.f.catalog, 150).await?;
    let root = s.f.catalog.read(TIMEOUT).await?;
    refresh.request.identity.expected_revision = root.revision();
    ensure!(matches!(
        s.f.catalog
            .refresh_restart_stop(&root, &refresh, TIMEOUT)
            .await,
        Err(FleetCatalogError::Conflict)
    ));
    ensure!(s.f.catalog.read(TIMEOUT).await?.revision() == root.revision());
    ensure!(s
        .f
        .catalog
        .restart_stop_consent(&root, &refresh.request, TIMEOUT)
        .await?
        .is_none());
    ensure!(!s.nodes[p].node.is_closed());
    s.close().await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn fleet_restart_stop_refresh_retains_selector_across_multiple_generations_and_fence(
) -> Result<()> {
    let s = Box::pin(Setup::new()).await?;
    let p = (primary(&s.f, &s.cells).await? + 1) % 3;
    let (gate, old) = admitted(&s, p).await?;
    let first = fresh(&s, &old, 160).await?;
    let root = s.f.catalog.read(TIMEOUT).await?;
    let a =
        s.f.catalog
            .refresh_restart_stop(&root, &first, TIMEOUT)
            .await?;
    let next = fresh(&s, &first.request, 162).await?;
    let root = s.f.catalog.read(TIMEOUT).await?;
    let b =
        s.f.catalog
            .refresh_restart_stop(&root, &next, TIMEOUT)
            .await?;
    ensure!(serde_json::to_value(a.stop_request()?)? == serde_json::to_value(b.stop_request()?)?);
    let root = s.f.catalog.read(TIMEOUT).await?;
    let stale = FleetRestartStopRefreshRequest {
        previous: old,
        request: next.request.clone(),
    };
    ensure!(matches!(
        s.f.catalog
            .refresh_restart_stop(&root, &stale, TIMEOUT)
            .await,
        Err(FleetCatalogError::PayloadConflict)
    ));
    trust(&s).await?;
    let root = s.f.catalog.read(TIMEOUT).await?;
    s.f.catalog
        .cancel_restart_gate(
            &root,
            &cancel(&gate, root.revision(), 246)?,
            Some(&s.spare),
            TIMEOUT,
        )
        .await?;
    let root = s.f.catalog.read(TIMEOUT).await?;
    ensure!(
        s.f.catalog
            .refresh_restart_stop(&root, &next, TIMEOUT)
            .await?
            .applied_revision()
            == b.applied_revision()
    );
    trust(&s).await?;
    ensure!(s
        .f
        .catalog
        .stop_restart_maintenance(&root, &next.request, &s.spare, TIMEOUT)
        .await
        .is_err());
    ensure!(!s.nodes[p].node.is_closed());
    s.close().await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn fleet_restart_stop_refresh_orphan_original_consent_grants_no_authority() -> Result<()> {
    let s = Box::pin(Setup::new()).await?;
    let p = (primary(&s.f, &s.cells).await? + 1) % 3;
    let (_, old, root) = consent(&s, p).await?;
    let independent = Fixture::open_catalog(&s.f.domain, s.f.store.clone())?;
    let cut = independent.read(TIMEOUT).await?;
    ensure!(cut.revision() == root.revision());
    s.f.store.fleet_cas_hang.store(true, Ordering::Release);
    s.f.store.hold_next_fleet_cas.store(true, Ordering::Release);
    let pending = Box::pin(independent.admit_restart_stop(&cut, &old, TIMEOUT));
    let pending = match futures_util::future::select(Box::pin(s.waiting()), pending).await {
        futures_util::future::Either::Left((ready, pending)) => {
            ready?;
            pending
        }
        futures_util::future::Either::Right(_) => anyhow::bail!("consent finished before held CAS"),
    };
    churn(&s, &s.f.catalog, 150).await?;
    s.f.store.fleet_cas_hang.store(false, Ordering::Release);
    s.f.store.fleet_cas_resume.notify_waiters();
    ensure!(matches!(pending.await, Err(FleetCatalogError::Conflict)));
    let refresh = fresh(&s, &old, 160).await?;
    let root = s.f.catalog.read(TIMEOUT).await?;
    ensure!(s
        .f
        .catalog
        .restart_stop_consent(&root, &old, TIMEOUT)
        .await?
        .is_none());
    ensure!(matches!(
        s.f.catalog
            .refresh_restart_stop(&root, &refresh, TIMEOUT)
            .await,
        Err(FleetCatalogError::Conflict)
    ));
    ensure!(s.f.catalog.read(TIMEOUT).await?.revision() == root.revision());
    ensure!(!s.nodes[p].node.is_closed());
    s.close().await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn fleet_restart_stop_refresh_held_cas_preserves_already_joined_closure() -> Result<()> {
    let mut s = Box::pin(Setup::new()).await?;
    let p = (primary(&s.f, &s.cells).await? + 1) % 3;
    let (_, old) = admitted(&s, p).await?;
    // Operations-only Ready publication preserves the original live basis.
    let refresh = fresh(&s, &old, 160).await?;
    trust(&s).await?;
    let independent = Fixture::open_catalog(&s.f.domain, s.f.store.clone())?;
    let root = independent.read(TIMEOUT).await?;
    s.f.store.fleet_cas_hang.store(true, Ordering::Release);
    s.f.store.hold_next_fleet_cas.store(true, Ordering::Release);
    let pending = Box::pin(independent.refresh_restart_stop(&root, &refresh, TIMEOUT));
    let pending = match futures_util::future::select(Box::pin(s.waiting()), pending).await {
        futures_util::future::Either::Left((ready, pending)) => {
            ready?;
            pending
        }
        futures_util::future::Either::Right(_) => {
            anyhow::bail!("refresh completed before held CAS")
        }
    };
    let local = s.f.catalog.read(TIMEOUT).await?;
    ensure!(local.revision() == root.revision());
    let result =
        s.f.catalog
            .stop_restart_maintenance(&local, &old, &s.spare, TIMEOUT)
            .await;
    ensure!(
        result.is_ok()
            || matches!(
                result,
                Err(FleetCatalogError::Closed | FleetCatalogError::Unconfirmed)
            ),
        "stop before refreshed CAS: {:?}",
        result.as_ref().err().map(ToString::to_string)
    );
    let mut target = s.nodes.remove(p);
    joined(&mut target.node, &s.f).await?;
    let original = s.f.catalog.restart_stop_closure(&old, TIMEOUT).await?;
    s.f.store.fleet_cas_hang.store(false, Ordering::Release);
    s.f.store.fleet_cas_resume.notify_waiters();
    let refreshed = pending.await?;
    ensure!(refreshed.applied_revision() == root.revision() + 1);
    let proof =
        s.f.catalog
            .restart_stop_closure(&refresh.request, TIMEOUT)
            .await?;
    ensure!(proof.digest() == original.digest());
    let root = s.f.catalog.read(TIMEOUT).await?;
    let offline = FleetRestartOfflineRequest {
        identity: identity(170, root.revision(), old.identity.operation),
        stop: refresh.request,
    };
    ensure!(
        s.f.catalog
            .confirm_restart_offline(&root, &offline, TIMEOUT)
            .await?
            .closure_digest()
            == proof.digest()
    );
    drop(target);
    s.close().await
}
