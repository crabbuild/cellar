// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Durable operator intent and recreated preparation controllers on real hosts.
#[path = "raft_restart_operator_cancel_native.rs"]
mod cancellation_cases;
use super::*;
use cellar::application::consensus::verification::{
    FleetRestartBirthResolution, FleetRestartOperatorDriver, FleetRestartOperatorPhase,
    FleetRestartOperatorRequest,
};
use cellar::application::consensus::InspectionError;

async fn intent(s: &Setup, p: usize, id: u8) -> Result<FleetRestartOperatorRequest> {
    let boot = s.nodes[p]
        .host
        .observe_node(TIMEOUT)
        .await?
        .fleet_boot
        .context("operator target boot")?;
    let root = s.f.catalog.read(TIMEOUT).await?;
    Ok(FleetRestartOperatorRequest {
        identity: request(id, root.revision()),
        node: s.nodes[p].spec.node,
        expected_process: ProcessGeneration::new(boot.process)?,
        grace_seconds: 120,
        birth_resolution: FleetRestartBirthResolution::Cancel,
    })
}

fn driver(s: &Setup) -> Result<FleetRestartOperatorDriver> {
    Ok(FleetRestartOperatorDriver::new(
        s.f.catalog.clone(),
        vec![s.namespace.clone()],
        s.spare.clone(),
        None,
    )?)
}

async fn routes(s: &Setup) -> Result<()> {
    discover(
        &s.f,
        &[
            &s.nodes[0].host,
            &s.nodes[1].host,
            &s.nodes[2].host,
            &s.spare,
        ],
    )
    .await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn fleet_restart_operator_intent_is_durable_without_gate_or_slot_authority() -> Result<()> {
    let s = Box::pin(Setup::new()).await?;
    let p = primary(&s.f, &s.cells).await?;
    let request = intent(&s, p, 188).await?;
    let root = s.f.catalog.read(TIMEOUT).await?;
    s.f.store
        .fail_fleet_confirmation
        .store(true, Ordering::Release);
    ensure!(s
        .f
        .catalog
        .begin_restart_operator(&root, &request, std::slice::from_ref(&s.namespace), TIMEOUT)
        .await
        .is_err());
    ensure!(!s.f.store.fail_fleet_confirmation.load(Ordering::Acquire));
    let catalog = Fixture::open_catalog(&s.f.domain, s.f.store.clone())?;
    let root = catalog.read(TIMEOUT).await?;
    let admitted = catalog
        .begin_restart_operator(&root, &request, &[], TIMEOUT)
        .await?;
    ensure!(
        admitted.identity() == &request.identity
            && admitted.applied_revision() == request.identity.expected_revision + 1
            && admitted.birth_resolution() == request.birth_resolution
    );
    let active = catalog
        .active_restart_operator(&root, TIMEOUT)
        .await?
        .context("active operator intent")?;
    ensure!(active.identity() == admitted.identity() && active.boot() == admitted.boot());
    ensure!(
        catalog
            .restart_operator(&root, ProposalId::new(request.identity.operation)?, TIMEOUT)
            .await?
            .context("operation lookup")?
            .identity()
            == admitted.identity()
    );
    ensure!(
        !s.nodes[p]
            .host
            .observe_node(TIMEOUT)
            .await?
            .node
            .profile
            .draining
    );
    ensure!(
        catalog
            .authorize_host(&root, request.node, TIMEOUT)
            .await?
            .state()
            == FleetNodeState::Active
    );
    ensure!(catalog
        .get(&root, FleetTable::Policy, "active-disruption", TIMEOUT)
        .await?
        .is_none());
    write_read(&s.f, &s.cells, 2, 5, 12).await?;
    let mut conflicting = request.clone();
    conflicting.birth_resolution = FleetRestartBirthResolution::Finish;
    ensure!(matches!(
        catalog
            .begin_restart_operator(&root, &conflicting, &[], TIMEOUT)
            .await,
        Err(FleetCatalogError::PayloadConflict)
    ));
    ensure!(catalog.read(TIMEOUT).await?.revision() == root.revision());
    s.close().await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn fleet_restart_operator_independent_intents_race_one_controller_slot() -> Result<()> {
    let s = Box::pin(Setup::new()).await?;
    let a = intent(&s, 0, 188).await?;
    let b = intent(&s, 1, 190).await?;
    let second = Fixture::open_catalog(&s.f.domain, s.f.store.clone())?;
    let ra = s.f.catalog.read(TIMEOUT).await?;
    let rb = second.read(TIMEOUT).await?;
    let namespaces = std::slice::from_ref(&s.namespace);
    let (a, b) = tokio::join!(
        s.f.catalog
            .begin_restart_operator(&ra, &a, namespaces, TIMEOUT),
        second.begin_restart_operator(&rb, &b, namespaces, TIMEOUT)
    );
    ensure!(a.is_ok() != b.is_ok());
    let root = s.f.catalog.read(TIMEOUT).await?;
    ensure!(
        root.revision() == ra.revision() + 1
            && s.f
                .catalog
                .active_restart_operator(&root, TIMEOUT)
                .await?
                .is_some()
    );
    for node in &s.nodes {
        ensure!(!node.host.observe_node(TIMEOUT).await?.node.profile.draining);
    }
    s.close().await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn fleet_restart_operator_generic_intent_and_cursor_projection_refuse() -> Result<()> {
    let s = Box::pin(Setup::new()).await?;
    let request = intent(&s, 0, 188).await?;
    let root = s.f.catalog.read(TIMEOUT).await?;
    s.f.catalog
        .begin_restart_operator(&root, &request, std::slice::from_ref(&s.namespace), TIMEOUT)
        .await?;
    let root = s.f.catalog.read(TIMEOUT).await?;
    for (n, key) in [
        format!(
            "restart-controller/{}",
            hex::encode(request.identity.operation)
        ),
        "restart-controller-active".into(),
        format!(
            "restart-controller-attempt/{}",
            hex::encode(request.identity.operation)
        ),
        format!(
            "restart-controller-cancel/{}",
            hex::encode(request.identity.operation)
        ),
        format!(
            "restart-controller-cancelled/{}",
            hex::encode(request.identity.operation)
        ),
    ]
    .into_iter()
    .enumerate()
    {
        let mutation = FleetMutation {
            id: [194 + n as u8; 16],
            operation: request.identity.operation,
            principal: "forged-operator".into(),
            expected_revision: root.revision(),
            changes: vec![FleetChange {
                table: FleetTable::Operations,
                key,
                value: Some(vec![1]),
            }],
        };
        ensure!(matches!(
            s.f.catalog.commit(&root, &mutation, TIMEOUT).await,
            Err(FleetCatalogError::Invalid("restart operator authority"))
        ));
    }
    ensure!(s.f.catalog.read(TIMEOUT).await?.revision() == root.revision());
    s.close().await
}

async fn preparation(recreate: bool, concurrent: bool, churn: bool) -> Result<()> {
    let s = Box::pin(Setup::new()).await?;
    let p = primary(&s.f, &s.cells).await?;
    let request = intent(&s, p, 188).await?;
    let root = s.f.catalog.read(TIMEOUT).await?;
    s.f.catalog
        .begin_restart_operator(&root, &request, std::slice::from_ref(&s.namespace), TIMEOUT)
        .await?;
    let mut controller = driver(&s)?;
    let other = if concurrent { Some(driver(&s)?) } else { None };
    let mut recreated = false;
    let mut abandoned = None;
    if churn {
        ensure!(controller.reconcile(TIMEOUT).await?.is_some());
        let root = s.f.catalog.read(TIMEOUT).await?;
        let bytes =
            s.f.catalog
                .get(
                    &root,
                    FleetTable::Operations,
                    &format!(
                        "restart-controller-attempt/{}",
                        hex::encode(request.identity.operation)
                    ),
                    TIMEOUT,
                )
                .await?
                .context("rooted preparation attempt")?;
        ensure!(bytes.len() > 45);
        let value: serde_json::Value = serde_json::from_slice(&bytes[13..bytes.len() - 32])?;
        let old: FleetRequestIdentity = serde_json::from_value(value["request"].clone())?;
        abandoned = Some(old.operation);
        s.f.store.fleet_cas_hang.store(true, Ordering::Release);
        s.f.store.hold_next_fleet_cas.store(true, Ordering::Release);
        let pending = Box::pin(controller.reconcile(TIMEOUT));
        let pending = match futures_util::future::select(Box::pin(s.waiting()), pending).await {
            futures_util::future::Either::Left((ready, pending)) => {
                ready?;
                pending
            }
            futures_util::future::Either::Right((result, _)) => {
                anyhow::bail!(
                    "preparation finished before held CAS: {}",
                    match result {
                        Ok(_) => "unexpected success".into(),
                        Err(e) => e.to_string(),
                    }
                );
            }
        };
        let independent = Fixture::open_catalog(&s.f.domain, s.f.store.clone())?;
        let root = independent.read(TIMEOUT).await?;
        independent
            .commit(
                &root,
                &FleetMutation {
                    id: [192; 16],
                    operation: [193; 16],
                    principal: "independent-root-writer".into(),
                    expected_revision: root.revision(),
                    changes: vec![FleetChange {
                        table: FleetTable::Operations,
                        key: "operator-root-churn".into(),
                        value: Some(vec![1]),
                    }],
                },
                TIMEOUT,
            )
            .await?;
        s.f.store.fleet_cas_hang.store(false, Ordering::Release);
        s.f.store.fleet_cas_resume.notify_waiters();
        ensure!(matches!(pending.await, Err(FleetCatalogError::Conflict)));
    }
    let ready = s
        .f
        .domain
        .run(cellar::asyncrt::timeout(TIMEOUT, async {
            loop {
                routes(&s).await?;
                let responses = if let Some(other) = other.as_ref() {
                    let (a, b) =
                        tokio::join!(controller.reconcile(TIMEOUT), other.reconcile(TIMEOUT));
                    vec![a, b]
                } else {
                    vec![controller.reconcile(TIMEOUT).await]
                };
                for response in responses {
                    match response {
                        Ok(Some(progress)) => {
                            ensure!(
                                progress.admission.identity() == &request.identity
                                    && progress.checked_histories <= progress.total_histories
                                    && progress.blocked.is_none()
                            );
                            if let Some(ready) = progress.ready {
                                ensure!(progress.phase == FleetRestartOperatorPhase::Ready);
                                return Ok::<_, anyhow::Error>(ready);
                            }
                            if recreate
                                && !recreated
                                && progress.phase == FleetRestartOperatorPhase::Verifying
                                && progress.checked_histories == 1
                            {
                                controller = driver(&s)?;
                                recreated = true;
                            }
                        }
                        Err(FleetCatalogError::Conflict | FleetCatalogError::PayloadConflict)
                            if concurrent => {}
                        Err(FleetCatalogError::Unconfirmed | FleetCatalogError::Overloaded)
                        | Err(FleetCatalogError::Observation(
                            InspectionError::Unavailable | InspectionError::Overloaded,
                        )) => {}
                        other => {
                            let root = s.f.catalog.read(TIMEOUT).await?;
                            let cursor =
                                s.f.catalog
                                    .get(
                                        &root,
                                        FleetTable::Operations,
                                        &format!(
                                            "restart-controller-attempt/{}",
                                            hex::encode(request.identity.operation)
                                        ),
                                        TIMEOUT,
                                    )
                                    .await?;
                            anyhow::bail!(
                                "operator preparation at revision {} ({:?}): {}",
                                root.revision(),
                                cursor
                                    .as_deref()
                                    .map(|b| String::from_utf8_lossy(&b[13..b.len() - 32])),
                                match other {
                                    Err(e) => e.to_string(),
                                    _ => "active operation missing".into(),
                                }
                            );
                        }
                    }
                }
                cellar::asyncrt::sleep(Duration::from_millis(5)).await;
            }
        }))
        .await???;
    ensure!(!recreate || recreated);
    ensure!(abandoned.is_none_or(|old| ready.identity().operation != old));
    ensure!(
        ready.boot().node == request.node.bytes()
            && ready.candidate().histories() == 1
            && ready.candidate().matched_histories() == 1
    );
    let root = s.f.catalog.read(TIMEOUT).await?;
    ensure!(
        s.f.catalog
            .authorize_host(&root, request.node, TIMEOUT)
            .await?
            .state()
            == FleetNodeState::RestartPending
    );
    ensure!(
        s.nodes[p]
            .host
            .observe_node(TIMEOUT)
            .await?
            .node
            .profile
            .draining
            && !s.nodes[p].node.is_closed()
    );
    ensure!(s
        .f
        .catalog
        .authorize_capacity(&root, request.node, TIMEOUT)
        .await
        .is_err());
    let survivors = s
        .cells
        .iter()
        .enumerate()
        .filter(|(i, _)| *i != p)
        .map(|(_, c)| c.clone())
        .collect::<Vec<_>>();
    write_read(&s.f, &survivors, 2, 5, 12).await?;
    s.close().await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn fleet_restart_operator_preparation_owns_gate_settlement_and_handoff() -> Result<()> {
    Box::pin(preparation(false, false, false)).await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn fleet_restart_operator_recreated_controller_repeats_live_proof() -> Result<()> {
    Box::pin(preparation(true, false, false)).await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn fleet_restart_operator_two_controllers_preserve_step_predecessors() -> Result<()> {
    Box::pin(preparation(false, true, false)).await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn fleet_restart_operator_fences_unadmitted_preparation_after_root_cas_loss() -> Result<()> {
    Box::pin(preparation(false, false, true)).await
}
