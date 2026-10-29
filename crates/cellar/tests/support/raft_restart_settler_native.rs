// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Durable owned birth decisions hold cancellation across ambiguous replies.
use super::*;
use cellar::application::consensus::verification::{
    BirthDisposition, FleetHistoryState, FleetRestartBirthResolution,
    FleetRestartSettlementProgress,
};

async fn birth(s: &Setup, id: u8, live: bool) -> Result<GroupScope> {
    let mut proposal = s.proposal.clone();
    proposal.scope.group = GroupId::new([id; 16])?;
    proposal.scope.cell = format!("Counter:owned-restart-{id}");
    let root = s.f.catalog.read(TIMEOUT).await?;
    let pending =
        s.f.catalog
            .reserve_birth(
                &root,
                &request(id, root.revision()),
                &s.namespace,
                proposal.clone(),
                TIMEOUT,
            )
            .await?;
    if live {
        s.f.domain
            .run(
                s.namespace
                    .resolve_managed_birth(&pending.authorization, TIMEOUT),
            )
            .await??;
    }
    Ok(proposal.scope)
}

async fn admit_birth_gate(s: &Setup, p: usize) -> Result<FleetRestartGateRequest> {
    let boot = s.nodes[p]
        .host
        .observe_node(TIMEOUT)
        .await?
        .fleet_boot
        .context("gate boot")?;
    let root = s.f.catalog.read(TIMEOUT).await?;
    let preparing = FleetRestartRequest {
        identity: request(210, root.revision()),
        node: s.nodes[p].spec.node,
        expected_process: ProcessGeneration::new(boot.process)?,
        grace_seconds: 120,
    };
    let prepared =
        s.f.catalog
            .begin_restart_maintenance(
                &root,
                &preparing,
                std::slice::from_ref(&s.namespace),
                TIMEOUT,
            )
            .await?;
    let mut identity = request(212, prepared.snapshot.revision());
    identity.operation = preparing.identity.operation;
    let gate = FleetRestartGateRequest {
        identity,
        node: preparing.node,
        expected_process: preparing.expected_process,
        gate: ProposalId::new([213; 16])?,
    };
    s.f.catalog
        .admit_restart_gate(&prepared.snapshot, &gate, TIMEOUT)
        .await?;
    let root = s.f.catalog.read(TIMEOUT).await?;
    let work =
        s.f.catalog
            .pending_restart_work(
                &root,
                ProposalId::new(gate.identity.operation)?,
                std::slice::from_ref(&s.namespace),
                TIMEOUT,
            )
            .await?;
    ensure!(work.pending_births > 0 && work.next_birth.is_some() && work.active_birth.is_none());
    ensure!(work.movements.is_empty() && work.reservations.is_empty());
    s.f.remote(&s.spare, gate.node, root.revision()).await?;
    Ok(gate)
}

fn cancel(gate: &FleetRestartGateRequest, revision: u64) -> Result<FleetRestartGateCancelRequest> {
    let mut identity = request(230, revision);
    identity.operation = gate.identity.operation;
    Ok(FleetRestartGateCancelRequest {
        identity,
        node: gate.node,
        expected_process: gate.expected_process,
        fence: ProposalId::new([231; 16])?,
    })
}

async fn pending(
    s: &Setup,
    catalog: &FleetCatalog,
    operation: ProposalId,
) -> Result<(
    cellar::application::consensus::verification::FleetSnapshot,
    cellar::application::consensus::verification::FleetRestartBirthAction,
)> {
    let root = catalog.read(TIMEOUT).await?;
    let work = catalog
        .pending_restart_work(
            &root,
            operation,
            std::slice::from_ref(&s.namespace),
            TIMEOUT,
        )
        .await?;
    Ok((
        root,
        work.active_birth.context("owned birth remains pending")?,
    ))
}

async fn refuses_cancel(
    s: &Setup,
    catalog: &FleetCatalog,
    gate: &FleetRestartGateRequest,
    root: &cellar::application::consensus::verification::FleetSnapshot,
    admission: [u8; 16],
) -> Result<()> {
    ensure!(
        matches!(catalog.cancel_restart_gate(root,&cancel(gate,root.revision())?,None,TIMEOUT).await,Err(FleetCatalogError::SettlementInProgress {admission:actual}) if actual==admission)
    );
    for key in [
        format!(
            "restart-birth-active/{}",
            hex::encode(gate.identity.operation)
        ),
        format!("restart-birth/{}", hex::encode(admission)),
    ] {
        let mutation = FleetMutation {
            id: [232; 16],
            operation: gate.identity.operation,
            expected_revision: root.revision(),
            principal: "forge-settlement-release".into(),
            changes: vec![FleetChange {
                table: FleetTable::Operations,
                key,
                value: None,
            }],
        };
        ensure!(matches!(
            catalog.commit(root, &mutation, TIMEOUT).await,
            Err(FleetCatalogError::Invalid("restart settlement authority"))
        ));
    }
    ensure!(catalog.read(TIMEOUT).await?.revision() == root.revision());
    ensure!(!s.nodes.iter().any(|n| n.node.is_closed()));
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn fleet_restart_settler_recovers_admission_and_terminal_confirmation() -> Result<()> {
    let s = Box::pin(Setup::new()).await?;
    let live = birth(&s, 220, true).await?;
    let cancelled = birth(&s, 221, false).await?;
    let gate = admit_birth_gate(&s, primary(&s.f, &s.cells).await?).await?;
    let operation = ProposalId::new(gate.identity.operation)?;
    s.f.store
        .fail_fleet_confirmation
        .store(true, Ordering::Release);
    ensure!(s
        .f
        .catalog
        .reconcile_restart_work(
            operation,
            FleetRestartBirthResolution::Cancel,
            std::slice::from_ref(&s.namespace),
            None,
            None,
            TIMEOUT
        )
        .await
        .is_err());
    ensure!(!s.f.store.fail_fleet_confirmation.load(Ordering::Acquire));
    s.f.store.fail_fleet_read.store(false, Ordering::Release);
    let reopened = Fixture::open_catalog(&s.f.domain, s.f.store.clone())?;
    let (root, active) = pending(&s, &reopened, operation).await?;
    refuses_cancel(&s, &reopened, &gate, &root, active.admission).await?;
    ensure!(matches!(
        reopened
            .reconcile_restart_work(
                operation,
                FleetRestartBirthResolution::Finish,
                std::slice::from_ref(&s.namespace),
                None,
                None,
                TIMEOUT
            )
            .await,
        Err(FleetCatalogError::PayloadConflict)
    ));
    ensure!(matches!(
        reopened
            .restart_birth_settlement(&root, ProposalId::new(active.admission)?, TIMEOUT)
            .await,
        Err(FleetCatalogError::SettlementInProgress { .. })
    ));
    s.f.store
        .fail_fleet_confirmation
        .store(true, Ordering::Release);
    ensure!(reopened
        .reconcile_restart_work(
            operation,
            FleetRestartBirthResolution::Cancel,
            std::slice::from_ref(&s.namespace),
            None,
            None,
            TIMEOUT
        )
        .await
        .is_err());
    ensure!(!s.f.store.fail_fleet_confirmation.load(Ordering::Acquire));
    s.f.store.fail_fleet_read.store(false, Ordering::Release);
    let now = reopened.read(TIMEOUT).await?;
    let receipt = reopened
        .restart_birth_settlement(&now, ProposalId::new(active.admission)?, TIMEOUT)
        .await?;
    let FleetRestartSettlementProgress::Birth {
        admission,
        group,
        state,
        applied_revision,
        ..
    } = &receipt
    else {
        anyhow::bail!("expected birth receipt");
    };
    ensure!(
        *admission == active.admission
            && *group == active.group
            && *applied_revision == root.revision() + 1
    );
    ensure!(
        *state
            == if *group == live {
                FleetHistoryState::Live
            } else {
                FleetHistoryState::Cancelled {
                    operation: active.admission,
                }
            }
    );
    let work = reopened
        .pending_restart_work(&now, operation, std::slice::from_ref(&s.namespace), TIMEOUT)
        .await?;
    ensure!(work.active_birth.is_none() && work.pending_births == 1);
    s.f.store
        .fail_fleet_confirmation
        .store(true, Ordering::Release);
    ensure!(reopened
        .reconcile_restart_work(
            operation,
            FleetRestartBirthResolution::Cancel,
            std::slice::from_ref(&s.namespace),
            None,
            None,
            TIMEOUT
        )
        .await
        .is_err());
    s.f.store.fail_fleet_read.store(false, Ordering::Release);
    let (later_root, later_action) = pending(&s, &reopened, operation).await?;
    ensure!(later_action.admission != active.admission);
    let historical = reopened
        .restart_birth_settlement(&later_root, ProposalId::new(active.admission)?, TIMEOUT)
        .await?;
    ensure!(
        matches!(historical,FleetRestartSettlementProgress::Birth {admission,applied_revision:revision,..} if admission==active.admission&&revision==*applied_revision)
    );
    let (_, still_active) = pending(&s, &reopened, operation).await?;
    ensure!(still_active == later_action);
    refuses_cancel(&s, &reopened, &gate, &later_root, later_action.admission).await?;
    let second = reopened
        .reconcile_restart_work(
            operation,
            FleetRestartBirthResolution::Cancel,
            std::slice::from_ref(&s.namespace),
            None,
            None,
            TIMEOUT,
        )
        .await?;
    ensure!(
        matches!(second,FleetRestartSettlementProgress::Birth {group:ref scope,..} if scope!=group)
    );
    ensure!(matches!(
        reopened
            .reconcile_restart_work(
                operation,
                FleetRestartBirthResolution::Cancel,
                std::slice::from_ref(&s.namespace),
                None,
                None,
                TIMEOUT
            )
            .await?,
        FleetRestartSettlementProgress::CatalogClear { .. }
    ));
    let winner =
        s.f.domain
            .run(s.namespace.birth_disposition(&cancelled.cell))
            .await??;
    ensure!(matches!(winner,Some(BirthDisposition::Cancelled {scope,..}) if scope==cancelled));
    let now = reopened.read(TIMEOUT).await?;
    s.f.remote(&s.spare, gate.node, now.revision()).await?;
    let ended = reopened
        .cancel_restart_gate(
            &now,
            &cancel(&gate, now.revision())?,
            Some(&s.spare),
            TIMEOUT,
        )
        .await?;
    let retry = reopened
        .restart_birth_settlement(&ended.snapshot, ProposalId::new(active.admission)?, TIMEOUT)
        .await?;
    ensure!(
        matches!(retry,FleetRestartSettlementProgress::Birth {admission,applied_revision:revision,..} if admission==active.admission&&revision==*applied_revision)
    );
    ensure!(reopened.read(TIMEOUT).await?.revision() == ended.snapshot.revision());
    s.close().await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn fleet_restart_settler_holds_cancellation_after_lost_namespace_winner() -> Result<()> {
    let s = Box::pin(Setup::new()).await?;
    let scope = birth(&s, 220, false).await?;
    let gate = admit_birth_gate(&s, primary(&s.f, &s.cells).await?).await?;
    let operation = ProposalId::new(gate.identity.operation)?;
    s.f.store
        .fail_birth_confirmation
        .store(true, Ordering::Release);
    ensure!(s
        .f
        .catalog
        .reconcile_restart_work(
            operation,
            FleetRestartBirthResolution::Cancel,
            std::slice::from_ref(&s.namespace),
            None,
            None,
            TIMEOUT
        )
        .await
        .is_err());
    ensure!(!s.f.store.fail_birth_confirmation.load(Ordering::Acquire));
    // The real conditional write won. Only its confirmation was lost; the
    // controller must retain its active claim while the root is still Reserved.
    s.f.store.fail_birth_read.store(false, Ordering::Release);
    let (root, active) = pending(&s, &s.f.catalog, operation).await?;
    ensure!(active.group == scope);
    refuses_cancel(&s, &s.f.catalog, &gate, &root, active.admission).await?;
    let winner =
        s.f.domain
            .run(s.namespace.birth_disposition(&scope.cell))
            .await??;
    ensure!(
        matches!(winner,Some(BirthDisposition::Cancelled {operation:actual,..}) if actual.bytes()==active.admission)
    );
    let reopened = Fixture::open_catalog(&s.f.domain, s.f.store.clone())?;
    ensure!(
        matches!(reopened.reconcile_restart_work(operation, FleetRestartBirthResolution::Cancel,std::slice::from_ref(&s.namespace),None,None,TIMEOUT).await?,FleetRestartSettlementProgress::Birth {admission,..} if admission==active.admission)
    );
    let root = reopened.read(TIMEOUT).await?;
    let work = reopened
        .pending_restart_work(
            &root,
            operation,
            std::slice::from_ref(&s.namespace),
            TIMEOUT,
        )
        .await?;
    ensure!(work.active_birth.is_none() && work.pending_births == 0);
    s.close().await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn fleet_restart_settler_rebases_terminal_after_root_conflict() -> Result<()> {
    let s = Box::pin(Setup::new()).await?;
    birth(&s, 220, false).await?;
    let gate = admit_birth_gate(&s, primary(&s.f, &s.cells).await?).await?;
    let operation = ProposalId::new(gate.identity.operation)?;
    s.f.store
        .fail_birth_confirmation
        .store(true, Ordering::Release);
    ensure!(s
        .f
        .catalog
        .reconcile_restart_work(
            operation,
            FleetRestartBirthResolution::Cancel,
            std::slice::from_ref(&s.namespace),
            None,
            None,
            TIMEOUT
        )
        .await
        .is_err());
    s.f.store.fail_birth_read.store(false, Ordering::Release);
    let (root, active) = pending(&s, &s.f.catalog, operation).await?;
    s.f.store.hold_next_fleet_cas.store(true, Ordering::Release);
    s.f.store.fleet_cas_hang.store(true, Ordering::Release);
    let catalog = s.f.catalog.clone();
    let namespaces = vec![s.namespace.clone()];
    let finishing = s.f.domain.spawn(async move {
        catalog
            .reconcile_restart_work(
                operation,
                FleetRestartBirthResolution::Cancel,
                &namespaces,
                None,
                None,
                TIMEOUT,
            )
            .await
    })?;
    s.waiting().await?;
    let independent = Fixture::open_catalog(&s.f.domain, s.f.store.clone())?;
    let race_root = independent.read(TIMEOUT).await?;
    ensure!(race_root.revision() == root.revision());
    let changed = independent
        .commit(
            &race_root,
            &FleetMutation {
                id: [233; 16],
                operation: gate.identity.operation,
                expected_revision: root.revision(),
                principal: "settler-conflict-test".into(),
                changes: vec![FleetChange {
                    table: FleetTable::Policy,
                    key: "restart-settler-test-churn".into(),
                    value: Some(vec![1]),
                }],
            },
            TIMEOUT,
        )
        .await?;
    s.f.store.fleet_cas_hang.store(false, Ordering::Release);
    s.f.store.fleet_cas_resume.notify_waiters();
    ensure!(matches!(finishing.await?, Err(FleetCatalogError::Conflict)));
    let (now, still_active) = pending(&s, &independent, operation).await?;
    ensure!(now.revision() == changed.snapshot.revision() && active == still_active);
    refuses_cancel(&s, &independent, &gate, &now, active.admission).await?;
    let progress = independent
        .reconcile_restart_work(
            operation,
            FleetRestartBirthResolution::Cancel,
            std::slice::from_ref(&s.namespace),
            None,
            None,
            TIMEOUT,
        )
        .await?;
    ensure!(
        matches!(progress,FleetRestartSettlementProgress::Birth {admission,applied_revision,..} if admission==active.admission&&applied_revision==now.revision()+1)
    );
    s.close().await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn fleet_restart_settler_finishes_reserved_birth_without_name_cancellation() -> Result<()> {
    let s = Box::pin(Setup::new()).await?;
    let scope = birth(&s, 220, false).await?;
    let gate = admit_birth_gate(&s, primary(&s.f, &s.cells).await?).await?;
    let operation = ProposalId::new(gate.identity.operation)?;
    s.f.store
        .fail_birth_confirmation
        .store(true, Ordering::Release);
    ensure!(s
        .f
        .catalog
        .reconcile_restart_work(
            operation,
            FleetRestartBirthResolution::Finish,
            std::slice::from_ref(&s.namespace),
            None,
            None,
            TIMEOUT
        )
        .await
        .is_err());
    ensure!(!s.f.store.fail_birth_confirmation.load(Ordering::Acquire));
    s.f.store.fail_birth_read.store(false, Ordering::Release);
    let reopened = Fixture::open_catalog(&s.f.domain, s.f.store.clone())?;
    let (root, active) = pending(&s, &reopened, operation).await?;
    ensure!(active.resolution == FleetRestartBirthResolution::Finish && active.group == scope);
    ensure!(matches!(
        reopened
            .reconcile_restart_work(
                operation,
                FleetRestartBirthResolution::Cancel,
                std::slice::from_ref(&s.namespace),
                None,
                None,
                TIMEOUT
            )
            .await,
        Err(FleetCatalogError::PayloadConflict)
    ));
    refuses_cancel(&s, &reopened, &gate, &root, active.admission).await?;
    let result = reopened
        .reconcile_restart_work(
            operation,
            FleetRestartBirthResolution::Finish,
            std::slice::from_ref(&s.namespace),
            None,
            None,
            TIMEOUT,
        )
        .await?;
    ensure!(
        matches!(result,FleetRestartSettlementProgress::Birth {admission,state:FleetHistoryState::Live,..} if admission==active.admission)
    );
    let winner =
        s.f.domain
            .run(s.namespace.birth_disposition(&scope.cell))
            .await??;
    ensure!(matches!(winner,Some(BirthDisposition::Live(birth)) if birth.scope()==&scope));
    let root = reopened.read(TIMEOUT).await?;
    let work = reopened
        .pending_restart_work(
            &root,
            operation,
            std::slice::from_ref(&s.namespace),
            TIMEOUT,
        )
        .await?;
    ensure!(work.active_birth.is_none() && work.pending_births == 0);
    s.close().await
}
