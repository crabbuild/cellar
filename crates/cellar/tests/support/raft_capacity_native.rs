// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Real empty managed hosts and TLS; resource evidence is explicitly a trusted
//! fixture deployment attestation, not measured kernel/container qualification.
use super::*;
use cellar::application::consensus::verification::NamespaceCatalog;

pub(super) async fn namespace(f: &Fixture) -> Result<NamespaceCatalog> {
    Ok(Catalog::new(
        f.store.clone(),
        CatalogOptions {
            prefix: "host-enrollment/".into(),
            ..Default::default()
        },
    )?
    .existing_namespace(f.declaration.namespace.clone())
    .await?)
}

pub(super) async fn attestation(
    f: &Fixture,
    origin: &RaftCells,
    target: &RaftCells,
) -> Result<FleetCapacityAttestation> {
    let revision = f.catalog.read(TIMEOUT).await?.revision();
    let local = target.observe_node(TIMEOUT).await?;
    let remote = f.remote(origin, local.node.identity.node, revision).await?;
    ensure!(
        remote.view.fleet_boot == local.fleet_boot
            && remote.view.startup_bounds == local.startup_bounds
    );
    Ok(FleetCapacityAttestation {
        boot: local.fleet_boot.unwrap(),
        profile_revision: 1,
        profile_digest: fleet_capacity_profile_digest(&local.node.profile)?,
        cpu_millis: 1000,
        memory_bytes: 3 * 1024 * 1024 * 1024,
        durable_bytes: local.node.profile.byte_capacity.max(1024 * 1024 * 1024),
        engine_slots: 4096,
        memory_swap_disabled: true,
        used_memory_bytes: 64 * 1024 * 1024,
        used_durable_bytes: 1024 * 1024,
        evidence_digest: [71; 32],
        evidence_reference: "fixture-only/deployment-attestation".into(),
    })
}

fn request(
    id: u8,
    revision: u64,
    attestation: FleetCapacityAttestation,
    action: FleetCapacityAction,
) -> FleetCapacityRequest {
    FleetCapacityRequest {
        identity: Fixture::request(id, revision),
        attestation,
        action,
    }
}

pub(super) async fn joined(node: cellar::sdk::Node) -> Result<()> {
    let NodeOutcome::Stopped(report) = node.shutdown(ShutdownMode::Preserve).await? else {
        anyhow::bail!("capacity fixture did not stop");
    };
    let raft = report.raft.unwrap();
    ensure!(!raft.startup_incomplete && raft.result?.unfinished.is_empty());
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn fleet_capacity_local_node_qualify_and_activate() -> Result<()> {
    let f = Fixture::with_managed_births(true).await?;
    let (_root, node, host) = f.empty_host().await?;
    let evidence = attestation(&f, &host, &host).await?;
    let boot = evidence.boot.clone();
    for (id, action, state) in [
        (101, FleetCapacityAction::Qualify, FleetNodeState::Ready),
        (102, FleetCapacityAction::Activate, FleetNodeState::Active),
    ] {
        let cut = f.catalog.read(TIMEOUT).await?;
        f.remote(&host, f.spec.node, cut.revision()).await?;
        let accepted = f
            .catalog
            .admit_node_capacity(
                &cut,
                &request(id, cut.revision(), evidence.clone(), action),
                &host,
                &[namespace(&f).await?],
                TIMEOUT,
            )
            .await?;
        ensure!(accepted.state() == state);
    }
    let cut = f.catalog.read(TIMEOUT).await?;
    let capacity = f
        .catalog
        .authorize_capacity(&cut, f.spec.node, TIMEOUT)
        .await?;
    ensure!(capacity.receive_replicas() && capacity.attestation().boot == boot);
    ensure!(host.observe_node(TIMEOUT).await?.registered_histories == 0);
    joined(node).await?;
    ensure!(host.resource_usage().control.bytes == 0);
    f.domain.shutdown().await;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn fleet_capacity_empty_node_qualify_and_activate() -> Result<()> {
    let f = Fixture::with_managed_births(true).await?;
    let (_origin_root, origin_node, origin) = f.empty_host().await?;
    let (_target_root, target_node, target) = f.peer_host(5).await?;
    let evidence = attestation(&f, &origin, &target).await?;
    let cut = f.catalog.read(TIMEOUT).await?;
    let ready = request(
        101,
        cut.revision(),
        evidence.clone(),
        FleetCapacityAction::Qualify,
    );
    let accepted = f
        .catalog
        .admit_node_capacity(&cut, &ready, &origin, &[namespace(&f).await?], TIMEOUT)
        .await?;
    ensure!(
        accepted.state() == FleetNodeState::Ready
            && accepted.applied_revision() == cut.revision() + 1
    );
    let cut = f.catalog.read(TIMEOUT).await?;
    let node = NodeId::new([5; 16])?;
    ensure!(!f
        .catalog
        .authorize_capacity(&cut, node, TIMEOUT)
        .await?
        .receive_replicas());
    ensure!(
        !f.catalog
            .authorize_host(&cut, node, TIMEOUT)
            .await?
            .eligibility()
            .receive_replicas
    );
    let mut conflicting = evidence.clone();
    conflicting.cpu_millis += 1;
    let conflicting = request(
        103,
        cut.revision(),
        conflicting,
        FleetCapacityAction::Activate,
    );
    ensure!(matches!(
        f.catalog
            .admit_node_capacity(
                &cut,
                &conflicting,
                &origin,
                &[namespace(&f).await?],
                TIMEOUT
            )
            .await,
        Err(FleetCatalogError::PayloadConflict)
    ));
    f.remote(&origin, node, cut.revision()).await?;
    let active = request(
        102,
        cut.revision(),
        evidence.clone(),
        FleetCapacityAction::Activate,
    );
    f.catalog
        .admit_node_capacity(&cut, &active, &origin, &[namespace(&f).await?], TIMEOUT)
        .await?;
    let cut = f.catalog.read(TIMEOUT).await?;
    let capacity = f.catalog.authorize_capacity(&cut, node, TIMEOUT).await?;
    ensure!(capacity.receive_replicas() && capacity.attestation() == &evidence);
    ensure!(
        f.catalog
            .authorize_host(&cut, node, TIMEOUT)
            .await?
            .eligibility()
            .receive_replicas
    );
    let view = f.remote(&origin, node, cut.revision()).await?;
    ensure!(
        view.view.node.groups == 0
            && view.view.registered_histories == 0
            && view.view.startup_bounds.bounded_buffer_bytes > 0
    );
    ensure!(origin.observe_node(TIMEOUT).await?.registered_histories == 0);
    let mut changed_limits = evidence.clone();
    changed_limits.profile_revision += 1;
    changed_limits.cpu_millis += 1;
    let blocked = request(
        104,
        cut.revision(),
        changed_limits,
        FleetCapacityAction::Activate,
    );
    ensure!(matches!(
        f.catalog
            .admit_node_capacity(&cut, &blocked, &origin, &[namespace(&f).await?], TIMEOUT)
            .await,
        Err(FleetCatalogError::Conflict)
    ));
    joined(target_node).await?;
    joined(origin_node).await?;
    f.domain.shutdown().await;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn fleet_capacity_lost_reply_reconciles_after_reopen() -> Result<()> {
    let f = Fixture::with_managed_births(true).await?;
    let (_origin_root, origin_node, origin) = f.empty_host().await?;
    let (_target_root, target_node, target) = f.peer_host(5).await?;
    let evidence = attestation(&f, &origin, &target).await?;
    let cut = f.catalog.read(TIMEOUT).await?;
    let active = request(101, cut.revision(), evidence, FleetCapacityAction::Activate);
    f.store
        .fail_fleet_confirmation
        .store(true, Ordering::Release);
    ensure!(f
        .catalog
        .admit_node_capacity(&cut, &active, &origin, &[namespace(&f).await?], TIMEOUT)
        .await
        .is_err());
    let applied = cut.revision() + 1;
    ensure!(f.catalog.read(TIMEOUT).await?.revision() == applied);
    joined(target_node).await?;
    f.state(NodeId::new([5; 16])?, FleetNodeState::Draining, 102)
        .await?;
    let reopened = Fixture::open_catalog(&f.domain, f.store.clone())?;
    let cut = reopened.read(TIMEOUT).await?;
    let historical = reopened
        .admit_node_capacity(&cut, &active, &origin, &[], TIMEOUT)
        .await?;
    ensure!(
        historical.applied_revision() == applied
            && historical.observed_revision() == cut.revision()
    );
    ensure!(matches!(
        reopened
            .authorize_capacity(&cut, NodeId::new([5; 16])?, TIMEOUT)
            .await,
        Err(FleetCatalogError::Conflict)
    ));
    let mut changed = active;
    changed.attestation.cpu_millis += 1;
    ensure!(matches!(
        reopened
            .admit_node_capacity(&cut, &changed, &origin, &[], TIMEOUT)
            .await,
        Err(FleetCatalogError::PayloadConflict)
    ));
    joined(origin_node).await?;
    f.domain.shutdown().await;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn fleet_capacity_reboot_invalidates_authorization() -> Result<()> {
    let f = Fixture::with_managed_births(true).await?;
    let (_origin_root, origin_node, origin) = f.empty_host().await?;
    let (target_root, target_node, target) = f.peer_host(5).await?;
    let evidence = attestation(&f, &origin, &target).await?;
    let old_boot = evidence.boot.clone();
    let cut = f.catalog.read(TIMEOUT).await?;
    let active = request(101, cut.revision(), evidence, FleetCapacityAction::Activate);
    f.catalog
        .admit_node_capacity(&cut, &active, &origin, &[namespace(&f).await?], TIMEOUT)
        .await?;
    joined(target_node).await?;
    // The physical boot fence selects a fresh nonce before endpoint
    // publication. The directory still refuses reuse of a process generation.
    // Neither this attempted boot nor a later successful one may inherit capacity.
    let cut = f.catalog.read(TIMEOUT).await?;
    let spec = f.peer_spec(5)?;
    let authorization = f.catalog.authorize_host(&cut, spec.node, TIMEOUT).await?;
    let restarted_node = public_node::node_named(
        target_root.path(),
        f.definition.application.clone(),
        &spec.label,
    )
    .await?;
    let (mut config, services) = f.config_for(&restarted_node, authorization, &spec).await?;
    config.infrastructure.log.mode = LogOpenMode::Resume;
    config.infrastructure.log.process = ProcessGeneration::new(old_boot.process)?;
    config.peers[0].process = ProcessGeneration::new(old_boot.process)?;
    let refused = restarted_node.start_raft_cells(config, services).await;
    ensure!(refused.as_ref().err().is_some_and(|error| error
        .to_string()
        .contains("superseded or reused node process")));
    let NodeOutcome::Stopped(report) = restarted_node.shutdown(ShutdownMode::Preserve).await?
    else {
        anyhow::bail!("failed capacity boot did not join");
    };
    // A normal refused startup joins its infrastructure and resets the empty
    // lifecycle slot. Only cancellation/poisoning leaves an incomplete slot.
    ensure!(report.raft.is_none());
    let cut = f.catalog.read(TIMEOUT).await?;
    let bytes = f
        .catalog
        .get(
            &cut,
            FleetTable::Nodes,
            &hex::encode(spec.node.bytes()),
            TIMEOUT,
        )
        .await?
        .unwrap();
    let record: serde_json::Value = serde_json::from_slice(&bytes[13..bytes.len() - 32])?;
    ensure!(record["boot"]["id"] != serde_json::json!(old_boot.boot));
    ensure!(record["boot"]["process"] == serde_json::json!(old_boot.process));
    let cut = f.catalog.read(TIMEOUT).await?;
    ensure!(matches!(
        f.catalog
            .authorize_capacity(&cut, NodeId::new([5; 16])?, TIMEOUT)
            .await,
        Err(FleetCatalogError::Conflict)
    ));
    ensure!(
        f.catalog
            .admit_node_capacity(&cut, &active, &origin, &[], TIMEOUT)
            .await?
            .applied_revision()
            == active.identity.expected_revision + 1
    );
    let stale = request(
        103,
        cut.revision(),
        active.attestation.clone(),
        FleetCapacityAction::Activate,
    );
    ensure!(matches!(
        f.catalog
            .admit_node_capacity(&cut, &stale, &origin, &[namespace(&f).await?], TIMEOUT)
            .await,
        Err(FleetCatalogError::Conflict)
    ));
    let authorization = f.catalog.authorize_host(&cut, spec.node, TIMEOUT).await?;
    let restarted_node = public_node::node_named(
        target_root.path(),
        f.definition.application.clone(),
        &spec.label,
    )
    .await?;
    let (mut config, services) = f.config_for(&restarted_node, authorization, &spec).await?;
    config.infrastructure.log.mode = LogOpenMode::Resume;
    config.infrastructure.log.process = ProcessGeneration::new([135; 16])?;
    config.peers[0].process = config.infrastructure.log.process;
    let restarted = restarted_node.start_raft_cells(config, services).await?;
    let fresh = attestation(&f, &origin, &restarted).await?;
    let cut = f.catalog.read(TIMEOUT).await?;
    let requalify = request(102, cut.revision(), fresh, FleetCapacityAction::Activate);
    f.catalog
        .admit_node_capacity(&cut, &requalify, &origin, &[namespace(&f).await?], TIMEOUT)
        .await?;
    let cut = f.catalog.read(TIMEOUT).await?;
    ensure!(f
        .catalog
        .authorize_capacity(&cut, spec.node, TIMEOUT)
        .await?
        .receive_replicas());
    joined(restarted_node).await?;
    joined(origin_node).await?;
    f.domain.shutdown().await;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn fleet_capacity_refuses_incomplete_inventory_and_unfit_attestation() -> Result<()> {
    let f = Fixture::with_managed_births(true).await?;
    let (_origin_root, origin_node, origin) = f.empty_host().await?;
    let (_target_root, target_node, target) = f.peer_host(5).await?;
    let evidence = attestation(&f, &origin, &target).await?;
    let cut = f.catalog.read(TIMEOUT).await?;
    let active = request(
        101,
        cut.revision(),
        evidence.clone(),
        FleetCapacityAction::Activate,
    );
    ensure!(matches!(
        f.catalog
            .admit_node_capacity(&cut, &active, &origin, &[], TIMEOUT)
            .await,
        Err(FleetCatalogError::InventoryIncomplete)
    ));
    for case in 0..6 {
        let mut invalid = evidence.clone();
        match case {
            0 => invalid.memory_bytes = 1,
            1 => invalid.durable_bytes = 1,
            2 => invalid.engine_slots = 1,
            3 => invalid.memory_swap_disabled = false,
            4 => invalid.boot.boot = [79; 16],
            _ => invalid.profile_digest = [79; 32],
        }
        let invalid = request(
            110 + case,
            cut.revision(),
            invalid,
            FleetCapacityAction::Activate,
        );
        ensure!(matches!(
            f.catalog
                .admit_node_capacity(&cut, &invalid, &origin, &[namespace(&f).await?], TIMEOUT)
                .await,
            Err(FleetCatalogError::Conflict | FleetCatalogError::Invalid(_))
        ));
    }
    ensure!(f.catalog.read(TIMEOUT).await?.revision() == cut.revision());
    ensure!(
        !f.catalog
            .authorize_host(&cut, NodeId::new([5; 16])?, TIMEOUT)
            .await?
            .eligibility()
            .receive_replicas
    );
    joined(target_node).await?;
    joined(origin_node).await?;
    f.domain.shutdown().await;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn fleet_capacity_raw_active_is_not_quantified_capacity() -> Result<()> {
    let f = Fixture::with_managed_births(true).await?;
    let (_root, node, _host) = f.empty_host().await?;
    f.state(f.spec.node, FleetNodeState::Active, 101).await?;
    let cut = f.catalog.read(TIMEOUT).await?;
    ensure!(
        f.catalog
            .authorize_host(&cut, f.spec.node, TIMEOUT)
            .await?
            .eligibility()
            .receive_replicas
    );
    ensure!(matches!(
        f.catalog
            .authorize_capacity(&cut, f.spec.node, TIMEOUT)
            .await,
        Err(FleetCatalogError::Conflict)
    ));
    joined(node).await?;
    f.domain.shutdown().await;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn fleet_capacity_expired_inventory_read_cannot_activate() -> Result<()> {
    let f = Fixture::with_managed_births(true).await?;
    let (_origin_root, origin_node, origin) = f.empty_host().await?;
    let (_target_root, target_node, target) = f.peer_host(5).await?;
    let evidence = attestation(&f, &origin, &target).await?;
    let namespaces = [namespace(&f).await?];
    let cut = f.catalog.read(TIMEOUT).await?;
    let active = request(101, cut.revision(), evidence, FleetCapacityAction::Activate);
    f.store.hold_fleet_reads.store(true, Ordering::Release);
    f.store.fleet_read_hang.store(true, Ordering::Release);
    let outcome = f
        .catalog
        .admit_node_capacity(
            &cut,
            &active,
            &origin,
            &namespaces,
            Duration::from_millis(50),
        )
        .await;
    f.store.hold_fleet_reads.store(false, Ordering::Release);
    f.store.fleet_read_hang.store(false, Ordering::Release);
    f.store.fleet_read_resume.notify_waiters();
    ensure!(matches!(outcome, Err(FleetCatalogError::Unconfirmed)));
    ensure!(f.catalog.read(TIMEOUT).await?.revision() == cut.revision());
    ensure!(matches!(
        f.catalog
            .authorize_capacity(&cut, NodeId::new([5; 16])?, TIMEOUT)
            .await,
        Err(FleetCatalogError::Conflict)
    ));
    f.remote(&origin, NodeId::new([5; 16])?, cut.revision())
        .await?;
    f.catalog
        .admit_node_capacity(&cut, &active, &origin, &namespaces, TIMEOUT)
        .await?;
    joined(target_node).await?;
    joined(origin_node).await?;
    f.domain.shutdown().await;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn fleet_capacity_concurrent_exclusion_defeats_activation() -> Result<()> {
    let f = Fixture::with_managed_births(true).await?;
    let (_origin_root, origin_node, origin) = f.empty_host().await?;
    let (_target_root, target_node, target) = f.peer_host(5).await?;
    let evidence = attestation(&f, &origin, &target).await?;
    let namespaces = [namespace(&f).await?];
    let cut = f.catalog.read(TIMEOUT).await?;
    let active = request(101, cut.revision(), evidence, FleetCapacityAction::Activate);
    f.store.fleet_cas_hang.store(true, Ordering::Release);
    f.store.hold_next_fleet_cas.store(true, Ordering::Release);
    let (catalog, snapshot, sender) = (f.catalog.clone(), cut.clone(), origin.clone());
    let pending = f.domain.spawn(async move {
        catalog
            .admit_node_capacity(&snapshot, &active, &sender, &namespaces, TIMEOUT)
            .await
    })?;
    f.domain
        .run(cellar::asyncrt::timeout(TIMEOUT, async {
            while !f.store.fleet_cas_waiting.load(Ordering::Acquire) {
                cellar::asyncrt::sleep(Duration::from_millis(1)).await;
            }
        }))
        .await??;
    let other = Fixture::open_catalog(&f.domain, f.store.clone())?;
    // Use the same canonical fixture mutation through an independent adapter.
    // This stands in for exclusion, not an evacuation/removal certificate.
    let excluded =
        Fixture::state_on(&other, NodeId::new([5; 16])?, FleetNodeState::Draining, 140).await;
    f.store.fleet_cas_hang.store(false, Ordering::Release);
    f.store.fleet_cas_resume.notify_waiters();
    excluded?;
    ensure!(matches!(pending.await?, Err(FleetCatalogError::Conflict)));
    let cut = f.catalog.read(TIMEOUT).await?;
    ensure!(
        f.catalog
            .authorize_host(&cut, NodeId::new([5; 16])?, TIMEOUT)
            .await?
            .state()
            == FleetNodeState::Draining
    );
    ensure!(matches!(
        f.catalog
            .authorize_capacity(&cut, NodeId::new([5; 16])?, TIMEOUT)
            .await,
        Err(FleetCatalogError::Conflict)
    ));
    joined(target_node).await?;
    joined(origin_node).await?;
    f.domain.shutdown().await;
    Ok(())
}
