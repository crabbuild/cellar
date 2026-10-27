// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Real inventory/boot/slot fencing before restart readiness is available.
use super::*;
use cellar::application::consensus::verification::FleetRestartRequest;
use sha2::{Digest, Sha256};

fn restart(id: u8, revision: u64, boot: &FleetBootBinding) -> Result<FleetRestartRequest> {
    Ok(FleetRestartRequest {
        identity: request(id, revision),
        node: NodeId::new(boot.node)?,
        expected_process: ProcessGeneration::new(boot.process)?,
        grace_seconds: 120,
    })
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn fleet_restart_preparation_retains_voters_and_reconciles_cancellation() -> Result<()> {
    let s = Box::pin(Setup::new()).await?;
    let p = primary(&s.f, &s.cells).await?;
    // This is a fixture deployment attestation, not a kernel/container
    // measurement. Exercise a real rooted certificate before invalidating it.
    let evidence =
        super::super::super::capacity_tests::attestation(&s.f, &s.spare, &s.nodes[p].host).await?;
    let capacity_root = s.f.catalog.read(TIMEOUT).await?;
    let capacity = FleetCapacityRequest {
        identity: request(225, capacity_root.revision()),
        attestation: evidence,
        action: FleetCapacityAction::Activate,
    };
    let capacity =
        s.f.catalog
            .admit_node_capacity(
                &capacity_root,
                &capacity,
                &s.spare,
                std::slice::from_ref(&s.namespace),
                TIMEOUT,
            )
            .await?;
    ensure!(s
        .f
        .catalog
        .authorize_capacity(
            &s.f.catalog.read(TIMEOUT).await?,
            s.nodes[p].spec.node,
            TIMEOUT
        )
        .await?
        .receive_replicas());
    ensure!(capacity.applied_revision() > capacity_root.revision());
    let before =
        s.f.remote(&s.spare, s.nodes[p].spec.node, capacity.applied_revision())
            .await?
            .view;
    let boot = before.fleet_boot.context("original boot")?;
    let root = s.f.catalog.read(TIMEOUT).await?;
    let intent = restart(230, root.revision(), &boot)?;
    for seconds in [0, 301] {
        let mut invalid = intent.clone();
        invalid.grace_seconds = seconds;
        ensure!(matches!(
            s.f.catalog
                .begin_restart_maintenance(
                    &root,
                    &invalid,
                    std::slice::from_ref(&s.namespace),
                    TIMEOUT
                )
                .await,
            Err(FleetCatalogError::Invalid(_))
        ));
    }
    let mut wrong = intent.clone();
    wrong.expected_process = ProcessGeneration::new([242; 16])?;
    ensure!(matches!(
        s.f.catalog
            .begin_restart_maintenance(&root, &wrong, std::slice::from_ref(&s.namespace), TIMEOUT)
            .await,
        Err(FleetCatalogError::Conflict)
    ));
    ensure!(matches!(
        s.f.catalog
            .begin_restart_maintenance(&root, &intent, &[], TIMEOUT)
            .await,
        Err(FleetCatalogError::InventoryIncomplete)
    ));
    ensure!(s.f.catalog.read(TIMEOUT).await?.revision() == root.revision());

    s.f.store
        .fail_fleet_confirmation
        .store(true, Ordering::Release);
    ensure!(s
        .f
        .catalog
        .begin_restart_maintenance(&root, &intent, std::slice::from_ref(&s.namespace), TIMEOUT)
        .await
        .is_err());
    s.f.store.fail_fleet_read.store(false, Ordering::Release);
    let reopened = Fixture::open_catalog(&s.f.domain, s.f.store.clone())?;
    let now = reopened.read(TIMEOUT).await?;
    let committed = reopened
        .begin_restart_maintenance(&now, &intent, &[], TIMEOUT)
        .await?;
    ensure!(committed.applied_revision == root.revision() + 1);
    let admission = reopened
        .restart_maintenance_admission(&now, ProposalId::new(intent.identity.operation)?, TIMEOUT)
        .await?;
    ensure!(
        admission.boot()? == boot
            && admission.histories() == 1
            && admission.namespaces() == 1
            && admission.grace_seconds() == 120
    );
    ensure!(
        admission.identity() == &intent.identity
            && admission.applied_revision() == committed.applied_revision
            && admission.enrollment_epoch()
                == before.current_enrollment.context("enrollment")?.epoch + 1
    );
    let view = s.f.remote(&s.spare, intent.node, now.revision()).await?;
    ensure!(
        view.view
            .current_enrollment
            .context("excluded enrollment")?
            .state
            == FleetNodeState::RestartPending
    );
    let host = reopened.authorize_host(&now, intent.node, TIMEOUT).await?;
    ensure!(
        !host.eligibility().receive_replicas
            && !host.eligibility().receive_leadership
            && !host.eligibility().evacuate_memberships
    );
    // Existing commands and elections are still live; preparation has not
    // frozen ticks or pretended that the original voter was evacuated.
    write_read(&s.f, &s.cells, 2, 5, 12).await?;
    let membership = s.cells[p]
        .observe_fleet_membership(s.authorization.clone(), TIMEOUT)
        .await?;
    let voters = membership
        .configuration()
        .voters()
        .iter()
        .map(|m| m.get())
        .collect::<std::collections::BTreeSet<_>>();
    ensure!(
        voters == std::collections::BTreeSet::from([1, 2, 3])
            && membership.status().consumed_sequence == 0
    );
    ensure!(matches!(
        reopened
            .authorize_capacity(&now, intent.node, TIMEOUT)
            .await,
        Err(FleetCatalogError::Conflict)
    ));
    let mut proposal = s.proposal.clone();
    proposal.scope.group = GroupId::new([223; 16])?;
    proposal.scope.cell = "Counter:restart-incoming".into();
    ensure!(matches!(
        reopened
            .reserve_birth(
                &now,
                &request(222, now.revision()),
                &s.namespace,
                proposal,
                TIMEOUT
            )
            .await,
        Err(FleetCatalogError::Conflict)
    ));

    for mutate in 0..4 {
        let mut changed = intent.clone();
        match mutate {
            0 => changed.grace_seconds = 121,
            1 => changed.expected_process = ProcessGeneration::new([243; 16])?,
            2 => changed.identity.principal = "another-principal".into(),
            _ => changed.node = NodeId::new([7; 16])?,
        }
        ensure!(matches!(
            reopened
                .begin_restart_maintenance(&now, &changed, &[], TIMEOUT)
                .await,
            Err(FleetCatalogError::PayloadConflict)
        ));
    }
    let other_boot = s
        .spare
        .observe_node(TIMEOUT)
        .await?
        .fleet_boot
        .context("spare boot")?;
    let other = restart(232, now.revision(), &other_boot)?;
    ensure!(
        matches!(reopened.begin_restart_maintenance(&now, &other, &[], TIMEOUT).await,
        Err(FleetCatalogError::DisruptionInProgress { operation }) if operation == intent.identity.operation)
    );
    ensure!(
        matches!(reopened.begin_evacuation(&now, &request(234, now.revision()),
        NodeId::new([7; 16])?, &[], TIMEOUT).await,
        Err(FleetCatalogError::DisruptionInProgress { operation }) if operation == intent.identity.operation)
    );
    ensure!(matches!(
        reopened
            .commit(
                &now,
                &FleetMutation {
                    id: [236; 16],
                    operation: [237; 16],
                    principal: "delete-slot".into(),
                    expected_revision: now.revision(),
                    changes: vec![FleetChange {
                        table: FleetTable::Policy,
                        key: "active-disruption".into(),
                        value: None,
                    }],
                },
                TIMEOUT
            )
            .await,
        Err(FleetCatalogError::Invalid(_))
    ));
    ensure!(reopened.read(TIMEOUT).await?.revision() == now.revision());

    let mut cancel = intent.clone();
    cancel.identity.id = [238; 16];
    cancel.identity.expected_revision = now.revision();
    s.f.store
        .fail_fleet_confirmation
        .store(true, Ordering::Release);
    ensure!(reopened
        .cancel_restart_preparation(&now, &cancel, TIMEOUT)
        .await
        .is_err());
    s.f.store.fail_fleet_read.store(false, Ordering::Release);
    let reopened = Fixture::open_catalog(&s.f.domain, s.f.store.clone())?;
    let now = reopened.read(TIMEOUT).await?;
    let cancelled = reopened
        .cancel_restart_preparation(&now, &cancel, TIMEOUT)
        .await?;
    let host = reopened.authorize_host(&now, intent.node, TIMEOUT).await?;
    ensure!(host.state() == FleetNodeState::Ready && !host.eligibility().receive_replicas);
    ensure!(host.storage_instance().map(|e| e.bytes()) == Some(boot.engine));
    ensure!(matches!(
        reopened
            .authorize_capacity(&now, intent.node, TIMEOUT)
            .await,
        Err(FleetCatalogError::Conflict)
    ));
    write_read(&s.f, &s.cells, 3, 9, 21).await?;
    let next = request(240, now.revision());
    let departed = reopened
        .begin_evacuation(
            &now,
            &next,
            NodeId::new([7; 16])?,
            std::slice::from_ref(&s.namespace),
            TIMEOUT,
        )
        .await?;
    let retry = reopened
        .cancel_restart_preparation(&departed.snapshot, &cancel, TIMEOUT)
        .await?;
    ensure!(
        retry.applied_revision == cancelled.applied_revision
            && retry.snapshot.revision() == departed.snapshot.revision()
    );
    let original = reopened
        .begin_restart_maintenance(&departed.snapshot, &intent, &[], TIMEOUT)
        .await?;
    ensure!(original.applied_revision == committed.applied_revision);
    let fresh_boot = s.nodes[(p + 1) % 3]
        .host
        .observe_node(TIMEOUT)
        .await?
        .fleet_boot
        .context("second restart boot")?;
    let fresh = restart(244, departed.snapshot.revision(), &fresh_boot)?;
    ensure!(
        matches!(reopened.begin_restart_maintenance(&departed.snapshot, &fresh, &[], TIMEOUT).await,
        Err(FleetCatalogError::DisruptionInProgress { operation }) if operation == next.operation)
    );
    ensure!(reopened.read(TIMEOUT).await?.revision() == departed.snapshot.revision());
    s.close().await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn fleet_restart_preparation_fences_new_boot_and_later_epoch_cancellation() -> Result<()> {
    let s = Box::pin(Setup::new()).await?;
    s.spare
        .register_managed_history(s.authorization.clone(), TIMEOUT)
        .await?;
    let boot = s
        .spare
        .observe_node(TIMEOUT)
        .await?
        .fleet_boot
        .context("boot")?;
    let root = s.f.catalog.read(TIMEOUT).await?;
    let intent = restart(230, root.revision(), &boot)?;
    let committed =
        s.f.catalog
            .begin_restart_maintenance(&root, &intent, std::slice::from_ref(&s.namespace), TIMEOUT)
            .await?;
    let Setup {
        f,
        nodes,
        cells,
        spare_root,
        spare_node,
        ..
    } = s;
    stopped(spare_node).await?;
    let spec = f.peer_spec(7)?;
    let authorization = f
        .catalog
        .authorize_host(&committed.snapshot, spec.node, TIMEOUT)
        .await?;
    let node = public_node::node_named(
        spare_root.path(),
        f.definition.application.clone(),
        &spec.label,
    )
    .await?;
    let (mut config, services) = f.config_for(&node, authorization, &spec).await?;
    config.infrastructure.log.mode = LogOpenMode::Resume;
    config.infrastructure.log.process = ProcessGeneration::new([245; 16])?;
    config.peers[0].process = config.infrastructure.log.process;
    let directory = config.infrastructure.log.directory.clone();
    let refused = node.start_raft_cells(config, services).await;
    ensure!(refused.as_ref().err().is_some_and(|error| error
        .to_string()
        .contains("maintenance restart requires joined owner proof")));
    let NodeOutcome::Stopped(report) = node.shutdown(ShutdownMode::Preserve).await? else {
        anyhow::bail!("refused maintenance boot did not join");
    };
    ensure!(report.raft.is_none());
    let metadata = f
        .domain
        .run(async move {
            cellar::asyncrt::blocking(move || cellar::asyncrt::fs().metadata(&directory)).await
        })
        .await???;
    ensure!(metadata.is_dir);
    let now = f.catalog.read(TIMEOUT).await?;
    ensure!(now.revision() == committed.snapshot.revision());
    let unchanged = f
        .catalog
        .restart_maintenance_admission(&now, ProposalId::new(intent.identity.operation)?, TIMEOUT)
        .await?;
    ensure!(unchanged.boot()? == boot);
    write_read(&f, &cells, 2, 5, 12).await?;

    // Emulate a later owned readiness epoch without manufacturing a stop
    // proof. The preparation-only cancellation must not reopen that epoch.
    let bytes = f
        .catalog
        .get(&now, FleetTable::Nodes, &hex::encode(boot.node), TIMEOUT)
        .await?
        .context("node record")?;
    let value: serde_json::Value = serde_json::from_slice(&bytes[13..bytes.len() - 32])?;
    let epoch = value["epoch"].as_u64().context("epoch")?;
    let payload = std::str::from_utf8(&bytes[13..bytes.len() - 32])?.replacen(
        &format!("\"epoch\":{epoch}"),
        &format!("\"epoch\":{}", epoch + 1),
        1,
    );
    let mut updated = bytes[..9].to_vec();
    updated.extend_from_slice(&(payload.len() as u32).to_be_bytes());
    updated.extend_from_slice(payload.as_bytes());
    updated.extend_from_slice(&Sha256::digest(&updated));
    let later = f
        .catalog
        .commit(
            &now,
            &FleetMutation {
                id: [246; 16],
                operation: intent.identity.operation,
                principal: "advance-maintenance-epoch".into(),
                expected_revision: now.revision(),
                changes: vec![FleetChange {
                    table: FleetTable::Nodes,
                    key: hex::encode(boot.node),
                    value: Some(updated),
                }],
            },
            TIMEOUT,
        )
        .await?;
    let mut cancel = intent.clone();
    cancel.identity.id = [247; 16];
    cancel.identity.expected_revision = later.snapshot.revision();
    ensure!(matches!(
        f.catalog
            .cancel_restart_preparation(&later.snapshot, &cancel, TIMEOUT)
            .await,
        Err(FleetCatalogError::Conflict)
    ));
    ensure!(f.catalog.read(TIMEOUT).await?.revision() == later.snapshot.revision());
    for node in nodes {
        stopped(node.node).await?;
    }
    drop(spare_root);
    f.domain.shutdown().await;
    Ok(())
}
