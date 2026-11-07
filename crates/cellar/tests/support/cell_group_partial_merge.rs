// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! A partial source set must never create child engines or serving authority.
use super::*;
use cellar::application::consensus::FleetGroupArchiveVerifier;

pub(super) async fn verify_operator_fence(
    cluster: &ManagedCluster,
    catalog: &FleetCatalog,
    directory: &RangeDirectory,
    transition: &ReopenedManagedCellGroupTransition,
    child: &GroupRoute,
    verifier: &FleetGroupArchiveVerifier,
) -> Result<RaftFleetTransitionChildSetupRequest> {
    let fixture = &cluster.fixtures[0];
    let operation = transition.plan.operation();
    let recovery = preparation::recovery_pool()?;
    let base = RaftFleetControl::new(&fixture.domain, catalog.clone(), vec![directory.clone()])?;
    let install = RaftFleetTransitionChildSetupRequest {
        catalog_id: base.catalog_id().into(),
        namespace_id: hex::encode(child.history.namespace.bytes()),
        operation_id: hex::encode(operation.bytes()),
        request_id: hex::encode([178; 16]),
        plan_digest: hex::encode(transition.plan.digest()),
        child: transition_group(child.history),
        step: RaftFleetTransitionChildStep::Install,
    };
    let before = base
        .transition_status(&install.namespace_id, &install.operation_id, TIMEOUT)
        .await?;
    ensure!(
        before.phase == Some(TransitionPhase::SealAuthorized)
            && before.fleet_slot_held
            && !before.parent_holds_released
            && before.transfer_archive_digest.is_none()
    );
    let held = catalog
        .staged_managed_sealed_sources(directory, operation, TIMEOUT)
        .await?;
    ensure!(held.len() == 2 && held[0].1.is_some() && held[1].1.is_none());
    let view = fixture.domain.run(directory.observe()).await??;
    ensure!(view.groups() == 2);

    // Configure every real prerequisite, including a reserved voter and a
    // private verifier. The install refusal must come from the incomplete
    // source set, rather than an absent management capability.
    for voter in &cluster.hosts[..3] {
        let control = base
            .clone()
            .with_node_observer(voter.clone())
            .with_transition_preparer(verifier.clone(), recovery.clone());
        for (step, id, code) in [
            (
                RaftFleetTransitionChildStep::Install,
                178,
                RaftFleetFailureCode::Unavailable,
            ),
            (
                RaftFleetTransitionChildStep::Attach,
                179,
                RaftFleetFailureCode::Conflict,
            ),
            (
                RaftFleetTransitionChildStep::Quorum,
                181,
                RaftFleetFailureCode::Conflict,
            ),
            (
                RaftFleetTransitionChildStep::InitializeArchive,
                183,
                RaftFleetFailureCode::Conflict,
            ),
        ] {
            let request = RaftFleetTransitionChildSetupRequest {
                step,
                request_id: hex::encode([id; 16]),
                ..install.clone()
            };
            let result = Box::pin(control.setup_transition_child(
                &transition.request.principal,
                &request.namespace_id,
                &request.operation_id,
                &request,
                TIMEOUT,
            ))
            .await;
            ensure!(
                matches!(&result, Err(failure) if failure.code == code
                && failure.outcome == RaftFleetOutcome::Unknown),
                "partial merge admitted {step:?} on {:?}: {result:?}",
                voter.identity()
            );
            ensure!(
                matches!(voter.group(child.history), Err(RuntimeError::NotFound)),
                "partial merge created a child engine on {:?}",
                voter.identity()
            );
            let usage = recovery.usage();
            ensure!(
                usage.operations == 0 && usage.retained_bytes == 0,
                "refused child setup leaked restore admission: {usage:?}"
            );
        }
    }
    ensure!(catalog
        .observe_managed_transfer_install_quorum(transition, directory, child, TIMEOUT,)
        .await?
        .is_none());
    let after = catalog.read(TIMEOUT).await?;
    ensure!(after.revision() == before.observed_fleet_revision);
    ensure!(
        *catalog
            .history(&after, &child.history, TIMEOUT)
            .await?
            .state()
            == FleetHistoryState::Reserved
    );
    ensure!(
        catalog
            .staged_managed_sealed_sources(directory, operation, TIMEOUT)
            .await?
            == held
    );
    ensure!(fixture.domain.run(directory.observe()).await??.digest() == view.digest());
    ensure!(
        base.transition_status(&install.namespace_id, &install.operation_id, TIMEOUT)
            .await?
            == before
    );
    Ok(install)
}
