// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! The receiving process independently resolves its current canonical permit.
use super::*;
use crate::application::consensus::LearnerActivation;
use crate::raft::member_state::RepairStep;
use cellar_logic::consensus::{LogIndex, MemberId};

impl FleetHostAuthorization {
    pub(crate) async fn admitted_learner_from(
        &self,
        scope: &GroupHistory,
        source: &cellar_logic::consensus::ReplicaIdentity<GroupHistory>,
        timeout: Duration,
    ) -> Result<FleetPermitAuthorization> {
        self.admitted_seed(scope, Some(source), timeout).await
    }

    async fn admitted_seed(
        &self,
        scope: &GroupHistory,
        source: Option<&cellar_logic::consensus::ReplicaIdentity<GroupHistory>>,
        timeout: Duration,
    ) -> Result<FleetPermitAuthorization> {
        let catalog = &self.catalog;
        catalog
            .run(timeout, async {
                let snapshot = catalog.inner.current().await?;
                let current = catalog.host_from_record(&snapshot, self.node()).await?;
                self.require_running_successor(&current)?;
                let key = catalog.binding(scope)?.key().map_err(|_| invalid())?;
                let bytes = catalog
                    .inner
                    .lookup(snapshot.head.tables[FleetTable::Admissions.slot()], &key)
                    .await?
                    .ok_or(FleetCatalogError::Conflict)?;
                let permit = crate::raft::member_state::FleetMembershipPermit::decode(&bytes)
                    .map_err(|_| invalid())?;
                let row = catalog.admitted_resource(&snapshot, permit.id()).await?;
                catalog.check_executable_movement(&snapshot, &row).await?;
                if row.node != self.node()
                    || row.permit.as_ref() != Some(&permit)
                    || permit.binding() != &catalog.binding(scope)?
                    || catalog
                        .movement_boot(&snapshot, &row, self.node(), false)
                        .await?
                        .0
                        != current.boot_binding()?
                {
                    return Err(FleetCatalogError::Conflict);
                }
                if let Some(source) = source {
                    if source.scope != *scope
                        || !row.seed_owners.as_ref().is_some_and(|owners| {
                            owners.iter().any(|owner| {
                                owner.member == source.member.get()
                                    && owner.node == source.node.bytes()
                            })
                        })
                    {
                        return Err(FleetCatalogError::Conflict);
                    }
                    let (boot, _) = catalog
                        .movement_boot(&snapshot, &row, source.node, false)
                        .await?;
                    if boot.disk != source.disk.bytes() || boot.process != source.process.bytes() {
                        return Err(FleetCatalogError::Conflict);
                    }
                }
                Ok(FleetPermitAuthorization::admitted(
                    permit,
                    snapshot.revision(),
                    Some(row.request.attestation.boot),
                ))
            })
            .await
    }

    pub(crate) async fn execute_movement(
        &self,
        command: MovementCommand,
        origin: BootBinding,
        minimum_revision: u64,
        gateway: &RaftCells,
        timeout: Duration,
    ) -> Result<()> {
        command.validate()?;
        origin.validate().map_err(|_| invalid())?;
        let catalog = &self.catalog;
        let deadline = super::super::super::deadline::Deadline::new(catalog, timeout)?;
        let snapshot = catalog.read(deadline.remaining()?).await?;
        if snapshot.revision() < minimum_revision {
            return Err(FleetCatalogError::Conflict);
        }
        let row = catalog
            .run(deadline.remaining()?, async {
                let current = catalog.host_from_record(&snapshot, self.node()).await?;
                self.require_running_successor(&current)?;
                if current.boot_binding()? != command.target {
                    return Err(FleetCatalogError::Conflict);
                }
                let origin_host = catalog
                    .host_from(&snapshot, NodeId::new(origin.node).map_err(|_| invalid())?)
                    .await?;
                if origin_host.boot_binding()? != origin
                    || !origin_host.state().permits_admitted_movement()
                {
                    return Err(FleetCatalogError::Conflict);
                }
                let row = catalog
                    .admitted_resource(&snapshot, command.reservation)
                    .await?;
                catalog.check_executable_movement(&snapshot, &row).await?;
                if hash(&row.encode()?) != command.resource_digest
                    || catalog
                        .movement_boot(
                            &snapshot,
                            &row,
                            self.node(),
                            command.action.finishes_joint(),
                        )
                        .await?
                        .0
                        != command.target
                {
                    return Err(FleetCatalogError::Conflict);
                }
                if let MovementAction::Activate {
                    source,
                    source_node,
                    ..
                } = command.action
                {
                    let owner = row
                        .seed_owners
                        .as_ref()
                        .ok_or_else(invalid)?
                        .iter()
                        .find(|owner| owner.member == source && owner.node == source_node)
                        .ok_or_else(invalid)?;
                    if row.node != self.node() {
                        return Err(FleetCatalogError::Conflict);
                    }
                    catalog
                        .movement_boot(
                            &snapshot,
                            &row,
                            NodeId::new(owner.node).map_err(|_| invalid())?,
                            false,
                        )
                        .await?;
                }
                Ok(row)
            })
            .await?;
        let permit = row.permit.as_ref().ok_or_else(invalid)?;
        let scope = permit.binding().scope().map_err(|_| invalid())?;
        let authorization = catalog
            .authorize_membership(&snapshot, &scope, deadline.remaining()?)
            .await?;
        if !matches!(command.action, MovementAction::Activate { .. }) {
            let group = gateway
                .group(scope)
                .map_err(|_| FleetCatalogError::Conflict)?;
            let history = catalog
                .authorize_group(&snapshot, &scope, deadline.remaining()?)
                .await?;
            return match command.action {
                MovementAction::Begin => group
                    .begin_managed_repair(
                        history,
                        authorization,
                        row.request.movement.as_ref().ok_or_else(invalid)?.plan()?,
                        deadline.remaining()?,
                    )
                    .await
                    .map(|_| ())
                    .map_err(control_failure),
                MovementAction::Promote { revision } | MovementAction::Finish { revision } => {
                    let step = if matches!(command.action, MovementAction::Promote { .. }) {
                        RepairStep::PromoteAndRemove
                    } else {
                        RepairStep::FinishJoint
                    };
                    group
                        .advance_managed_repair(
                            history,
                            authorization,
                            step,
                            LogIndex::new(revision),
                            deadline.remaining()?,
                        )
                        .await
                        .map(|_| ())
                        .map_err(control_failure)
                }
                MovementAction::Activate { .. } => Err(invalid()),
            };
        }
        let history = catalog
            .authorize_group(&snapshot, &scope, deadline.remaining()?)
            .await?;
        match command.action {
            MovementAction::Activate {
                source,
                source_node,
                from_s3,
            } => gateway
                .activate_managed_learner_group(
                    history,
                    authorization,
                    LearnerActivation {
                        member: permit.replacement().map_err(|_| invalid())?.member,
                        source: MemberId::new(source).map_err(|_| invalid())?,
                        source_node: NodeId::new(source_node).map_err(|_| invalid())?,
                        from_s3,
                        timeout: deadline.remaining()?,
                    },
                )
                .await
                .map(|_| ())
                .map_err(startup_failure),
            _ => Err(invalid()),
        }
    }
}

fn control_failure(
    error: crate::application::consensus::CellGroupControlError,
) -> FleetCatalogError {
    use crate::application::consensus::CellGroupControlError;
    use crate::raft::completion::RuntimeError;
    match error {
        CellGroupControlError::Unconfirmed(_) => FleetCatalogError::Unconfirmed,
        CellGroupControlError::Refused(RuntimeError::Overloaded) => FleetCatalogError::Overloaded,
        CellGroupControlError::Refused(
            RuntimeError::Invalid(_) | RuntimeError::StaleCompletion,
        ) => FleetCatalogError::Conflict,
        CellGroupControlError::Refused(RuntimeError::Storage(
            crate::raft::storage::LogError::Corrupt(_),
        )) => FleetCatalogError::Corrupt("group repair state"),
        CellGroupControlError::Refused(_) => FleetCatalogError::Unconfirmed,
    }
}

fn startup_failure(
    error: crate::application::consensus::CellGroupStartupError,
) -> FleetCatalogError {
    use crate::application::consensus::CellGroupStartupError;
    use crate::raft::cell_groups::CellGroupControllerError as C;
    match error {
        // Entered recovery may have synced or attached even if its delivery failed.
        CellGroupStartupError::Unconfirmed(_) => FleetCatalogError::Unconfirmed,
        CellGroupStartupError::Refused(C::Runtime(
            crate::raft::completion::RuntimeError::Overloaded,
        )) => FleetCatalogError::Overloaded,
        CellGroupStartupError::Refused(C::Catalog(error)) => {
            use crate::raft::catalog::CatalogError as E;
            match error.as_ref() {
                E::Invalid(reason) => FleetCatalogError::Invalid(reason),
                E::Corrupt(reason) => FleetCatalogError::Corrupt(reason),
                E::Limit => FleetCatalogError::Limit,
                E::Log(crate::raft::storage::LogError::Corrupt(_)) | E::LostStableState => {
                    FleetCatalogError::Corrupt("learner group state")
                }
                E::Conflict(_) | E::ManagedAdmissionRequired | E::NotInitialMember | E::Fenced => {
                    FleetCatalogError::Conflict
                }
                E::FleetAuthority(authority) => match authority.as_ref() {
                    FleetCatalogError::Invalid(reason) => FleetCatalogError::Invalid(reason),
                    FleetCatalogError::Corrupt(reason) => FleetCatalogError::Corrupt(reason),
                    FleetCatalogError::Overloaded => FleetCatalogError::Overloaded,
                    FleetCatalogError::Limit => FleetCatalogError::Limit,
                    FleetCatalogError::Conflict => FleetCatalogError::Conflict,
                    FleetCatalogError::Closed => FleetCatalogError::Closed,
                    // Preserve the complete typed cause for local diagnosis;
                    // transport delivery still classifies unknown outcomes.
                    _ => FleetCatalogError::Storage(
                        CellGroupStartupError::Refused(C::Catalog(error)).into(),
                    ),
                },
                _ => FleetCatalogError::Storage(
                    CellGroupStartupError::Refused(C::Catalog(error)).into(),
                ),
            }
        }
        CellGroupStartupError::Refused(C::Invalid(_)) => {
            FleetCatalogError::Invalid("learner group capability")
        }
        CellGroupStartupError::Refused(_) => FleetCatalogError::Unconfirmed,
    }
}
