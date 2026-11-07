// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Receiving authority is the current gate plus the immutable admitted birth.
use super::*;
use crate::raft::fleet_stop::BootBinding;

impl FleetCatalog {
    pub(super) async fn restart_genesis_history(
        &self,
        snapshot: &FleetSnapshot,
        command: &RestartActionContext,
    ) -> Result<FleetHistory> {
        let action = self.restart_action_history(snapshot, command).await?;
        let target = NodeId::new(command.target.node)
            .map_err(|_| FleetCatalogError::Invalid("genesis target"))?;
        let member = action
            .voters
            .get(&target)
            .ok_or(FleetCatalogError::Conflict)?;
        let history = action.history;
        if !history.allows_managed_genesis()
            || !history
                .voters
                .iter()
                .any(|v| v.node == command.target.node && v.member == member.get())
        {
            return Err(FleetCatalogError::InventoryIncomplete);
        }
        Ok(history)
    }
}

impl FleetHostAuthorization {
    pub(crate) fn execute_restart_genesis<'a>(
        &'a self,
        command: RestartActionContext,
        origin: BootBinding,
        minimum_revision: u64,
        gateway: &'a RaftCells,
        timeout: Duration,
    ) -> BoxFuture<'a, Result<()>> {
        Box::pin(async move {
            command.validate()?;
            origin
                .validate()
                .map_err(|_| FleetCatalogError::Invalid("genesis origin"))?;
            let catalog = &self.catalog;
            let deadline = Deadline::new(catalog, timeout)?;
            let snapshot = catalog.read(deadline.remaining()?).await?;
            if snapshot.revision() < minimum_revision {
                return Err(FleetCatalogError::Conflict);
            }
            let history = catalog
                .run(deadline.remaining()?, async {
                    let current = catalog.host_from_record(&snapshot, self.node()).await?;
                    self.require_running_successor(&current)?;
                    if current.boot_binding()? != command.target {
                        return Err(FleetCatalogError::Conflict);
                    }
                    let sender = catalog
                        .host_from_record(
                            &snapshot,
                            NodeId::new(origin.node)
                                .map_err(|_| FleetCatalogError::Invalid("genesis origin"))?,
                        )
                        .await?;
                    if sender.boot_binding()? != origin
                        || !sender.state().permits_admitted_movement()
                    {
                        return Err(FleetCatalogError::Conflict);
                    }
                    catalog.restart_genesis_history(&snapshot, &command).await
                })
                .await?;
            let scope = history
                .binding()
                .scope()
                .map_err(|_| FleetCatalogError::Corrupt("genesis scope"))?;
            let authorization = catalog
                .authorize_group(&snapshot, &scope, deadline.remaining()?)
                .await?;
            // The controller owns complete-group recovery and attachment after
            // admission. A receipt is descriptive; fresh quorum is checked later.
            let receipt = gateway
                .activate_managed_initial_group(authorization, deadline.remaining()?)
                .await
                .map_err(startup_failure)?;
            if &receipt.binding != history.binding() {
                return Err(FleetCatalogError::Conflict);
            }
            catalog
                .run(deadline.remaining()?, async {
                    let current = catalog.inner.current().await?;
                    catalog.restart_genesis_history(&current, &command).await?;
                    Ok(())
                })
                .await?;
            deadline.remaining()?;
            Ok(())
        })
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
                    FleetCatalogError::Corrupt("genesis group state")
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
            FleetCatalogError::Invalid("genesis group capability")
        }
        CellGroupStartupError::Refused(_) => FleetCatalogError::Unconfirmed,
    }
}
