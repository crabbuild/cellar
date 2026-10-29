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
            let cell = match gateway.cell(scope.namespace, &scope.cell) {
                Ok(cell) if cell.scope() == &scope => cell,
                Ok(_) => return Err(FleetCatalogError::Conflict),
                Err(_) => gateway
                    .register_managed_history(authorization.clone(), deadline.remaining()?)
                    .await
                    .map_err(command_failure)?,
            };
            // This pipeline owns initialization after submission, even when the
            // control waiter disconnects. It refuses an already-claimed blank
            // engine, occupied/retired slot, wrong schema or substituted boot.
            let receipt = cell
                .activate_managed_initial_member(authorization, deadline.remaining()?)
                .await
                .map_err(command_failure)?;
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

fn command_failure(error: crate::application::consensus::CommandFailure) -> FleetCatalogError {
    use crate::application::consensus::CommandFailure as C;
    match error {
        C::Overloaded => FleetCatalogError::Overloaded,
        C::CommandConflict => FleetCatalogError::Conflict,
        C::CorruptState => FleetCatalogError::Corrupt("genesis host state"),
        C::UnsupportedCapability => FleetCatalogError::Invalid("genesis host capability"),
        _ => FleetCatalogError::Unconfirmed,
    }
}
