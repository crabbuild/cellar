// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Every final source member must apply retirement before any owner detaches.
use super::histories::{catalog_error, validate_request};
use super::*;
use crate::application::consensus::RaftCells;
use crate::raft::cell_groups::RangeDirectory;
use cellar_logic::{cell_group::GroupHistory, consensus::NodeId};

impl FleetCatalog {
    /// Existing selected retirement metadata, exact enrolled boots and complete
    /// physical inventories gate local drainage. A birth/history can acquire
    /// only one source tombstone; the root-selected immutable plan and final
    /// configuration are independently checked before inspecting its members.
    pub(crate) async fn quiesce_managed_transfer_source(
        &self,
        transition: &ReopenedManagedCellGroupTransition,
        directory: &RangeDirectory,
        observer: &RaftCells,
        history: GroupHistory,
        timeout: Duration,
    ) -> Result<()> {
        validate_request(&transition.request)?;
        let deadline = deadline::Deadline::new(self, timeout)?;
        let (current, permit) = self
            .run(
                deadline.remaining()?,
                Box::pin(async {
                    let current = self.inner.current().await?;
                    let permit = self
                        .managed_transition_permit_at(
                            &current,
                            &transition.request,
                            directory,
                            &transition.plan,
                        )
                        .await?;
                    Ok((current, permit))
                }),
            )
            .await?;
        let (_, sources) = self
            .inner
            .domain
            .run(Box::pin(crate::asyncrt::timeout(
                deadline.remaining()?,
                Box::pin(directory.managed_retired_sources(&transition.plan, &permit)),
            )))
            .await
            .map_err(|_| FleetCatalogError::Closed)?
            .map_err(|_| FleetCatalogError::Unconfirmed)?
            .map_err(catalog_error)?;
        let source = sources
            .iter()
            .find(|source| source.route.history == history)
            .ok_or(FleetCatalogError::Conflict)?;
        if !source
            .members
            .iter()
            .any(|member| member.node == observer.identity().node.bytes())
        {
            return Err(FleetCatalogError::Conflict);
        }
        for member in &source.members {
            let node = NodeId::new(member.node)
                .map_err(|_| FleetCatalogError::Corrupt("retired source node"))?;
            let boot = self
                .run(deadline.remaining()?, self.host_from(&current, node))
                .await?
                .boot_binding()?;
            let sample = observer
                .inspect_peer_physical_inventory(node, current.revision(), deadline.remaining()?)
                .await
                .map_err(FleetCatalogError::Observation)?;
            if sample.view.fleet_boot.as_ref() != Some(&boot)
                || sample.view.node.identity.node != node
                || sample.view.node.identity.disk.bytes() != boot.disk
                || sample.view.node.identity.process.bytes() != boot.process
                || sample.view.node.storage_instance.bytes() != boot.engine
                || member.disk.is_some_and(|disk| disk != boot.disk)
            {
                return Err(FleetCatalogError::Unconfirmed);
            }
            let inventory = sample
                .inventory
                .ok_or(FleetCatalogError::InventoryIncomplete)?;
            let row = inventory
                .group_history(&history)
                .ok_or(FleetCatalogError::InventoryIncomplete)?;
            if row.birth_digest() != source.route.birth_digest
                || row.member().get() != member.member
                || !row.source_retirement_applied()
            {
                return Err(FleetCatalogError::Unconfirmed);
            }
        }
        // Install successor routes through the existing joined directory owner
        // before this process becomes ingress without an attached parent.
        // It independently resolves selected births and fresh quorum membership.
        let (_, children) = directory
            .transition_routes(&transition.plan)
            .map_err(catalog_error)?;
        for child in children {
            observer
                .refresh_managed_router(child.history, deadline.remaining()?)
                .await
                .map_err(|error| match error {
                    crate::raft::completion::RuntimeError::Overloaded => {
                        FleetCatalogError::Overloaded
                    }
                    _ => FleetCatalogError::Unconfirmed,
                })?;
        }
        self.run(deadline.remaining()?, async {
            if self.inner.current().await?.token != current.token {
                return Err(FleetCatalogError::Conflict);
            }
            Ok(())
        })
        .await?;
        observer
            .quiesce_managed_source(transition, history, deadline.remaining()?)
            .await
            .map_err(|error| match error {
                crate::raft::cell_groups::CellGroupControllerError::Runtime(
                    crate::raft::completion::RuntimeError::Overloaded,
                ) => FleetCatalogError::Overloaded,
                _ => FleetCatalogError::Unconfirmed,
            })
    }
}
