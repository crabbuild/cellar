// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Failure admission is a permanent exact-boot isolation intent. A controller
//! may have dispatched the platform action even if no result was recorded.
use super::nodes::{NodeRecord, NODE_KIND};
use super::*;

impl FleetCatalog {
    pub(super) async fn check_failure_recovery_mutation(
        &self,
        snapshot: &FleetSnapshot,
        mutation: &FleetMutation,
        authority: resources::Authority,
    ) -> Result<()> {
        for change in &mutation.changes {
            if change.table == FleetTable::Policy && change.key == disruption::SLOT_KEY {
                if let Some(bytes) = &change.value {
                    if let Ok(record) =
                        decode::<evacuation::Record>(evacuation::ADMISSION_KIND, bytes, 4096)
                    {
                        if record.failure_boot().is_some()
                            && authority != resources::Authority::FailureAdmission
                        {
                            return Err(FleetCatalogError::Invalid(
                                "owned failure admission required",
                            ));
                        }
                    }
                }
            }
        }
        // This guard is on management mutations, not on the Raft write path.
        if snapshot.head.disruption.is_none()
            || !mutation.changes.iter().any(|c| {
                c.table == FleetTable::Nodes
                    || c.table == FleetTable::Operations && c.key.len() == 32
                    || c.table == FleetTable::Policy && c.key == disruption::SLOT_KEY
            })
        {
            return Ok(());
        }
        let Some(disruption::Disruption::Evacuation(record)) =
            self.active_disruption(snapshot).await?
        else {
            return Ok(());
        };
        let Some(boot) = record.failure_boot() else {
            return Ok(());
        };
        for change in &mutation.changes {
            if change.table == FleetTable::Operations
                && change.key == hex::encode(record.operation())
            {
                return Err(FleetCatalogError::Invalid("immutable failure intent"));
            }
            if change.table == FleetTable::Nodes && change.key == hex::encode(boot.node) {
                let next: NodeRecord = decode(
                    NODE_KIND,
                    change
                        .value
                        .as_deref()
                        .ok_or(FleetCatalogError::Invalid("failure node retention"))?,
                    4096,
                )?;
                next.validate(&self.inner)?;
                if next.boot_binding(self.birth_owner()?.catalog)? != *boot
                    || !matches!(
                        next.state,
                        FleetNodeState::Draining
                            | FleetNodeState::Finalizing
                            | FleetNodeState::Removed
                    )
                {
                    return Err(FleetCatalogError::Invalid("failure boot remains fenced"));
                }
            }
            if change.table == FleetTable::Policy && change.key == disruption::SLOT_KEY {
                // Only terminal node removal may release this slot. Timeout,
                // service shutdown and operator cancellation cannot undo a
                // platform action whose response may have been lost.
                let removed = mutation.changes.iter().any(|c| {
                    c.table == FleetTable::Nodes
                        && c.key == hex::encode(boot.node)
                        && c.value
                            .as_deref()
                            .and_then(|bytes| decode::<NodeRecord>(NODE_KIND, bytes, 4096).ok())
                            .is_some_and(|n| n.state == FleetNodeState::Removed)
                });
                if !removed {
                    return Err(FleetCatalogError::Invalid(
                        "failure recovery requires terminal removal",
                    ));
                }
            }
        }
        Ok(())
    }
}
