// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! A completed replacement authorizes only its obsolete local replica's GC.
use super::*;
use crate::raft::{
    cell_groups::member_reclaim::{Member, Permit},
    storage::ReplicaLog,
};

fn invalid() -> FleetCatalogError {
    FleetCatalogError::Invalid("member reclamation")
}

impl FleetHostAuthorization {
    pub(crate) async fn prepare_member_reclamation(
        &self,
        reservation: [u8; 16],
        log: &ReplicaLog,
        bytes_per_group: u64,
        timeout: Duration,
    ) -> Result<Permit> {
        let catalog = &self.catalog;
        catalog
            .run(timeout, async {
                let snapshot = catalog.inner.current().await?;
                let current = catalog.host_from_record(&snapshot, self.node()).await?;
                self.require_running_successor(&current)?;
                let boot = current.boot_binding()?;
                if boot.engine != log.storage_instance().map_err(|_| invalid())?.bytes()
                    || boot.node != log.limits().node.bytes()
                    || boot.disk != log.limits().disk.bytes()
                    || boot.process != log.limits().process.bytes()
                {
                    return Err(FleetCatalogError::Conflict);
                }
                let row = catalog.admitted_resource(&snapshot, reservation).await?;
                if !matches!(
                    catalog.resource_settlement(&snapshot, &row).await?,
                    Some((completion::Settlement::Complete, _))
                ) {
                    return Err(FleetCatalogError::Unconfirmed);
                }
                let permit = row.permit.as_ref().ok_or_else(invalid)?;
                let scope = permit.binding().scope().map_err(|_| invalid())?;
                let obsolete = row
                    .request
                    .movement
                    .as_ref()
                    .ok_or_else(invalid)?
                    .plan()?
                    .obsolete;
                let owner = row
                    .seed_owners
                    .as_ref()
                    .ok_or_else(invalid)?
                    .iter()
                    .find(|owner| {
                        owner.member == obsolete.get() && owner.node == self.node().bytes()
                    })
                    .ok_or(FleetCatalogError::Conflict)?;
                // A new process must prove the retained disk/engine lineage and
                // freshly qualify its rooted boot; node identity alone is no GC grant.
                if !owner.matches(&boot)? {
                    catalog
                        .confirm_retained_movement_source(&snapshot, owner, &boot)
                        .await?;
                }
                // Later moves cannot resurrect this obsolete member ID. The exact
                // local Complete marker and immutable completion bind its removal;
                // a newer local member fails Member::from_group instead.
                let member = if let Some(previous) = log
                    .member_reclamation(scope)
                    .await
                    .map_err(|_| FleetCatalogError::Unconfirmed)?
                {
                    if previous
                        .record()
                        .member
                        .authorization()
                        .map_err(|_| invalid())?
                        == *permit
                    {
                        previous.record().member.clone()
                    } else {
                        let group = log
                            .cell_group(&scope)
                            .map_err(|_| FleetCatalogError::Conflict)?;
                        Member::from_group(&group, permit)
                            .map_err(|_| FleetCatalogError::Unconfirmed)?
                    }
                } else {
                    let group = log
                        .cell_group(&scope)
                        .map_err(|_| FleetCatalogError::Conflict)?;
                    Member::from_group(&group, permit)
                        .map_err(|_| FleetCatalogError::Unconfirmed)?
                };
                if member.member != obsolete.get() {
                    return Err(FleetCatalogError::Conflict);
                }
                Permit::new(log, member, bytes_per_group)
                    .map_err(|_| FleetCatalogError::Unconfirmed)
            })
            .await
    }
}
