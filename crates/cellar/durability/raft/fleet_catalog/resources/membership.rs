// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Rooted settled intent locates voters; it does not replace fresh quorum proof.
use super::completion::Settlement;
use super::*;
use crate::raft::member_state::FleetMembershipPermit;
use cellar_logic::consensus::MemberId;
use std::collections::{BTreeMap, BTreeSet};

impl FleetCatalog {
    pub(in crate::raft::fleet_catalog) async fn resource_permit_authorization(
        &self,
        snapshot: &FleetSnapshot,
        permit: FleetMembershipPermit,
    ) -> Result<FleetPermitAuthorization> {
        if self
            .inner
            .lookup(
                snapshot.head.tables[FleetTable::Operations.slot()],
                &row_key(permit.id()),
            )
            .await?
            .is_none()
        {
            // Preserve the standalone low-level permit API. Managed physical
            // admission independently requires Some(original destination boot).
            return Ok(FleetPermitAuthorization::admitted(
                permit,
                snapshot.revision(),
                None,
            ));
        }
        let row = self.admitted_resource(snapshot, permit.id()).await?;
        if row.permit.as_ref() != Some(&permit) {
            return Err(FleetCatalogError::Corrupt("resource permit projection"));
        }
        Ok(FleetPermitAuthorization::admitted(
            permit,
            snapshot.revision(),
            Some(row.request.attestation.boot),
        ))
    }

    // Called within an owned catalog turn. Return only the configuration pinned
    // by immutable birth/permit/settlement journals, never a current-health lease.
    pub(in crate::raft::fleet_catalog) async fn settled_group_voters(
        &self,
        snapshot: &FleetSnapshot,
        history: &super::super::histories::FleetHistory,
    ) -> Result<(BTreeMap<NodeId, MemberId>, Option<FleetMembershipPermit>)> {
        let key = history
            .binding
            .key()
            .map_err(|_| FleetCatalogError::Corrupt("topology group key"))?;
        let latest = self
            .inner
            .lookup(snapshot.head.tables[FleetTable::Admissions.slot()], &key)
            .await?
            .map(|bytes| {
                FleetMembershipPermit::decode(&bytes)
                    .map_err(|_| FleetCatalogError::Corrupt("topology permit"))
            })
            .transpose()?;
        let mut voters = BTreeMap::new();
        if let Some(permit) = &latest {
            let row = self.admitted_resource(snapshot, permit.id()).await?;
            let (kind, _) = self
                .resource_settlement(snapshot, &row)
                .await?
                .ok_or(FleetCatalogError::InventoryIncomplete)?;
            if row.permit.as_ref() != Some(permit) || permit.binding() != history.binding() {
                return Err(FleetCatalogError::Conflict);
            }
            for owner in row
                .seed_owners
                .as_ref()
                .ok_or(FleetCatalogError::InventoryIncomplete)?
            {
                voters.insert(
                    NodeId::new(owner.node)
                        .map_err(|_| FleetCatalogError::Corrupt("topology seed owner"))?,
                    MemberId::new(owner.member)
                        .map_err(|_| FleetCatalogError::Corrupt("topology seed member"))?,
                );
            }
            if kind == Settlement::Complete {
                let plan = row
                    .request
                    .movement
                    .as_ref()
                    .ok_or(FleetCatalogError::InventoryIncomplete)?
                    .plan()?;
                voters.retain(|_, member| *member != plan.obsolete);
                voters.insert(plan.replacement.node, plan.replacement.member);
            }
        } else {
            if history.voters.len() != 3 || !history.learners.is_empty() {
                return Err(FleetCatalogError::InventoryIncomplete);
            }
            for member in &history.voters {
                voters.insert(
                    NodeId::new(member.node)
                        .map_err(|_| FleetCatalogError::Corrupt("topology voter"))?,
                    MemberId::new(member.member)
                        .map_err(|_| FleetCatalogError::Corrupt("topology member"))?,
                );
            }
        }
        if voters.len() != 3 || voters.values().copied().collect::<BTreeSet<_>>().len() != 3 {
            return Err(FleetCatalogError::InventoryIncomplete);
        }
        Ok((voters, latest))
    }
}
