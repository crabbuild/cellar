// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Exact lookup of admitted learner provenance after Raft prunes a binding.
//! The index is not a physical-release grant and is rooted with its permit.
use super::completion::Settlement;
use super::records::{Record, Reference};
use super::*;
use cellar_logic::consensus::MemberId;

pub(super) const PREFIX: &str = "resource-binding/";
const KIND: u8 = 66;

fn key(group: &str, member: MemberId, node: NodeId) -> String {
    format!(
        "{PREFIX}{group}/{:016x}/{}",
        member.get(),
        hex::encode(node.bytes())
    )
}

pub(super) fn change(record: &Record) -> Result<FleetChange> {
    let plan = record
        .request
        .movement
        .as_ref()
        .ok_or(FleetCatalogError::Invalid("binding movement"))?
        .plan()?;
    Ok(FleetChange {
        table: FleetTable::Operations,
        key: key(
            &record.request.group,
            plan.replacement.member,
            plan.replacement.node,
        ),
        value: Some(encode(
            KIND,
            &Reference {
                id: record.request.identity.id,
                digest: hash(&record.encode()?),
            },
            4096,
        )?),
    })
}

impl FleetCatalog {
    async fn resource_binding_row(
        &self,
        snapshot: &FleetSnapshot,
        group: &str,
        member: MemberId,
        node: NodeId,
    ) -> Result<Option<Record>> {
        let Some(bytes) = self
            .inner
            .lookup(
                snapshot.head.tables[FleetTable::Operations.slot()],
                &key(group, member, node),
            )
            .await?
        else {
            return Ok(None);
        };
        let reference: Reference = decode(KIND, &bytes, 4096)?;
        let row = self.admitted_resource(snapshot, reference.id).await?;
        let plan = row
            .request
            .movement
            .as_ref()
            .ok_or(FleetCatalogError::Corrupt("binding movement"))?
            .plan()?;
        if reference.digest != hash(&row.encode()?)
            || row.request.group != group
            || row.node != node
            || plan.replacement.node != node
            || plan.replacement.member != member
        {
            return Err(FleetCatalogError::Corrupt("binding index projection"));
        }
        Ok(Some(row))
    }

    /// Called inside the surrounding resource-admission turn. A fenced unused
    /// proposal did not consume a member ID or target history, so a later exact
    /// admission may replace its lookup. Consumed bindings are never rebound.
    pub(super) async fn check_resource_binding_change(
        &self,
        snapshot: &FleetSnapshot,
        record: &Record,
        proposed: &FleetChange,
    ) -> Result<()> {
        if *proposed != change(record)? {
            return Err(FleetCatalogError::Invalid("binding index mutation"));
        }
        let plan = record
            .request
            .movement
            .as_ref()
            .ok_or(FleetCatalogError::Invalid("binding movement"))?
            .plan()?;
        if let Some(prior) = self
            .resource_binding_row(
                snapshot,
                &record.request.group,
                plan.replacement.member,
                plan.replacement.node,
            )
            .await?
        {
            let old = prior
                .permit
                .as_ref()
                .ok_or(FleetCatalogError::Corrupt("binding permit"))?;
            let new = record
                .permit
                .as_ref()
                .ok_or(FleetCatalogError::Invalid("binding permit"))?;
            if old.sequence() >= new.sequence()
                || prior.request.attestation.boot.disk != record.request.attestation.boot.disk
                || prior.request.attestation.boot.engine != record.request.attestation.boot.engine
                || !matches!(
                    self.resource_settlement(snapshot, &prior).await?,
                    Some((Settlement::Fenced, _))
                )
            {
                return Err(FleetCatalogError::Conflict);
            }
        }
        Ok(())
    }

    /// Historical binding provenance only. The receipt must independently prove
    /// a Complete or retained Aborted target. Empty/fenced, orphan, different
    /// engine and unconfirmed records cannot legitimize physical occupancy.
    pub(in crate::raft::fleet_catalog) async fn retained_resource_binding(
        &self,
        snapshot: &FleetSnapshot,
        group: &str,
        member: MemberId,
        boot: &BootBinding,
    ) -> Result<bool> {
        let node =
            NodeId::new(boot.node).map_err(|_| FleetCatalogError::Corrupt("binding node"))?;
        let Some(row) = self
            .resource_binding_row(snapshot, group, member, node)
            .await?
        else {
            return Ok(false);
        };
        if row.request.attestation.boot.disk != boot.disk
            || row.request.attestation.boot.engine != boot.engine
        {
            return Err(FleetCatalogError::Conflict);
        }
        self.resource_retained_target(snapshot, &row).await
    }
}
