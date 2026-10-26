// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Root-CAS placement fencing. Historical committed retries are resolved before
//! this guard; already rooted references may finish after exclusion.
use super::*;
use crate::raft::member_state::FleetMembershipPermit;

fn protected(change: &FleetChange) -> bool {
    change.table == FleetTable::Policy && matches!(change.key.as_str(), POLICY_KEY | ACTIVE_KEY)
        || change.table == FleetTable::Operations && change.key.starts_with(ROW_PREFIX)
}

impl FleetCatalog {
    pub(in super::super::super) async fn check_topology_mutation(
        &self,
        snapshot: &FleetSnapshot,
        change: &FleetMutation,
        authority: Authority,
    ) -> Result<()> {
        if !change.changes.iter().any(|c| {
            protected(c)
                || matches!(c.table, FleetTable::Histories | FleetTable::Admissions)
                || c.table == FleetTable::Operations && c.key.starts_with(super::super::ROW_PREFIX)
                || c.table == FleetTable::Policy
                    && c.key == super::super::super::evacuation::SLOT_KEY
        }) {
            return Ok(());
        }
        Box::pin(self.check_topology_mutation_inner(snapshot, change, authority)).await
    }

    async fn check_topology_mutation_inner(
        &self,
        snapshot: &FleetSnapshot,
        change: &FleetMutation,
        authority: Authority,
    ) -> Result<()> {
        let protected_count = change.changes.iter().filter(|c| protected(c)).count();
        if protected_count > 0
            && !matches!(
                authority,
                Authority::Topology | Authority::TopologyAdmission
            )
        {
            return Err(FleetCatalogError::Invalid("topology mutation authority"));
        }
        if authority == Authority::Topology
            && (protected_count != change.changes.len()
                || !change.changes.iter().any(|c| {
                    c.table == FleetTable::Operations && c.key == row_key(change.operation)
                }))
        {
            return Err(FleetCatalogError::Invalid("topology transition shape"));
        }
        let active = self.active_intent(snapshot).await?;
        if let Some(record) = &active {
            if change.changes.iter().any(|c| {
                c.table == FleetTable::Policy && c.key == super::super::super::evacuation::SLOT_KEY
            }) {
                return Err(FleetCatalogError::DisruptionInProgress {
                    operation: record.request.operation,
                });
            }
        }
        let policy = self.placement_policy(snapshot).await?;
        // With topology management unused, preserve the generic catalog's
        // opaque table contract. Typed placement validation applies only when
        // a rooted policy/operation governs admission. The protected-key and
        // private-authority checks above still apply to every mutation.
        if active.is_none() && policy.is_none() && authority != Authority::TopologyAdmission {
            return Ok(());
        }
        let keep = policy.as_ref().map(|p| p.definition.nodes()).transpose()?;
        let mut resource = None;
        for proposed in &change.changes {
            if proposed.table == FleetTable::Operations
                && proposed.key.starts_with(super::super::ROW_PREFIX)
            {
                let row = super::super::records::Record::decode(
                    self,
                    proposed
                        .value
                        .as_deref()
                        .ok_or(FleetCatalogError::Invalid("resource deletion"))?,
                )?;
                if keep.as_ref().is_some_and(|set| !set.contains(&row.node)) {
                    return Err(FleetCatalogError::Invalid("excluded resource target"));
                }
                resource = Some(row);
            }
            if proposed.table == FleetTable::Histories {
                let Some(bytes) = &proposed.value else {
                    continue;
                };
                let history = super::super::super::histories::FleetHistory::decode(bytes)?;
                let old = self
                    .inner
                    .lookup(
                        snapshot.head.tables[FleetTable::Histories.slot()],
                        &proposed.key,
                    )
                    .await?
                    .map(|b| super::super::super::histories::FleetHistory::decode(&b))
                    .transpose()?;
                for member in history.voters.iter().chain(&history.learners) {
                    let existed = old
                        .as_ref()
                        .is_some_and(|h| h.voters.iter().chain(&h.learners).any(|m| m == member));
                    let node = NodeId::new(member.node)
                        .map_err(|_| FleetCatalogError::Corrupt("history target"))?;
                    if !existed && keep.as_ref().is_some_and(|set| !set.contains(&node)) {
                        return Err(FleetCatalogError::Invalid("excluded birth target"));
                    }
                }
            }
            if proposed.table == FleetTable::Admissions {
                let previous = self
                    .inner
                    .lookup(
                        snapshot.head.tables[FleetTable::Admissions.slot()],
                        &proposed.key,
                    )
                    .await?;
                if previous == proposed.value {
                    continue;
                }
                if let Some(record) = &active {
                    if authority != Authority::TopologyAdmission
                        || record.phase != FleetTopologyPhase::Running
                        || change.operation != record.request.operation
                    {
                        return Err(FleetCatalogError::DisruptionInProgress {
                            operation: record.request.operation,
                        });
                    }
                }
                if let Some(bytes) = &proposed.value {
                    let permit = FleetMembershipPermit::decode(bytes)
                        .map_err(|_| FleetCatalogError::Invalid("topology permit"))?;
                    let target = permit
                        .replacement()
                        .map_err(|_| FleetCatalogError::Invalid("topology replacement"))?
                        .node;
                    if keep.as_ref().is_some_and(|set| !set.contains(&target)) {
                        return Err(FleetCatalogError::Invalid("excluded membership target"));
                    }
                }
            }
        }
        if let Some(row) = resource {
            if let Some(record) = &active {
                if authority != Authority::TopologyAdmission
                    || record.phase != FleetTopologyPhase::Running
                    || change.operation != record.request.operation
                    || row.request.movement.is_none()
                {
                    return Err(FleetCatalogError::DisruptionInProgress {
                        operation: record.request.operation,
                    });
                }
            }
            if authority == Authority::TopologyAdmission {
                let record = active.as_ref().ok_or(FleetCatalogError::Conflict)?;
                let expected = self.next_topology_movement(snapshot, record, &row).await?;
                if change.changes.len() != 5
                    || protected_count != 1
                    || change.changes[4] != expected.change()?
                {
                    return Err(FleetCatalogError::Invalid("topology movement projection"));
                }
            }
        } else if authority == Authority::TopologyAdmission {
            return Err(FleetCatalogError::Invalid("missing topology debit"));
        }
        Ok(())
    }
}
