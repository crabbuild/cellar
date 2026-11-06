// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Catalog-required voter lineages for a managed transition fence.
use super::*;
use crate::raft::{
    catalog::{ManagedTransitionRecoverySelection, RangeDirectory},
    cell_groups::CellGroupArchiveCoverage,
};
use cellar_logic::{
    cell_group::GroupHistory,
    consensus::{DiskLineage, IncarnationId, MemberId, NodeId},
};
use std::collections::{BTreeMap, BTreeSet};

const MAX_RECOVERY_LINEAGES: usize = 4096;

/// Permanent voter or learner placement a deployment fence must cover. A
/// platform also needs its complete launch inventory for this disk and host.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct FleetRecoveryVoterLineage {
    pub group: GroupHistory,
    pub member: MemberId,
    pub node: NodeId,
    pub disk: DiskLineage,
    pub physical_host: IncarnationId,
}

pub(super) struct RecoveryLineages {
    nodes: BTreeMap<NodeId, (DiskLineage, IncarnationId)>,
    required: BTreeSet<FleetRecoveryVoterLineage>,
}

impl RecoveryLineages {
    pub(super) fn include(
        &mut self,
        group: GroupHistory,
        member: MemberId,
        node: NodeId,
        expected_disk: Option<DiskLineage>,
    ) -> Result<()> {
        let &(disk, physical_host) = self
            .nodes
            .get(&node)
            .ok_or(FleetCatalogError::InventoryIncomplete)?;
        if expected_disk.is_some_and(|expected| expected != disk) {
            return Err(FleetCatalogError::Conflict);
        }
        self.required.insert(FleetRecoveryVoterLineage {
            group,
            member,
            node,
            disk,
            physical_host,
        });
        if self.required.len() > MAX_RECOVERY_LINEAGES {
            return Err(FleetCatalogError::Limit);
        }
        Ok(())
    }
}

impl FleetCatalog {
    /// Collect both sides from one exact Fleet root. Keep retired and reserved
    /// placements, including admitted replacements that never became voters.
    pub(super) async fn transition_recovery_voter_lineages<'a>(
        &self,
        snapshot: &FleetSnapshot,
        directory: &RangeDirectory,
        selection: &ManagedTransitionRecoverySelection,
        archives: impl Iterator<Item = &'a CellGroupArchiveCoverage>,
    ) -> Result<Vec<FleetRecoveryVoterLineage>> {
        let mut nodes = BTreeMap::new();
        for record in self.node_records(snapshot).await? {
            let node = self.host_record(snapshot, record)?;
            nodes.insert(node.node(), (node.disk(), node.physical_host()));
        }
        let mut lineages = RecoveryLineages {
            nodes,
            required: BTreeSet::new(),
        };
        let mut groups = BTreeMap::new();
        for route in selection.parents.iter().chain(&selection.children) {
            let binding = self.binding(&route.history)?;
            let key = binding
                .key()
                .map_err(|_| FleetCatalogError::Corrupt("recovery history key"))?;
            let history = self.history_inner(snapshot, &key).await?;
            if history.binding() != &binding || !history.check_route(directory, route) {
                return Err(FleetCatalogError::Conflict);
            }
            for member in history.voters.iter().chain(&history.learners) {
                let id = MemberId::new(member.member)
                    .map_err(|_| FleetCatalogError::Corrupt("recovery initial member"))?;
                let node = NodeId::new(member.node)
                    .map_err(|_| FleetCatalogError::Corrupt("recovery initial node"))?;
                lineages.include(route.history, id, node, None)?;
            }
            groups.insert(key, route.history);
        }
        for archive in archives {
            for member in archive.recovery().membership().retained_lineages() {
                let member = member
                    .map_err(|_| FleetCatalogError::Corrupt("recovery archive membership"))?;
                lineages.include(
                    archive.marker().route().history,
                    member.member,
                    member.node,
                    Some(member.disk),
                )?;
            }
        }
        self.collect_recovery_movements(snapshot, &groups, &mut lineages)
            .await?;
        Ok(lineages.required.into_iter().collect())
    }
}
