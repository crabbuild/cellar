// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Keep permanent movement admissions in the required recovery fence scope.
use super::records::Record;
use super::*;
use crate::raft::fleet_catalog::managed_recovery_inventory::RecoveryLineages;
use cellar_logic::consensus::MemberId;
use std::collections::BTreeMap;

impl FleetCatalog {
    pub(in crate::raft::fleet_catalog) async fn collect_recovery_movements(
        &self,
        snapshot: &FleetSnapshot,
        groups: &BTreeMap<String, GroupHistory>,
        lineages: &mut RecoveryLineages,
    ) -> Result<()> {
        let operations = snapshot.head.tables[FleetTable::Operations.slot()];
        let mut cursor = None;
        loop {
            let page = self.inner.scan(operations, cursor.as_ref(), 32).await?;
            for entry in page.entries {
                if !entry.key.starts_with(ROW_PREFIX) {
                    continue;
                }
                let candidate = Record::decode(self, &entry.value)?;
                let Some(&history) = groups.get(&candidate.request.group) else {
                    continue;
                };
                let id = candidate.request.identity.id;
                if entry.key != row_key(id) {
                    return Err(FleetCatalogError::Corrupt("recovery movement key"));
                }
                if candidate.request.movement.is_none() {
                    continue;
                }
                let admitted = self.admitted_resource(snapshot, id).await?;
                let replacement = admitted
                    .request
                    .movement
                    .as_ref()
                    .ok_or(FleetCatalogError::Corrupt("recovery movement plan"))?
                    .plan()?
                    .replacement;
                lineages.include(
                    history,
                    replacement.member,
                    replacement.node,
                    Some(replacement.disk),
                )?;
                for owner in admitted
                    .seed_owners
                    .as_ref()
                    .ok_or(FleetCatalogError::Corrupt("recovery movement owners"))?
                {
                    let node = NodeId::new(owner.node)
                        .map_err(|_| FleetCatalogError::Corrupt("recovery seed node"))?;
                    let member = MemberId::new(owner.member)
                        .map_err(|_| FleetCatalogError::Corrupt("recovery seed member"))?;
                    lineages.include(history, member, node, None)?;
                }
            }
            let Some(next) = page.next else {
                return Ok(());
            };
            cursor = Some(next);
        }
    }
}
