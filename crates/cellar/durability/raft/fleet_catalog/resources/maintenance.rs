// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Rooted continuation obligations; never infer absence from genesis placement.
use super::*;
use crate::raft::member_state::FleetMembershipPermit;
use cellar_logic::consensus::MemberId;
use std::collections::BTreeMap;

impl FleetCatalog {
    // Locate only settled, current voting obligations. An unrelated active
    // movement must not stall a restart, but a touching movement cannot be
    // mistaken for retained or absent occupancy.
    pub(in crate::raft::fleet_catalog) async fn restart_voters(
        &self,
        snapshot: &FleetSnapshot,
        history: &FleetHistory,
        target: NodeId,
    ) -> Result<Option<(BTreeMap<NodeId, MemberId>, Option<FleetMembershipPermit>)>> {
        let key = history
            .binding()
            .key()
            .map_err(|_| FleetCatalogError::Corrupt("restart group"))?;
        if let Some(bytes) = self
            .inner
            .lookup(snapshot.head.tables[FleetTable::Admissions.slot()], &key)
            .await?
        {
            let permit = FleetMembershipPermit::decode(&bytes)
                .map_err(|_| FleetCatalogError::Corrupt("restart permit"))?;
            let row = self.admitted_resource(snapshot, permit.id()).await?;
            if row.permit.as_ref() != Some(&permit) || permit.binding() != history.binding() {
                return Err(FleetCatalogError::Conflict);
            }
            if self.resource_settlement(snapshot, &row).await?.is_none() {
                let owners = row
                    .seed_owners
                    .as_ref()
                    .ok_or(FleetCatalogError::InventoryIncomplete)?;
                if row.node == target || owners.iter().any(|owner| owner.node == target.bytes()) {
                    return Err(FleetCatalogError::InventoryIncomplete);
                }
                return Ok(None);
            }
        }
        let (voters, permit) = self.settled_group_voters(snapshot, history).await?;
        Ok(voters.contains_key(&target).then_some((voters, permit)))
    }

    // Called inside the maintenance catalog turn after full inventory checking.
    pub(in crate::raft::fleet_catalog) async fn restart_pending_work_inner(
        &self,
        snapshot: &FleetSnapshot,
        node: NodeId,
    ) -> Result<FleetRestartPendingWork> {
        let mut result = FleetRestartPendingWork {
            observed_revision: snapshot.revision(),
            pending_births: 0,
            next_birth: None,
            active_birth: None,
            movements: vec![],
            reservations: vec![],
        };
        let mut cursor = None;
        loop {
            let page = self
                .inner
                .scan(
                    snapshot.head.tables[FleetTable::Histories.slot()],
                    cursor.as_ref(),
                    128,
                )
                .await?;
            for entry in page.entries {
                let history = FleetHistory::decode(&entry.value)?;
                if *history.state() == FleetHistoryState::Reserved
                    && history
                        .voters
                        .iter()
                        .chain(&history.learners)
                        .any(|member| member.node == node.bytes())
                {
                    result.pending_births = result
                        .pending_births
                        .checked_add(1)
                        .filter(|n| *n <= HISTORY_LIMIT)
                        .ok_or(FleetCatalogError::Limit)?;
                    if result.next_birth.is_none() {
                        result.next_birth = Some(
                            history
                                .binding()
                                .scope()
                                .map_err(|_| FleetCatalogError::Corrupt("restart birth scope"))?,
                        );
                    }
                }
            }
            cursor = page.next;
            if cursor.is_none() {
                break;
            }
        }
        let book = self.resource_book(snapshot).await?;
        let rows = self.resource_rows(snapshot, &book).await?;
        self.check_resource_permits(snapshot, &rows).await?;
        for row in rows {
            let history = self.history_inner(snapshot, &row.request.group).await?;
            let touches = row.node == node
                || match &row.seed_owners {
                    Some(owners) => owners.iter().any(|owner| owner.node == node.bytes()),
                    None => self
                        .settled_group_voters(snapshot, &history)
                        .await?
                        .0
                        .contains_key(&node),
                };
            if !touches {
                continue;
            }
            let group = history
                .binding()
                .scope()
                .map_err(|_| FleetCatalogError::Corrupt("restart resource scope"))?;
            if let Some(movement) = &row.request.movement {
                result.movements.push(FleetPendingMovement {
                    reservation: row.request.identity.id,
                    group: group.clone(),
                    plan: movement.plan()?,
                });
            }
            result.reservations.push(FleetPendingReservation {
                reservation: row.request.identity.id,
                group,
                target: row.node,
                boot: row.request.attestation.boot,
                peak: row.request.peak,
                executable_movement: row.request.movement.is_some(),
            });
        }
        Ok(result)
    }
}
