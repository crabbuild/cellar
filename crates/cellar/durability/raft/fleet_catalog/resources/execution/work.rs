// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Reconstruct exact admitted work; cached descriptors never mint a lease.
use super::*;
use crate::raft::fleet_catalog::deadline::Deadline;
use crate::raft::inspection::InspectionError;
use cellar_logic::consensus::MemberId;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub(crate) enum MovementAction {
    Begin,
    Activate {
        source: u64,
        source_node: [u8; 16],
        from_s3: bool,
    },
    Promote {
        revision: u64,
    },
    Finish {
        revision: u64,
    },
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct MovementCommand {
    pub reservation: [u8; 16],
    pub resource_digest: [u8; 32],
    pub target: BootBinding,
    pub action: MovementAction,
}

impl MovementAction {
    pub(super) fn finishes_joint(&self) -> bool {
        matches!(self, Self::Finish { .. })
    }
}

impl MovementCommand {
    pub(crate) fn validate(&self) -> Result<()> {
        ProposalId::new(self.reservation).map_err(|_| invalid())?;
        self.target.validate().map_err(|_| invalid())?;
        if self.resource_digest == [0; 32] {
            return Err(invalid());
        }
        match self.action {
            MovementAction::Activate {
                source,
                source_node,
                ..
            } => {
                MemberId::new(source).map_err(|_| invalid())?;
                NodeId::new(source_node).map_err(|_| invalid())?;
            }
            MovementAction::Promote { revision } | MovementAction::Finish { revision }
                if revision == 0 || revision >= u64::MAX - 2 =>
            {
                return Err(invalid())
            }
            _ => (),
        }
        Ok(())
    }
}

impl FleetCatalog {
    pub(super) async fn check_executable_movement(
        &self,
        snapshot: &FleetSnapshot,
        row: &Record,
    ) -> Result<()> {
        let permit = row.permit.as_ref().ok_or_else(invalid)?;
        let movement = row.request.movement.as_ref().ok_or_else(invalid)?;
        permit
            .check_plan(&movement.plan()?)
            .map_err(|_| invalid())?;
        let book = self.resource_book(snapshot).await?;
        let latest = self
            .inner
            .lookup(
                snapshot.head.tables[FleetTable::Admissions.slot()],
                &row.request.group,
            )
            .await?;
        let history = self.history_inner(snapshot, &row.request.group).await?;
        if !book.rows.contains(&Reference {
            id: row.request.identity.id,
            digest: hash(&row.encode()?),
        }) || latest.as_deref() != Some(permit.encode().map_err(|_| invalid())?.as_slice())
            || history.binding() != permit.binding()
            || *history.state() != FleetHistoryState::Live
        {
            return Err(FleetCatalogError::Conflict);
        }
        Ok(())
    }

    pub(super) async fn movement_boot(
        &self,
        snapshot: &FleetSnapshot,
        row: &Record,
        node: NodeId,
        retained_target: bool,
    ) -> Result<BootBinding> {
        let host = self.host_from(snapshot, node).await?;
        if !host.state().permits_admitted_movement() {
            return Err(FleetCatalogError::Conflict);
        }
        let boot = host.boot_binding()?;
        if node == row.node {
            if boot != row.request.attestation.boot {
                if !retained_target {
                    return Err(FleetCatalogError::Conflict);
                }
                self.confirm_retained_movement_target(snapshot, row, &boot)
                    .await?;
            }
        } else {
            let owner = row
                .seed_owners
                .as_ref()
                .ok_or_else(invalid)?
                .iter()
                .find(|owner| owner.node == node.bytes())
                .ok_or_else(invalid)?;
            if !owner.matches(&boot)? {
                return Err(FleetCatalogError::Conflict);
            }
        }
        Ok(boot)
    }

    pub(super) async fn movement_leader(
        &self,
        snapshot: &FleetSnapshot,
        row: &Record,
        observer: &RaftCells,
        deadline: &Deadline<'_>,
    ) -> Result<RaftPeerFleetMembershipObservation> {
        let permit = row.permit.as_ref().ok_or_else(invalid)?;
        let scope = permit.binding().scope().map_err(|_| invalid())?;
        let authorization = self
            .authorize_group(snapshot, &scope, deadline.remaining()?)
            .await?;
        let mut nodes = vec![row
            .request
            .movement
            .as_ref()
            .ok_or_else(invalid)?
            .source()?];
        for owner in row.seed_owners.as_ref().ok_or_else(invalid)? {
            let node = NodeId::new(owner.node).map_err(|_| invalid())?;
            if !nodes.contains(&node) {
                nodes.push(node);
            }
        }
        nodes.push(row.node);
        // A lost observation or a concurrent leader change can leave one
        // sweep empty despite a live quorum. Resample once within the original
        // turn budget; the caller still dispatches each action only once.
        for _ in 0..2 {
            let mut retryable = false;
            for &node in &nodes {
                let boot = match self
                    .run(
                        deadline.remaining()?,
                        self.movement_boot(snapshot, row, node, true),
                    )
                    .await
                {
                    Ok(boot) => boot,
                    Err(FleetCatalogError::Conflict) => continue,
                    Err(error) => return Err(error),
                };
                let sample = match observer
                    .observe_peer_fleet_membership(
                        node,
                        authorization.clone(),
                        snapshot.revision(),
                        deadline.remaining()?.min(Duration::from_millis(500)),
                    )
                    .await
                {
                    Ok(sample) => sample,
                    Err(InspectionError::Overloaded) => return Err(FleetCatalogError::Overloaded),
                    Err(InspectionError::Unreachable | InspectionError::Unavailable) => {
                        retryable = true;
                        continue;
                    }
                    Err(InspectionError::Refused | InspectionError::StalePeer) => continue,
                };
                if sample.sample_age_ms() > MAX_SAMPLE_AGE_MS
                    || sample.node().fleet_boot.as_ref() != Some(&boot)
                    || sample.membership().status().binding != *permit.binding()
                {
                    return Err(FleetCatalogError::Unconfirmed);
                }
                let status = sample.membership().status();
                if node == row.node
                    && boot != row.request.attestation.boot
                    && (status.permit.as_ref() != Some(permit)
                        || status.consumed_sequence != permit.sequence()
                        || !matches!(
                            status.phase,
                            Some(FleetPermitPhase::Joint | FleetPermitPhase::Complete)
                        ))
                {
                    continue;
                }
                if sample
                    .progress()
                    .member
                    .as_ref()
                    .is_some_and(|member| member.is_leader)
                {
                    return Ok(sample);
                }
                retryable = true;
            }
            if !retryable {
                break;
            }
        }
        Err(FleetCatalogError::Unconfirmed)
    }

    pub(super) async fn send_movement_action(
        &self,
        snapshot: &FleetSnapshot,
        row: &Record,
        node: NodeId,
        action: MovementAction,
        observer: &RaftCells,
        deadline: &Deadline<'_>,
    ) -> Result<()> {
        let command = self
            .run(deadline.remaining()?, async {
                if self.inner.current().await?.token != snapshot.token {
                    return Err(FleetCatalogError::Conflict);
                }
                self.check_executable_movement(snapshot, row).await?;
                let command = MovementCommand {
                    reservation: row.request.identity.id,
                    resource_digest: hash(&row.encode()?),
                    target: self
                        .movement_boot(snapshot, row, node, action.finishes_joint())
                        .await?,
                    action,
                };
                command.validate()?;
                Ok(command)
            })
            .await?;
        observer
            .request_peer_movement(command, snapshot.revision(), deadline.remaining()?)
            .await
    }
}
