// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Durable permit/configuration progress is the cursor. Each turn performs at
//! most one seed and one configuration action, or one terminal settlement CAS.
mod receive;
mod service;
mod work;
use super::completion::Settlement;
use super::records::{Record, Reference};
use super::*;
use crate::application::consensus::RaftCells;
use crate::raft::host::RaftPeerFleetMembershipObservation;
use crate::raft::member_state::FleetPermitPhase;
use cellar_logic::consensus::ProposalId;
use rand::RngCore;
pub(crate) use service::MovementServiceWork;
pub use service::{
    FleetMovementEvidence, FleetMovementService, FleetMovementServiceEntry,
    FleetMovementServiceFailure, FleetMovementServiceOptions, FleetMovementServiceStatus,
};
pub(crate) use work::{MovementAction, MovementCommand};

/// Caller-retained deployment occupancy evidence is required for settlement.
/// The receiver separately measures current physical lower bounds. This value
/// grants no reservation and cannot change an admitted move's plan.
#[derive(Clone, Debug)]
pub struct FleetMovementExecutionRequest {
    pub reservation: [u8; 16],
    pub attestation: FleetResourceAttestation,
    pub from_s3: bool,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum FleetMovementPhase {
    Reserved,
    Seeding,
    Joint,
    Complete,
    Fenced,
    Aborted,
}

/// Phase is the fresh observation before this turn's action, never a prediction
/// from an RPC acknowledgement. Only settled=true denotes the verified terminal
/// receipt/root-CAS boundary; retaining it supplies no current-health lease.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct FleetMovementProgress {
    pub reservation: [u8; 16],
    pub phase: FleetMovementPhase,
    pub settled: bool,
    pub observed_revision: u64,
}

/// Descriptive bounded active-work inventory; it is not an execution grant.
#[derive(Clone, Debug)]
pub struct FleetPendingMovement {
    pub reservation: [u8; 16],
    pub group: GroupScope,
    pub plan: crate::raft::member_state::RepairPlan,
}

impl FleetCatalog {
    /// List active resource-backed membership moves (at most two). Resource-only
    /// reservations have no executable membership intent and are not returned.
    pub async fn pending_movements(
        &self,
        snapshot: &FleetSnapshot,
        timeout: Duration,
    ) -> Result<Vec<FleetPendingMovement>> {
        self.check_snapshot(snapshot)?;
        self.run(timeout, async {
            if self.inner.current().await?.token != snapshot.token {
                return Err(FleetCatalogError::Conflict);
            }
            let book = self.resource_book(snapshot).await?;
            let rows = self.resource_rows(snapshot, &book).await?;
            rows.into_iter()
                .filter(|row| row.request.movement.is_some())
                .map(|row| {
                    Ok(FleetPendingMovement {
                        reservation: row.request.identity.id,
                        group: row
                            .permit
                            .as_ref()
                            .ok_or_else(invalid)?
                            .binding()
                            .scope()
                            .map_err(|_| invalid())?,
                        plan: row.request.movement.as_ref().ok_or_else(invalid)?.plan()?,
                    })
                })
                .collect()
        })
        .await
    }

    /// Reconcile one exact admitted move through the authenticated control lane.
    /// Recreated callers discover the same work from pending_movements. A lost
    /// action reply requires another fresh observation; it never releases debt.
    /// No internal retry loop or application traffic is needed for progress.
    pub async fn reconcile_movement(
        &self,
        snapshot: &FleetSnapshot,
        request: &FleetMovementExecutionRequest,
        observer: &RaftCells,
        timeout: Duration,
    ) -> Result<FleetMovementProgress> {
        self.check_snapshot(snapshot)?;
        ProposalId::new(request.reservation).map_err(|_| invalid())?;
        request.attestation.validate()?;
        let deadline = super::super::deadline::Deadline::new(self, timeout)?;
        let (row, terminal) = self
            .run(deadline.remaining()?, async {
                if self.inner.current().await?.token != snapshot.token {
                    return Err(FleetCatalogError::Conflict);
                }
                let row = self
                    .admitted_resource(snapshot, request.reservation)
                    .await?;
                let terminal = self.resource_settlement(snapshot, &row).await?;
                if terminal.is_none() {
                    self.check_executable_movement(snapshot, &row).await?;
                }
                Ok((row, terminal))
            })
            .await?;
        let progress = |phase, settled, observed_revision| FleetMovementProgress {
            reservation: request.reservation,
            phase,
            settled,
            observed_revision,
        };
        if let Some((kind, _)) = terminal {
            return Ok(progress(
                match kind {
                    Settlement::Complete => FleetMovementPhase::Complete,
                    Settlement::Fenced => FleetMovementPhase::Fenced,
                },
                true,
                snapshot.revision(),
            ));
        }
        if request.attestation.boot != row.request.attestation.boot
            || request.attestation.profile_revision != row.request.attestation.profile_revision
        {
            return Err(FleetCatalogError::Conflict);
        }
        let proof = self
            .movement_leader(snapshot, &row, observer, &deadline)
            .await?;
        let permit = row.permit.as_ref().ok_or_else(invalid)?;
        let status = proof.membership().status();
        let phase = if status.consumed_sequence.checked_add(1) == Some(permit.sequence())
            && status.phase.is_none_or(|phase| !phase.active())
            && proof.membership().membership_revision() == permit.expected_revision()
        {
            FleetMovementPhase::Reserved
        } else if status.permit.as_ref() == Some(permit)
            && status.consumed_sequence == permit.sequence()
        {
            match status.phase {
                Some(FleetPermitPhase::Seeding) => FleetMovementPhase::Seeding,
                Some(FleetPermitPhase::Joint) => FleetMovementPhase::Joint,
                Some(FleetPermitPhase::Complete) => FleetMovementPhase::Complete,
                Some(FleetPermitPhase::Fenced) => FleetMovementPhase::Fenced,
                Some(FleetPermitPhase::Aborted) => FleetMovementPhase::Aborted,
                None => return Err(FleetCatalogError::Conflict),
            }
        } else {
            return Err(FleetCatalogError::Conflict);
        };
        let leader = proof.membership().replica().node;
        let revision = proof.membership().membership_revision().get();
        let action = match phase {
            FleetMovementPhase::Reserved => MovementAction::Begin,
            FleetMovementPhase::Seeding => {
                self.send_movement_action(
                    snapshot,
                    &row,
                    row.node,
                    MovementAction::Activate {
                        source: proof.membership().replica().member.get(),
                        source_node: leader.bytes(),
                        from_s3: request.from_s3,
                    },
                    observer,
                    &deadline,
                )
                .await?;
                MovementAction::Promote { revision }
            }
            FleetMovementPhase::Joint => MovementAction::Finish { revision },
            FleetMovementPhase::Aborted => return Ok(progress(phase, false, snapshot.revision())),
            FleetMovementPhase::Complete | FleetMovementPhase::Fenced => {
                // Every attempt removes the same exact active row. On an
                // ambiguous reply the next turn first reads its permanent
                // receipt, including after all caller-local state was lost.
                let identity = self
                    .run(deadline.remaining()?, async {
                        let mut id = [0; 16];
                        crate::asyncrt::rng("fleet-movement-settlement").fill_bytes(&mut id);
                        ProposalId::new(id).map_err(|_| FleetCatalogError::Limit)?;
                        Ok(FleetRequestIdentity {
                            id,
                            operation: row.request.identity.operation,
                            principal: "fleet-movement-executor".into(),
                            expected_revision: snapshot.revision(),
                        })
                    })
                    .await?;
                let settlement = FleetMovementCompletionRequest {
                    identity,
                    reservation: request.reservation,
                    attestation: request.attestation.clone(),
                };
                let observed = match phase {
                    FleetMovementPhase::Complete => self
                        .complete_movement(snapshot, &settlement, observer, deadline.remaining()?)
                        .await?
                        .observed_revision(),
                    _ => self
                        .settle_fenced_movement(
                            snapshot,
                            &settlement,
                            observer,
                            deadline.remaining()?,
                        )
                        .await?
                        .observed_revision(),
                };
                return Ok(progress(phase, true, observed));
            }
        };
        self.send_movement_action(snapshot, &row, leader, action, observer, &deadline)
            .await?;
        Ok(progress(phase, false, snapshot.revision()))
    }
}

fn invalid() -> FleetCatalogError {
    FleetCatalogError::Invalid("movement execution")
}
