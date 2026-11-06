// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Reconcile the fleet's park reservation with physical voter evidence.

use super::*;
use crate::{
    application::consensus::RaftGroupHandle,
    raft::{
        cell_groups::CellGroupControllerError,
        completion::RuntimeError,
        fleet_catalog::{ManagedParkOperation, ManagedParkPhase, ManagedParkSettlement},
        transport::TransportError,
    },
};
use cellar_logic::cell_group::GroupHistory;
use serde::{Deserialize, Serialize};

/// Bind an operator request to one group incarnation and fleet revision.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RaftFleetParkRequest {
    pub catalog_id: String,
    pub namespace_id: String,
    pub group: RaftFleetTransitionGroup,
    pub operation_id: String,
    pub request_id: String,
    pub settlement_request_id: String,
    pub expected_revision: u64,
}

impl RaftFleetParkRequest {
    pub fn validate(&self, namespace: &str, operation: &str) -> Result<()> {
        if self.namespace_id != namespace || self.operation_id != operation {
            return Err(RaftFleetFailure::invalid("park selector mismatch"));
        }
        self.ids().map(|_| ())
    }

    fn ids(&self) -> Result<(ProposalId, [u8; 16], [u8; 16])> {
        records::catalog_id(&self.catalog_id)?;
        records::revision(self.expected_revision)?;
        for id in [
            &self.namespace_id,
            &self.group.group_id,
            &self.group.incarnation_id,
        ] {
            records::id(id)?;
        }
        let id = records::id(&self.request_id)?;
        let settlement_id = records::id(&self.settlement_request_id)?;
        if id == settlement_id {
            return Err(RaftFleetFailure::invalid(
                "Park admission and settlement require distinct request IDs.",
            ));
        }
        let operation = ProposalId::new(records::id(&self.operation_id)?)
            .map_err(|_| RaftFleetFailure::invalid("park operation ID"))?;
        Ok((operation, id, settlement_id))
    }
}

/// Historical outcome of the reservation, independent of current readiness.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RaftFleetParkPhase {
    Negotiating,
    Delivered,
    Superseded,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RaftFleetParkStatus {
    pub catalog_id: String,
    pub namespace_id: String,
    pub group: RaftFleetTransitionGroup,
    pub operation_id: String,
    pub generation: u64,
    pub phase: RaftFleetParkPhase,
    pub observed_revision: u64,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RaftFleetWakeStatus {
    pub park: RaftFleetParkStatus,
    pub applied_index: u64,
    pub applied_term: u64,
}

struct Selection {
    history: GroupHistory,
    operation: ProposalId,
    identity: FleetRequestIdentity,
    settlement_id: [u8; 16],
}

impl RaftFleetControl {
    /// Deliver the park certificate to every registered voter and settle the
    /// reservation. Retry with the same request after an unknown result.
    pub async fn park_group(
        &self,
        principal: &str,
        request: &RaftFleetParkRequest,
        timeout: Duration,
    ) -> Result<RaftFleetParkStatus> {
        let turn = Turn::new(&self.domain, timeout)?;
        let selection = self.park_selection(principal, request)?;
        let existing = self.observe_park(&selection, &turn).await?;
        let voter = match &existing {
            Some(park) if park.phase != ManagedParkPhase::Negotiating => {
                return park_status(request, park);
            }
            _ => self.park_voter(selection.history)?,
        };
        let permit = if let Some(park) = existing {
            park.permit
        } else {
            let snapshot = self.catalog.read(turn.remaining()?).await?;
            if snapshot.revision() != selection.identity.expected_revision {
                return Err(FleetCatalogError::Conflict.into());
            }
            let observation = voter
                .observe_park_admission(turn.remaining()?)
                .await
                .map_err(park_failure)?;
            self.catalog
                .admit_managed_park(
                    &snapshot,
                    &selection.identity,
                    &observation,
                    turn.remaining()?,
                )
                .await?
            // The observation pins the leader's active state until dropped.
        };
        let receipt = voter
            .execute_managed_park(&permit, turn.remaining()?)
            .await
            .map_err(park_failure)?;
        let snapshot = self.catalog.read(turn.remaining()?).await?;
        let settled = self
            .catalog
            .settle_managed_park(
                &snapshot,
                &selection.settlement(snapshot.revision()),
                &permit,
                ManagedParkSettlement::Delivered(&receipt),
                turn.remaining()?,
            )
            .await?;
        park_status(
            request,
            &ManagedParkOperation {
                permit,
                phase: ManagedParkPhase::Delivered,
                revision: settled.snapshot.revision(),
            },
        )
    }

    /// Read the rooted operation without sending any messages to voters.
    pub async fn park_group_status(
        &self,
        principal: &str,
        request: &RaftFleetParkRequest,
        timeout: Duration,
    ) -> Result<Option<RaftFleetParkStatus>> {
        let turn = Turn::new(&self.domain, timeout)?;
        let selection = self.park_selection(principal, request)?;
        self.observe_park(&selection, &turn)
            .await?
            .as_ref()
            .map(|park| park_status(request, park))
            .transpose()
    }

    /// Wake the exact generation and wait for a fresh quorum read. A pending
    /// reservation settles only once the current leader proves a later cut.
    pub async fn wake_group(
        &self,
        principal: &str,
        request: &RaftFleetParkRequest,
        timeout: Duration,
    ) -> Result<RaftFleetWakeStatus> {
        let turn = Turn::new(&self.domain, timeout)?;
        let selection = self.park_selection(principal, request)?;
        let mut park =
            self.observe_park(&selection, &turn)
                .await?
                .ok_or_else(|| RaftFleetFailure {
                    code: RaftFleetFailureCode::NotFound,
                    outcome: RaftFleetOutcome::NotSubmitted,
                    message: "The park operation is absent from the fleet catalog.".into(),
                })?;
        let unsettled = park.phase == ManagedParkPhase::Negotiating;
        let observation = self
            .park_voter(selection.history)?
            .wake_managed_park(&park.permit, unsettled, turn.remaining()?)
            .await
            .map_err(park_failure)?;
        let applied = observation.root().position();
        if unsettled {
            let snapshot = self.catalog.read(turn.remaining()?).await?;
            let settled = self
                .catalog
                .settle_managed_park(
                    &snapshot,
                    &selection.settlement(snapshot.revision()),
                    &park.permit,
                    ManagedParkSettlement::Advanced(&observation),
                    turn.remaining()?,
                )
                .await?;
            park.phase = ManagedParkPhase::Superseded;
            park.revision = settled.snapshot.revision();
        }
        Ok(RaftFleetWakeStatus {
            park: park_status(request, &park)?,
            applied_index: applied.index.get(),
            applied_term: applied.term.get(),
        })
    }

    fn park_selection(&self, principal: &str, request: &RaftFleetParkRequest) -> Result<Selection> {
        let (operation, id, settlement_id) = request.ids()?;
        self.check_catalog(&request.catalog_id)?;
        let directory = self.managed_namespace(&request.namespace_id)?;
        Ok(Selection {
            history: request.group.history(directory)?,
            operation,
            identity: FleetRequestIdentity {
                id,
                operation: operation.bytes(),
                principal: principal.into(),
                expected_revision: request.expected_revision,
            },
            settlement_id,
        })
    }

    async fn observe_park(
        &self,
        selection: &Selection,
        turn: &Turn<'_>,
    ) -> Result<Option<ManagedParkOperation>> {
        let observed = self
            .catalog
            .observe_managed_park(selection.operation, turn.remaining()?)
            .await?;
        if observed.as_ref().is_some_and(|park| {
            !park
                .permit
                .matches_request(selection.history, &selection.identity)
        }) {
            return Err(FleetCatalogError::PayloadConflict.into());
        }
        Ok(observed)
    }

    fn park_voter(&self, history: GroupHistory) -> Result<RaftGroupHandle> {
        self.node_observer
            .as_ref()
            .ok_or_else(RaftFleetFailure::unsupported)?
            .group(history)
            .map_err(|error| park_failure(error.into()))
    }
}

impl Selection {
    fn settlement(&self, revision: u64) -> FleetRequestIdentity {
        FleetRequestIdentity {
            id: self.settlement_id,
            expected_revision: revision,
            ..self.identity.clone()
        }
    }
}

fn park_status(
    request: &RaftFleetParkRequest,
    park: &ManagedParkOperation,
) -> Result<RaftFleetParkStatus> {
    let phase = match park.phase {
        ManagedParkPhase::Negotiating => RaftFleetParkPhase::Negotiating,
        ManagedParkPhase::Delivered => RaftFleetParkPhase::Delivered,
        ManagedParkPhase::Superseded => RaftFleetParkPhase::Superseded,
    };
    Ok(RaftFleetParkStatus {
        catalog_id: request.catalog_id.clone(),
        namespace_id: request.namespace_id.clone(),
        group: request.group.clone(),
        operation_id: request.operation_id.clone(),
        generation: park.permit.generation()?.get(),
        phase,
        observed_revision: park.revision,
    })
}

fn park_failure(error: CellGroupControllerError) -> RaftFleetFailure {
    use RaftFleetFailureCode as Code;
    let code = match error {
        CellGroupControllerError::Runtime(error)
        | CellGroupControllerError::Transport(TransportError::Runtime(error)) => match error {
            RuntimeError::Overloaded => Code::Busy,
            RuntimeError::DeadlineExceeded => Code::WaitTimeout,
            RuntimeError::StaleCompletion => Code::Conflict,
            RuntimeError::NotFound => Code::NotFound,
            _ => Code::Unavailable,
        },
        CellGroupControllerError::Transport(TransportError::Timeout) => Code::WaitTimeout,
        CellGroupControllerError::Transport(TransportError::Overloaded) => Code::Busy,
        _ => Code::Unavailable,
    };
    RaftFleetFailure {
        code,
        outcome: RaftFleetOutcome::Unknown,
        message: "The group operation could not be confirmed. Check its status and retry the exact request on the current leader.".into(),
    }
}
