// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Explicit bounded controller turns; durable roots own ambiguous work.
use super::*;
use crate::application::consensus::RaftCells;
use crate::raft::catalog::NamespaceCatalog;
use cellar_logic::consensus::GroupScope;
use rand::RngCore;
use std::pin::Pin;

/// Explicit intent for an undecided reserved name. Either policy follows an
/// already permanent winner. An active claim cannot change this choice.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum FleetRestartBirthResolution {
    Finish,
    Cancel,
}

/// Descriptive active claim. Only its owned winner/confirmation path can settle
/// it; these fields grant no namespace mutation or cancellation authority.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct FleetRestartBirthAction {
    pub admission: [u8; 16],
    pub group: GroupScope,
    pub resolution: FleetRestartBirthResolution,
}

#[derive(Clone, Debug, PartialEq, Eq)]
/// Missing integration or an unexecutable debit never becomes release credit.
pub enum FleetRestartSettlementBlocker {
    ResourceOnly {
        reservation: [u8; 16],
        group: GroupScope,
    },
    MovementServicesUnavailable {
        reservation: [u8; 16],
        group: GroupScope,
    },
}

/// CatalogClear means no rooted pending work at this cut. It is never live
/// configuration, survivor, handoff, RestartReady or stop authority.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum FleetRestartSettlementProgress {
    Birth {
        admission: [u8; 16],
        resolution: FleetRestartBirthResolution,
        group: GroupScope,
        state: FleetHistoryState,
        applied_revision: u64,
        observed_revision: u64,
    },
    Movement(FleetMovementProgress),
    Blocked(FleetRestartSettlementBlocker),
    CatalogClear {
        observed_revision: u64,
    },
}

pub(super) fn identity(
    snapshot: &FleetSnapshot,
    operation: [u8; 16],
) -> Result<FleetRequestIdentity> {
    let mut id = [0; 16];
    crate::asyncrt::rng("fleet-restart-settlement").fill_bytes(&mut id);
    ProposalId::new(id).map_err(|_| FleetCatalogError::Limit)?;
    Ok(FleetRequestIdentity {
        id,
        operation,
        principal: "fleet-restart-settler".into(),
        expected_revision: snapshot.revision(),
    })
}

impl FleetCatalog {
    /// Advance one pending birth or one admitted movement under one original
    /// deadline. Requires the rooted reversible gate. A durable birth claim
    /// precedes namespace I/O and prevents cancellation until its actual winner
    /// and root confirmation settle. Restart/drop resumes that same claim.
    /// The resolution choice applies only to an undecided reserved name;
    /// permanent winners are preserved and an active choice cannot be rebound.
    /// Missing evidence and resource-only debits are explicit blockers; no
    /// hidden mutation retry or inferred physical release occurs.
    pub fn reconcile_restart_work<'a>(
        &'a self,
        operation: ProposalId,
        resolution: FleetRestartBirthResolution,
        namespaces: &'a [NamespaceCatalog],
        observer: Option<&'a RaftCells>,
        evidence: Option<&'a dyn FleetMovementEvidence>,
        timeout: Duration,
    ) -> Pin<Box<dyn Future<Output = Result<FleetRestartSettlementProgress>> + Send + 'a>> {
        Box::pin(async move {
            let deadline = super::super::deadline::Deadline::new(self, timeout)?;
            let snapshot = self.read(deadline.remaining()?).await?;
            let active = self
                .run(deadline.remaining()?, async {
                    self.checked_restart_gate(&snapshot, operation.bytes())
                        .await?;
                    let active = self
                        .active_restart_birth(&snapshot, operation.bytes())
                        .await?;
                    if active.as_ref().is_some_and(|a| a.resolution != resolution) {
                        return Err(FleetCatalogError::PayloadConflict);
                    }
                    Ok(active)
                })
                .await?;
            let work = self
                .pending_restart_work(&snapshot, operation, namespaces, deadline.remaining()?)
                .await?;
            let (action, snapshot) = if let Some(action) = active {
                (action, snapshot)
            } else if let Some(scope) = work.next_birth {
                let namespace = namespaces
                    .iter()
                    .find(|n| n.config().namespace == scope.namespace)
                    .ok_or(FleetCatalogError::InventoryIncomplete)?;
                self.recover_birth_reservation(&snapshot, namespace, &scope, deadline.remaining()?)
                    .await?;
                self.admit_restart_birth(
                    &snapshot,
                    operation,
                    &scope,
                    resolution,
                    deadline.remaining()?,
                )
                .await?
            } else if let Some(movement) = work.movements.first() {
                let (Some(observer), Some(evidence)) = (observer, evidence) else {
                    return Ok(FleetRestartSettlementProgress::Blocked(
                        FleetRestartSettlementBlocker::MovementServicesUnavailable {
                            reservation: movement.reservation,
                            group: movement.group.clone(),
                        },
                    ));
                };
                let request = self
                    .inner
                    .domain
                    .run(Box::pin(crate::asyncrt::timeout(
                        deadline.remaining()?,
                        evidence.refresh(movement, deadline.remaining()?),
                    )))
                    .await
                    .map_err(|_| FleetCatalogError::Closed)?
                    .map_err(|_| FleetCatalogError::Unconfirmed)??;
                if request.reservation != movement.reservation {
                    return Err(FleetCatalogError::PayloadConflict);
                }
                return self
                    .reconcile_movement(&snapshot, &request, observer, deadline.remaining()?)
                    .await
                    .map(FleetRestartSettlementProgress::Movement);
            } else if let Some(reservation) = work.reservations.first() {
                return Ok(FleetRestartSettlementProgress::Blocked(
                    FleetRestartSettlementBlocker::ResourceOnly {
                        reservation: reservation.reservation,
                        group: reservation.group.clone(),
                    },
                ));
            } else {
                return Ok(FleetRestartSettlementProgress::CatalogClear {
                    observed_revision: snapshot.revision(),
                });
            };
            let scope = self
                .run(deadline.remaining()?, async {
                    let history = self.history_inner(&snapshot, &action.group).await?;
                    action.check_history(&history)?;
                    history
                        .binding()
                        .scope()
                        .map_err(|_| FleetCatalogError::Corrupt("restart birth scope"))
                })
                .await?;
            let namespace = namespaces
                .iter()
                .find(|n| n.config().namespace == scope.namespace)
                .ok_or(FleetCatalogError::InventoryIncomplete)?;
            let authorization = self
                .recover_birth_reservation(&snapshot, namespace, &scope, deadline.remaining()?)
                .await?;
            let namespace_timeout = deadline.remaining()?;
            let decision = async {
                use crate::raft::catalog::{BirthDisposition, CatalogError};
                match action.resolution {
                    FleetRestartBirthResolution::Cancel => {
                        namespace
                            .cancel_managed_birth(
                                &authorization,
                                ProposalId::new(action.request.id)
                                    .map_err(|_| CatalogError::Corrupt("restart birth ID"))?,
                                namespace_timeout,
                            )
                            .await
                    }
                    FleetRestartBirthResolution::Finish => {
                        if let Some(winner) = namespace.birth_disposition(&scope.cell).await? {
                            return Ok(winner);
                        }
                        match namespace
                            .resolve_managed_birth(&authorization, namespace_timeout)
                            .await
                        {
                            Ok(birth) => Ok(BirthDisposition::Live(birth)),
                            Err(CatalogError::BirthCancelled) => namespace
                                .birth_disposition(&scope.cell)
                                .await?
                                .ok_or(CatalogError::Unconfirmed),
                            Err(error) => Err(error),
                        }
                    }
                }
            };
            let winner = self
                .inner
                .domain
                .run(Box::pin(crate::asyncrt::timeout(
                    namespace_timeout,
                    decision,
                )))
                .await
                .map_err(|_| FleetCatalogError::Closed)?
                .map_err(|_| FleetCatalogError::Unconfirmed)?
                .map_err(super::super::histories::catalog_error)?;
            let current = self.read(deadline.remaining()?).await?;
            self.complete_restart_birth(&current, &action, namespace, winner, deadline.remaining()?)
                .await
        })
    }
}
