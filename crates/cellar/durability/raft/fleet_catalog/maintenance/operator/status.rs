// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Snapshot-scoped durable progress; polling never executes physical work.
use super::attempt::Stage;
use super::*;
use crate::raft::fleet_catalog::deadline::Deadline;

/// Root-journaled facts only. Phases and identities are descriptive: Ready is a
/// historical assessment and these fields confer no health or stop authority.
pub struct FleetRestartOperatorStatus {
    pub admission: FleetRestartOperatorAdmission,
    pub phase: FleetRestartOperatorPhase,
    pub execution_operation: Option<[u8; 16]>,
    pub readiness: Option<FleetRequestIdentity>,
    pub cancellation: Option<FleetRequestIdentity>,
    pub cancelled: Option<FleetRestartOperatorCancelled>,
}

impl FleetCatalog {
    /// Inspect a fixed Root without a configured driver or live observer. None
    /// means absent at this snapshot; an in-flight admission may still commit.
    pub fn restart_operator_status<'a>(
        &'a self,
        snapshot: &'a FleetSnapshot,
        operation: ProposalId,
        timeout: Duration,
    ) -> BoxFuture<'a, Result<Option<FleetRestartOperatorStatus>>> {
        Box::pin(async move {
            self.check_snapshot(snapshot)?;
            let deadline = Deadline::new(self, timeout)?;
            let Some(admission) = self
                .restart_operator(snapshot, operation, deadline.remaining()?)
                .await?
            else {
                return Ok(None);
            };
            let cancelled = self
                .restart_operator_cancelled(snapshot, operation, deadline.remaining()?)
                .await?;
            self.run(deadline.remaining()?, async {
                let attempt = self
                    .restart_operator_attempt(snapshot, &admission.record)
                    .await?;
                let cancellation = self
                    .operator_cancel_command(snapshot, &admission.record)
                    .await?;
                let execution_operation = attempt.as_ref().map(|a| a.request.operation);
                let phase = if cancelled.is_some() {
                    FleetRestartOperatorPhase::Cancelled
                } else if let Some(command) = cancellation.as_ref() {
                    if command.execution != execution_operation {
                        return Err(FleetCatalogError::Corrupt(
                            "operator cancellation execution",
                        ));
                    }
                    if attempt
                        .as_ref()
                        .is_some_and(|a| matches!(a.stage, Stage::Cancel { .. }))
                    {
                        FleetRestartOperatorPhase::Cancelling
                    } else {
                        FleetRestartOperatorPhase::CancelRequested
                    }
                } else {
                    if attempt
                        .as_ref()
                        .is_some_and(|a| matches!(a.stage, Stage::Cancel { .. }))
                    {
                        return Err(FleetCatalogError::Corrupt("missing operator cancellation"));
                    }
                    attempt
                        .as_ref()
                        .map_or(FleetRestartOperatorPhase::Requested, |a| a.stage.phase())
                };
                let readiness = if phase == FleetRestartOperatorPhase::Ready {
                    let Some(Stage::AwaitStop { ready }) = attempt.as_ref().map(|a| &a.stage)
                    else {
                        return Err(FleetCatalogError::Corrupt("operator readiness stage"));
                    };
                    let request = super::driver::ready_request(&admission.record, ready.clone())?;
                    self.restart_readiness_inner(&request)
                        .await?
                        .ok_or(FleetCatalogError::Corrupt("missing operator readiness"))?;
                    Some(ready.clone())
                } else {
                    None
                };
                Ok(Some(FleetRestartOperatorStatus {
                    admission,
                    phase,
                    execution_operation,
                    readiness,
                    cancellation: cancellation.map(|c| c.request),
                    cancelled,
                }))
            })
            .await
        })
    }
}
