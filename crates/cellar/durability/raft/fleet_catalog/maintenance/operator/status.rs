// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Snapshot-scoped durable progress; polling never executes physical work.
use super::attempt::Stage;
use super::*;
use crate::raft::fleet_catalog::deadline::Deadline;

/// Root-journaled history only. Neither phases nor child identities carry a
/// current health lease, stop grant, boot capability or capacity authorization.
pub struct FleetRestartOperatorStatus {
    pub admission: FleetRestartOperatorAdmission,
    pub phase: FleetRestartOperatorPhase,
    pub execution_operation: Option<[u8; 16]>,
    pub readiness: Option<FleetRequestIdentity>,
    pub cancellation: Option<FleetRequestIdentity>,
    pub cancelled: Option<FleetRestartOperatorCancelled>,
    pub cancellation_refused: bool,
    pub completed: Option<FleetRestartOperatorCompleted>,
    pub stop_grant: Option<FleetRequestIdentity>,
    pub return_grant: Option<FleetRequestIdentity>,
    pub resume_grant: Option<FleetRequestIdentity>,
    pub stop: Option<FleetRequestIdentity>,
    pub offline: Option<FleetRequestIdentity>,
    pub returning: Option<FleetRestartReturnRequest>,
    pub resume: Option<FleetRequestIdentity>,
}

impl FleetCatalog {
    /// Read the canonical retained-start request for a deployment adapter.
    /// This performs no admission or startup. The exclusive engine owner still
    /// validates the actual persisted predecessor and consumes M8 at startup.
    pub fn restart_operator_return_request<'a>(
        &'a self,
        snapshot: &'a FleetSnapshot,
        operation: ProposalId,
        timeout: Duration,
    ) -> BoxFuture<'a, Result<Option<FleetRestartReturnRequest>>> {
        Box::pin(async move {
            self.check_snapshot(snapshot)?;
            let deadline = Deadline::new(self, timeout)?;
            let request = self
                .run(deadline.remaining()?, async {
                    let Some(admission) = self.restart_operator_inner(snapshot, operation).await?
                    else {
                        return Ok(None);
                    };
                    let Some(mut attempt) = self
                        .restart_operator_attempt(snapshot, &admission.record)
                        .await?
                    else {
                        return Ok(None);
                    };
                    let Some(returning) = attempt.facts.returning.clone() else {
                        return Ok(None);
                    };
                    let grant = self
                        .operator_action(snapshot, &admission.record, "return")
                        .await?
                        .ok_or(FleetCatalogError::Corrupt("missing return grant"))?;
                    attempt.request = returning;
                    Ok(Some(lifecycle::work::returning(
                        &admission.record,
                        &attempt,
                        &grant,
                    )?))
                })
                .await?;
            let Some(request) = request else {
                return Ok(None);
            };
            self.restart_return_consent(snapshot, &request, deadline.remaining()?)
                .await?
                .ok_or(FleetCatalogError::Corrupt("unadmitted operator return"))?;
            Ok(Some(request))
        })
    }

    /// Inspect one immutable Root. No driver or observer is constructed and no
    /// mutation, physical command or proof walk is submitted by this read.
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
            let completed = self
                .restart_operator_completed(snapshot, operation, deadline.remaining()?)
                .await?;
            let (
                attempt,
                cancellation,
                cancellation_refused,
                stop_grant,
                return_grant,
                resume_grant,
            ) = self
                .run(deadline.remaining()?, async {
                    let attempt = self
                        .restart_operator_attempt(snapshot, &admission.record)
                        .await?;
                    let cancellation = self
                        .operator_cancel_command(snapshot, &admission.record)
                        .await?;
                    let refused = if let Some(command) = &cancellation {
                        self.operator_cancel_refused(snapshot, &admission.record, command)
                            .await?
                    } else {
                        false
                    };
                    let stop = self
                        .operator_action(snapshot, &admission.record, "stop")
                        .await?
                        .map(|g| g.request);
                    let returning = self
                        .operator_action(snapshot, &admission.record, "return")
                        .await?
                        .map(|g| g.request);
                    let resume = self
                        .operator_action(snapshot, &admission.record, "resume")
                        .await?
                        .map(|g| g.request);
                    Ok((attempt, cancellation, refused, stop, returning, resume))
                })
                .await?;
            let execution_operation = attempt.as_ref().map(|a| a.request.operation);
            if cancellation
                .as_ref()
                .is_some_and(|c| c.execution != execution_operation)
            {
                return Err(FleetCatalogError::Corrupt(
                    "operator cancellation execution",
                ));
            }
            if cancellation.is_none()
                && attempt
                    .as_ref()
                    .is_some_and(|a| matches!(a.stage, Stage::Cancel { .. }))
            {
                return Err(FleetCatalogError::Corrupt("missing operator cancellation"));
            }
            let phase = if completed.is_some() {
                FleetRestartOperatorPhase::Complete
            } else if cancelled.is_some() {
                FleetRestartOperatorPhase::Cancelled
            } else if cancellation.is_some() && !cancellation_refused {
                if attempt
                    .as_ref()
                    .is_some_and(|a| matches!(a.stage, Stage::Cancel { .. }))
                {
                    FleetRestartOperatorPhase::Cancelling
                } else {
                    FleetRestartOperatorPhase::CancelRequested
                }
            } else {
                attempt
                    .as_ref()
                    .map_or(FleetRestartOperatorPhase::Requested, |a| a.stage.phase())
            };
            let facts = attempt
                .as_ref()
                .map_or_else(lifecycle::Facts::default, |a| a.facts.clone());
            let readiness = match attempt.as_ref().map(|a| &a.stage) {
                Some(Stage::AwaitStop { ready } | Stage::ConsentStop { ready }) => {
                    Some(ready.clone())
                }
                _ => facts.stop.as_ref().map(|s| s.ready.clone()),
            };
            if let Some(ready) = &readiness {
                self.restart_readiness(
                    &super::driver::ready_request(&admission.record, ready.clone())?,
                    deadline.remaining()?,
                )
                .await?
                .ok_or(FleetCatalogError::Corrupt("missing operator readiness"))?;
            }
            if let Some(stop) = &facts.stop {
                self.restart_stop_consent(
                    snapshot,
                    &stop.request(&admission.record)?,
                    deadline.remaining()?,
                )
                .await?
                .ok_or(FleetCatalogError::Corrupt("unadmitted operator stop"))?;
            }
            if let Some(offline) = &facts.offline {
                let receipt = self
                    .restart_offline(
                        snapshot,
                        ProposalId::new(offline.operation)
                            .map_err(|_| FleetCatalogError::Corrupt("offline operation"))?,
                        deadline.remaining()?,
                    )
                    .await?;
                if receipt.identity() != offline {
                    return Err(FleetCatalogError::Corrupt("operator offline predecessor"));
                }
            }
            let returning = self
                .restart_operator_return_request(snapshot, operation, deadline.remaining()?)
                .await?;
            if let Some(resume) = &facts.resume {
                let grant = self
                    .run(
                        deadline.remaining()?,
                        self.operator_action(snapshot, &admission.record, "resume"),
                    )
                    .await?
                    .ok_or(FleetCatalogError::Corrupt("missing resume grant"))?;
                self.restart_resume_admission(
                    snapshot,
                    &resume.request(&facts, &grant.action)?,
                    deadline.remaining()?,
                )
                .await?;
            }
            Ok(Some(FleetRestartOperatorStatus {
                admission,
                phase,
                execution_operation,
                readiness,
                cancellation: cancellation.map(|c| c.request),
                cancelled,
                cancellation_refused,
                completed,
                stop_grant,
                return_grant,
                resume_grant,
                stop: facts.stop.map(|s| s.identity),
                offline: facts.offline,
                returning,
                resume: facts.resume.map(|s| s.identity),
            }))
        })
    }
}
