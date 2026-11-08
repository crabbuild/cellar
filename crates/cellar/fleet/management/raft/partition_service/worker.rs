// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

use super::*;
use crate::raft::cell_groups::TransitionPhase;
use sha2::{Digest, Sha256};

pub(super) struct Work {
    pub control: RaftFleetControl,
    pub scheduler: RaftFleetPartitionScheduler,
    pub planner: Arc<dyn RaftFleetPartitionPlanner>,
    pub options: RaftFleetPartitionServiceOptions,
    pub lease: ControlServiceLease,
    pub stop: watch::Receiver<bool>,
    pub status: watch::Sender<RaftFleetPartitionServiceStatus>,
    pub finished: watch::Sender<bool>,
    pub child: RaftFleetTransitionService,
}

impl Work {
    pub async fn run(mut self) {
        let _done = ControlServiceCompletion::new(self.finished.clone());
        let mut stop = self.stop.clone();
        let mut node_stop = self.lease.stop_receiver();
        self.status.send_modify(|s| s.running = true);
        loop {
            if *stop.borrow() || *node_stop.borrow() {
                break;
            }
            let control = self.control.clone();
            let result = crate::asyncrt::select! {
                _ = stop.changed() => break,
                _ = node_stop.changed() => break,
                result = Box::pin(self.turn(&control)) => result,
            };
            self.status.send_modify(|s| {
                s.rounds = s.rounds.saturating_add(1);
                s.transition = self.child.status();
                match result {
                    Ok(()) => {
                        s.last_failure = None;
                        s.consecutive_failures = 0;
                    }
                    Err(error) => {
                        s.needs_operator = matches!(
                            error.code,
                            RaftFleetFailureCode::InvalidInput
                                | RaftFleetFailureCode::PayloadConflict
                                | RaftFleetFailureCode::Forbidden
                                | RaftFleetFailureCode::Unsupported
                        );
                        s.last_failure = Some(error);
                        s.consecutive_failures = s.consecutive_failures.saturating_add(1);
                    }
                }
                if s.transition.needs_operator {
                    s.needs_operator = true;
                }
            });
            if self.status.borrow().needs_operator {
                break;
            }
            let failures = self.status.borrow().consecutive_failures;
            let delay = self
                .options
                .interval
                .saturating_mul(1u32 << failures.min(8))
                .min(self.options.retry_max_interval);
            let Ok(now) = control.domain.mono_ms() else {
                break;
            };
            let Ok(timer) = control
                .domain
                .sleep_until(now.saturating_add(delay.as_millis() as u64))
            else {
                break;
            };
            crate::asyncrt::select! {
                _ = stop.changed() => break,
                _ = node_stop.changed() => break,
                _ = timer => (),
            }
        }
        self.child.shutdown().await;
        self.status.send_modify(|s| {
            s.running = false;
            s.transition.running = false;
        });
        drop(self.scheduler);
        drop(self.lease);
    }

    async fn turn(&mut self, control: &RaftFleetControl) -> Result<()> {
        let turn = Turn::new(&control.domain, self.options.turn_timeout)?;
        // Restoration precedes callbacks, including when this node never saw
        // the original decision or its actual child reservation confirmations.
        self.scheduler.restore_pending(turn.remaining()?).await?;
        if self.scheduler.pending_request().is_some() {
            return Box::pin(self.advance(control, &turn)).await;
        }
        self.status.send_modify(|s| {
            s.operation_id = None;
            s.phase = None;
        });
        self.action("observe");
        let pressure = turn.run(self.planner.pressure(turn.remaining()?)).await??;
        let poll = self.scheduler.poll(pressure, turn.remaining()?).await?;
        // Another controller may have selected work during the pressure call.
        if self.scheduler.pending_request().is_some() {
            return Ok(());
        }
        let Some(recommendation) = poll.recommendation else {
            return Ok(());
        };
        let history = match &recommendation {
            RaftFleetPartitionRecommendation::Split { basis, .. } => basis.route.history,
            RaftFleetPartitionRecommendation::Merge { left, .. } => left.route.history,
            RaftFleetPartitionRecommendation::Wait(_) => return Ok(()),
        };
        let basis = control
            .transition_basis(&hex::encode(history.namespace.bytes()), turn.remaining()?)
            .await?;
        let context = RaftFleetPartitionPlanContext {
            recommendation,
            basis,
        };
        self.action("plan");
        let request = turn
            .run(self.planner.plan(context.clone(), turn.remaining()?))
            .await??;
        validate_plan(control, &context, &request)?;
        self.action("stage");
        let staged = Box::pin(self.scheduler.stage(
            &self.options.principal,
            &request,
            turn.remaining()?,
        ))
        .await;
        if staged.is_err() {
            // This turn never enters child admission. Drop only volatile
            // candidate state; the next turn must first rediscover any committed
            // Root cursor, including a CAS whose response was lost. A competing
            // root selection fences an older in-flight selection by revision.
            self.scheduler.discard_unadmitted_candidate();
        }
        staged
    }

    async fn advance(&mut self, control: &RaftFleetControl, turn: &Turn<'_>) -> Result<()> {
        if self.scheduler.pending_principal() != Some(self.options.principal.as_str()) {
            return Err(RaftFleetFailure {
                code: RaftFleetFailureCode::Forbidden,
                outcome: RaftFleetOutcome::NotSubmitted,
                message: "Selected partition belongs to another operator principal.".into(),
            });
        }
        if Box::pin(self.scheduler.resume_cancellation(turn.remaining()?)).await? {
            self.action("cancelled");
            return Ok(());
        }
        let request = self
            .scheduler
            .pending_request()
            .ok_or(FleetCatalogError::Closed)?
            .clone();
        self.status
            .send_modify(|s| s.operation_id = Some(request.operation_id.clone()));
        let status = match control
            .transition_status(
                &request.namespace_id,
                &request.operation_id,
                turn.remaining()?,
            )
            .await
        {
            Ok(status) => status,
            Err(error) if error.code == RaftFleetFailureCode::NotFound => {
                self.action("resume-admission");
                Box::pin(self.scheduler.resume_pending(turn.remaining()?)).await?;
                return Ok(());
            }
            Err(error) => return Err(error),
        };
        self.status.send_modify(|s| s.phase = status.phase);
        if status.phase.is_none() {
            self.action("resume-admission");
            Box::pin(self.scheduler.resume_pending(turn.remaining()?)).await?;
            return Ok(());
        }
        if !status.fleet_slot_held
            && matches!(
                status.phase,
                Some(TransitionPhase::Retired | TransitionPhase::Cancelled)
            )
        {
            self.action("settle");
            self.scheduler.settle(turn.remaining()?).await?;
        } else if self.options.execution == RaftFleetPartitionExecutionPolicy::Complete {
            self.action("consent");
            consent(control, &self.options.principal, &request, &status, turn).await?;
        } else {
            self.action("await-consent");
        }
        Ok(())
    }

    fn action(&self, action: &'static str) {
        self.status.send_modify(|s| s.last_action = Some(action));
    }
}

fn validate_plan(
    control: &RaftFleetControl,
    context: &RaftFleetPartitionPlanContext,
    request: &RaftFleetTransitionAdmissionRequest,
) -> Result<()> {
    let basis = &context.basis;
    if request.catalog_id != basis.catalog_id
        || request.namespace_id != basis.namespace_id
        || request.expected_fleet_revision != basis.fleet_revision
        || request.expected_directory_digest != basis.directory_digest
    {
        return Err(RaftFleetFailure::invalid("partition plan basis mismatch"));
    }
    let directory = control.managed_namespace(&request.namespace_id)?;
    let matches = match (&context.recommendation, &request.partition) {
        (
            RaftFleetPartitionRecommendation::Split { basis, pivot, .. },
            RaftFleetTransitionPartition::Split { parent, pivot_cell },
        ) => {
            parent.history(directory)? == basis.route.history && pivot.cell().as_str() == pivot_cell
        }
        (
            RaftFleetPartitionRecommendation::Merge { left: a, right: b },
            RaftFleetTransitionPartition::Merge { left, right },
        ) => {
            left.history(directory)? == a.route.history
                && right.history(directory)? == b.route.history
        }
        _ => false,
    };
    if !matches {
        return Err(RaftFleetFailure::invalid(
            "partition plan selection mismatch",
        ));
    }
    Ok(())
}

async fn consent(
    control: &RaftFleetControl,
    principal: &str,
    request: &RaftFleetTransitionAdmissionRequest,
    status: &RaftFleetTransitionStatus,
    turn: &Turn<'_>,
) -> Result<()> {
    async fn existing(
        control: &RaftFleetControl,
        principal: &str,
        request: &RaftFleetTransitionAdmissionRequest,
        status: &RaftFleetTransitionStatus,
        timeout: Duration,
    ) -> Result<bool> {
        let Some(consent) = control.catalog.active_transition_execution(timeout).await? else {
            return Ok(false);
        };
        let directory = control.managed_namespace(&request.namespace_id)?;
        if consent.identity.operation != records::id(&request.operation_id)?
            || consent.identity.principal != principal
            || consent.namespace != directory.namespace().root_digest()
            || hex::encode(consent.plan) != status.plan_digest
        {
            return Err(FleetCatalogError::PayloadConflict.into());
        }
        Ok(true)
    }
    if existing(control, principal, request, status, turn.remaining()?).await? {
        return Ok(());
    }
    let mut digest = Sha256::new();
    digest.update(b"cellar-partition-service-execution-v1");
    digest.update(
        serde_json::to_vec(&(principal, request))
            .map_err(|_| RaftFleetFailure::invalid("partition execution identity"))?,
    );
    let id: [u8; 32] = digest.finalize().into();
    let execution = RaftFleetTransitionExecutionRequest {
        catalog_id: request.catalog_id.clone(),
        namespace_id: request.namespace_id.clone(),
        operation_id: request.operation_id.clone(),
        request_id: hex::encode(&id[..16]),
        expected_revision: status.observed_fleet_revision,
        plan_digest: status.plan_digest.clone(),
    };
    match control
        .authorize_transition_execution(principal, &execution, turn.remaining()?)
        .await
    {
        Err(error) if error.code == RaftFleetFailureCode::PayloadConflict => {
            if existing(control, principal, request, status, turn.remaining()?).await? {
                Ok(())
            } else {
                Err(error)
            }
        }
        result => result,
    }
}
