// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! An owned job outlives its caller, retaining the original transport and fence.
use super::{hex, Job, Reply, Request};
use anyhow::{ensure, Context as _, Result};
use cellar::sdk::containers::{
    ExecutionClaim, ExecutionOperation, ExecutionOperationKind, ExecutionOutcome,
};
use std::time::Duration;

fn observation(operation: &ExecutionOperation) -> bool {
    matches!(
        operation.kind,
        ExecutionOperationKind::Inspect | ExecutionOperationKind::Read
    )
}

impl Job {
    pub(super) async fn run(
        &self,
        claim: ExecutionClaim,
        operation: ExecutionOperation,
        request: Request,
        deadline: u64,
    ) -> Result<ExecutionOutcome> {
        let service = &self.backend;
        let id = hex(&operation.id);
        if service.check_operation(&claim, &operation).is_err() {
            let expired = cellar::asyncrt::mono_ms() >= deadline
                || u64::try_from(cellar::asyncrt::wall_ms())? >= operation.expires_unix_ms;
            let outcome = if expired {
                ExecutionOutcome::ExpiredBeforeSubmission
            } else {
                ExecutionOutcome::Refused
            };
            service
                .record_outcome(self.key.clone(), id, outcome.clone())
                .await?;
            return Ok(outcome);
        }
        let result = self.execute(&claim, &operation, request, deadline).await;
        let settled = async {
            let bytes = serde_json::to_vec(&result?)?;
            ensure!(
                bytes.len() <= service.options.max_body_bytes * 2 + 4096,
                "Docker result exceeds bound"
            );
            let outcome = ExecutionOutcome::Completed(bytes);
            service
                .record_outcome(self.key.clone(), id, outcome.clone())
                .await?;
            Ok(outcome)
        }
        .await;
        if let Err(error) = &settled {
            if !observation(&operation) {
                service
                    .quarantine_mutation(&self.key)
                    .await
                    .with_context(|| {
                        format!("failed mutation could not be fenced after: {error}")
                    })?;
            }
        }
        settled
    }

    async fn execute(
        &self,
        claim: &ExecutionClaim,
        operation: &ExecutionOperation,
        request: Request,
        deadline: u64,
    ) -> Result<Reply> {
        let service = &self.backend;
        let remaining = deadline.saturating_sub(cellar::asyncrt::mono_ms());
        let work = service.engine.execute(service, claim, operation, request);
        tokio::pin!(work);
        cellar::asyncrt::select! {
            result = &mut work => result,
            _ = cellar::asyncrt::sleep(Duration::from_millis(remaining)) => {
                // Observation transport is owned too, but cannot mutate the
                // resource. Its deadline must not terminate unrelated compute.
                if observation(operation) { return (&mut work).await; }
                let key = self.key.clone();
                let expired = claim.clone();
                let inactive = service.update(move |state| {
                    let slot = state.slots.get_mut(&key).context("Docker fence missing")?;
                    if slot.high != expired || !slot.active { return Ok(None); }
                    slot.active = false;
                    slot.transition = slot.transition.checked_add(1).context("Docker fence transition exhausted")?;
                    Ok(Some(slot.transition))
                }).await;
                // Always attempt termination, even after a journal failure.
                // Retain and join the original I/O: dropping the future cannot
                // cancel a command already accepted by the Engine.
                let stopped = service.engine.stop_all(Some(&self.key)).await;
                let result = (&mut work).await;
                let rescanned = service.engine.stop_all(Some(&self.key)).await;
                let transition = inactive?;
                stopped?;
                rescanned?;
                if let Some(transition) = transition.filter(|_| result.is_ok()) {
                    let key = self.key.clone();
                    let original = claim.clone();
                    service.update(move |state| {
                        let slot = state.slots.get_mut(&key).context("Docker fence missing")?;
                        if slot.high == original && slot.transition == transition { slot.active = true; }
                        Ok(())
                    }).await?;
                }
                result
            }
        }
    }
}
