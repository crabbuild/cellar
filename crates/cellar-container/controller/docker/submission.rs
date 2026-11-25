// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Bounded submission custody spans reservation and physical-job installation.
//! Caller cancellation cannot strand a joined, never-entered reservation.
use super::{hex, journal, DockerBackend, Job, PhysicalOperation, Request};
use crate::Limits;
use anyhow::{ensure, Context as _, Result};
use cellar::sdk::containers::{
    ExecutionClaim, ExecutionControllerFailure, ExecutionOperation, ExecutionOperationKind,
    ExecutionOutcome,
};
use std::sync::{atomic::Ordering, Arc};
use tokio::sync::OwnedSemaphorePermit;

/// Capture expiry and charge capacity before dispatching an owned continuation.
/// No journal or Engine work has started when construction returns an error.
pub(super) struct Submission {
    claim: ExecutionClaim,
    operation: ExecutionOperation,
    request: Request,
    deadline: u64,
    capacity: OwnedSemaphorePermit,
    observation: Option<OwnedSemaphorePermit>,
}

impl DockerBackend {
    pub(super) fn prepare_submission(
        &self,
        claim: ExecutionClaim,
        operation: ExecutionOperation,
        payload: Vec<u8>,
    ) -> Result<Submission> {
        // Capture the receiver's monotonic expiry before admission or disk I/O.
        // Later wall-clock movement can shorten admission, never extend it.
        let remaining = operation
            .expires_unix_ms
            .saturating_sub(u64::try_from(cellar::asyncrt::wall_ms())?);
        ensure!(
            remaining <= self.options.max_operation_ms,
            ExecutionControllerFailure::ConflictingRequest
        );
        let deadline = cellar::asyncrt::mono_ms()
            .checked_add(remaining)
            .context("Docker operation deadline overflow")?;
        ensure!(
            ExecutionOperation::new(
                operation.id,
                operation.kind,
                &payload,
                operation.expires_unix_ms
            )? == operation,
            ExecutionControllerFailure::ConflictingRequest
        );
        let request = Request::decode(
            &claim,
            &operation,
            &payload,
            &Limits {
                max_body_bytes: self.options.max_body_bytes,
            },
        )?;
        self.engine.validate(&request)?;
        let observation = if matches!(
            operation.kind,
            ExecutionOperationKind::Inspect | ExecutionOperationKind::Read
        ) {
            Some(
                self.observations
                    .clone()
                    .try_acquire_owned()
                    .map_err(|_| ExecutionControllerFailure::Busy)?,
            )
        } else {
            None
        };
        let _capacity = self
            .capacity
            .clone()
            .try_acquire_owned()
            .map_err(|_| ExecutionControllerFailure::Busy)?;
        Ok(Submission {
            claim,
            operation,
            request,
            deadline,
            capacity: _capacity,
            observation,
        })
    }

    pub(super) async fn submit(
        self: Arc<Self>,
        submission: Submission,
    ) -> Result<ExecutionOutcome> {
        let Submission {
            claim,
            operation,
            request,
            deadline,
            capacity: _capacity,
            observation,
        } = submission;
        let key = journal::key(&claim);
        let admission = self.admission.resource(&key).await?;
        ensure!(
            !self.closed.load(Ordering::Acquire),
            ExecutionControllerFailure::Unavailable
        );
        self.journal.check(&claim)?;
        let id = hex(&operation.id);
        let existing = self.journal.view(|state| {
            Ok(state
                .slots
                .get(&key)
                .and_then(|slot| slot.operations.get(&id))
                .cloned())
        })?;
        if let Some(existing) = existing {
            ensure!(
                existing.claim == claim && existing.operation == operation,
                ExecutionControllerFailure::ConflictingRequest
            );
            return existing
                .outcome
                .ok_or(ExecutionControllerFailure::UnknownOperation.into());
        }
        ensure!(
            u64::try_from(cellar::asyncrt::wall_ms())? < operation.expires_unix_ms
                && cellar::asyncrt::mono_ms() < deadline,
            ExecutionControllerFailure::UnknownOperation
        );
        let stored = PhysicalOperation {
            claim: claim.clone(),
            operation: operation.clone(),
            outcome: None,
        };
        self.update(move |state| {
            // Another binding's physical write may have occupied the writer
            // after the snapshot check. Expiry must not create a new unknown
            // reservation for work that has never entered the Engine.
            ensure!(
                cellar::asyncrt::mono_ms() < deadline
                    && u64::try_from(cellar::asyncrt::wall_ms())?
                        < stored.operation.expires_unix_ms,
                ExecutionControllerFailure::UnknownOperation
            );
            state.reserve(stored)
        })
        .await?;
        let replaced = self
            .jobs
            .lock()
            .map_err(|_| anyhow::anyhow!("Docker job inventory poisoned"))?
            .entry(key.clone())
            .or_default()
            .insert(operation.id, deadline);
        ensure!(
            replaced.is_none(),
            ExecutionControllerFailure::ConflictingRequest
        );
        let job = Job {
            backend: self.clone(),
            key: key.clone(),
            operation: operation.id,
        };
        drop(admission);
        // The job retains admission and the independent journal lock even if
        // its controller RPC disappears. Revocation joins it before successor
        // admission. An error after physical entry leaves the original unknown.
        self.domain
            .spawn(async move {
                let _capacity = _capacity;
                let _observation = observation;
                job.run(claim, operation, request, deadline).await
            })?
            .await?
    }
}
