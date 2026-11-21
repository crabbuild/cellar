// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! An active invocation retains one intermediate producer until its exact
//! receipt or a newer applied term resolves it. No task exists for idle cells.

use super::*;
use crate::raft::cell_groups::{CellGroupRuntime, CellReconciliationAuthority};
use cellar_logic::cell_group::CellVersion;
use std::future::Future;

#[derive(Clone)]
pub(crate) struct RpcCommitContext {
    pub runtime: CellGroupRuntime,
    pub archive: Option<crate::raft::cell_groups::S3Client>,
    pub route: cellar_logic::cell_group::GroupRoute,
    pub limits: ExecutionLimits,
    pub deadline: u64,
    /// Captured by command admission and retained with every continuation.
    pub admission: CommandAdmission,
    pub bindings: Option<Arc<dyn crate::application::cell_bindings::Host>>,
}

enum AuthorityPhase {
    Entry,
    Continuation,
}

impl RpcCommitContext {
    /// Recheck the exact cell after acquiring its writer. Only certified
    /// mutations for other cells may remain in the committed apply suffix.
    pub(super) async fn entry_authority(
        &self,
        domain: &crate::asyncrt::DomainToken,
        cell: &CellHistory,
        fence: &crate::raft::cell_groups::CellActivationFence,
    ) -> Result<CellExecutionAuthority, ExecutionError> {
        self.authority(domain, cell, fence, AuthorityPhase::Entry)
            .await
    }

    /// The parked invocation has reacquired its writer. Refresh its proven
    /// cell image without waiting for unrelated cells' committed apply work.
    pub(super) async fn continuation_authority(
        &self,
        domain: &crate::asyncrt::DomainToken,
        cell: &CellHistory,
        fence: &crate::raft::cell_groups::CellActivationFence,
    ) -> Result<CellExecutionAuthority, ExecutionError> {
        self.authority(domain, cell, fence, AuthorityPhase::Continuation)
            .await
    }

    async fn authority(
        &self,
        domain: &crate::asyncrt::DomainToken,
        cell: &CellHistory,
        fence: &crate::raft::cell_groups::CellActivationFence,
        phase: AuthorityPhase,
    ) -> Result<CellExecutionAuthority, ExecutionError> {
        let timeout = domain
            .sleep_until(self.deadline)
            .map_err(|_| unavailable())?;
        let waiting = async {
            loop {
                if !fence.term_current() {
                    return Err(unavailable());
                }
                let now = domain.mono_ms().map_err(|_| unavailable())?;
                if now >= self.deadline {
                    return Err(crate::raft::execution::authority_failure(
                        RuntimeError::DeadlineExceeded,
                    ));
                }
                let current = match phase {
                    AuthorityPhase::Entry => {
                        self.runtime
                            .independent_cell_authority(self.route.history, cell.clone())
                            .await
                    }
                    AuthorityPhase::Continuation => self
                        .runtime
                        .reconciliation_authority(self.route.history, cell.clone())
                        .await
                        .and_then(|proof| fence.resume_authority(&proof)),
                };
                match current {
                    Ok(authority) if fence.accepts_continuation(&authority) => {
                        return Ok(authority)
                    }
                    Ok(_) => return Err(unavailable()),
                    // Another cell can have a committed entry awaiting apply
                    // after this writer reaches the head of its local queue.
                    // Keep the invocation; only authority readiness is retried.
                    Err(RuntimeError::NotReady | RuntimeError::Overloaded) => pause(domain).await?,
                    Err(error) => return Err(crate::raft::execution::authority_failure(error)),
                }
            }
        };
        crate::asyncrt::select_biased! {
            "original RPC authority deadline";
            _=timeout=>Err(crate::raft::execution::authority_failure(RuntimeError::DeadlineExceeded)),
            result=waiting=>result,
        }
    }
}

pub(super) enum CheckpointProof {
    Current(
        Box<CellExecutionAuthority>,
        Box<CellReconciliationAuthority>,
    ),
    Reconcile(Box<CellReconciliationAuthority>),
    Rejected,
}

pub(super) async fn wait_checkpoint(
    runtime: CellGroupRuntime,
    domain: crate::asyncrt::DomainToken,
    prepared: CellPreparedCommand,
    expected: CellVersion,
    fence: crate::raft::cell_groups::CellActivationFence,
    retained: Arc<Bytes>,
    deadline: Option<u64>,
) -> Result<CheckpointProof, ExecutionError> {
    if !submit_checkpoint(&runtime, &domain, &prepared, retained, deadline, || Ok(())).await? {
        return Ok(CheckpointProof::Rejected);
    }
    wait_checkpoint_proof(runtime, domain, prepared, expected, fence).await
}

/// Admission runs independently of the SQLite owner. An accepted or uncertain
/// submission grants no applied-image or caller acknowledgement authority.
pub(super) async fn submit_checkpoint(
    runtime: &CellGroupRuntime,
    domain: &crate::asyncrt::DomainToken,
    prepared: &CellPreparedCommand,
    retained: Arc<Bytes>,
    deadline: Option<u64>,
    check: impl Fn() -> Result<(), ExecutionError>,
) -> Result<bool, ExecutionError> {
    submit_checkpoint_attempts(
        domain,
        deadline,
        check,
        || {
            prepared
                .cancel_before_submission()
                .map(|proof| proof.is_some())
        },
        || {
            let prepared = prepared.clone();
            let retained = retained.clone();
            async move {
                runtime
                    .submit_retained(prepared, Some(retained))
                    .await
                    .map(|outcome| match outcome {
                        CellSubmissionOutcome::Accepted(_) => true,
                        CellSubmissionOutcome::Rejected(_) => false,
                    })
            }
        },
    )
    .await
}

// Every attempt below resubmits the same captured preparation. Waiter liveness
// and an owner's positive pre-admission seal are separate observations.
async fn submit_checkpoint_attempts<F: Future<Output = Result<bool, RuntimeError>>>(
    domain: &crate::asyncrt::DomainToken,
    deadline: Option<u64>,
    check: impl Fn() -> Result<(), ExecutionError>,
    sealed_before_submission: impl Fn() -> Result<bool, RuntimeError>,
    mut attempt: impl FnMut() -> F,
) -> Result<bool, ExecutionError> {
    loop {
        if let Err(error) = check() {
            if sealed_before_submission().map_err(crate::raft::execution::authority_failure)? {
                return Err(error);
            }
            // The waiter cannot revoke an entering or uncertain preparation.
            // Keep submitting the same command identity and captured bytes;
            // the original SQL is never rerun.
        }
        if deadline.is_some_and(|deadline| domain.mono_ms().unwrap_or(u64::MAX) >= deadline)
            && sealed_before_submission().map_err(crate::raft::execution::authority_failure)?
        {
            return Ok(false);
        }
        match attempt().await {
            Ok(admitted) => return Ok(admitted),
            Err(RuntimeError::Overloaded | RuntimeError::NotReady) => pause(domain).await?,
            Err(RuntimeError::NotLeader | RuntimeError::StaleCompletion) => {
                return Ok(!sealed_before_submission()
                    .map_err(crate::raft::execution::authority_failure)?);
            }
            Err(error) => return Err(crate::raft::execution::authority_failure(error)),
        }
    }
}

pub(super) async fn wait_checkpoint_proof(
    runtime: CellGroupRuntime,
    domain: crate::asyncrt::DomainToken,
    prepared: CellPreparedCommand,
    expected: CellVersion,
    fence: crate::raft::cell_groups::CellActivationFence,
) -> Result<CheckpointProof, ExecutionError> {
    let term = prepared.term();
    loop {
        match runtime
            .reconciliation_authority(prepared.group(), expected.history.clone())
            .await
        {
            Ok(proof) if proof.term() > term => {
                return Ok(CheckpointProof::Reconcile(Box::new(proof)))
            }
            Ok(proof) if proof.term() == term && proof.cell().version == expected => {
                // This invocation retains its writer and its original ready
                // barrier. Only its exact applied checkpoint can advance that
                // authority; later apply work for another cell cannot revoke
                // it. Reconciliation alone still grants no execution, and a
                // changed leader/route/image must fail the original fence.
                match fence.resume_authority(&proof) {
                    Ok(execution) => {
                        return Ok(CheckpointProof::Current(
                            Box::new(execution),
                            Box::new(proof),
                        ));
                    }
                    Err(RuntimeError::NotReady) => (),
                    Err(error) => return Err(crate::raft::execution::authority_failure(error)),
                }
            }
            Ok(_)
            | Err(RuntimeError::NotReady | RuntimeError::NotLeader | RuntimeError::Overloaded) => {}
            Err(error) => return Err(crate::raft::execution::authority_failure(error)),
        }
        pause(&domain).await?;
    }
}

async fn pause(domain: &crate::asyncrt::DomainToken) -> Result<(), ExecutionError> {
    let now = domain.mono_ms().map_err(|_| unavailable())?;
    domain
        .sleep_until(now.saturating_add(10))
        .map_err(|_| unavailable())?
        .await;
    Ok(())
}

#[cfg(test)]
#[path = "rpc_commit_tests.rs"]
mod tests;
