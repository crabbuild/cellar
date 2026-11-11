// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! An active invocation retains one intermediate producer until its exact
//! receipt or a newer applied term resolves it. No task exists for idle cells.

use super::*;
use crate::raft::cell_groups::{CellGroupRuntime, CellReconciliationAuthority};
use cellar_logic::cell_group::CellVersion;

#[derive(Clone)]
pub(crate) struct RpcCommitContext {
    pub runtime: CellGroupRuntime,
    pub archive: Option<crate::raft::cell_groups::S3Client>,
    pub route: cellar_logic::cell_group::GroupRoute,
    pub limits: ExecutionLimits,
    pub deadline: u64,
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
    retained: Arc<Bytes>,
    deadline: Option<u64>,
) -> Result<CheckpointProof, ExecutionError> {
    let term = prepared.term();
    loop {
        if deadline.is_some_and(|deadline| domain.mono_ms().unwrap_or(u64::MAX) >= deadline)
            && prepared
                .cancel_before_submission()
                .map_err(crate::raft::execution::authority_failure)?
                .is_some()
        {
            return Ok(CheckpointProof::Rejected);
        }
        match runtime
            .submit_retained(prepared.clone(), Some(retained.clone()))
            .await
        {
            Ok(CellSubmissionOutcome::Accepted(_)) => break,
            Ok(CellSubmissionOutcome::Rejected(_)) => return Ok(CheckpointProof::Rejected),
            Err(RuntimeError::Overloaded | RuntimeError::NotReady) => pause(&domain).await?,
            Err(RuntimeError::NotLeader | RuntimeError::StaleCompletion) => {
                if prepared
                    .cancel_before_submission()
                    .map_err(crate::raft::execution::authority_failure)?
                    .is_some()
                {
                    return Ok(CheckpointProof::Rejected);
                }
                break;
            }
            Err(error) => return Err(crate::raft::execution::authority_failure(error)),
        }
    }
    loop {
        match runtime
            .execution_authority(prepared.group(), expected.history.clone())
            .await
        {
            Ok(execution) if execution.term() == term && execution.cell().version == expected => {
                match runtime
                    .reconciliation_authority(prepared.group(), expected.history.clone())
                    .await
                {
                    Ok(proof) if proof.term() == term && proof.cell().version == expected => {
                        return Ok(CheckpointProof::Current(
                            Box::new(execution),
                            Box::new(proof),
                        ));
                    }
                    Ok(proof) if proof.term() > term => {
                        return Ok(CheckpointProof::Reconcile(Box::new(proof)))
                    }
                    Ok(_)
                    | Err(
                        RuntimeError::NotReady | RuntimeError::NotLeader | RuntimeError::Overloaded,
                    ) => (),
                    Err(error) => return Err(crate::raft::execution::authority_failure(error)),
                }
            }
            Ok(_)
            | Err(RuntimeError::NotReady | RuntimeError::NotLeader | RuntimeError::Overloaded) => {}
            Err(error) => return Err(crate::raft::execution::authority_failure(error)),
        }
        match runtime
            .reconciliation_authority(prepared.group(), expected.history.clone())
            .await
        {
            Ok(proof) if proof.term() > term => {
                return Ok(CheckpointProof::Reconcile(Box::new(proof)))
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
