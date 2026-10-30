// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Controller-only execution: response expiry never cancels a physical phase.
use super::*;
use std::collections::BTreeSet;
use tokio::sync::watch;

impl CellCommandInvocation {
    pub(crate) fn controller_admission(
        &self,
        domain: &DomainToken,
    ) -> Result<(CommandId, u64, usize), CellCommandError> {
        if !self.dispatcher.domain.same_owner(domain)
            || self.attempt.is_some()
            || self.dispatched
            || self.origin.is_some()
        {
            return Err(self.error(CellDispatchError::Invalid(
                "controller requires a fresh local invocation",
            )));
        }
        let deadline = self.session.deadline_ms();
        if domain.mono_ms().map_err(|_| self.error(closed()))? >= deadline {
            return Err(self.error(CellDispatchError::Runtime(RuntimeError::DeadlineExceeded)));
        }
        let bytes = self
            .request
            .input
            .len()
            .checked_mul(2)
            .and_then(|n| n.checked_add(8192))
            .ok_or_else(|| self.error(CellDispatchError::Runtime(RuntimeError::Overloaded)))?;
        Ok((self.request.id, deadline, bytes))
    }

    pub(crate) fn unresolved(&self) -> bool {
        self.origin.is_some()
    }

    fn expired(&self) -> Result<bool, CellDispatchError> {
        Ok(self.dispatcher.domain.mono_ms().map_err(|_| closed())? >= self.session.deadline_ms())
    }

    async fn cancel_unsubmitted(
        &mut self,
        progress: &mut watch::Receiver<()>,
    ) -> Result<bool, CellDispatchError> {
        let prepared = self
            .origin
            .as_ref()
            .and_then(|origin| origin.prepared.as_ref())
            .ok_or(CellDispatchError::Invalid("missing controller preparation"))?;
        let Some(proof) = prepared
            .cancel_before_submission()
            .map_err(CellDispatchError::Runtime)?
        else {
            return Ok(false);
        };
        loop {
            match self.reject(proof.clone()).await {
                Ok(_) => return Ok(true),
                Err(CellDispatchError::Execution(
                    ExecutionError::Busy | ExecutionError::Refused(CommandFailure::Overloaded),
                )) => {
                    progress.changed().await.map_err(|_| closed())?;
                }
                Err(error) => return Err(error),
            }
        }
    }

    /// The caller must keep this future/owner until resolution or joined node
    /// shutdown. It never resets routing deadlines or re-executes a callback.
    pub(crate) async fn run_owned(
        &mut self,
        runtime: &CellGroupRuntime,
        sql: &CellGroupSqlPool,
        options: CellSqlOptions,
        roster: &BTreeSet<cellar_logic::cell_group::GroupHistory>,
        progress: &mut watch::Receiver<()>,
    ) -> Result<CellCommandResponse, CellCommandError> {
        self.begin().await?;
        let result = self.run_at(runtime, sql, options, roster, progress).await;
        result.map_err(|error| self.error(error))
    }

    async fn run_at(
        &mut self,
        runtime: &CellGroupRuntime,
        sql: &CellGroupSqlPool,
        options: CellSqlOptions,
        roster: &BTreeSet<cellar_logic::cell_group::GroupHistory>,
        progress: &mut watch::Receiver<()>,
    ) -> Result<CellCommandResponse, CellDispatchError> {
        let attempt = self
            .attempt
            .as_ref()
            .ok_or(CellDispatchError::Invalid("missing route"))?
            .clone();
        self.session
            .validate_attempt(&attempt)
            .map_err(CellDispatchError::Routing)?;
        if !roster.contains(&attempt.route().history) {
            return Err(CellDispatchError::Runtime(RuntimeError::NotFound));
        }
        self.dispatched = true;
        // Unlike the response-facing phase API, do not drop this SQL waiter at
        // the response deadline. It carries the exact preparation or refusal.
        match self.prepare_at(&attempt, runtime, sql, options).await? {
            CellCommandPreparation::Committed(response) => return Ok(response),
            CellCommandPreparation::Prepared => (),
        }
        loop {
            if self.expired()? {
                if self.cancel_unsubmitted(progress).await? {
                    return Err(CellDispatchError::Runtime(RuntimeError::DeadlineExceeded));
                }
                break;
            }
            match self.submit_at().await {
                Ok(CellCommandSubmission::Accepted(_)) => break,
                Ok(CellCommandSubmission::Rejected) => {
                    return Err(CellDispatchError::Execution(ExecutionError::Refused(
                        CommandFailure::Overloaded,
                    )))
                }
                Err(CellDispatchError::Runtime(
                    RuntimeError::NotReady | RuntimeError::Overloaded,
                ))
                | Err(CellDispatchError::Execution(
                    ExecutionError::Busy | ExecutionError::Refused(CommandFailure::Overloaded),
                )) => {
                    // Retry only the exact prepared submission; no new callback.
                    progress.changed().await.map_err(|_| closed())?;
                }
                Err(CellDispatchError::Runtime(
                    RuntimeError::NotLeader | RuntimeError::StaleCompletion,
                )) => break,
                Err(error) => {
                    // An untouched preparation can be sealed even if the SQL
                    // mark/submission mailbox refused. Other errors stay owned.
                    self.cancel_unsubmitted(progress).await?;
                    return Err(error);
                }
            }
        }
        // Submission may have entered the owner despite a lost/error response.
        // Only an exact receipt or newer applied term can retire this producer.
        self.origin
            .as_mut()
            .ok_or(CellDispatchError::Invalid("missing origin"))?
            .submitted = true;
        loop {
            match self.settle_at().await {
                Ok(CellCommandSettlement::Committed(response)) => return Ok(response),
                Ok(CellCommandSettlement::NotCommitted) => {
                    return Err(CellDispatchError::Execution(ExecutionError::Refused(
                        CommandFailure::Unavailable,
                    )))
                }
                Ok(CellCommandSettlement::Refused(reason)) => {
                    return Err(CellDispatchError::Execution(ExecutionError::Refused(
                        reason,
                    )))
                }
                Err(CellDispatchError::Runtime(
                    RuntimeError::NotReady
                    | RuntimeError::NotLeader
                    | RuntimeError::Overloaded
                    | RuntimeError::StaleCompletion,
                ))
                | Err(CellDispatchError::Execution(
                    ExecutionError::Uncertain
                    | ExecutionError::Busy
                    | ExecutionError::Refused(CommandFailure::Overloaded),
                )) => (),
                Err(error) => return Err(error),
            }
            progress.changed().await.map_err(|_| closed())?;
        }
    }
}
