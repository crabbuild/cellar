// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Controller-only execution: response expiry never cancels a physical phase.
use super::*;
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
            || self.output.is_some()
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

    pub(crate) fn unresolved_group(&self) -> Option<cellar_logic::cell_group::GroupHistory> {
        self.origin.as_ref().map(|origin| origin.route.history)
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
        roster: &crate::raft::cell_groups::controller::GroupRoster,
        progress: &mut watch::Receiver<()>,
        transport: Option<&crate::raft::transport::RaftTransport>,
    ) -> Result<CellCommandResponse, CellCommandError> {
        self.session.track_command(runtime.log().performance());
        if let Some(expected) = &self.remote_route {
            // The sender selected one exact local destination. Replicated
            // seal/activation/recovery state fences it, independently of hint
            // freshness. SQL still requires the actual leader's cell authority.
            let selected = runtime
                .log()
                .cell_group(&expected.group)
                .and_then(|group| group.group_manifest())
                .map_err(|error| self.error(CellDispatchError::Runtime(error.into())))?
                .ok_or_else(|| self.error(CellDispatchError::Runtime(RuntimeError::NotFound)))?;
            if !selected.is_active() || !expected.matches(selected.route()) {
                return Err(self.error(CellDispatchError::Runtime(RuntimeError::NotReady)));
            }
            let attempt = self
                .session
                .begin_destination(selected.route().clone())
                .map_err(|error| self.error(CellDispatchError::Routing(error)))?;
            self.attempt = Some(attempt);
            self.dispatched = false;
        } else {
            self.begin().await?;
        }
        // Only an originating invocation may forward. An authenticated incoming
        // command stays local, so stale hints cannot create forwarding cycles.
        if let Some(transport) = transport.filter(|_| !self.system && self.remote_route.is_none()) {
            loop {
                let remote = loop {
                    let selected = Box::pin(super::super::mutation_route::mutation_peer(
                        runtime,
                        transport,
                        &self.session,
                        self.current()?,
                        &self.dispatcher.keys,
                        &self.dispatcher.domain,
                    ))
                    .await;
                    match selected {
                        Ok(member) => break member,
                        Err(CellDispatchError::Runtime(
                            RuntimeError::NotReady | RuntimeError::NotFound,
                        )) if usize::from(self.current()?.ordinal())
                            < self.session.maximum_attempts() =>
                        {
                            // No callback or producer has entered. Only discovery
                            // retries here; a transmitted mutation is never retried.
                            let previous = self.current()?.clone();
                            let refreshed = self
                                .session
                                .refresh(&previous)
                                .await
                                .map_err(CellDispatchError::Routing)
                                .map_err(|cause| self.error(cause))?;
                            self.attempt = Some(refreshed);
                        }
                        Err(cause) => return Err(self.error(cause)),
                    }
                };
                if let Some(member) = remote {
                    match self.execute_remote(transport, member).await {
                        Ok(response) => {
                            return Ok(CellCommandResponse {
                                output: CommandOutput::Remote(Box::new(response)),
                            })
                        }
                        Err(error)
                            if !error.ambiguous
                                && matches!(
                                    error.cause.refusal(),
                                    Some(CommandFailure::NotLeader { .. })
                                )
                                && usize::from(self.current()?.ordinal())
                                    < self.session.maximum_attempts() =>
                        {
                            // Only an authenticated pre-admission rejection permits
                            // another destination. Inputs, ID and deadline stay owned
                            // by this invocation. An earlier unknown result wins.
                            self.retry().await?;
                            continue;
                        }
                        Err(error) => return Err(error),
                    }
                }
                break;
            }
        }
        let result = self.run_at(runtime, sql, options, roster, progress).await;
        result.map_err(|error| self.error(error))
    }

    async fn run_at(
        &mut self,
        runtime: &CellGroupRuntime,
        sql: &CellGroupSqlPool,
        options: CellSqlOptions,
        roster: &crate::raft::cell_groups::controller::GroupRoster,
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
        if !roster
            .contains(&attempt.route().history)
            .map_err(CellDispatchError::Runtime)?
        {
            return Err(CellDispatchError::Runtime(RuntimeError::NotFound));
        }
        self.dispatched = true;
        // Unlike the response-facing phase API, do not drop this SQL waiter at
        // the response deadline. It carries the exact preparation or refusal.
        match self
            .prepare_when_applied(&attempt, runtime, sql, options, progress)
            .await
        {
            Ok(CellCommandPreparation::Committed(response)) => return Ok(response),
            Ok(CellCommandPreparation::Prepared) => (),
            Err(error) if self.output.is_some() && output_retryable(&error) => {
                return self.finish_owned_output(progress).await;
            }
            Err(error) => return Err(error),
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
                Err(
                    error @ CellDispatchError::Runtime(
                        RuntimeError::NotReady
                        | RuntimeError::NotLeader
                        | RuntimeError::Overloaded
                        | RuntimeError::StaleCompletion,
                    ),
                )
                | Err(
                    error @ CellDispatchError::Execution(
                        ExecutionError::Uncertain
                        | ExecutionError::Busy
                        | ExecutionError::Refused(CommandFailure::Overloaded),
                    ),
                ) => {
                    if self.output.is_some() && output_retryable(&error) {
                        return self.finish_owned_output(progress).await;
                    }
                    // SQL has already resolved before an archive/output error.
                    // Never restart reconciliation after retiring its origin.
                    if self.origin.is_none() {
                        return Err(error);
                    }
                }
                Err(error) => return Err(error),
            }
            progress.changed().await.map_err(|_| closed())?;
        }
    }

    // A sampled ready leader can commit another cell's entry before this
    // command asks for execution authority. Wait for that durable apply prefix
    // without minting a SQL producer, spending another routing attempt or
    // weakening the owner's applied == committed check. Once physical SQL may
    // have entered, the existing exact-producer reconciliation owns the result.
    async fn prepare_when_applied(
        &mut self,
        attempt: &CellRouteAttempt,
        runtime: &CellGroupRuntime,
        sql: &CellGroupSqlPool,
        options: CellSqlOptions,
        progress: &mut watch::Receiver<()>,
    ) -> Result<CellCommandPreparation, CellDispatchError> {
        let mut observed_cut = None;
        loop {
            if self.expired()? {
                return Err(CellDispatchError::Runtime(RuntimeError::DeadlineExceeded));
            }
            // Subscribe before the authority request. Apply may finish while
            // that request or its subsequent read-only status reply is in flight.
            progress.borrow_and_update();
            match self
                .prepare_at(attempt, runtime, sql, options.clone())
                .await
            {
                Err(CellDispatchError::Runtime(RuntimeError::NotReady))
                    if !self.ambiguous && self.origin.is_none() && self.output.is_none() =>
                {
                    let status = runtime
                        .status(attempt.route().history)
                        .await
                        .map_err(CellDispatchError::Runtime)?;
                    let cut = (
                        status.term.get(),
                        status.applied.index.get(),
                        status.committed.get(),
                    );
                    if status.applied.index >= status.committed {
                        // The failed request may have raced the final apply.
                        // Retry a newly observed ready cut once; a stable fence,
                        // sealed source or inactive history must still refuse.
                        if status.ready && observed_cut != Some(cut) {
                            observed_cut = Some(cut);
                            continue;
                        }
                        return Err(CellDispatchError::Runtime(RuntimeError::NotReady));
                    }
                    observed_cut = Some(cut);
                    tracing::debug!(
                        target: "cellar::raft::execution_readiness",
                        history = ?attempt.route().history,
                        applied = ?status.applied,
                        committed = ?status.committed,
                        "command waits for committed apply before SQL"
                    );
                    let timer = self
                        .dispatcher
                        .domain
                        .sleep_until(self.session.deadline_ms())
                        .map_err(|_| closed())?;
                    crate::asyncrt::select! {
                        result = progress.changed() => result.map_err(|_| closed())?,
                        _ = timer => return Err(CellDispatchError::Runtime(RuntimeError::DeadlineExceeded)),
                    }
                }
                result => return result,
            }
        }
    }

    async fn finish_owned_output(
        &mut self,
        progress: &mut watch::Receiver<()>,
    ) -> Result<CellCommandResponse, CellDispatchError> {
        let domain = self.dispatcher.domain.clone();
        let deadline = self.session.deadline_ms();
        loop {
            let timer = domain.sleep_until(deadline).map_err(|_| closed())?;
            let result = crate::asyncrt::select! {
                result = self.finish_output() => result,
                _ = timer => Err(CellDispatchError::Runtime(RuntimeError::DeadlineExceeded)),
            };
            match result {
                Ok(response) => return Ok(response),
                Err(error) if output_retryable(&error) => (),
                Err(error) => return Err(error),
            }
            // This is buffered committed output, not an unresolved SQL producer.
            // Progress wakes a fresh fence check without callbacks or proposals.
            let timer = domain.sleep_until(deadline).map_err(|_| closed())?;
            crate::asyncrt::select! {
                result = progress.changed() => result.map_err(|_| closed())?,
                _ = timer => return Err(CellDispatchError::Runtime(RuntimeError::DeadlineExceeded)),
            }
        }
    }
}

fn output_retryable(error: &CellDispatchError) -> bool {
    matches!(
        error,
        CellDispatchError::Runtime(
            RuntimeError::NotReady | RuntimeError::Overloaded | RuntimeError::StaleCompletion
        )
    )
}
