// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Resolve possibly transmitted public commands through positive receipts only.
use super::*;
use crate::raft::cell_groups::dispatch::receipt::ReceiptExecution;
use crate::raft::transport::TransportError;

enum Repair {
    Next,
    Backoff,
    Placement,
}

impl CellCommandInvocation {
    pub(super) async fn reconcile_remote(
        &mut self,
        runtime: &CellGroupRuntime,
        sql: &CellGroupSqlPool,
        options: CellSqlOptions,
        routing: &MutationRouting<'_>,
        unknown: CellCommandError,
    ) -> Result<CellCommandResponse, CellCommandError> {
        let mut repair = Repair::Next;
        loop {
            if self.expired().map_err(|cause| self.error(cause))?
                || usize::from(self.current()?.ordinal()) >= self.session.maximum_attempts()
            {
                return Err(unknown);
            }
            if matches!(repair, Repair::Backoff) {
                routing
                    .backoff(
                        &self.dispatcher.domain,
                        self.session.deadline_ms(),
                        self.session.maximum_attempts() - usize::from(self.current()?.ordinal()),
                    )
                    .await
                    .map_err(|cause| self.error(cause))?;
            }
            // Retain the command session, fingerprint, captured inputs and
            // admission. A receipt read consumes the next original ticket.
            let previous = self.current()?.clone();
            let attempt = match repair {
                Repair::Placement => self.session.refresh(&previous).await,
                Repair::Next | Repair::Backoff => self.session.retry(&previous).await,
            }
            .map_err(|cause| self.error(CellDispatchError::Routing(cause)))?;
            self.attempt = Some(attempt);
            self.dispatched = false;
            let member = match Box::pin(super::super::mutation_route::mutation_peer(
                runtime,
                routing.transport,
                &self.session,
                self.current()?,
                &self.dispatcher.keys,
                &self.dispatcher.domain,
            ))
            .await
            {
                Ok(member) => member,
                Err(MutationSelectionError::Retry(_)) => {
                    repair = Repair::Backoff;
                    continue;
                }
                Err(MutationSelectionError::StalePlacement) => {
                    repair = Repair::Placement;
                    continue;
                }
                Err(cause) => return Err(self.error(cause.into_dispatch())),
            };
            let attempt = self.current()?.clone();
            self.dispatched = true;
            let execution = ReceiptExecution::new(
                &self.dispatcher,
                &self.cell,
                (&self.request).into(),
                &self.session,
                attempt,
            );
            let result = match member {
                Some(member) => execution
                    .execute_remote(routing.transport, member)
                    .await
                    .map(CellReceiptResponse::remote),
                None => execution.execute(runtime, sql, options.clone()).await,
            };
            match result {
                Ok(response) => {
                    if let Some(response) = CellCommandResponse::from_receipt(response) {
                        return Ok(response);
                    }
                    // A current quorum/S3 image without a retained receipt is
                    // still unknown. Never enter SQL mutation or resubmit it.
                    repair = Repair::Backoff;
                }
                Err(cause) if retryable(&cause) => {
                    repair = if matches!(
                        cause,
                        CellDispatchError::Runtime(RuntimeError::NotFound)
                            | CellDispatchError::Transport(
                                TransportError::UnknownGroup | TransportError::StalePeer
                            )
                    ) {
                        Repair::Placement
                    } else {
                        Repair::Backoff
                    };
                }
                Err(cause) => return Err(self.error(cause)),
            }
        }
    }
}

/// Availability may trigger receipt-only repair; authentication, permission,
/// schema, identity, protocol and application failures remain terminal.
pub(super) fn retryable(error: &CellDispatchError) -> bool {
    matches!(
        error.refusal(),
        Some(
            CommandFailure::NotLeader { .. }
                | CommandFailure::Unavailable
                | CommandFailure::Overloaded
        )
    ) || matches!(
        error,
        CellDispatchError::Runtime(
            RuntimeError::NotLeader
                | RuntimeError::NotReady
                | RuntimeError::NotFound
                | RuntimeError::DeadlineExceeded
                | RuntimeError::Overloaded
                | RuntimeError::StaleCompletion
        ) | CellDispatchError::Transport(
            TransportError::Io(_)
                | TransportError::Timeout
                | TransportError::UnknownGroup
                | TransportError::StalePeer
                | TransportError::Overloaded
                | TransportError::Closed
        )
    )
}
