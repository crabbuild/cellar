// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Receipt reads follow selected groups without staging any mutation.
use super::*;
use crate::raft::cell_groups::dispatch::MutationSelectionError;
use crate::raft::cell_groups::{
    CellDispatchError, CellReceiptInvocation, CellReceiptResponse, CellSqlOptions,
};
use cellar_logic::consensus::CommandFailure;

pub(super) async fn execute(
    shared: &Shared,
    mut invocation: CellReceiptInvocation,
    options: CellSqlOptions,
    transport: Option<&RaftTransport>,
) -> Result<CellReceiptResponse, CellDispatchError> {
    let routed = !invocation.is_remote_destination() && transport.is_some();
    invocation.begin().await?;
    loop {
        let attempt = invocation
            .attempt()
            .ok_or(CellDispatchError::Runtime(RuntimeError::NotFound))?;
        let ordinal = usize::from(attempt.ordinal());
        let local = shared
            .groups
            .contains(&attempt.route().history)
            .map_err(CellDispatchError::Runtime)?;
        let result = if local {
            let sql = shared
                .sql
                .upgrade()
                .ok_or(CellDispatchError::Runtime(RuntimeError::Closed))?;
            invocation
                .execute(&shared.runtime, &sql, options.clone())
                .await
        } else if !routed {
            // Received RPCs always execute locally; they never become routers.
            Err(CellDispatchError::Runtime(RuntimeError::NotFound))
        } else if let Some(transport) = transport {
            match invocation.remote_member(&shared.runtime, transport).await {
                Ok(Some(member)) => invocation
                    .execute_remote(transport, member)
                    .await
                    .map(CellReceiptResponse::remote),
                Ok(None) => Err(CellDispatchError::Runtime(RuntimeError::NotFound)),
                Err(MutationSelectionError::Retry(_))
                    if ordinal < invocation.maximum_attempts() =>
                {
                    invocation
                        .retry_leader(shared.limits.retry_interval_ms)
                        .await?;
                    continue;
                }
                Err(MutationSelectionError::StalePlacement)
                    if ordinal < invocation.maximum_attempts() =>
                {
                    invocation.refresh().await?;
                    continue;
                }
                Err(error) => return Err(error.into_dispatch()),
            }
        } else {
            Err(CellDispatchError::Runtime(RuntimeError::NotFound))
        };
        match result {
            Ok(response) => return Ok(response),
            Err(error)
                if routed
                    && ordinal < invocation.maximum_attempts()
                    && matches!(
                        error.refusal(),
                        Some(CommandFailure::NotLeader { .. } | CommandFailure::Overloaded)
                    ) =>
            {
                invocation
                    .retry_leader(shared.limits.retry_interval_ms)
                    .await?;
            }
            Err(error)
                if routed
                    && ordinal < invocation.maximum_attempts()
                    && super::query_routes::retryable(&error) =>
            {
                invocation.refresh().await?;
            }
            Err(error) => return Err(error),
        }
    }
}
