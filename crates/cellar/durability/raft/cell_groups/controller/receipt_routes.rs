// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Receipt reads follow selected groups without staging any mutation.
use super::*;
use crate::raft::cell_groups::{
    CellDispatchError, CellReceiptInvocation, CellReceiptResponse, CellSqlOptions,
};

pub(super) async fn execute(
    shared: &Shared,
    mut invocation: CellReceiptInvocation,
    options: CellSqlOptions,
) -> Result<CellReceiptResponse, CellDispatchError> {
    let incoming = invocation.is_remote_destination();
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
        } else if incoming {
            // Received RPCs always execute locally; they never become routers.
            Err(CellDispatchError::Runtime(RuntimeError::NotFound))
        } else {
            match invocation
                .remote_member(&shared.runtime, &shared.transport)
                .await
            {
                Ok(Some(member)) => invocation
                    .execute_remote(&shared.transport, member)
                    .await
                    .map(CellReceiptResponse::remote),
                Ok(None) => Err(CellDispatchError::Runtime(RuntimeError::NotFound)),
                Err(error) => Err(error),
            }
        };
        match result {
            Ok(response) => return Ok(response),
            Err(error)
                if !incoming
                    && ordinal < invocation.maximum_attempts()
                    && super::query_routes::retryable(&error) =>
            {
                invocation.refresh().await?;
            }
            Err(error) => return Err(error),
        }
    }
}
