// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Receipt admission and one atomic business/receipt transaction for both owners.

use super::*;

pub(super) enum Admission {
    Duplicate,
    New(Option<crate::raft::receipt_maintenance::Plan>),
}

pub(super) fn prepare(
    connection: &rusqlite::Connection,
    limits: ExecutionLimits,
    cursor: &mut Option<ClientId>,
    attempt: &CommandAttempt,
    allow_new: bool,
) -> Result<Admission, ExecutionError> {
    if attempt.request.operation.is_empty() || attempt.request.operation.len() > 128 {
        return Err(ExecutionError::Invalid("command operation"));
    }
    if receipts::preflight_identity(connection, &attempt.request, &limits)?.is_some() {
        return Ok(Admission::Duplicate);
    }
    let count = receipts::retained_count(connection)?;
    let ceiling = limits.max_retained_receipts;
    let maintenance = match limits.receipt_maintenance {
        Some(limits) if count >= ceiling - ceiling / 4 => {
            let plan = crate::raft::receipt_maintenance::plan(
                connection,
                attempt.inputs.now_unix_millis,
                limits,
                *cursor,
            )?;
            *cursor = plan.next_cursor;
            Some(plan)
        }
        _ => None,
    };
    let removed = maintenance.as_ref().map_or(0, |plan| plan.rows_removed);
    if count
        .checked_sub(removed)
        .is_none_or(|count| count >= ceiling)
    {
        return Err(ExecutionError::Refused(CommandFailure::Overloaded));
    }
    if attempt.request.retry_until_ms < attempt.inputs.now_unix_millis
        || attempt
            .request
            .retry_until_ms
            .checked_sub(attempt.inputs.now_unix_millis)
            .is_none_or(|window| window > limits.max_retry_window_ms)
    {
        return Err(ExecutionError::Invalid("command retry window"));
    }
    // Duplicate receipt lookup above remains available under archive
    // pressure. A known refusal has not mutated SQL or capture state and
    // must not force the next request to reconstruct the private image.
    if !allow_new {
        return Err(ExecutionError::Refused(CommandFailure::Overloaded));
    }
    Ok(Admission::New(maintenance))
}

pub(super) fn apply(
    database: &crate::storage::CommandDatabase,
    limits: ExecutionLimits,
    attempt: &CommandAttempt,
    maintenance: Option<crate::raft::receipt_maintenance::Plan>,
    command: impl FnOnce(&CommandTransaction<'_>) -> anyhow::Result<Vec<u8>>,
) -> Result<(), ExecutionError> {
    database
        .transaction(attempt.inputs, |connection, transaction| {
            if let Some(maintenance) = maintenance {
                maintenance
                    .apply(connection, attempt.inputs.now_unix_millis)
                    .map_err(anyhow::Error::new)?;
            }
            let result = command(transaction)?;
            if result.len() > limits.max_result_bytes {
                return Err(anyhow::Error::new(CommandFailure::Overloaded));
            }
            receipts::record(connection, &attempt.request, result)?;
            let limits = &limits;
            if limits.receipt_reserve_bytes != 0 {
                let usage =
                    crate::storage::CommandDatabase::capacity(connection, limits.max_image_bytes)?;
                limits
                    .check_capacity(
                        usage.application_bytes,
                        usage.receipt_bytes,
                        usage.structural_reserve_bytes,
                    )
                    .map_err(anyhow::Error::new)?;
            }
            Ok(())
        })
        .map_err(private_failure)?;
    Ok(())
}
