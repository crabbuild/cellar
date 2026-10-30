// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Group isolation never converts shared-engine uncertainty into availability.
use super::*;
use crate::raft::{cell_groups::CellDispatchError, storage::LogError};

fn runtime(error: &RuntimeError) -> bool {
    matches!(error, RuntimeError::Closed)
        || matches!(error, RuntimeError::Storage(error) if storage(error))
}

fn storage(error: &LogError) -> bool {
    matches!(
        error,
        LogError::Io(_) | LogError::Corrupt(_) | LogError::LostStableState | LogError::Closed
    )
}

fn execution(error: &ExecutionError) -> bool {
    match error {
        // A lost shared SQL worker reply cannot prove that the pool remains
        // usable. Preserve its existing joined node shutdown boundary.
        ExecutionError::Refused(cellar_logic::consensus::CommandFailure::Unavailable) => true,
        ExecutionError::Storage(error) => {
            error.downcast_ref::<LogError>().is_some_and(storage)
                || error.downcast_ref::<RuntimeError>().is_some_and(runtime)
        }
        _ => false,
    }
}

fn dispatch(error: &CellDispatchError) -> bool {
    match error {
        CellDispatchError::Runtime(error) => runtime(error),
        CellDispatchError::Execution(error) => execution(error),
        _ => false,
    }
}

pub(super) fn node_wide(error: &CellGroupControllerError) -> bool {
    match error {
        CellGroupControllerError::Runtime(error) => runtime(error),
        CellGroupControllerError::Apply(error) => execution(error),
        CellGroupControllerError::Command(error) => dispatch(&error.cause),
        CellGroupControllerError::Enrollment(error) => dispatch(&error.cause),
        CellGroupControllerError::Closed | CellGroupControllerError::Transport(_) => true,
        CellGroupControllerError::Invalid(_) => false,
    }
}
