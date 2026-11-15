// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Only immutable resource queries may retry temporary reader admission.

use super::CellDispatchError;
use crate::raft::completion::RuntimeError;
use std::sync::Arc;

pub(super) fn retryable(error: &anyhow::Error) -> bool {
    let dispatch = error
        .downcast_ref::<Arc<CellDispatchError>>()
        .map(AsRef::as_ref)
        .or_else(|| error.downcast_ref::<CellDispatchError>());
    matches!(
        dispatch,
        Some(CellDispatchError::Runtime(
            RuntimeError::NotReady | RuntimeError::Overloaded
        ))
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn retries_only_typed_reader_admission_after_context_wrapping() {
        for error in [RuntimeError::NotReady, RuntimeError::Overloaded] {
            let wrapped = anyhow::Error::from(Arc::new(CellDispatchError::Runtime(error)))
                .context("resource history discovery");
            assert!(retryable(&wrapped));
        }
        for error in [
            RuntimeError::Closed,
            RuntimeError::DeadlineExceeded,
            RuntimeError::NotFound,
            RuntimeError::NotLeader,
        ] {
            assert!(!retryable(&CellDispatchError::Runtime(error).into()));
        }
        let fenced = CellDispatchError::Execution(crate::raft::execution::ExecutionError::Refused(
            crate::application::consensus::CommandFailure::Unavailable,
        ));
        assert!(!retryable(&fenced.into()));
        assert!(!retryable(
            &crate::application::consensus::CommandFailure::Overloaded.into()
        ));
        assert!(!retryable(&anyhow::anyhow!("NotReady")));
    }
}
