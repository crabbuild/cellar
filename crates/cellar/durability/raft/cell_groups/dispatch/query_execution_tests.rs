// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

use super::*;

#[test]
fn transient_reader_unavailability_does_not_wait_for_a_cell_mutation() {
    for may_wait in [true, false] {
        // Immutable SQL rechecks a selected read authority after admission.
        // A changed leader/source epoch may refuse it without changing this
        // cell's image. Waiting for an image mutation cannot repair that read.
        let error = ExecutionError::Storage(
            anyhow::Error::new(RuntimeError::NotReady).context("selected query authority"),
        );
        assert!(
            matches!(
                classify_query_error(error, may_wait),
                QueryError::Return(CellDispatchError::Runtime(RuntimeError::NotReady))
            ),
            "transient reader refusal must request fresh authority, not a cell mutation"
        );
    }
}

#[test]
fn read_error_policy_preserves_deadlines_fences_and_storage_failures() {
    for may_wait in [true, false] {
        assert!(matches!(
            classify_query_error(
                ExecutionError::Storage(anyhow::Error::new(RuntimeError::DeadlineExceeded)),
                may_wait
            ),
            QueryError::Return(CellDispatchError::Runtime(RuntimeError::DeadlineExceeded))
        ));
        for error in [
            ExecutionError::Storage(anyhow::anyhow!("NotReady")),
            ExecutionError::Storage(anyhow::Error::new(std::io::Error::other(
                "image read failed",
            ))),
            ExecutionError::Refused(crate::application::consensus::CommandFailure::Unavailable),
        ] {
            assert!(matches!(
                classify_query_error(error, may_wait),
                QueryError::Return(CellDispatchError::Execution(_))
            ));
        }
    }
}
