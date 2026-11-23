// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

use super::*;

#[test]
fn nested_host_failure_keeps_its_reason_at_the_sql_binding_boundary() {
    let cause = anyhow::Error::from(crate::raft::completion::RuntimeError::DeadlineExceeded)
        .context("pinned resource query");
    let failure = error(cause);
    assert_eq!(failure.family, "SQL_ERROR");
    assert!(failure.message.contains("pinned resource query"));
    assert!(failure.message.contains("DeadlineExceeded"), "{failure}");
    let runtime =
        std::iter::successors(std::error::Error::source(&failure), |&cause| cause.source())
            .find_map(|cause| cause.downcast_ref::<crate::raft::completion::RuntimeError>())
            .expect("SQL binding must retain its original typed outcome");
    assert_eq!(
        runtime,
        &crate::raft::completion::RuntimeError::DeadlineExceeded
    );
}

#[test]
fn existing_sql_error_families_keep_their_identity_and_message() {
    for family in [
        "SQL_ERROR",
        "SQL_EXEC_ERROR",
        "SQL_TYPE_ERROR",
        "SQL_COLUMN_NOTFOUND",
    ] {
        let failure = error(Error::new(family, "original detail"));
        assert_eq!(failure.family, family);
        assert_eq!(failure.message, "original detail");
    }
}
