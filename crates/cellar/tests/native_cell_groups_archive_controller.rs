// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Controller-owned archive publication; no fixture event pumping after startup.
#![allow(clippy::disallowed_methods)]

#[path = "support/cell_group_checkpoint.rs"]
#[allow(dead_code)]
mod checkpoint_fixture;
#[path = "support/raft_storage_fault.rs"]
#[allow(dead_code)]
mod fault;
#[path = "support/raft_ltx_fixture.rs"]
#[allow(dead_code)]
mod images;
#[path = "support/cell_group_runtime.rs"]
#[allow(dead_code, unused_imports)]
mod runtime_fixture;
#[path = "support/cell_group_rustfs.rs"]
mod rustfs;

include!("support/cell_group_engine_fixture.rs");

#[tokio::test(flavor = "multi_thread", worker_threads = 3)]
async fn s3_namespace_minimum_cannot_be_downgraded() -> Result<()> {
    for rooted in [false, true] {
        Box::pin(runtime_fixture::s3_namespace_minimum(
            rooted,
            Arc::new(object_store::memory::InMemory::new()),
        ))
        .await?;
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 3)]
#[ignore = "requires isolated local RustFS; tools/multi-raft/cell_groups.py"]
async fn rustfs_s3_namespace_minimum_cannot_be_downgraded() -> Result<()> {
    for rooted in [false, true] {
        Box::pin(runtime_fixture::s3_namespace_minimum(
            rooted,
            rustfs::store("s3-minimum", rooted)?,
        ))
        .await?;
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 3)]
async fn s3_command_requires_the_sibling_archive_dependencies() -> Result<()> {
    for rooted in [false, true] {
        Box::pin(runtime_fixture::s3_missing_sibling(
            rooted,
            Arc::new(object_store::memory::InMemory::new()),
        ))
        .await?;
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 3)]
#[ignore = "requires isolated local RustFS; tools/multi-raft/cell_groups.py"]
async fn rustfs_s3_command_requires_the_sibling_archive_dependencies() -> Result<()> {
    for rooted in [false, true] {
        Box::pin(runtime_fixture::s3_missing_sibling(
            rooted,
            rustfs::store("s3-sibling", rooted)?,
        ))
        .await?;
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 3)]
async fn s3_commands_require_exact_archived_results() -> Result<()> {
    for rooted in [false, true] {
        Box::pin(runtime_fixture::s3_commands(
            rooted,
            Arc::new(object_store::memory::InMemory::new()),
        ))
        .await?;
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 3)]
async fn s3_command_deadlines_do_not_quarantine_committed_sql() -> Result<()> {
    for rooted in [false, true] {
        for cancel in [false, true] {
            Box::pin(runtime_fixture::s3_deadline(
                rooted,
                Arc::new(object_store::memory::InMemory::new()),
                cancel,
            ))
            .await?;
        }
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 3)]
#[ignore = "requires isolated local RustFS; tools/multi-raft/cell_groups.py"]
async fn rustfs_s3_commands_require_exact_archived_results() -> Result<()> {
    for rooted in [false, true] {
        Box::pin(runtime_fixture::s3_commands(
            rooted,
            rustfs::store("s3-commands", rooted)?,
        ))
        .await?;
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 3)]
#[ignore = "requires isolated local RustFS; tools/multi-raft/cell_groups.py"]
async fn rustfs_s3_command_deadlines_do_not_quarantine_committed_sql() -> Result<()> {
    for rooted in [false, true] {
        for cancel in [false, true] {
            Box::pin(runtime_fixture::s3_deadline(
                rooted,
                rustfs::store(if cancel { "s3-cancel" } else { "s3-deadline" }, rooted)?,
                cancel,
            ))
            .await?;
        }
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 3)]
async fn controller_archive_publishes_stable_cuts_while_quorum_advances() -> Result<()> {
    for rooted in [false, true] {
        Box::pin(runtime_fixture::archive_controller_publish(rooted)).await?;
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 3)]
async fn controller_archive_reconciles_exact_intent_and_joins_close() -> Result<()> {
    for rooted in [false, true] {
        Box::pin(runtime_fixture::archive_controller_reconcile(rooted)).await?;
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 3)]
#[ignore = "requires isolated local RustFS; tools/multi-raft/cell_groups.py"]
async fn rustfs_controller_archive_publishes_stable_cuts() -> Result<()> {
    for rooted in [false, true] {
        Box::pin(runtime_fixture::archive_controller_publish_store(
            rooted,
            rustfs::store("publish", rooted)?,
        ))
        .await?;
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 3)]
#[ignore = "requires isolated local RustFS; tools/multi-raft/cell_groups.py"]
async fn rustfs_controller_archive_reconciles_exact_intent_and_joins_close() -> Result<()> {
    for rooted in [false, true] {
        Box::pin(runtime_fixture::archive_controller_reconcile_store(
            rooted,
            rustfs::store("reconcile", rooted)?,
        ))
        .await?;
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 3)]
#[ignore = "requires isolated local RustFS; tools/multi-raft/cell_groups.py"]
async fn rustfs_unproved_s3_responses_refuse() -> Result<()> {
    for rooted in [false, true] {
        Box::pin(runtime_fixture::unproved_s3_responses(
            rooted,
            rustfs::store("responses", rooted)?,
        ))
        .await?;
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 3)]
async fn s3_enrollment_requires_the_original_archived_creation() -> Result<()> {
    for rooted in [false, true] {
        Box::pin(runtime_fixture::s3_enrollment(
            rooted,
            Arc::new(object_store::memory::InMemory::new()),
        ))
        .await?;
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 3)]
async fn s3_enrollment_deadlines_preserve_resolved_ownership() -> Result<()> {
    for rooted in [false, true] {
        for cancel in [false, true] {
            Box::pin(runtime_fixture::s3_enrollment_deadline(
                rooted,
                Arc::new(object_store::memory::InMemory::new()),
                cancel,
            ))
            .await?;
        }
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 3)]
#[ignore = "requires isolated local RustFS; tools/multi-raft/cell_groups.py"]
async fn rustfs_s3_enrollment_requires_the_original_archived_creation() -> Result<()> {
    for rooted in [false, true] {
        Box::pin(runtime_fixture::s3_enrollment(
            rooted,
            rustfs::store("s3-enrollment", rooted)?,
        ))
        .await?;
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 3)]
#[ignore = "requires isolated local RustFS; tools/multi-raft/cell_groups.py"]
async fn rustfs_s3_enrollment_deadlines_preserve_resolved_ownership() -> Result<()> {
    for rooted in [false, true] {
        for cancel in [false, true] {
            Box::pin(runtime_fixture::s3_enrollment_deadline(
                rooted,
                rustfs::store(
                    if cancel {
                        "s3-enrollment-cancel"
                    } else {
                        "s3-enrollment-deadline"
                    },
                    rooted,
                )?,
                cancel,
            ))
            .await?;
        }
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 3)]
async fn s3_receipt_lookup_tracks_archived_observations() -> Result<()> {
    for rooted in [false, true] {
        Box::pin(runtime_fixture::s3_receipt_observations(
            rooted,
            Arc::new(object_store::memory::InMemory::new()),
        ))
        .await?;
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 3)]
#[ignore = "requires isolated local RustFS; tools/multi-raft/cell_groups.py"]
async fn rustfs_s3_receipt_lookup_tracks_archived_observations() -> Result<()> {
    for rooted in [false, true] {
        Box::pin(runtime_fixture::s3_receipt_observations(
            rooted,
            rustfs::store("s3-receipt-observations", rooted)?,
        ))
        .await?;
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 3)]
async fn s3_receipt_lookup_honors_namespace_and_stored_minimums() -> Result<()> {
    for rooted in [false, true] {
        Box::pin(runtime_fixture::s3_receipt_minimums(
            rooted,
            Arc::new(object_store::memory::InMemory::new()),
        ))
        .await?;
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 3)]
#[ignore = "requires isolated local RustFS; tools/multi-raft/cell_groups.py"]
async fn rustfs_s3_receipt_lookup_honors_namespace_and_stored_minimums() -> Result<()> {
    for rooted in [false, true] {
        Box::pin(runtime_fixture::s3_receipt_minimums(
            rooted,
            rustfs::store("s3-receipt-minimums", rooted)?,
        ))
        .await?;
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 3)]
async fn s3_receipt_lookup_preserves_typed_failures_and_result_admission() -> Result<()> {
    for rooted in [false, true] {
        Box::pin(runtime_fixture::s3_receipt_failures(
            rooted,
            Arc::new(object_store::memory::InMemory::new()),
        ))
        .await?;
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 3)]
#[ignore = "requires isolated local RustFS; tools/multi-raft/cell_groups.py"]
async fn rustfs_s3_receipt_lookup_preserves_typed_failures_and_result_admission() -> Result<()> {
    for rooted in [false, true] {
        Box::pin(runtime_fixture::s3_receipt_failures(
            rooted,
            rustfs::store("s3-receipt-failures", rooted)?,
        ))
        .await?;
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 3)]
async fn s3_receipt_lookup_deadlines_refund_read_admission() -> Result<()> {
    for rooted in [false, true] {
        for cancel in [false, true] {
            Box::pin(runtime_fixture::s3_receipt_deadline(
                rooted,
                Arc::new(object_store::memory::InMemory::new()),
                cancel,
            ))
            .await?;
        }
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 3)]
#[ignore = "requires isolated local RustFS; tools/multi-raft/cell_groups.py"]
async fn rustfs_s3_receipt_lookup_deadlines_refund_read_admission() -> Result<()> {
    for rooted in [false, true] {
        for cancel in [false, true] {
            Box::pin(runtime_fixture::s3_receipt_deadline(
                rooted,
                rustfs::store(
                    if cancel {
                        "s3-receipt-cancel"
                    } else {
                        "s3-receipt-deadline"
                    },
                    rooted,
                )?,
                cancel,
            ))
            .await?;
        }
    }
    Ok(())
}
