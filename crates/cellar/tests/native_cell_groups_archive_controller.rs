// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Controller-owned archive publication; no fixture event pumping after startup.
#![allow(clippy::disallowed_methods)]

#[path = "support/cell_group_archive_store.rs"]
#[allow(dead_code)]
mod archive_store;
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

#[path = "support/cell_group_archive_intents.rs"]
mod archive_intents;
use archive_intents::persist_archive_intent;

include!("support/cell_group_engine_fixture.rs");

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn short_archived_groups_checkpoint_and_recover_independently() -> Result<()> {
    Box::pin(runtime_fixture::archived_groups_reclaim_independently(
        Arc::new(object_store::memory::InMemory::new()),
    ))
    .await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires isolated local RustFS"]
async fn rustfs_short_archived_groups_checkpoint_and_recover_independently() -> Result<()> {
    Box::pin(runtime_fixture::archived_groups_reclaim_independently(
        rustfs::store("concurrent-archive-reclamation", false)?,
    ))
    .await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 3)]
async fn s3_replays_from_peer_archive_without_new_upload() -> Result<()> {
    for rooted in [false, true] {
        Box::pin(runtime_fixture::s3_replays_from_peer_archive(
            rooted,
            Arc::new(object_store::memory::InMemory::new()),
        ))
        .await?;
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 3)]
#[ignore = "requires isolated local RustFS; tools/multi-raft/cell_groups.py"]
async fn rustfs_s3_replays_from_peer_archive_without_new_upload() -> Result<()> {
    for rooted in [false, true] {
        Box::pin(runtime_fixture::s3_replays_from_peer_archive(
            rooted,
            rustfs::store("peer-archive", rooted)?,
        ))
        .await?;
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 3)]
async fn archived_progress_wakes_commands_without_maintenance_poll() -> Result<()> {
    for rooted in [false, true] {
        Box::pin(runtime_fixture::archive_progress_wakes_commands(rooted)).await?;
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 3)]
#[ignore = "requires isolated local RustFS; tools/multi-raft/cell_groups.py"]
async fn rustfs_quorum_continues_bounded_s3_outage() -> Result<()> {
    for rooted in [false, true] {
        runtime_fixture::full_backend_outage(
            rooted,
            rustfs::outage_store("full-backend-outage", rooted)?,
            Some(rustfs::control_directory()?),
        )
        .await?;
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 3)]
#[ignore = "requires isolated local RustFS; tools/multi-raft/cell_groups.py"]
async fn rustfs_group_archive_requires_all_dependencies() -> Result<()> {
    for rooted in [false, true] {
        runtime_fixture::archive_dependencies(
            rooted,
            rustfs::store("archive-dependencies", rooted)?,
        )
        .await?;
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 3)]
#[ignore = "requires isolated local RustFS; tools/multi-raft/cell_groups.py"]
async fn rustfs_group_reclaim_restart_keeps_history() -> Result<()> {
    for rooted in [false, true] {
        Box::pin(runtime_fixture::physical_log_reclamation(
            rooted,
            rustfs::store("physical-log-reclaim", rooted)?,
        ))
        .await?;
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 3)]
async fn archive_coverage_gates_local_compaction() -> Result<()> {
    for rooted in [false, true] {
        Box::pin(runtime_fixture::archive_checkpoint_frontier(
            rooted,
            Arc::new(object_store::memory::InMemory::new()),
        ))
        .await?;
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 3)]
#[ignore = "requires isolated local RustFS; tools/multi-raft/cell_groups.py"]
async fn rustfs_archive_coverage_gates_local_compaction() -> Result<()> {
    for rooted in [false, true] {
        Box::pin(runtime_fixture::archive_checkpoint_frontier(
            rooted,
            rustfs::store("archive-compaction", rooted)?,
        ))
        .await?;
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 3)]
async fn s3_remote_outputs_preserve_durability_and_creation() -> Result<()> {
    for rooted in [false, true] {
        Box::pin(runtime_fixture::s3_remote_outputs(
            rooted,
            Arc::new(object_store::memory::InMemory::new()),
        ))
        .await?;
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 3)]
async fn s3_remote_deadlines_preserve_ambiguity_and_callbacks() -> Result<()> {
    for rooted in [false, true] {
        Box::pin(runtime_fixture::s3_remote_deadlines(
            rooted,
            Arc::new(object_store::memory::InMemory::new()),
        ))
        .await?;
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 3)]
async fn s3_remote_slow_delivery_retains_proofs_and_bounds() -> Result<()> {
    for rooted in [false, true] {
        Box::pin(runtime_fixture::s3_remote_slow_delivery(
            rooted,
            Arc::new(object_store::memory::InMemory::new()),
        ))
        .await?;
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 3)]
#[ignore = "requires isolated local RustFS; tools/multi-raft/cell_groups.py"]
async fn rustfs_s3_remote_outputs_preserve_durability_and_creation() -> Result<()> {
    for rooted in [false, true] {
        Box::pin(runtime_fixture::s3_remote_outputs(
            rooted,
            rustfs::store("s3-remote-outputs", rooted)?,
        ))
        .await?;
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 3)]
#[ignore = "requires isolated local RustFS; tools/multi-raft/cell_groups.py"]
async fn rustfs_s3_remote_deadlines_preserve_ambiguity_and_callbacks() -> Result<()> {
    for rooted in [false, true] {
        Box::pin(runtime_fixture::s3_remote_deadlines(
            rooted,
            rustfs::store("s3-remote-deadlines", rooted)?,
        ))
        .await?;
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 3)]
#[ignore = "requires isolated local RustFS; tools/multi-raft/cell_groups.py"]
async fn rustfs_s3_remote_slow_delivery_retains_proofs_and_bounds() -> Result<()> {
    for rooted in [false, true] {
        Box::pin(runtime_fixture::s3_remote_slow_delivery(
            rooted,
            rustfs::store("s3-remote-delivery", rooted)?,
        ))
        .await?;
    }
    Ok(())
}

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
async fn controller_archive_reservation_recovers_after_immutable_response_loss() -> Result<()> {
    for rooted in [false, true] {
        Box::pin(runtime_fixture::archive_controller_reservation_recovery(
            rooted,
            Arc::new(object_store::memory::InMemory::new()),
        ))
        .await?;
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 3)]
#[ignore = "requires isolated local RustFS; tools/multi-raft/cell_groups.py"]
async fn rustfs_controller_archive_reservation_recovers_after_immutable_response_loss() -> Result<()>
{
    for rooted in [false, true] {
        Box::pin(runtime_fixture::archive_controller_reservation_recovery(
            rooted,
            rustfs::store("reservation-recovery", rooted)?,
        ))
        .await?;
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
