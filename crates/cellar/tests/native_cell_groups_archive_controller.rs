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

include!("support/cell_group_engine_fixture.rs");

#[tokio::test(flavor = "multi_thread", worker_threads = 3)]
async fn s3_payload_credit_release_wakes_original_request_with_reused_archive() -> Result<()> {
    Box::pin(runtime_fixture::s3_payload_capacity_release(false, false)).await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 3)]
async fn rooted_s3_payload_credit_release_wakes_original_request_with_reused_archive() -> Result<()>
{
    Box::pin(runtime_fixture::s3_payload_capacity_release(true, false)).await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 3)]
async fn background_payload_credit_release_preserves_archive_cadence() -> Result<()> {
    for rooted in [false, true] {
        Box::pin(runtime_fixture::s3_payload_capacity_release(rooted, true)).await?;
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 3)]
async fn archive_retries_payload_admission_after_the_actual_reader_joins() -> Result<()> {
    Box::pin(runtime_fixture::payload_read_failure(
        false,
        Arc::new(object_store::memory::InMemory::new()),
        runtime_fixture::PayloadFailure::Backpressure,
    ))
    .await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 3)]
async fn rooted_archive_retries_payload_admission_after_the_actual_reader_joins() -> Result<()> {
    Box::pin(runtime_fixture::payload_read_failure(
        true,
        Arc::new(object_store::memory::InMemory::new()),
        runtime_fixture::PayloadFailure::Backpressure,
    ))
    .await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 3)]
async fn archive_retries_temporary_payload_reads_with_its_original_intent() -> Result<()> {
    Box::pin(runtime_fixture::payload_read_failure(
        false,
        Arc::new(object_store::memory::InMemory::new()),
        runtime_fixture::PayloadFailure::Unavailable,
    ))
    .await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 3)]
async fn rooted_archive_retries_temporary_payload_reads_with_its_original_intent() -> Result<()> {
    Box::pin(runtime_fixture::payload_read_failure(
        true,
        Arc::new(object_store::memory::InMemory::new()),
        runtime_fixture::PayloadFailure::Unavailable,
    ))
    .await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 3)]
async fn archive_missing_payload_remains_terminal() -> Result<()> {
    for rooted in [false, true] {
        Box::pin(runtime_fixture::payload_read_failure(
            rooted,
            Arc::new(object_store::memory::InMemory::new()),
            runtime_fixture::PayloadFailure::Missing,
        ))
        .await?;
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 3)]
async fn archive_corrupt_payload_remains_terminal() -> Result<()> {
    for rooted in [false, true] {
        Box::pin(runtime_fixture::payload_read_failure(
            rooted,
            Arc::new(object_store::memory::InMemory::new()),
            runtime_fixture::PayloadFailure::Corrupt,
        ))
        .await?;
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 3)]
async fn quorum_command_reconnects_after_peer_idle_close() -> Result<()> {
    for rooted in [false, true] {
        Box::pin(runtime_fixture::peer_connection_idle(rooted, false)).await?;
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 3)]
async fn raft_control_reconnects_after_peer_idle_close() -> Result<()> {
    for rooted in [false, true] {
        Box::pin(runtime_fixture::peer_connection_idle(rooted, true)).await?;
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn short_archived_groups_checkpoint_and_recover_independently() -> Result<()> {
    for rooted in [false, true] {
        Box::pin(runtime_fixture::archived_groups_reclaim_independently(
            rooted,
            Arc::new(object_store::memory::InMemory::new()),
        ))
        .await?;
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires isolated local RustFS"]
async fn rustfs_short_archived_groups_checkpoint_and_recover_independently() -> Result<()> {
    for rooted in [false, true] {
        Box::pin(runtime_fixture::archived_groups_reclaim_independently(
            rooted,
            rustfs::store("concurrent-archive-reclamation", rooted)?,
        ))
        .await?;
    }
    Ok(())
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
async fn s3_response_precedes_previous_archive_hold_retirement() -> Result<()> {
    for rooted in [false, true] {
        Box::pin(runtime_fixture::s3_response_precedes_hold_retirement(
            rooted,
            Arc::new(object_store::memory::InMemory::new()),
        ))
        .await?;
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 3)]
async fn archive_retirement_coalesces_the_next_durable_intent() -> Result<()> {
    Box::pin(runtime_fixture::retirement_coalesces_next_archive_intent(
        false,
        Arc::new(object_store::memory::InMemory::new()),
    ))
    .await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 3)]
async fn rooted_archive_retirement_coalesces_the_next_durable_intent() -> Result<()> {
    Box::pin(runtime_fixture::retirement_coalesces_next_archive_intent(
        true,
        Arc::new(object_store::memory::InMemory::new()),
    ))
    .await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 3)]
#[ignore = "requires isolated local RustFS"]
async fn rustfs_s3_response_precedes_previous_archive_hold_retirement() -> Result<()> {
    for rooted in [false, true] {
        Box::pin(runtime_fixture::s3_response_precedes_hold_retirement(
            rooted,
            rustfs::store("archive-deferred-retirement", rooted)?,
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
async fn follower_archiver_bootstraps_the_first_prefix() -> Result<()> {
    for rooted in [false, true] {
        Box::pin(runtime_fixture::follower_bootstraps_first_prefix(
            rooted,
            Arc::new(object_store::memory::InMemory::new()),
        ))
        .await?;
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 3)]
#[ignore = "requires isolated local RustFS"]
async fn rustfs_follower_archiver_bootstraps_the_first_prefix() -> Result<()> {
    for rooted in [false, true] {
        Box::pin(runtime_fixture::follower_bootstraps_first_prefix(
            rooted,
            rustfs::store("follower-first-archive-prefix", rooted)?,
        ))
        .await?;
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 3)]
async fn follower_archiver_verifies_a_published_prefix_without_upload() -> Result<()> {
    for rooted in [false, true] {
        Box::pin(runtime_fixture::follower_acquires_published_prefix(
            rooted,
            Arc::new(object_store::memory::InMemory::new()),
        ))
        .await?;
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 3)]
#[ignore = "requires isolated local RustFS"]
async fn rustfs_follower_archiver_verifies_a_published_prefix_without_upload() -> Result<()> {
    for rooted in [false, true] {
        Box::pin(runtime_fixture::follower_acquires_published_prefix(
            rooted,
            rustfs::store("follower-archive-prefix", rooted)?,
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
async fn s3_remote_unknown_reply_requires_exact_s3_receipt() -> Result<()> {
    for rooted in [false, true] {
        Box::pin(runtime_fixture::s3_remote_receipt_reconciliation(
            rooted,
            Arc::new(object_store::memory::InMemory::new()),
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
async fn controller_archive_extends_committed_tails_and_rolls_checkpoints() -> Result<()> {
    for rooted in [false, true] {
        Box::pin(runtime_fixture::archive_controller_tail_rollover(rooted)).await?;
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 3)]
async fn controller_archive_bounds_hot_cell_replay_work() -> Result<()> {
    for rooted in [false, true] {
        Box::pin(runtime_fixture::archive_hot_cell_tail_bound(rooted)).await?;
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
async fn s3_enrollment_output_waits_for_serving_authority_without_reinitialization() -> Result<()> {
    for rooted in [false, true] {
        for expire in [false, true] {
            Box::pin(runtime_fixture::enrollment_output_during_apply(
                rooted,
                Arc::new(object_store::memory::InMemory::new()),
                expire,
            ))
            .await?;
        }
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 3)]
#[ignore = "requires isolated local RustFS"]
async fn rustfs_s3_enrollment_output_waits_for_serving_authority_without_reinitialization(
) -> Result<()> {
    for rooted in [false, true] {
        for expire in [false, true] {
            Box::pin(runtime_fixture::enrollment_output_during_apply(
                rooted,
                rustfs::store(
                    if expire {
                        "enrollment-output-expiry"
                    } else {
                        "enrollment-output-progress"
                    },
                    rooted,
                )?,
                expire,
            ))
            .await?;
        }
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
async fn new_s3_receipt_bypasses_inflight_archive_maintenance() -> Result<()> {
    for rooted in [false, true] {
        Box::pin(
            runtime_fixture::s3_newer_receipt_bypasses_inflight_maintenance(
                rooted,
                Arc::new(object_store::memory::InMemory::new()),
            ),
        )
        .await?;
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 3)]
#[ignore = "requires isolated local RustFS; tools/multi-raft/cell_groups.py"]
async fn rustfs_new_s3_receipt_bypasses_inflight_archive_maintenance() -> Result<()> {
    for rooted in [false, true] {
        Box::pin(
            runtime_fixture::s3_newer_receipt_bypasses_inflight_maintenance(
                rooted,
                rustfs::store("foreground-archive-maintenance", rooted)?,
            ),
        )
        .await?;
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 3)]
async fn late_s3_receipt_reuses_completed_background_verification() -> Result<()> {
    for rooted in [false, true] {
        Box::pin(
            runtime_fixture::s3_late_receipt_uses_completed_verification(
                rooted,
                Arc::new(object_store::memory::InMemory::new()),
            ),
        )
        .await?;
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 3)]
async fn idle_s3_observation_reuses_completed_verification() -> Result<()> {
    for rooted in [false, true] {
        Box::pin(runtime_fixture::s3_idle_observation_reuses_verification(
            rooted,
            Arc::new(object_store::memory::InMemory::new()),
        ))
        .await?;
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 3)]
#[ignore = "requires isolated local RustFS; tools/multi-raft/cell_groups.py"]
async fn rustfs_idle_s3_observation_reuses_completed_verification() -> Result<()> {
    for rooted in [false, true] {
        Box::pin(runtime_fixture::s3_idle_observation_reuses_verification(
            rooted,
            rustfs::store("idle-observation-verification", rooted)?,
        ))
        .await?;
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 3)]
#[ignore = "requires isolated local RustFS; tools/multi-raft/cell_groups.py"]
async fn rustfs_late_s3_receipt_reuses_completed_background_verification() -> Result<()> {
    for rooted in [false, true] {
        Box::pin(
            runtime_fixture::s3_late_receipt_uses_completed_verification(
                rooted,
                rustfs::store("late-receipt-verification", rooted)?,
            ),
        )
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

#[tokio::test(flavor = "multi_thread", worker_threads = 3)]
async fn s3_output_credit_release_wakes_original_request() -> Result<()> {
    for rooted in [false, true] {
        Box::pin(runtime_fixture::s3_capacity_release(
            rooted,
            Arc::new(object_store::memory::InMemory::new()),
            false,
        ))
        .await?;
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 3)]
async fn background_credit_release_preserves_archive_cadence() -> Result<()> {
    for rooted in [false, true] {
        Box::pin(runtime_fixture::s3_background_capacity_cadence(
            rooted, false,
        ))
        .await?;
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 3)]
async fn expired_s3_credit_release_preserves_archive_cadence() -> Result<()> {
    for rooted in [false, true] {
        Box::pin(runtime_fixture::s3_background_capacity_cadence(
            rooted, true,
        ))
        .await?;
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 3)]
async fn s3_checkpoint_credit_release_wakes_original_request() -> Result<()> {
    for rooted in [false, true] {
        Box::pin(runtime_fixture::s3_capacity_release(
            rooted,
            Arc::new(object_store::memory::InMemory::new()),
            true,
        ))
        .await?;
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 3)]
#[ignore = "requires isolated local RustFS"]
async fn rustfs_archive_retirement_coalesces_the_next_durable_intent() -> Result<()> {
    Box::pin(runtime_fixture::retirement_coalesces_next_archive_intent(
        false,
        rustfs::store("retirement-coalescing", false)?,
    ))
    .await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 3)]
#[ignore = "requires isolated local RustFS"]
async fn rustfs_rooted_archive_retirement_coalesces_the_next_durable_intent() -> Result<()> {
    Box::pin(runtime_fixture::retirement_coalesces_next_archive_intent(
        true,
        rustfs::store("retirement-coalescing", true)?,
    ))
    .await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 3)]
#[ignore = "requires isolated local RustFS"]
async fn rustfs_archive_retries_temporary_payload_reads_with_its_original_intent() -> Result<()> {
    Box::pin(runtime_fixture::payload_read_failure(
        false,
        rustfs::store("payload-archive-retry", false)?,
        runtime_fixture::PayloadFailure::Unavailable,
    ))
    .await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 3)]
#[ignore = "requires isolated local RustFS"]
async fn rustfs_rooted_archive_retries_temporary_payload_reads_with_its_original_intent(
) -> Result<()> {
    Box::pin(runtime_fixture::payload_read_failure(
        true,
        rustfs::store("payload-archive-retry", true)?,
        runtime_fixture::PayloadFailure::Unavailable,
    ))
    .await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 3)]
#[ignore = "requires isolated local RustFS"]
async fn rustfs_archive_missing_payload_remains_terminal() -> Result<()> {
    for rooted in [false, true] {
        Box::pin(runtime_fixture::payload_read_failure(
            rooted,
            rustfs::store("payload-archive-missing", rooted)?,
            runtime_fixture::PayloadFailure::Missing,
        ))
        .await?;
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 3)]
#[ignore = "requires isolated local RustFS"]
async fn rustfs_archive_corrupt_payload_remains_terminal() -> Result<()> {
    for rooted in [false, true] {
        Box::pin(runtime_fixture::payload_read_failure(
            rooted,
            rustfs::store("payload-archive-corrupt", rooted)?,
            runtime_fixture::PayloadFailure::Corrupt,
        ))
        .await?;
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 3)]
#[ignore = "requires isolated local RustFS"]
async fn rustfs_archive_retries_payload_admission_after_the_actual_reader_joins() -> Result<()> {
    Box::pin(runtime_fixture::payload_read_failure(
        false,
        rustfs::store("payload-archive-pressure", false)?,
        runtime_fixture::PayloadFailure::Backpressure,
    ))
    .await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 3)]
#[ignore = "requires isolated local RustFS"]
async fn rustfs_rooted_archive_retries_payload_admission_after_the_actual_reader_joins(
) -> Result<()> {
    Box::pin(runtime_fixture::payload_read_failure(
        true,
        rustfs::store("payload-archive-pressure", true)?,
        runtime_fixture::PayloadFailure::Backpressure,
    ))
    .await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 3)]
async fn archive_retirement_shares_next_intent_without_early_allocation() -> Result<()> {
    for rooted in [false, true] {
        Box::pin(runtime_fixture::archive_retirement_joins_next_intent(
            rooted,
            Arc::new(object_store::memory::InMemory::new()),
        ))
        .await?;
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 3)]
#[ignore = "requires isolated local RustFS"]
async fn rustfs_archive_retirement_shares_next_intent_without_early_allocation() -> Result<()> {
    for rooted in [false, true] {
        Box::pin(runtime_fixture::archive_retirement_joins_next_intent(
            rooted,
            rustfs::store("archive-retirement-intent", rooted)?,
        ))
        .await?;
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 3)]
async fn archive_reservation_intent_recovers_failed_chunk_io_without_ticket_rewrite() -> Result<()>
{
    for rooted in [false, true] {
        Box::pin(
            runtime_fixture::archive_reservation_retries_from_durable_intent(
                rooted,
                Arc::new(object_store::memory::InMemory::new()),
            ),
        )
        .await?;
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 3)]
#[ignore = "requires isolated local RustFS"]
async fn rustfs_archive_reservation_intent_recovers_failed_chunk_io_without_ticket_rewrite(
) -> Result<()> {
    for rooted in [false, true] {
        Box::pin(
            runtime_fixture::archive_reservation_retries_from_durable_intent(
                rooted,
                rustfs::store("archive-reserve-recovery", rooted)?,
            ),
        )
        .await?;
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 3)]
async fn payload_get_outage_recovers_without_application_requests() -> Result<()> {
    for rooted in [false, true] {
        Box::pin(runtime_fixture::archive_controller_payload_outage(
            rooted,
            Arc::new(object_store::memory::InMemory::new()),
        ))
        .await?;
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 3)]
#[ignore = "requires isolated local RustFS"]
async fn rustfs_payload_get_outage_recovers_without_application_requests() -> Result<()> {
    for rooted in [false, true] {
        Box::pin(runtime_fixture::archive_controller_payload_outage(
            rooted,
            rustfs::store("payload-get-outage-retry", rooted)?,
        ))
        .await?;
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 3)]
async fn foreground_s3_receipt_precedes_queued_background_archive() -> Result<()> {
    for rooted in [false, true] {
        Box::pin(
            runtime_fixture::s3_foreground_receipt_precedes_queued_background_archive(
                rooted,
                Arc::new(object_store::memory::InMemory::new()),
            ),
        )
        .await?;
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 3)]
#[ignore = "requires isolated local RustFS"]
async fn rustfs_foreground_s3_receipt_precedes_queued_background_archive() -> Result<()> {
    for rooted in [false, true] {
        Box::pin(
            runtime_fixture::s3_foreground_receipt_precedes_queued_background_archive(
                rooted,
                rustfs::store("foreground-publication-priority", rooted)?,
            ),
        )
        .await?;
    }
    Ok(())
}
