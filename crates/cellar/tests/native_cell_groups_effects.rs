// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Shared-group effect scheduling and disposable execution resource lifetimes.
#![allow(clippy::disallowed_methods)]
// The shared engine fixture has helpers selected by sibling native targets.
#![allow(dead_code, unused_imports)]

#[path = "support/cell_group_archive_store.rs"]
mod archive_store;
#[path = "support/cell_group_checkpoint.rs"]
mod checkpoint_fixture;
#[path = "support/raft_storage_fault.rs"]
mod fault;
#[path = "support/raft_ltx_fixture.rs"]
mod images;
#[path = "support/cell_group_runtime.rs"]
mod runtime_fixture;
#[path = "support/cell_group_rustfs.rs"]
mod rustfs;

include!("support/cell_group_engine_fixture.rs");

#[tokio::test(flavor = "multi_thread", worker_threads = 3)]
async fn effect_identity_stable_across_group_binding() -> Result<()> {
    Box::pin(runtime_fixture::effect_identity_across_bindings()).await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 3)]
async fn idle_cache_reopens_exact_cell_base() -> Result<()> {
    Box::pin(runtime_fixture::idle_cache_reopens_exact_base()).await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 3)]
async fn materialized_cleanup_failure_preserves_outcome_and_group_capacity() -> Result<()> {
    Box::pin(runtime_fixture::cleanup_failure_retains_outcome()).await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 3)]
async fn due_effect_without_client_request() -> Result<()> {
    Box::pin(runtime_fixture::alarms_without_clients()).await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 3)]
async fn hot_due_cell_rotates_without_hydrating_cold_cells() -> Result<()> {
    Box::pin(runtime_fixture::hot_and_cold_alarms()).await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 3)]
async fn pending_command_or_dispatch_pins_cache() -> Result<()> {
    for rooted in [false, true] {
        Box::pin(runtime_fixture::routed_commands(rooted)).await?;
    }
    Box::pin(runtime_fixture::effect_leader_change()).await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 3)]
async fn cache_and_effect_shutdown_joins() -> Result<()> {
    Box::pin(runtime_fixture::shutdown_joins_alarm()).await?;
    Box::pin(runtime_fixture::effect_delivery_shutdown()).await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 3)]
async fn receiver_lost_reply_keeps_exact_delivery_identity() -> Result<()> {
    Box::pin(runtime_fixture::effect_lost_reply()).await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 3)]
async fn s3_claim_receiver_and_completion_use_verified_coverage() -> Result<()> {
    for rooted in [false, true] {
        Box::pin(runtime_fixture::effect_s3_lost_reply(
            rooted,
            Arc::new(object_store::memory::InMemory::new()),
        ))
        .await?;
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 3)]
async fn committed_s3_output_survives_later_group_apply() -> Result<()> {
    for rooted in [false, true] {
        Box::pin(runtime_fixture::output_during_apply(rooted)).await?;
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 3)]
#[ignore = "requires isolated local RustFS"]
async fn rustfs_claim_receiver_and_completion_use_verified_coverage() -> Result<()> {
    for rooted in [false, true] {
        Box::pin(runtime_fixture::effect_s3_lost_reply(
            rooted,
            rustfs::store("cell-group-effects", rooted)?,
        ))
        .await?;
    }
    Ok(())
}
