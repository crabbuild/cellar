// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! CG-05 acceptance over the same real engines, SQLite images and TLS controllers
//! as native_cell_groups_execution. Each named case retains direct fixture oracles.
#![allow(clippy::disallowed_methods)]

#[path = "support/raft_storage_fault.rs"]
#[allow(dead_code)]
mod fault;
#[path = "support/raft_ltx_fixture.rs"]
#[allow(dead_code)]
mod images;
// These shared fixture modules expose other execution scenarios to the sibling
// integration target. Only the CG-05 scenarios are selected in this target.
#[path = "support/cell_group_checkpoint.rs"]
#[allow(dead_code)]
mod checkpoint_fixture;
#[path = "support/cell_group_runtime.rs"]
#[allow(dead_code, unused_imports)]
mod runtime_fixture;

include!("support/cell_group_engine_fixture.rs");

// Construct large fixture futures outside the composite test's poll frame.
// Boxing at the call site alone still leaves their construction temporaries on
// that frame in debug builds, consuming stack while a nested fixture is polled.
#[inline(never)]
fn fixture<F: std::future::Future<Output = Result<()>>>(
    make: impl FnOnce() -> F,
) -> std::pin::Pin<Box<F>> {
    Box::pin(make())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 3)]
async fn snapshot_contains_every_cell_at_cut() -> Result<()> {
    // Hold an actual source while two other cell mutations commit, then verify
    // every original image and manifest page at one cut in both representations.
    checkpoint_fixture::coherent().await?;
    checkpoint_fixture::refusals().await?;
    checkpoint_fixture::corruption().await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 3)]
async fn partial_snapshot_never_serves() -> Result<()> {
    runtime_fixture::refuse_checkpoint().await?;
    for rooted in [false, true] {
        runtime_fixture::checkpoint_owner(rooted).await?;
        Box::pin(runtime_fixture::seed_receiver_faults(rooted)).await?;
        Box::pin(runtime_fixture::seed_transfer_revocation(rooted)).await?;
        Box::pin(runtime_fixture::seed_persistence(rooted)).await?;
        // These parents launch this same executable's ignored child and verify
        // actual retained disks after owned SIGKILL at all six install cuts.
        Box::pin(runtime_fixture::seed_crash(rooted)).await?;
        fixture(|| runtime_fixture::seed_retry(rooted)).await?;
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 3)]
async fn whole_group_learner_and_joint_change() -> Result<()> {
    for rooted in [false, true] {
        // Erase one voter, install every cell into a fresh learner, require
        // catch-up before promotion, reconnect and refuse isolated commits.
        fixture(|| runtime_fixture::seed_source(rooted)).await?;
        fixture(|| runtime_fixture::seed_install(rooted)).await?;
        fixture(|| runtime_fixture::membership_lifecycle(rooted)).await?;
        fixture(|| runtime_fixture::membership_crash(rooted)).await?;
    }
    fixture(runtime_fixture::membership_sync).await?;
    fixture(runtime_fixture::seed_minority).await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 3)]
async fn snapshot_empty_cell_and_extent_restart() -> Result<()> {
    for rooted in [false, true] {
        // Raft generates the snapshot over TLS after prefix compaction. The
        // selected/retained closures include empty enrollment and later writes,
        // and cold reopen verifies the exact checkpoint plus surviving suffix.
        runtime_fixture::tcp_publication(rooted).await?;
        runtime_fixture::install_checkpoint(rooted).await?;
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 3)]
async fn snapshot_memory_and_pin_bounds() -> Result<()> {
    for rooted in [false, true] {
        runtime_fixture::transfer_pressure(rooted).await?;
        fixture(|| runtime_fixture::shared_inventory(rooted)).await?;
    }
    checkpoint_fixture::ownership().await?;
    runtime_fixture::local_shutdown().await?;
    runtime_fixture::checkpoint_shutdown().await?;
    fixture(|| runtime_fixture::checkpoint_during_apply(false)).await?;
    fixture(|| runtime_fixture::checkpoint_during_apply(true)).await?;
    fixture(|| runtime_fixture::controller_checkpoint_shutdown(false)).await?;
    fixture(|| runtime_fixture::controller_checkpoint_shutdown(true)).await?;
    runtime_fixture::automatic_checkpoints(false, true).await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 3)]
#[ignore = "owned child process controlled by partial_snapshot_never_serves"]
async fn seed_process_fixture() -> Result<()> {
    Box::pin(runtime_fixture::seed_process_fixture()).await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 3)]
#[ignore = "owned child process controlled by whole_group_learner_and_joint_change"]
async fn membership_process_fixture() -> Result<()> {
    Box::pin(runtime_fixture::membership_process_fixture()).await
}
