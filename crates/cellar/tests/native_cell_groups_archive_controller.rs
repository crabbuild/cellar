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
