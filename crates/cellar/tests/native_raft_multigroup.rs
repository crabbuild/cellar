// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Legacy multi-group SQL, repair and archive qualification during the cutover.
#![allow(clippy::disallowed_methods)]
#[path = "support/raft_archive_reclaim.rs"]
mod archive_reclaim;
#[path = "support/raft_background_admission.rs"]
mod background_admission;
#[allow(dead_code)]
#[path = "support/raft_catalog_fault.rs"]
mod bundle_faults;
#[allow(dead_code)]
#[path = "support/raft_ltx_fixture.rs"]
mod bundle_images;
#[path = "support/raft_bundle_native.rs"]
mod bundles;
#[allow(dead_code)] // Shared native fixture also serves membership and snapshot drills.
#[path = "support/raft_cluster.rs"]
mod native_cluster;
#[allow(dead_code)]
#[path = "support/raft_node.rs"]
mod public_node;
#[path = "support/raft_repair_load.rs"]
mod repair_load;

use anyhow::{ensure, Result};
use std::path::Path;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn background_sql_admits_one_pending_command() -> Result<()> {
    background_admission::run(false).await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn cancelled_command_waiting_for_background_sql_does_not_execute() -> Result<()> {
    background_admission::run(true).await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn repair_budget_under_load() -> Result<()> {
    repair_load::run().await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn archive_bundle_group_index() -> Result<()> {
    bundles::run().await
}

#[test]
fn harness_rejects_stale_binary() -> Result<()> {
    let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("../..");
    let output = std::process::Command::new("python3")
        .args(["-m", "unittest", "test_raft_run.Freshness"])
        .current_dir(root.join("tools/perf"))
        .output()?;
    ensure!(
        output.status.success(),
        "image refusal cases failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn short_archived_groups_checkpoint_and_recover_without_reexecuting() -> Result<()> {
    archive_reclaim::run().await
}
