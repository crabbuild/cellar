// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Legacy archive qualification during the cutover.
#![allow(clippy::disallowed_methods)]
#[allow(dead_code)]
#[path = "support/raft_catalog_fault.rs"]
mod bundle_faults;
#[allow(dead_code)]
#[path = "support/raft_ltx_fixture.rs"]
mod bundle_images;
#[path = "support/raft_bundle_native.rs"]
mod bundles;
#[allow(dead_code)]
#[path = "support/raft_node.rs"]
mod public_node;

use anyhow::{ensure, Result};
use std::path::Path;

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
