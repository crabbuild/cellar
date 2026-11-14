// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! CG-14 exercises the existing physical RF3/SQLite owner and campaign refusals.
#![allow(clippy::disallowed_methods)]

#[path = "support/raft_storage_fault.rs"]
pub mod storage_fault;

#[path = "support/cell_group_host_context.rs"]
mod fixture;

use anyhow::{ensure, Result};
use std::{path::Path, process::Command};

#[tokio::test(flavor = "multi_thread", worker_threads = 3)]
async fn harness_counts_real_cells_and_groups() -> Result<()> {
    // Eight independent SQLite histories, one physical group on each RF3 voter,
    // distinct results and receipts, physical engine counts and both image modes.
    Box::pin(fixture::namespace_host_many_cells_share_one_physical_group()).await
}

fn campaign_case(class: &str) -> Result<()> {
    let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("../..");
    let output = Command::new("python3")
        .args(["-B", "-m", "unittest", &format!("test_cell_groups.{class}")])
        .env("PYTHONDONTWRITEBYTECODE", "1")
        .current_dir(root.join("tools/perf"))
        .output()?;
    ensure!(
        output.status.success(),
        "campaign contract failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    require_selected_python_tests(&String::from_utf8_lossy(&output.stderr))?;
    Ok(())
}

fn require_selected_python_tests(output: &str) -> Result<()> {
    let count = output.lines().find_map(|line| {
        line.strip_prefix("Ran ")?
            .split_once(" test")?
            .0
            .parse::<usize>()
            .ok()
    });
    ensure!(
        count.is_some_and(|count| count > 0) && output.trim_end().ends_with("\nOK"),
        "Python oracle must select nonzero tests without skips"
    );
    Ok(())
}

#[test]
fn harness_python_zero_tests_refuse() -> Result<()> {
    require_selected_python_tests("Ran 1 test in 0.1s\n\nOK\n")?;
    for output in [
        "Ran 0 tests in 0.0s\n\nOK\n",
        "Ran 1 test in 0.1s\n\nOK (skipped=1)\n",
        "OK\n",
    ] {
        ensure!(require_selected_python_tests(output).is_err());
    }
    Ok(())
}

#[test]
fn harness_owned_resources_only() -> Result<()> {
    campaign_case("Ownership")
}

#[test]
fn harness_ledger_preserves_all_outcomes() -> Result<()> {
    campaign_case("Ledger")
}

#[test]
fn harness_rejects_missing_or_drifted_evidence() -> Result<()> {
    campaign_case("Evidence")
}

#[test]
fn harness_profile_gates_measured() -> Result<()> {
    campaign_case("Profiles")
}
