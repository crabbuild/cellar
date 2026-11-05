// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Public SDK/process acceptance. Other CG-07 gates remain separate requirements.
#![allow(clippy::disallowed_methods)]

use anyhow::{ensure, Context as _, Result};
use sha2::{Digest, Sha256};
use std::{path::Path, process::Command, sync::Mutex};

fn digest(path: &Path) -> Result<Vec<u8>> {
    Ok(Sha256::digest(std::fs::read(path)?).to_vec())
}

// Both public cases use the same six development listeners.
static COUNTER_PORTS: Mutex<()> = Mutex::new(());

#[test]
fn sdk_many_cells_share_group() -> Result<()> {
    counter_process(false)
}

#[test]
fn occupancy_one_uses_shared_path() -> Result<()> {
    counter_process(true)
}

fn counter_process(single_cell: bool) -> Result<()> {
    let _ports = COUNTER_PORTS
        .lock()
        .map_err(|_| anyhow::anyhow!("counter port lock poisoned"))?;
    let expected_cells = if single_cell { 1 } else { 2 };
    let binary = Path::new(env!("CARGO_BIN_EXE_cellar-raft-counter"));
    let expected = digest(binary)?;
    let repository = Path::new(env!("CARGO_MANIFEST_DIR")).join("../..");
    let scratch = Path::new(env!("CARGO_TARGET_TMPDIR"));
    std::fs::create_dir_all(scratch)?;
    // Retain success and failure disks/logs for process and receipt inspection.
    let root = tempfile::Builder::new()
        .prefix("cell-group-public-sdk-")
        .tempdir_in(scratch)?
        .keep();

    let data = root.join("replicas");
    let mut command = Command::new("python3");
    command
        .arg(repository.join("examples/raft-counter/smoke.py"))
        .arg(binary)
        .arg(&data)
        .current_dir(&repository);
    if single_cell {
        command.arg("--single-cell");
    }
    let output = command
        .output()
        .context("start original public native/Axum process drill")?;
    std::fs::write(root.join("stdout.log"), &output.stdout)?;
    std::fs::write(root.join("stderr.log"), &output.stderr)?;
    ensure!(
        output.status.success(),
        "public SDK process drill failed; retained evidence at {}",
        root.display()
    );
    ensure!(digest(binary)? == expected, "public SDK executable changed");
    let inventory: serde_json::Value =
        serde_json::from_slice(&std::fs::read(data.join("inventory.json"))?)?;
    let replicas = inventory.as_array().context("replica inventory array")?;
    ensure!(replicas.len() == 3, "incomplete replica inventory");
    for (i, replica) in replicas.iter().enumerate() {
        ensure!(replica["groups"] == 1, "expected one observed shared group");
        ensure!(
            replica["cells"] == expected_cells,
            "unexpected number of logical cell histories"
        );
        ensure!(
            replica["observed_cells"] == expected_cells,
            "owner occupancy disagrees with resources"
        );
        ensure!(
            replica["group"] == replicas[0]["group"]
                && replica["group_incarnation"] == replicas[0]["group_incarnation"],
            "replicas observe different physical histories"
        );
        ensure!(replica["member"] == i as u64 + 1, "wrong physical member");
        let progress = ["checkpoint", "applied", "committed", "persisted"]
            .map(|key| {
                replica[key]
                    .as_u64()
                    .with_context(|| format!("missing physical {key}"))
            })
            .into_iter()
            .collect::<Result<Vec<_>>>()?;
        ensure!(
            progress.windows(2).all(|pair| pair[0] <= pair[1]),
            "incoherent physical progress"
        );
        ensure!(
            replica["retained_entries"].as_u64() == Some(progress[3] - progress[0]),
            "retained entries differ from the durable log"
        );
    }
    eprintln!("Public SDK/process evidence retained at {}", root.display());
    Ok(())
}
