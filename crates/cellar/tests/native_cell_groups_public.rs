// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Public SDK/process acceptance. Other CG-07 gates remain separate requirements.
#![allow(clippy::disallowed_methods)]

use anyhow::{ensure, Context as _, Result};
use sha2::{Digest, Sha256};
use std::{
    net::{Ipv4Addr, TcpListener},
    path::Path,
    process::Command,
    time::{Duration, Instant},
};

fn digest(path: &Path) -> Result<Vec<u8>> {
    Ok(Sha256::digest(std::fs::read(path)?).to_vec())
}

#[test]
fn sdk_many_cells_share_group() -> Result<()> {
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

    // The original process drill deliberately binds a non-reusing socket to
    // force late startup failure. A previous run's TCP TIME_WAIT must settle;
    // no test assertion, operation timeout or application admission is changed.
    let deadline = Instant::now() + Duration::from_secs(60);
    loop {
        let ports = [9901, 9902, 9903, 9911, 9912, 9913]
            .into_iter()
            .map(|port| TcpListener::bind((Ipv4Addr::LOCALHOST, port)))
            .collect::<std::io::Result<Vec<_>>>();
        if ports.is_ok() {
            break;
        }
        ensure!(
            Instant::now() < deadline,
            "public SDK fixture ports unavailable"
        );
        std::thread::sleep(Duration::from_millis(100));
    }

    let data = root.join("replicas");
    let output = Command::new("python3")
        .arg(repository.join("examples/raft-counter/smoke.py"))
        .arg(binary)
        .arg(&data)
        .current_dir(&repository)
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
    for replica in replicas {
        ensure!(replica["groups"] == 1, "expected one observed shared group");
        ensure!(
            replica["cells"] == 2,
            "expected two independent cell histories"
        );
    }
    eprintln!("Public SDK/process evidence retained at {}", root.display());
    Ok(())
}
