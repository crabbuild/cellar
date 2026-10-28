// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Each phase owns a new native process, Node process and empty node directory.

use anyhow::{ensure, Context as _, Result};
use std::{
    fs::File,
    io::Read as _,
    path::Path,
    process::{Command, Stdio},
    time::{Duration, Instant},
};

pub const ROOT: &str = "CELLAR_SANDBOX_BACKUP_ROOT";
pub const PHASE: &str = "CELLAR_SANDBOX_BACKUP_PHASE";

pub fn run(test: &str) -> Result<()> {
    let directory = tempfile::tempdir()?;
    for phase in ["create", "recover"] {
        let log = directory.path().join(format!("{phase}.log"));
        let output = File::create(&log)?;
        let mut child = Command::new(std::env::current_exe()?)
            .args([
                "--exact",
                test,
                "--ignored",
                "--nocapture",
                "--test-threads=1",
            ])
            .env(ROOT, directory.path())
            .env(PHASE, phase)
            .stdin(Stdio::null())
            .stdout(output.try_clone()?)
            .stderr(output)
            .spawn()?;
        let started = Instant::now();
        let status = loop {
            if let Some(status) = child.try_wait()? {
                break status;
            }
            if started.elapsed() > Duration::from_secs(300) {
                // Keep the original handle through termination and wait. An
                // unknown provider outcome is still a failed campaign.
                let killed = child.kill();
                let joined = child.wait();
                anyhow::bail!(
                    "{phase} exceeded the campaign bound; kill={killed:?}, join={joined:?}; {}",
                    read_log(&log)?
                );
            }
            std::thread::sleep(Duration::from_millis(10));
        };
        let output = read_log(&log)?;
        eprintln!("Sandbox process phase {phase}:\n{output}");
        ensure!(status.success(), "{phase} native process failed: {status}");
    }
    let database = directory.path().join("objects.db");
    let connection = rusqlite::Connection::open(database)?;
    for table in ["__sys_local_uploads", "__sys_local_parts"] {
        let count: u64 =
            connection.query_row(&format!("SELECT count(*) FROM {table}"), [], |row| {
                row.get(0)
            })?;
        ensure!(count == 0, "directory backup left {count} rows in {table}");
    }
    Ok(())
}

fn read_log(path: &Path) -> Result<String> {
    let mut bytes = Vec::new();
    File::open(path)?
        .take(128 * 1024 + 1)
        .read_to_end(&mut bytes)?;
    ensure!(
        bytes.len() <= 128 * 1024,
        "campaign process log exceeded its bound"
    );
    String::from_utf8(bytes).context("campaign process returned non-UTF8 logs")
}
