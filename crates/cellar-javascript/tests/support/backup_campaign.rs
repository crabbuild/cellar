// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Fresh processes, plus a verified hard crash after actual part publication.

use anyhow::{ensure, Context as _, Result};
use std::{
    fs::File,
    io::Read as _,
    path::Path,
    process::{Command, Stdio},
    time::{Duration, Instant},
};
#[path = "backup_process.rs"]
mod process;

pub const ROOT: &str = "CELLAR_SANDBOX_BACKUP_ROOT";
pub const PHASE: &str = "CELLAR_SANDBOX_BACKUP_PHASE";
pub const LAUNCHER: &str = "CELLAR_SANDBOX_BACKUP_LAUNCHER";
const WORKER_PID: &str = "CELLAR_SANDBOX_BACKUP_WORKER_PID";

pub fn run(test: &str) -> Result<()> {
    let directory = tempfile::tempdir()?;
    let launcher = directory.path().join("launch-node");
    std::fs::write(&launcher, "#!/bin/sh\nset -eu\nprintf '%s\\n' \"$$\" > \"$CELLAR_SANDBOX_BACKUP_WORKER_PID\"\nexec node \"$@\"\n")?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt as _;
        std::fs::set_permissions(&launcher, std::fs::Permissions::from_mode(0o700))?;
    }
    let result = run_phases(test, directory.path(), &launcher);
    if result.is_err() {
        // Failure hygiene only. Never use forced cleanup as positive recovery
        // evidence or replace the original failed campaign with success.
        let cleaned = with_engine_deadline(async {
            let docker = docker()?;
            let host = owner(directory.path());
            for id in owned_resources(&docker, &host).await? {
                docker
                    .expect(
                        "DELETE",
                        &format!("/containers/{id}?force=true"),
                        None,
                        "remove failed campaign resource",
                    )
                    .await?;
            }
            crate::image_cleanup::cleanup(&docker, &host).await
        });
        if let Err(error) = cleaned {
            eprintln!("backup campaign emergency cleanup failed: {error:#}");
        }
    }
    result
}

fn run_phases(test: &str, directory: &Path, launcher: &Path) -> Result<()> {
    for phase in ["create", "crash", "recover"] {
        let log = directory.join(format!("{phase}.log"));
        let worker_pid = directory.join(format!("{phase}-worker.pid"));
        let output = File::create(&log)?;
        let mut child = Command::new(std::env::current_exe()?)
            .args([
                "--exact",
                test,
                "--ignored",
                "--nocapture",
                "--test-threads=1",
            ])
            .env(ROOT, directory)
            .env(PHASE, phase)
            .env(LAUNCHER, launcher)
            .env(WORKER_PID, &worker_pid)
            .stdin(Stdio::null())
            .stdout(output.try_clone()?)
            .stderr(output)
            .spawn()?;
        let started = Instant::now();
        let result = (|| -> Result<()> {
            loop {
                if let Some(status) = child.try_wait()? {
                    ensure!(
                        status.success() && phase != "crash",
                        "{phase} native process terminated before expected recovery: {status}"
                    );
                    break;
                }
                if phase == "crash" && directory.join("crash-ready").exists() {
                    let worker = std::fs::read_to_string(&worker_pid)?.trim().parse()?;
                    process::crash(&mut child, worker)?;
                    let resources = with_engine_deadline(async {
                        owned_resources(&docker()?, &owner(directory)).await
                    })?;
                    ensure!(resources.len() == 2, "hard crash did not leave the application and its network helper: {resources:?}");
                    std::fs::write(
                        directory.join("crash-resources.json"),
                        serde_json::to_vec(&resources)?,
                    )?;
                    break;
                }
                if started.elapsed() > Duration::from_secs(300) {
                    anyhow::bail!("{phase} exceeded the original campaign bound");
                }
                std::thread::sleep(Duration::from_millis(10));
            }
            Ok(())
        })();
        if result.is_err() && child.try_wait().is_ok_and(|status| status.is_none()) {
            if let Ok(worker) = std::fs::read_to_string(&worker_pid)
                .and_then(|value| value.trim().parse::<u32>().map_err(std::io::Error::other))
            {
                if let Err(error) = process::crash(&mut child, worker) {
                    eprintln!("failed phase process cleanup: {error:#}");
                }
            }
            if child.try_wait().is_ok_and(|status| status.is_none()) {
                let _ = child.kill();
                let _ = child.wait();
            }
        }
        let output = read_log(&log)?;
        eprintln!("Sandbox process phase {phase}:\n{output}");
        result?;
    }
    let database = directory.join("objects.db");
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

pub fn owner(directory: &Path) -> String {
    use sha2::{Digest as _, Sha256};
    let namespace = format!(
        "sandbox-backups-{}",
        directory.file_name().unwrap().to_string_lossy()
    );
    format!(
        "{:x}",
        Sha256::digest(format!("javascript-test\0namespace:{namespace}").as_bytes())
    )
}

pub fn upload_counts(connection: &rusqlite::Connection) -> Result<(u64, u64)> {
    Ok(connection.query_row("SELECT (SELECT count(*) FROM __sys_local_uploads), (SELECT count(*) FROM __sys_local_parts)", [], |row| Ok((row.get(0)?, row.get(1)?)))?)
}

fn docker() -> Result<cellar::docker::Docker> {
    Ok(cellar::docker::Docker::new(std::path::PathBuf::from(
        std::env::var_os("CELLAR_SANDBOX_ENGINE_SOCKET").context("isolated Engine socket")?,
    )))
}

fn with_engine_deadline<T>(future: impl std::future::Future<Output = Result<T>>) -> Result<T> {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?
        .block_on(async {
            tokio::time::timeout(Duration::from_secs(10), future)
                .await
                .context("campaign Engine observation/cleanup timed out")?
        })
}

async fn owned_resources(docker: &cellar::docker::Docker, host: &str) -> Result<Vec<String>> {
    let filters = serde_json::json!({"label":[format!("cellar.host={host}")]}).to_string();
    let query = url::form_urlencoded::Serializer::new(String::new())
        .append_pair("all", "true")
        .append_pair("filters", &filters)
        .finish();
    let response = docker
        .expect(
            "GET",
            &format!("/containers/json?{query}"),
            None,
            "inspect campaign resources",
        )
        .await?
        .json()?;
    let mut ids = Vec::new();
    for resource in response.as_array().context("campaign resource list")? {
        let id = resource["Id"].as_str().context("campaign resource ID")?;
        ensure!(
            id.len() == 64 && id.bytes().all(|byte| byte.is_ascii_hexdigit()),
            "invalid immutable resource ID"
        );
        let inspected = docker
            .expect(
                "GET",
                &format!("/containers/{id}/json"),
                None,
                "verify campaign ownership",
            )
            .await?
            .json()?;
        ensure!(
            inspected["Config"]["Labels"]["cellar.host"] == host
                && inspected["Config"]["Labels"]["cellar.node"] == "javascript-test",
            "foreign campaign resource"
        );
        ids.push((
            inspected["Config"]["Labels"]["cellar.network.role"] == "helper",
            id.to_string(),
        ));
    }
    ids.sort_by_key(|(helper, _)| *helper);
    Ok(ids.into_iter().map(|(_, id)| id).collect())
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
