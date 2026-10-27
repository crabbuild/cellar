// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Production PydanticAI entrypoint, staged package and cold file-tool recovery.
#![cfg(all(feature = "embed", unix))]
// This client/process harness runs outside a cell execution domain.
#![allow(clippy::disallowed_methods)]
use anyhow::{ensure, Context as _, Result};
use cellar::{
    bucket::Bucket,
    native_deploy::{current_target, Package},
};
use serde_json::{json, Value};
use std::{
    os::unix::process::ExitStatusExt,
    path::{Path, PathBuf},
    process::Child,
    time::{Duration, Instant},
};

#[path = "../../../tools/python/installed_host.rs"]
#[allow(dead_code)]
mod installed_host;

struct Process(Child);

impl Drop for Process {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

fn database(directory: &Path) -> Result<Option<PathBuf>> {
    if !directory.exists() {
        return Ok(None);
    }
    for entry in std::fs::read_dir(directory)? {
        let entry = entry?;
        if entry.file_type()?.is_dir() {
            if let Some(file) = database(&entry.path())? {
                return Ok(Some(file));
            }
        } else if entry.file_name() == "db.sqlite" {
            return Ok(Some(entry.path()));
        }
    }
    Ok(None)
}

fn stored_state(directory: &Path) -> Result<Option<Value>> {
    let Some(path) = database(directory)? else {
        return Ok(None);
    };
    let db =
        rusqlite::Connection::open_with_flags(path, rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY)?;
    if !db.query_row(
        "SELECT EXISTS(SELECT 1 FROM sqlite_master WHERE name='__sys_cell_kv')",
        [],
        |row| row.get::<_, bool>(0),
    )? {
        return Ok(None);
    }
    let value = db.query_row(
        "SELECT v FROM __sys_cell_kv WHERE k='agent' LIMIT 1",
        [],
        |row| row.get::<_, String>(0),
    );
    match value {
        Ok(value) => Ok(Some(serde_json::from_str(&value)?)),
        Err(rusqlite::Error::QueryReturnedNoRows) => Ok(None),
        Err(error) => Err(error.into()),
    }
}

fn file_snapshot(directory: &Path) -> Result<Value> {
    let db = rusqlite::Connection::open_with_flags(
        database(directory)?.context("cell database missing")?,
        rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY,
    )?;
    let row = db.query_row("SELECT CAST(i.id AS TEXT), CAST(i.version AS TEXT),
        (SELECT substr(data,1,min(i.size,4096)) FROM __sys_fs_chunks WHERE inode=i.id AND chunk_index=0)
        FROM __sys_fs_entries e JOIN __sys_fs_inodes i ON e.inode=i.id
        WHERE e.parent=1 AND i.kind=0 LIMIT 1", [], |row| {
            Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?, row.get::<_, Vec<u8>>(2)?))
        })?;
    Ok(json!({"inode":row.0,"version":row.1,"text":String::from_utf8(row.2)?}))
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires CELLAR_PYTHON_AGENT_PACKAGE and its pinned Python dependencies"]
async fn agent_filesystem_package_recovers_effects_and_saved_reads() -> Result<()> {
    let domain = cellar::sdk::ExecutionDomain::new(tokio::runtime::Handle::current());
    let result = domain.run(package_scenario()).await;
    domain.shutdown().await;
    result?
}

async fn runtime_profile(client: &reqwest::Client, origin: &str) -> Result<Value> {
    let profile: Value = client
        .get(format!("{origin}/health"))
        .send()
        .await?
        .error_for_status()?
        .json()
        .await?;
    ensure!(
        profile["framework"] == "pydantic-ai"
            && profile["model"] == "demo"
            && profile["free_threaded"].is_boolean()
            && profile["gil_enabled"].is_boolean(),
        "installed agent runtime profile is incomplete: {profile}"
    );
    ensure!(
        profile["free_threaded"] != true || profile["gil_enabled"] == false,
        "free-threaded agent enabled the GIL: {profile}"
    );
    Ok(profile)
}

async fn package_scenario() -> Result<()> {
    let staged = PathBuf::from(
        std::env::var_os("CELLAR_PYTHON_AGENT_PACKAGE")
            .context("CELLAR_PYTHON_AGENT_PACKAGE required")?,
    )
    .canonicalize()?;
    let package: Package = serde_json::from_slice(&std::fs::read(staged.join("package.json"))?)?;
    package.validate()?;
    let site = std::env::var_os("CELLAR_PYTHON_SITE_PACKAGES");
    let client = reqwest::Client::builder()
        .no_proxy()
        .timeout(Duration::from_secs(10))
        .build()?;
    for (label, receipts) in [("file-commit", 1), ("saved-read", 5)] {
        let directory = tempfile::tempdir()?;
        let root = directory.path();
        let store = root.join("bucket.sqlite");
        let bucket = Bucket::open_dev(&store)?;
        package.publish_staged(&bucket, &staged).await?;
        let installed = root.join("python-agent");
        let python_root = package
            .primary
            .native
            .as_ref()
            .context("native package required")?
            .python
            .first()
            .map(|runtime| runtime.prefix.as_path());
        package
            .install_with_runtimes(&bucket, current_target(), &installed, python_root, None)
            .await?;
        drop(bucket);
        let first_name = format!("{label}-first");
        let first_data = root.join(format!("{first_name}-node"));
        std::fs::create_dir(&first_data)?;
        let configure = |command: &mut std::process::Command| {
            command
                .env("CELLAR_AGENT_MODEL", "demo")
                .env("CELLAR_WAKER_TICK_MS", "250")
                .env("CELLAR_HANDLER_BUDGET_S", "60");
            if let Some(site) = &site {
                command.env("CELLAR_PYTHON_SITE_PACKAGES", site);
            }
        };
        let mut first = installed_host::InstalledHost::start_configured(
            &installed,
            &store,
            root,
            &first_name,
            configure,
        )
        .await?;
        let initial_profile = runtime_profile(&client, &first.origin).await?;
        let request =
            json!({"prompt":"files: exercise the bounded file tools","step_delay_ms":1000});
        let response = client
            .post(format!("{}/agents/task", first.origin))
            .json(&request)
            .send()
            .await?;
        ensure!(
            response.status().as_u16() == 202,
            "agent acceptance failed: {}",
            response.text().await?
        );
        let deadline = Instant::now() + Duration::from_secs(60);
        loop {
            first.ensure_live()?;
            let state: Value = client
                .get(format!("{}/agents/task", first.origin))
                .send()
                .await?
                .error_for_status()?
                .json()
                .await?;
            ensure!(state["phase"] != "failed", "agent failed: {state}");
            let count = state["filesystem_receipts"]
                .as_object()
                .context("filesystem receipt map missing")?
                .len();
            if count == receipts {
                break;
            }
            ensure!(
                count < receipts && Instant::now() < deadline,
                "missed proven file checkpoint: {state}"
            );
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
        ensure!(
            first.crash()?.signal() == Some(9),
            "qualification did not SIGKILL the owned host"
        );
        let captured = file_snapshot(&first_data)?;
        std::fs::remove_dir_all(&first_data)?;
        ensure!(!first_data.exists(), "old local state was not erased");
        let second_name = format!("{label}-replacement");
        let second_data = root.join(format!("{second_name}-node"));
        std::fs::create_dir(&second_data)?;
        ensure!(
            std::fs::read_dir(&second_data)?.next().is_none(),
            "replacement local state is not empty"
        );
        let mut second = installed_host::InstalledHost::start_configured(
            &installed,
            &store,
            root,
            &second_name,
            configure,
        )
        .await?;
        let deadline = Instant::now() + Duration::from_secs(60);
        // Observe only the replacement's private database. No task HTTP request
        // may activate its cell before the durable alarm finishes the run.
        loop {
            second.ensure_live()?;
            if let Some(state) = stored_state(&second_data)? {
                ensure!(
                    state["phase"] != "failed",
                    "recovered agent failed: {state}"
                );
                if state["phase"] == "complete" {
                    break;
                }
            }
            ensure!(
                Instant::now() < deadline,
                "alarm did not complete the recovered agent"
            );
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        let final_state: Value = client
            .get(format!("{}/agents/task", second.origin))
            .send()
            .await?
            .error_for_status()?
            .json()
            .await?;
        ensure!(
            final_state["phase"] == "complete"
                && final_state["steps"] == 14
                && final_state["attempts"] == 5,
            "unexpected final agent state: {final_state}"
        );
        let saved = &final_state["filesystem_receipts"];
        ensure!(
            saved.as_object().context("receipt map missing")?.len() == 9,
            "receipt count changed"
        );
        let db = rusqlite::Connection::open_with_flags(
            database(&second_data)?.context("recovered database missing")?,
            rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY,
        )?;
        let live_files: u64 = db.query_row("SELECT count(*) FROM __sys_fs_entries e JOIN __sys_fs_inodes i ON e.inode=i.id WHERE i.kind=0", [], |row| row.get(0))?;
        ensure!(
            live_files == 0,
            "delete receipt survived without deleting the file"
        );
        drop(db);
        ensure!(
            saved["file-read"]["result"]["text"] == "first contents",
            "original read was lost"
        );
        ensure!(
            saved["file-read-renamed"]["result"]["text"] == "other contents",
            "edited read differs"
        );
        ensure!(
            saved["file-stale-edit"]["result"]["error"]
                == json!({"code":"Conflict","errno":"EBUSY","outcome":"not_committed"}),
            "stale edit result differs"
        );
        ensure!(
            saved["file-read"]["result"]["inode"] == captured["inode"],
            "file identity changed"
        );
        if receipts == 1 {
            ensure!(
                saved["file-read"]["result"]["version"] == captured["version"]
                    && captured["text"] == "first contents",
                "recovery repeated the committed write"
            );
        } else {
            let before = saved["file-read"]["result"]["version"]
                .as_str()
                .context("read version missing")?
                .parse::<u64>()?;
            let edited = captured["version"]
                .as_str()
                .context("file version missing")?
                .parse::<u64>()?;
            ensure!(
                edited > before && captured["text"] == "other contents",
                "the crash did not follow an edit"
            );
        }
        let duplicate = client
            .post(format!("{}/agents/task", second.origin))
            .json(&request)
            .send()
            .await?;
        ensure!(
            duplicate.status().as_u16() == 202,
            "duplicate acceptance changed"
        );
        let duplicate_state: Value = client
            .get(format!("{}/agents/task", second.origin))
            .send()
            .await?
            .error_for_status()?
            .json()
            .await?;
        ensure!(
            duplicate_state == final_state,
            "duplicate changed the completed run"
        );
        ensure!(
            client
                .post(format!("{}/agents/task", second.origin))
                .json(&json!({"prompt":"different"}))
                .send()
                .await?
                .status()
                .as_u16()
                == 409,
            "changed task input was accepted"
        );
        ensure!(
            client
                .get(format!("{}/agents/other", second.origin))
                .send()
                .await?
                .status()
                .as_u16()
                == 404,
            "another cell could see this agent"
        );
        let final_profile = runtime_profile(&client, &second.origin).await?;
        ensure!(
            final_profile == initial_profile,
            "agent runtime mode changed through model/tools/recovery"
        );
        eprintln!(
            "PYTHON_AGENT_RUNTIME: {}",
            json!({"case":label,"before":initial_profile,"after":final_profile})
        );
        second.stop(&client, true).await?;
        eprintln!("PASS {label}: cold package recovery, nine receipts, five model decisions, saved reads and complete shutdown");
    }
    Ok(())
}
