// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! A pending Python task is not a clean drain; process exit bounds its lifetime.
use super::{case, recovery, Case, Trigger, DELTA};
use crate::{application::Composite, crash::process::wait_groups_gone};
use anyhow::{ensure, Context as _, Result};
use cellar_python::{Incomplete, PythonApplication, Report};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::os::unix::process::CommandExt;
use std::{
    path::PathBuf,
    sync::{mpsc::Receiver, Arc},
    time::Duration,
};

#[derive(Deserialize, Serialize)]
struct Config {
    format: u32,
    root: PathBuf,
    spec: Case,
}

pub async fn child(
    base: Arc<Composite>,
    python: Arc<PythonApplication>,
    path: PathBuf,
) -> Result<Value> {
    let config: Config = serde_json::from_slice(&std::fs::read(path)?)?;
    config.spec.validate()?;
    ensure!(
        config.format == 1
            && config.root.is_absolute()
            && config.root.is_dir()
            && config.spec.stubborn,
        "invalid Python supervision child configuration"
    );
    case::run(base, python, &config.root, config.spec).await
}

pub fn finish(
    result: Result<Report>,
    receiver: Receiver<Value>,
    path: Option<PathBuf>,
) -> Result<()> {
    let error = result
        .err()
        .context("persistent typed Python task fabricated a clean shutdown")?;
    let incomplete = error
        .downcast_ref::<Incomplete>()
        .context("Python child failed without typed incomplete-shutdown evidence")?;
    let report = &incomplete.report;
    validate_incomplete(report)?;
    let evidence = receiver
        .try_recv()
        .context("Python child did not complete its native authority/recovery audit")?;
    let path = path.context("Python child requires an evidence report path")?;
    if let Some(parent) = path
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
    {
        std::fs::create_dir_all(parent)?;
    }
    std::fs::write(
        path,
        serde_json::to_vec_pretty(&json!({"format":1,"suite":"python-supervision-child",
        "expected_incomplete":true,"case":evidence,"python_report":report}))?,
    )?;
    // The supervisor requires this nonzero exit AND the typed report above.
    anyhow::bail!(
        "controlled PythonShutdownIncomplete: persistent typed handler requires process teardown"
    )
}

fn validate_incomplete(report: &Report) -> Result<()> {
    ensure!(
        !report.complete()
            && report.pending_tasks == 1
            && report.work.active_invocations == 1
            && report.work.active_tasks == 1
            && report.pending_objects == 0
            && report.objects_created == report.objects_retired
            && report.work.resident_objects == 0
            && report.work.pending_retirements == 0
            && !report.host_failed
            && !report.runtime_failed
            && report.pending_threads == 0
            && report.unraisable_errors == 0
            && report.thread_errors == 0,
        "Python child retained work beyond its single known suppression task: {report:?}"
    );
    ensure!(
        report.bridge.native_invocations == 0
            && report.bridge.queued_bytes == 0
            && report.bridge.pending_reservations == 0
            && report.bridge.operations.settled(),
        "Python child retained native resources: {:?}",
        report.bridge
    );
    Ok(())
}

async fn supervised(
    base: Arc<Composite>,
    python: Arc<PythonApplication>,
    spec: Case,
) -> Result<Value> {
    let directory = tempfile::tempdir()?;
    let root = directory.path().canonicalize()?;
    let config = root.join("case.json");
    let report_path = root.join("child.json");
    std::fs::write(
        &config,
        serde_json::to_vec(&Config {
            format: 1,
            root: root.clone(),
            spec: spec.clone(),
        })?,
    )?;
    let log = std::fs::File::create(root.join("child.log"))?;
    let mut command = tokio::process::Command::new(std::env::current_exe()?);
    command
        .arg("--python-lifecycle-child")
        .arg(config)
        .arg("--report")
        .arg(&report_path)
        .stdout(log.try_clone()?)
        .stderr(log)
        .kill_on_drop(true);
    command.as_std_mut().process_group(0);
    let mut child = command
        .spawn()
        .context("spawn typed Python suppression child")?;
    let group = libc::pid_t::try_from(child.id().context("Python child has no PID")?)?;
    ensure!(group > 1, "unsafe Python child process group");
    let waited = tokio::time::timeout(Duration::from_secs(45), child.wait()).await;
    let started = std::fs::read(root.join("started.json"))
        .ok()
        .and_then(|bytes| serde_json::from_slice::<Value>(&bytes).ok());
    // Missing/invalid metadata can never authorize signalling another group.
    // The success path below requires this provenance instead of defaulting it.
    let expected_spec = serde_json::to_value(&spec)?;
    let worker = started
        .as_ref()
        .filter(|value| {
            value["format"] == 1 && value["pid"] == group && value["spec"] == expected_spec
        })
        .and_then(|value| value["worker_pid"].as_i64())
        .and_then(|pid| libc::pid_t::try_from(pid).ok())
        .filter(|pid| *pid > 1 && *pid != group && *pid != std::process::id() as libc::pid_t);
    if waited.is_err() {
        // The owned handle avoids signalling a possibly reused numeric PID.
        // Closing the leader's IPC makes the fixed Node worker exit. Its group
        // must independently disappear; a metadata PID never authorizes a kill.
        child
            .start_kill()
            .context("signal owned timed-out Python child")?;
        child.wait().await.context("join timed-out Python child")?;
        let mut groups = vec![("python", group)];
        if let Some(pid) = worker {
            groups.push(("node", pid));
        }
        wait_groups_gone(&groups).await?;
        anyhow::bail!(
            "Python suppression child exceeded supervisor deadline: {}",
            std::fs::read_to_string(root.join("child.log"))?
        );
    }
    let status = waited.context("Python child wait deadline")??;
    ensure!(
        status.code() == Some(1),
        "Python child did not exit with the required incomplete-shutdown failure: {status}: {}",
        std::fs::read_to_string(root.join("child.log"))?
    );
    let started = started.context("Python child never published its owned process identities")?;
    ensure!(
        started["format"] == 1
            && started["pid"] == group
            && started["spec"] == serde_json::to_value(&spec)?,
        "Python child process provenance differs"
    );
    let worker = worker.context("Python child has no verified owned worker group")?;
    wait_groups_gone(&[("python", group), ("node", worker)]).await?;
    let bytes = std::fs::read(&report_path).with_context(|| {
        let detail = match std::fs::read_to_string(root.join("child.log")) {
            Ok(log) => log,
            Err(error) => format!("could not read child diagnostics: {error}"),
        };
        format!("Python child produced no incomplete-shutdown report: {detail}")
    })?;
    let child_report: Value = serde_json::from_slice(&bytes)?;
    ensure!(
        child_report["format"] == 1
            && child_report["expected_incomplete"] == true
            && child_report["case"]["spec"] == serde_json::to_value(&spec)?,
        "Python child report provenance differs"
    );
    let parsed: Report = serde_json::from_value(child_report["python_report"].clone())?;
    validate_incomplete(&parsed)?;
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(20))
        .build()?;
    let cold = recovery::run(base, python, &root, &client, spec.operation(), DELTA).await?;
    Ok(
        json!({"spec":spec,"child_report":child_report,"exit_code":1,
        "owner_pid":group,"worker_pid":worker,"owner_and_worker_groups_gone":true,"fresh_process_recovery":cold}),
    )
}

pub async fn run(base: Arc<Composite>, python: Arc<PythonApplication>) -> Result<Value> {
    let mut cases = Vec::new();
    for caller in ["rust", "python", "v8", "node"] {
        for peer in [false, true] {
            for after in [false, true] {
                let spec = Case {
                    caller: caller.into(),
                    peer,
                    after,
                    stubborn: true,
                    trigger: Trigger::Shutdown,
                };
                base.audit.reset_after_node_drain();
                eprintln!("Python supervised suppression: {spec:?}");
                cases.push(supervised(base.clone(), python.clone(), spec).await?);
            }
        }
    }
    Ok(
        json!({"format":1,"suite":"python-supervision","case_count":cases.len(),"cases":cases,"remote_fleet_qualified":false}),
    )
}

pub async fn smoke(base: Arc<Composite>, python: Arc<PythonApplication>) -> Result<Value> {
    let spec = Case {
        caller: "rust".into(),
        peer: false,
        after: true,
        stubborn: true,
        trigger: Trigger::Shutdown,
    };
    let case = supervised(base, python, spec).await?;
    Ok(
        json!({"format":1,"suite":"python-supervision-smoke","case_count":1,"cases":[case],"remote_fleet_qualified":false}),
    )
}

#[cfg(test)]
#[path = "process_tests.rs"]
mod tests;
