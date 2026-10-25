// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Run the shipped process watchdog against an actually installed Python host.
#![allow(clippy::disallowed_methods)]
use super::{installed_host::InstalledHost, rolling, Process};
use anyhow::{ensure, Context as _, Result};
use cellar::{
    bucket::Bucket,
    native_deploy::{current_target, Package},
};
use serde_json::Value;
use std::{
    path::{Path, PathBuf},
    process::{Command, Stdio},
    time::{Duration, Instant},
};

struct Supervised {
    process: Process,
    records: PathBuf,
    log: PathBuf,
}

impl Supervised {
    fn events(&self) -> Result<Vec<Value>> {
        std::fs::read_to_string(&self.records)?
            .split_inclusive('\n')
            .filter(|line| line.ends_with('\n'))
            .map(|line| Ok(serde_json::from_str(line)?))
            .collect()
    }

    async fn ready(&mut self, attempt: u32) -> Result<u32> {
        let deadline = Instant::now() + Duration::from_secs(45);
        loop {
            if let Some(pid) = self
                .events()?
                .iter()
                .find(|row| row["event"] == "supervisor_ready" && row["attempt"] == attempt)
                .and_then(|row| row["pid"].as_u64())
                .and_then(|pid| u32::try_from(pid).ok())
            {
                return Ok(pid);
            }
            ensure!(
                self.process.0.try_wait()?.is_none(),
                "Python supervisor exited: {}",
                std::fs::read_to_string(&self.log)?
            );
            ensure!(
                Instant::now() < deadline,
                "Python supervisor readiness expired: {}",
                std::fs::read_to_string(&self.log)?
            );
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    }
}

impl Drop for Supervised {
    fn drop(&mut self) {
        if self.process.0.try_wait().ok().flatten().is_none() {
            let _ = signal(self.process.0.id(), "-TERM");
            let deadline = Instant::now() + Duration::from_secs(15);
            while self.process.0.try_wait().ok().flatten().is_none() && Instant::now() < deadline {
                std::thread::sleep(Duration::from_millis(25));
            }
        }
        // On failure, Process also kills/reaps the supervisor. Its emitted
        // child IDs restrict cleanup to groups still owned by this private
        // drill. Never signal historical, already-joined PIDs after reuse.
        if let Ok(events) = self.events() {
            let mut active = std::collections::BTreeSet::new();
            for row in events {
                if let Some(pid) = row["pid"].as_u64().and_then(|pid| u32::try_from(pid).ok()) {
                    if row["event"] == "supervisor_started" {
                        active.insert(pid);
                    }
                    if row["event"] == "supervisor_stopped" {
                        active.remove(&pid);
                    }
                }
            }
            for pid in active {
                #[cfg(unix)]
                if let Ok(pid) = libc::pid_t::try_from(pid) {
                    // SAFETY: positive IDs came from our owned supervisor's
                    // private records; negation selects its child group.
                    if pid > 0 {
                        unsafe {
                            libc::kill(-pid, libc::SIGKILL);
                        }
                    }
                }
            }
        }
    }
}

fn signal(pid: u32, signal: &str) -> Result<()> {
    ensure!(
        signal == "-TERM",
        "unsupported watchdog qualification signal"
    );
    ensure!(
        Command::new("/bin/kill")
            .args([signal, &pid.to_string()])
            .status()?
            .success(),
        "failed to signal owned watchdog process"
    );
    Ok(())
}

pub(super) async fn qualify(package: &Path, scratch: &Path, cellar: &Path) -> Result<()> {
    let data: Package = serde_json::from_slice(&std::fs::read(package.join("package.json"))?)?;
    data.validate()?;
    let runtime = data
        .primary
        .native
        .as_ref()
        .context("Python descriptor missing")?
        .python
        .iter()
        .find(|entry| entry.target == current_target())
        .context("current Python runtime missing")?;
    ensure!(
        runtime.prefix.starts_with(scratch) && runtime.prefix != scratch,
        "watchdog runtime outside private scratch"
    );
    let work = tempfile::tempdir_in(scratch)?;
    ensure!(
        !work.path().starts_with(&runtime.prefix),
        "watchdog scratch inside runtime"
    );
    let store = work.path().join("fleet.sqlite");
    let bucket = Bucket::open_dev(&store)?;
    data.publish_staged(&bucket, package).await?;
    let host = work.path().join("supervised-host");
    data.install_with_python(&bucket, current_target(), &host, Some(&runtime.prefix))
        .await?;
    // A real handoff peer is necessary for a complete TERM shutdown. Do not
    // relabel a lone owner's refused handoff as a successful production drain.
    let peer = InstalledHost::start(&host, &store, work.path(), "watchdog-peer").await?;
    let listener = std::net::TcpListener::bind("127.0.0.1:0")?;
    let address = listener.local_addr()?;
    drop(listener);
    let origin = format!("http://{address}");
    let records = work.path().join("supervisor.jsonl");
    let log = work.path().join("supervisor-child.log");
    let process = Process(
        Command::new(cellar)
            .env_clear()
            .args([
                "supervise",
                "--json",
                "--health-url",
                &format!("{origin}/health"),
                "--startup-ms",
                "30000",
                "--probe-ms",
                "250",
                "--interval-ms",
                "100",
                "--failures",
                "2",
                "--shutdown-ms",
                "8000",
                "--restart-ms",
                "100",
                "--max-restarts",
                "1",
                "--",
            ])
            .arg(&host)
            .args([
                "--no-control-plane",
                "--listen",
                &address.to_string(),
                "--internal-listen",
                "127.0.0.1:0",
            ])
            .env("CELLAR_NATIVE_FLEET", "1")
            .env("CELLAR_INTERNAL_DEV_STORE", &store)
            .env("CELLAR_BUCKET", "cellar-dev")
            .env("CELLAR_NODE", "supervised-python")
            .env("CELLAR_WATCH", work.path().join("supervised-node"))
            .env("CELLAR_TOKIO_THREADS", "2")
            .env("CELLAR_PLACEMENT_WEIGHT", "1")
            .env("CELLAR_REBALANCE_INTERVAL_MS", "0")
            .env("CELLAR_DURABILITY", "bucket")
            .env("CELLAR_SHUTDOWN_TOTAL_MS", "8000")
            .env("CELLAR_SHUTDOWN_REPORT_JSON", "1")
            .stdout(Stdio::from(std::fs::File::create(&records)?))
            .stderr(Stdio::from(std::fs::File::create(&log)?))
            .spawn()?,
    );
    let mut supervised = Supervised {
        process,
        records,
        log,
    };
    let before = supervised.ready(0).await?;
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(10))
        .build()?;
    let receipt = rolling::increment(&client, &origin, "supervised-receipt", 7).await?;
    ensure!(receipt.value()?["count"] == 7);
    // Block Python/GIL in a real C call in the private generated fixture,
    // leaving native lease renewal and the handoff peer operational.
    let mut trigger = Box::pin(
        client
            .get(format!("{origin}/__cellar_qualification_stall"))
            .send(),
    );
    if let futures_util::future::Either::Left((response, _)) = futures_util::future::select(
        trigger.as_mut(),
        Box::pin(tokio::time::sleep(Duration::from_millis(250))),
    )
    .await
    {
        anyhow::bail!(
            "watchdog fixture returned instead of blocking: {:?}",
            response.map(|response| response.status())
        );
    }
    let after = supervised.ready(1).await?;
    drop(trigger);
    ensure!(before != after, "watchdog reused a terminated process");
    ensure!(
        rolling::increment(&client, &origin, "supervised-receipt", 7).await? == receipt,
        "watchdog restart changed a durable receipt"
    );
    ensure!(
        rolling::increment(&client, &origin, "supervised-next", 2)
            .await?
            .value()?["count"]
            == 9,
        "watchdog lost an acknowledged write"
    );
    signal(supervised.process.0.id(), "-TERM")?;
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        if let Some(status) = supervised.process.0.try_wait()? {
            ensure!(
                status.success(),
                "production supervisor failed cooperative stop: {}",
                std::fs::read_to_string(&supervised.log)?
            );
            break;
        }
        ensure!(
            Instant::now() < deadline,
            "production supervisor did not join"
        );
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    let events = supervised.events()?;
    let stops = events
        .iter()
        .filter(|row| row["event"] == "supervisor_stopped")
        .collect::<Vec<_>>();
    ensure!(
        stops.len() == 2
            && stops[0]["reason"] == "health_failed"
            && stops[0]["forced"] == true
            && stops[0]["exit_signal"] == libc::SIGKILL
            && stops[1]["reason"] == "signal"
            && stops[1]["forced"] == false
            && stops[1]["exit_code"] == 0,
        "watchdog did not exercise the required forced stop and clean replacement: {events:?}"
    );
    let reports = std::fs::read_to_string(&supervised.log)?
        .lines()
        .filter_map(|line| serde_json::from_str::<Value>(line).ok())
        .filter(|row| row["event"] == "embedded_python_shutdown")
        .collect::<Vec<_>>();
    ensure!(
        reports.len() == 1,
        "killed process fabricated a shutdown report"
    );
    let report = &reports[0]["report"];
    ensure!(
        report["runtime_failed"] == false
            && report["host_failed"] == false
            && report["pending_tasks"] == 0
            && report["pending_objects"] == 0
            && report["pending_threads"] == 0
            && report["bridge"]["native_invocations"] == 0
            && report["bridge"]["pending_reservations"] == 0
            && report["native_outcomes"].as_array().is_some_and(
                |rows| !rows.is_empty() && rows.iter().all(|row| row["complete"] == true)
            ),
        "watchdog replacement did not drain: {report}"
    );
    peer.stop(&client, true).await?;
    cellar::cli_output::Output::new(cellar::cli_output::Format::Text).line(format_args!("PASS production Python supervisor: instance health, forced kill, joined restart, exact receipt, new write and complete handoff"))?;
    Ok(())
}
