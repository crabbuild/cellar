// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Caller-owned installed-host processes and complete shutdown assertions.
//! Filesystem, timing and process control are outside node execution domains.
#![allow(clippy::disallowed_methods)]
use super::{Process, Result};
use anyhow::{ensure, Context as _};
use serde_json::Value;
use std::{
    path::{Path, PathBuf},
    process::{Command, Stdio},
    time::{Duration, Instant},
};

pub(super) struct InstalledHost {
    process: Process,
    log: PathBuf,
    pub origin: String,
    internal: String,
}

impl InstalledHost {
    pub async fn start(host: &Path, store: &Path, work: &Path, name: &str) -> Result<Self> {
        Self::start_configured(host, store, work, name, |_| {}).await
    }

    pub async fn start_configured(
        host: &Path,
        store: &Path,
        work: &Path,
        name: &str,
        configure: impl FnOnce(&mut Command),
    ) -> Result<Self> {
        let output = work.join(format!("{name}.log"));
        let stdout = std::fs::File::create(&output)?;
        let mut command = Command::new(host);
        command
            .env_clear()
            .args([
                "--no-control-plane",
                "--listen",
                "127.0.0.1:0",
                "--internal-listen",
                "127.0.0.1:0",
            ])
            .env("CELLAR_NATIVE_FLEET", "1")
            .env("CELLAR_INTERNAL_DEV_STORE", store)
            .env("CELLAR_BUCKET", "cellar-dev")
            .env("CELLAR_NODE", name)
            .env("CELLAR_TEST_DATA_DIR", work.join(format!("{name}-node")))
            .env("CELLAR_TOKIO_THREADS", "2")
            .env("CELLAR_PLACEMENT_WEIGHT", "1")
            .env("CELLAR_REBALANCE_INTERVAL_MS", "0")
            .env("CELLAR_DURABILITY", "bucket")
            .env("CELLAR_SHUTDOWN_TOTAL_MS", "8000")
            .env("CELLAR_SHUTDOWN_REPORT_JSON", "1")
            .stdout(Stdio::from(stdout.try_clone()?))
            .stderr(Stdio::from(stdout));
        configure(&mut command);
        let mut process = Process(command.spawn()?);
        let client = reqwest::Client::builder()
            .timeout(Duration::from_secs(5))
            .build()?;
        let deadline = Instant::now() + Duration::from_secs(30);
        let (origin, internal) = loop {
            ensure!(
                process.0.try_wait()?.is_none(),
                "installed host exited: {}",
                std::fs::read_to_string(&output)?
            );
            let text = std::fs::read_to_string(&output)?;
            if let Some(address) = text.lines().find_map(|line| {
                line.strip_prefix("cellar listening on ")
                    .and_then(|rest| rest.split_whitespace().next())
            }) {
                let origin = format!("http://{address}");
                let internal = text.lines().find_map(|line| {
                    line.strip_prefix("cellar internal listening on ")
                        .and_then(|rest| rest.split_whitespace().next())
                });
                if let Some(internal) = internal {
                    if client
                        .get(format!("{origin}/health"))
                        .send()
                        .await
                        .is_ok_and(|response| response.status().is_success())
                    {
                        break (origin, format!("http://{internal}"));
                    }
                }
            }
            ensure!(
                Instant::now() < deadline,
                "installed host readiness expired: {text}"
            );
            tokio::time::sleep(Duration::from_millis(25)).await;
        };
        Ok(Self {
            process,
            log: output,
            origin,
            internal,
        })
    }

    pub async fn stop(self, client: &reqwest::Client, preserve: bool) -> Result<()> {
        client
            .post(format!(
                "{}/shutdown{}",
                self.internal,
                if preserve { "?handoff=preserve" } else { "" }
            ))
            .send()
            .await?
            .error_for_status()?;
        self.join().await
    }

    pub async fn signal(self, signal: &str) -> Result<()> {
        // Address exactly this owned child. The CLI's sole signal owner must
        // perform handoff; killing the process is only Drop's failure cleanup.
        ensure!(
            matches!(signal, "-TERM" | "-INT"),
            "unsupported qualification signal"
        );
        let status = Command::new("/bin/kill")
            .args([signal, &self.process.0.id().to_string()])
            .status()?;
        ensure!(status.success(), "failed to signal owned Python host");
        self.join().await
    }

    pub fn ensure_live(&mut self) -> Result<()> {
        ensure!(
            self.process.0.try_wait()?.is_none(),
            "installed host exited: {}",
            std::fs::read_to_string(&self.log)?
        );
        Ok(())
    }

    /// Abruptly stop this owned child for crash-recovery qualification.
    #[allow(dead_code)] // Shared qualification entries exercise different shutdown modes.
    pub fn crash(mut self) -> Result<std::process::ExitStatus> {
        self.ensure_live()?;
        self.process.0.kill()?;
        Ok(self.process.0.wait()?)
    }

    pub async fn observe_refused_replacement(&mut self, version: &str) -> Result<()> {
        let deadline = Instant::now() + Duration::from_secs(30);
        loop {
            self.ensure_live()?;
            let text = std::fs::read_to_string(&self.log)?;
            if text
                .lines()
                .any(|line| line.contains("deployment_adoption_failed") && line.contains(version))
            {
                return Ok(());
            }
            ensure!(
                Instant::now() < deadline,
                "old process did not observe/refuse replacement: {text}"
            );
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    }

    async fn join(mut self) -> Result<()> {
        let output = &self.log;
        let process = &mut self.process;
        let deadline = Instant::now() + Duration::from_secs(30);
        loop {
            if let Some(status) = process.0.try_wait()? {
                ensure!(
                    status.success(),
                    "installed host failed shutdown: {}",
                    std::fs::read_to_string(output)?
                );
                break;
            }
            ensure!(Instant::now() < deadline, "installed host failed to join");
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
        let text = std::fs::read_to_string(output)?;
        let report: Value = text
            .lines()
            .filter_map(|line| serde_json::from_str::<Value>(line).ok())
            .find(|value| value["event"] == "embedded_python_shutdown")
            .context("missing installed-host combined report")?;
        let report = &report["report"];
        ensure!(
            report["runtime_failed"] == false
                && report["host_failed"] == false
                && report["objects_created"] == report["objects_retired"],
            "installed Python host reported incomplete cleanup"
        );
        ensure!(
            report["pending_tasks"] == 0
                && report["pending_threads"] == 0
                && report["pending_objects"] == 0
                && report["bridge"]["native_invocations"] == 0
                && report["bridge"]["pending_reservations"] == 0
                && report["bridge"]["queued_bytes"] == 0,
            "installed host left work pending"
        );
        ensure!(
            report["native_outcomes"]
                .as_array()
                .is_some_and(|outcomes| !outcomes.is_empty()
                    && outcomes.iter().all(|outcome| outcome["complete"] == true)),
            "installed native host did not drain"
        );
        Ok(())
    }
}
