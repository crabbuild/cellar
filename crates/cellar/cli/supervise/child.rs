// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Owned process group; final cleanup never relies on the monitored interpreter.
#![allow(clippy::disallowed_methods)] // Process supervision is outside cell execution.
use super::options::Options;
use anyhow::{Context as _, Result};
use std::process::{ExitStatus, Stdio};
use tokio::process::{Child, Command};

pub(super) struct Owned {
    pub child: Child,
    pub pid: u32,
    group: Option<u32>,
}

impl Owned {
    pub fn spawn(options: &Options, instance: &str) -> Result<Self> {
        let mut command = Command::new(&options.program);
        command
            .args(&options.arguments)
            .env("CELLAR_SUPERVISOR_INSTANCE", instance)
            .stdin(Stdio::null())
            .stderr(Stdio::inherit())
            .kill_on_drop(true);
        #[cfg(unix)]
        {
            use std::os::fd::AsFd as _;
            command.stdout(Stdio::from(std::io::stderr().as_fd().try_clone_to_owned()?));
        }
        crate::dev::own_child_command(&mut command);
        let child = command.spawn().context("start supervised process")?;
        let pid = child.id().context("supervised process has no PID")?;
        Ok(Self {
            child,
            pid,
            group: Some(pid),
        })
    }

    pub async fn stop(
        &mut self,
        grace: std::time::Duration,
        signals: &mut crate::dev::ShutdownSignals,
    ) -> Result<(ExitStatus, bool)> {
        if let Some(status) = self
            .child
            .try_wait()
            .context("inspect supervised process")?
        {
            self.finish_group()?;
            return Ok((status, false));
        }
        signal(self.pid, libc::SIGTERM)?;
        let status = crate::asyncrt::select_biased! {
            "an already-joined child takes precedence over forced termination";
            status = self.child.wait() => Some(status.context("join supervised process")?),
            _ = tokio::time::sleep(grace) => None,
            _ = signals.received() => None,
        };
        let forced = status.is_none();
        self.finish_group()?;
        let status = match status {
            Some(status) => status,
            None => tokio::time::timeout(std::time::Duration::from_secs(5), self.child.wait())
                .await
                .context("supervised process did not reap after KILL; refusing restart")?
                .context("reap killed supervised process")?,
        };
        Ok((status, forced))
    }

    fn finish_group(&mut self) -> Result<()> {
        if let Some(group) = self.group {
            signal(group, libc::SIGKILL)?;
            self.group = None;
        }
        Ok(())
    }
}

impl Drop for Owned {
    fn drop(&mut self) {
        if let Some(group) = self.group {
            let _ = signal(group, libc::SIGKILL);
        }
    }
}

fn signal(group: u32, signal: libc::c_int) -> Result<()> {
    #[cfg(unix)]
    {
        let group = libc::pid_t::try_from(group).context("invalid owned process group")?;
        anyhow::ensure!(group > 0, "invalid owned process group");
        // SAFETY: this positive PID is the separately grouped child we spawned.
        // Negation selects only its group; no pointers cross this C call.
        if unsafe { libc::kill(-group, signal) } != 0 {
            let error = std::io::Error::last_os_error();
            if error.raw_os_error() != Some(libc::ESRCH) {
                return Err(error).context("signal supervised process group");
            }
        }
    }
    #[cfg(not(unix))]
    {
        let _ = (group, signal);
        anyhow::bail!("process supervision requires Unix process groups");
    }
    Ok(())
}
