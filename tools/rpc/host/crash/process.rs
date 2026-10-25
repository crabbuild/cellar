// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Own only child process groups created for this qualification run.
use anyhow::{ensure, Context, Result};
use serde_json::Value;
use std::{
    os::unix::process::{CommandExt, ExitStatusExt},
    path::Path,
    process::Stdio,
    time::Duration,
};

pub struct Owner {
    child: tokio::process::Child,
    group: libc::pid_t,
    worker: Option<libc::pid_t>,
    owned: bool,
}

impl Owner {
    pub async fn start(root: &Path, name: &str) -> Result<(Self, String)> {
        let ready = root.join("ready.json");
        let log = std::fs::File::create(root.join("owner.log"))?;
        let mut command = tokio::process::Command::new(std::env::current_exe()?);
        command
            .args(["--serve-name", name])
            .arg("--serve-data")
            .arg(root.join("owner"))
            .arg("--serve-bucket")
            .arg(root.join("fleet.sqlite"))
            .arg("--serve-ready")
            .arg(&ready)
            .stdin(Stdio::null())
            .stdout(log.try_clone()?)
            .stderr(log)
            .kill_on_drop(true);
        command.as_std_mut().process_group(0);
        let child = command
            .spawn()
            .context("spawn isolated crash-owner process")?;
        let group = child.id().context("crash owner has no PID")?.try_into()?;
        let mut owner = Self {
            child,
            group,
            worker: None,
            owned: true,
        };
        let url = tokio::time::timeout(Duration::from_secs(30), async {
            loop {
                if let Some(status) = owner.child.try_wait()? {
                    anyhow::bail!(
                        "crash owner failed before readiness: {status}: {}",
                        std::fs::read_to_string(root.join("owner.log"))?
                    );
                }
                if ready.is_file() {
                    let view: Value = serde_json::from_slice(&std::fs::read(&ready)?)?;
                    ensure!(
                        view["pid"].as_u64() == Some(owner.group as u64) && view["node"] == name,
                        "crash owner readiness identity differs"
                    );
                    return Ok::<_, anyhow::Error>(
                        view["url"]
                            .as_str()
                            .context("missing crash owner URL")?
                            .to_string(),
                    );
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await??;
        Ok((owner, url))
    }

    pub fn worker(&mut self, pid: u64) -> Result<()> {
        let pid = libc::pid_t::try_from(pid)?;
        ensure!(
            pid > 0 && pid != self.group,
            "invalid Node worker process identity"
        );
        self.worker = Some(pid);
        Ok(())
    }

    pub async fn kill(&mut self) -> Result<()> {
        self.child
            .start_kill()
            .context("signal owned crash process")?;
        let status = self
            .child
            .wait()
            .await
            .context("join owned crash process")?;
        ensure!(
            status.signal() == Some(libc::SIGKILL),
            "owner did not terminate with SIGKILL: {status}"
        );
        wait_groups_gone(&[
            ("owner", self.group),
            (
                "node",
                self.worker.context("Node worker PID was not observed")?,
            ),
        ])
        .await?;
        self.owned = false;
        Ok(())
    }
}

/// Success requires every owned process group to report ESRCH. This helper
/// never sends a signal and never treats permission denial as proof of exit.
pub(crate) async fn wait_groups_gone(groups: &[(&str, libc::pid_t)]) -> Result<()> {
    ensure!(
        !groups.is_empty() && groups.len() <= 2 && groups.iter().all(|(_, group)| *group > 1),
        "invalid owned process groups"
    );
    let mut last = Vec::with_capacity(groups.len());
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            last.clear();
            let mut alive = false;
            for (role, group) in groups {
                // SAFETY: these positive group IDs come from our independently
                // created child processes. Signal 0 only observes existence.
                let exists = unsafe { libc::kill(-*group, 0) };
                if exists == 0 {
                    last.push((*role, *group, "present"));
                    alive = true;
                    continue;
                }
                let error = std::io::Error::last_os_error();
                match error.raw_os_error() {
                    Some(libc::ESRCH) => last.push((*role, *group, "gone")),
                    Some(libc::EPERM) => {
                        // Darwin can retain a group with only zombie members
                        // while excluding them from its signal-permission
                        // check. Exit remains unproven until ESRCH; persistent
                        // denial still fails at the original five-second bound.
                        last.push((*role, *group, "permission_denied"));
                        alive = true;
                    }
                    _ => {
                        return Err(error).with_context(|| {
                            format!("observe owned {role} process group {group} after kill")
                        })
                    }
                }
            }
            if !alive {
                return Ok::<_, anyhow::Error>(());
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .with_context(|| {
        format!("owned process groups did not disappear after termination: {last:?}")
    })?
}

#[cfg(test)]
#[path = "process_tests.rs"]
mod tests;

impl Drop for Owner {
    fn drop(&mut self) {
        if self.owned {
            // SAFETY: this group was created solely for our child and its Node
            // worker. Failure cleanup must not leave a qualification process.
            unsafe {
                libc::kill(-self.group, libc::SIGKILL);
            }
            if let Some(worker) = self.worker {
                // SAFETY: this PID was observed from the worker our owner
                // started, which has its own dedicated process group.
                unsafe {
                    libc::kill(-worker, libc::SIGKILL);
                }
            }
            let _ = self.child.start_kill();
        }
    }
}
