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
        tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                // SAFETY: group is a positive PID returned for our child, which
                // starts a new process group. Signal 0 only observes existence.
                let mut alive = false;
                for group in [
                    self.group,
                    self.worker.context("Node worker PID was not observed")?,
                ] {
                    // SAFETY: these are the independently created groups of our
                    // owner and its observed Node worker; signal 0 is read-only.
                    let exists = unsafe { libc::kill(-group, 0) };
                    if exists == 0 {
                        alive = true;
                        continue;
                    }
                    let error = std::io::Error::last_os_error();
                    if error.raw_os_error() != Some(libc::ESRCH) {
                        return Err(error).context("observe owned crash process group after kill");
                    }
                }
                if !alive {
                    return Ok::<_, anyhow::Error>(());
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .context("owner's Node worker survived host process loss")??;
        self.owned = false;
        Ok(())
    }
}

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
