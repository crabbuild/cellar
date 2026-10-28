// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Kill only the observed worker of a stopped, unreaped test child. Stopping
//! the native owner pins its worker PID until the group has been signalled.

use anyhow::{ensure, Context as _, Result};
use std::{
    process::{Child, Command},
    time::{Duration, Instant},
};

#[cfg(unix)]
pub fn crash(child: &mut Child, worker: u32) -> Result<()> {
    use std::os::unix::process::ExitStatusExt as _;

    ensure!(
        child.try_wait()?.is_none(),
        "native crash target already exited"
    );
    let native = libc::pid_t::try_from(child.id())?;
    let worker = libc::pid_t::try_from(worker)?;
    ensure!(
        native > 1 && worker > 1 && native != worker,
        "invalid owned crash identities"
    );
    // SAFETY: Child is our unreaped native process. SIGSTOP prevents it from
    // reaping/reusing its worker's PID while parentage and group are checked.
    ensure!(
        unsafe { libc::kill(native, libc::SIGSTOP) } == 0,
        "stop native crash target: {}",
        std::io::Error::last_os_error()
    );
    let result = (|| {
        let started = Instant::now();
        loop {
            let mut status = 0;
            // SAFETY: observe only our exact child. A stopped status does not
            // reap it; terminal statuses return failure without other signals.
            let observed =
                unsafe { libc::waitpid(native, &mut status, libc::WUNTRACED | libc::WNOHANG) };
            ensure!(
                observed >= 0,
                "observe stopped child: {}",
                std::io::Error::last_os_error()
            );
            if observed == native {
                ensure!(
                    libc::WIFSTOPPED(status) && libc::WSTOPSIG(status) == libc::SIGSTOP,
                    "native target terminated before crash barrier"
                );
                break;
            }
            ensure!(
                started.elapsed() < Duration::from_secs(5),
                "native crash target did not stop"
            );
            std::thread::sleep(Duration::from_millis(10));
        }
        // Read one numeric field for the known worker, never argv/environment
        // or a process inventory. Refuse a reused/foreign PID before signalling.
        let parent = Command::new("ps")
            .args(["-o", "ppid=", "-p", &worker.to_string()])
            .output()?;
        ensure!(
            parent.status.success(),
            "owned worker parentage unavailable"
        );
        let parent = std::str::from_utf8(&parent.stdout)?
            .trim()
            .parse::<libc::pid_t>()?;
        ensure!(
            parent == native,
            "crash worker is not a child of the stopped native owner"
        );
        // SAFETY: the stopped native parent cannot reap this verified child;
        // NodeRuntime created its process group with the worker's own PID.
        ensure!(
            unsafe { libc::getpgid(worker) } == worker,
            "worker group changed"
        );
        // SAFETY: signal the verified, pinned worker group before its parent.
        ensure!(
            unsafe { libc::kill(-worker, libc::SIGKILL) } == 0,
            "kill owned worker group: {}",
            std::io::Error::last_os_error()
        );
        child.kill()?;
        let status = child.wait()?;
        ensure!(
            status.signal() == Some(libc::SIGKILL),
            "native process did not receive SIGKILL: {status}"
        );
        let started = Instant::now();
        loop {
            // SAFETY: signal 0 only observes the already-signalled group. No
            // further mutation is allowed after the native parent is reaped.
            if unsafe { libc::kill(-worker, 0) } == -1 {
                let error = std::io::Error::last_os_error();
                match error.raw_os_error() {
                    Some(libc::ESRCH) => break,
                    // Permission denial is an uncertain observation, not a
                    // death certificate. Keep the same target and bound, and
                    // require ESRCH before declaring the group gone.
                    Some(libc::EPERM) => {}
                    _ => anyhow::bail!("worker group observation failed: {error:?}"),
                }
            }
            ensure!(
                started.elapsed() < Duration::from_secs(5),
                "killed worker group did not confirm termination"
            );
            std::thread::sleep(Duration::from_millis(10));
        }
        Ok(())
    })();
    if result.is_err() && child.try_wait().is_ok_and(|status| status.is_none()) {
        // SAFETY: an owned, unreaped native child remains. Resume it so the
        // harness can still supervise its ordinary cleanup or verified kill.
        unsafe {
            libc::kill(native, libc::SIGCONT);
        }
    }
    result
}

#[cfg(not(unix))]
pub fn crash(_: &mut Child, _: u32) -> Result<()> {
    anyhow::bail!("backup crash qualification requires Unix process supervision")
}

#[cfg(all(test, unix))]
mod tests {
    use super::*;
    use std::{os::unix::process::CommandExt as _, process::Stdio};

    const FIXTURE: &str = "CELLAR_BACKUP_CRASH_PROCESS_FIXTURE";

    #[test]
    fn owned_process_fixture() -> Result<()> {
        let Some(path) = std::env::var_os(FIXTURE) else {
            return Ok(());
        };
        let mut worker = Command::new("node")
            .args(["--eval", "setInterval(() => {}, 1000)"])
            .process_group(0)
            .spawn()?;
        std::fs::write(path, worker.id().to_string())?;
        worker.wait()?;
        Ok(())
    }

    fn fixture() -> Result<(tempfile::TempDir, Child, u32)> {
        let directory = tempfile::tempdir()?;
        let path = directory.path().join("worker");
        let mut child = Command::new(std::env::current_exe()?)
            .args([
                "--exact",
                "backup_campaign::process::tests::owned_process_fixture",
                "--test-threads=1",
            ])
            .env(FIXTURE, &path)
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()?;
        let started = Instant::now();
        while !path.exists() {
            ensure!(
                child.try_wait()?.is_none(),
                "process fixture exited before readiness"
            );
            ensure!(
                started.elapsed() < Duration::from_secs(5),
                "process fixture readiness timed out"
            );
            std::thread::sleep(Duration::from_millis(10));
        }
        let worker = std::fs::read_to_string(path)?
            .parse()
            .context("worker PID")?;
        Ok((directory, child, worker))
    }

    #[test]
    fn verified_child_and_worker_group_are_killed_without_native_cleanup() -> Result<()> {
        let (_directory, mut child, worker) = fixture()?;
        crash(&mut child, worker)?;
        ensure!(child.try_wait()?.is_some());
        Ok(())
    }

    #[test]
    fn foreign_worker_refuses_without_signalling_and_resumes_native_owner() -> Result<()> {
        let (_directory, mut child, worker) = fixture()?;
        let result = crash(&mut child, std::process::id());
        ensure!(result
            .unwrap_err()
            .to_string()
            .contains("not a child of the stopped native owner"));
        ensure!(
            child.try_wait()?.is_none(),
            "wrong worker killed the native owner"
        );
        // SAFETY: signal 0 observes this fixture's own known worker group.
        ensure!(
            unsafe { libc::kill(-(worker as libc::pid_t), 0) } == 0,
            "wrong worker killed our real worker group"
        );
        crash(&mut child, worker)?;
        Ok(())
    }
}
