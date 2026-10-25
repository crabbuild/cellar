// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Exercise the actual syscall seam, including an exited, unreaped group.
use super::*;

#[tokio::test]
async fn group_cleanup_waits_for_live_and_unreaped_children() -> Result<()> {
    let mut child = std::process::Command::new("/bin/sh")
        .args(["-c", "exec sleep 60"])
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .process_group(0)
        .spawn()?;
    let group = libc::pid_t::try_from(child.id())?;
    let groups = [("test", group)];
    let mut pending = Box::pin(wait_groups_gone(&groups));
    let live_waited = tokio::time::timeout(Duration::from_millis(30), pending.as_mut())
        .await
        .is_err();
    child.kill()?;
    // SAFETY: siginfo_t is a C output buffer valid when zero-initialized.
    let mut info: libc::siginfo_t = unsafe { std::mem::zeroed() };
    // SAFETY: waitid observes only our child. WNOWAIT deliberately keeps its
    // exited process in the process table until child.wait() below reaps it.
    let waited = unsafe {
        libc::waitid(
            libc::P_PID,
            child.id(),
            &mut info,
            libc::WEXITED | libc::WNOWAIT,
        )
    };
    let zombie_waited = tokio::time::timeout(Duration::from_millis(30), pending.as_mut())
        .await
        .is_err();
    // Always reap before asserting: a failing regression must not leave its
    // child behind, and reaping is precisely what makes ESRCH possible.
    let status = child.wait()?;
    ensure!(waited == 0, "waitid could not observe the exited child");
    ensure!(status.signal() == Some(libc::SIGKILL));
    ensure!(live_waited, "a live process group passed cleanup");
    ensure!(
        zombie_waited,
        "unreaped group returned a premature cleanup result"
    );
    pending.await?;
    Ok(())
}

#[tokio::test]
async fn group_cleanup_rejects_broadcast_or_current_group_selectors() {
    for group in [i32::MIN, -1, 0, 1] {
        assert!(wait_groups_gone(&[("invalid", group)]).await.is_err());
    }
}
