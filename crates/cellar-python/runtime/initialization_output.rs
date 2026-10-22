//! Suppress CPython's raw C diagnostics during exclusive process bootstrap.
// This precedes any node/execution domain or framework-owned host thread.
#![allow(clippy::disallowed_methods)]

use anyhow::{ensure, Result};
use std::{
    fs::OpenOptions,
    io::{Error, Write},
    os::fd::{AsRawFd, FromRawFd, OwnedFd},
};

pub(super) struct Diagnostics {
    saved: Option<OwnedFd>,
    flags: libc::c_int,
    redirected: bool,
}

impl Diagnostics {
    pub(super) fn silence() -> Result<Self> {
        // SAFETY: F_GETFD inspects a descriptor without borrowing its storage.
        let flags = unsafe { libc::fcntl(libc::STDERR_FILENO, libc::F_GETFD) };
        if flags < 0 {
            ensure!(
                Error::last_os_error().raw_os_error() == Some(libc::EBADF),
                "inspect CPython bootstrap diagnostic descriptor failed"
            );
            // Preserve a closed stderr, including CPython's normal None streams.
            // There is no diagnostic recipient and no descriptor to redirect.
            return Ok(Self {
                saved: None,
                flags: 0,
                redirected: false,
            });
        }
        let saved = duplicate(libc::STDERR_FILENO)?;
        let sink = OpenOptions::new()
            .write(true)
            .open("/dev/null")
            .map_err(|_| anyhow::anyhow!("open CPython bootstrap diagnostic sink failed"))?;
        // Preserve buffered host output before changing descriptor 2. CPython's
        // configure_c_stdio=0 leaves any host-selected C buffering unchanged.
        std::io::stderr().flush().map_err(|_| {
            anyhow::anyhow!("flush host diagnostics before CPython bootstrap failed")
        })?;
        flush_c_stderr()?;
        replace(sink.as_raw_fd())?;
        Ok(Self {
            saved: Some(saved),
            flags,
            redirected: true,
        })
    }

    pub(super) fn restore(&mut self) -> Result<()> {
        if !self.redirected {
            return Ok(());
        }
        // Discard even C diagnostics buffered by a host-selected stderr mode.
        // Always restore the descriptor, including after a failed flush.
        let flushed = flush_c_stderr();
        self.restore_descriptor()?;
        flushed
    }

    fn restore_descriptor(&mut self) -> Result<()> {
        let Some(saved) = &self.saved else {
            return Ok(());
        };
        replace(saved.as_raw_fd())?;
        // dup2 clears FD_CLOEXEC. Preserve the caller's descriptor flags too.
        // SAFETY: F_SETFD updates the valid restored descriptor, with the flags
        // inspected before bootstrap. No framework host thread exists yet.
        ensure!(
            unsafe { libc::fcntl(libc::STDERR_FILENO, libc::F_SETFD, self.flags) } >= 0,
            "restore CPython bootstrap diagnostic descriptor flags failed"
        );
        self.redirected = false;
        Ok(())
    }
}

impl Drop for Diagnostics {
    fn drop(&mut self) {
        if self.redirected {
            let _ = self.restore();
        }
    }
}

fn duplicate(descriptor: libc::c_int) -> Result<OwnedFd> {
    // SAFETY: fcntl returns a new exclusively owned descriptor on success;
    // CLOEXEC prevents leaking the saved host diagnostic recipient to children.
    let copy = unsafe { libc::fcntl(descriptor, libc::F_DUPFD_CLOEXEC, 3) };
    ensure!(
        copy >= 0,
        "save CPython bootstrap diagnostic descriptor failed"
    );
    // SAFETY: the successful fcntl above returned a fresh owned descriptor.
    Ok(unsafe { OwnedFd::from_raw_fd(copy) })
}

fn replace(descriptor: libc::c_int) -> Result<()> {
    loop {
        // SAFETY: descriptor is held by a live owned file/descriptor. Replacing
        // stderr is confined to exclusive process bootstrap, before host work.
        if unsafe { libc::dup2(descriptor, libc::STDERR_FILENO) } >= 0 {
            return Ok(());
        }
        ensure!(
            Error::last_os_error().kind() == std::io::ErrorKind::Interrupted,
            "replace CPython bootstrap diagnostic descriptor failed"
        );
    }
}

fn flush_c_stderr() -> Result<()> {
    extern "C" {
        #[cfg_attr(target_os = "macos", link_name = "__stderrp")]
        #[cfg_attr(not(target_os = "macos"), link_name = "stderr")]
        static C_STDERR: *mut libc::FILE;
    }
    // SAFETY: the platform C runtime owns this process-lifetime FILE pointer;
    // fflush synchronizes its stream internally. Bootstrap changes its file
    // descriptor, never the FILE pointer or its buffering configuration.
    ensure!(
        unsafe { libc::fflush(C_STDERR) } == 0,
        "flush CPython bootstrap diagnostics failed"
    );
    Ok(())
}
