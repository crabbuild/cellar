// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! A finished owner must release its lock despite unrelated spawned children.
#![allow(clippy::disallowed_methods)]

#[cfg(unix)]
#[test]
fn closing_a_lock_releases_it_while_a_forked_child_keeps_the_inherited_descriptor() {
    use cellar_ltx::{DirectFileSystem, FileSystem};

    struct Child(libc::pid_t);

    impl Drop for Child {
        fn drop(&mut self) {
            // SAFETY: this parent exclusively owns this positive child PID.
            // Kill and reap even when the assertion below unwinds.
            unsafe {
                libc::kill(self.0, libc::SIGKILL);
                let mut status = 0;
                while libc::waitpid(self.0, &mut status, 0) == -1 {
                    if std::io::Error::last_os_error().kind() != std::io::ErrorKind::Interrupted {
                        break;
                    }
                }
            }
        }
    }

    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("owner.lock");
    let lock = DirectFileSystem.lock_file(&path).unwrap();
    assert!(DirectFileSystem.lock_file(&path).is_err());
    // SAFETY: getpid takes no pointers and changes no shared state.
    let owner = unsafe { libc::getpid() };
    // SAFETY: the child performs only async-signal-safe process observation,
    // nanosleep and _exit. It never enters Rust/SQLite allocation, logging,
    // destructors or inherited harness locks. Parent death also ends the probe.
    let pid = unsafe { libc::fork() };
    if pid == 0 {
        // SAFETY: getppid takes no pointers and changes no shared state.
        while unsafe { libc::getppid() } == owner {
            let delay = libc::timespec {
                tv_sec: 0,
                tv_nsec: 100_000_000,
            };
            // SAFETY: delay is valid for this synchronous syscall; no remaining
            // interval is requested. Interrupted sleeps simply recheck parent.
            unsafe {
                libc::nanosleep(&delay, std::ptr::null_mut());
            }
        }
        // SAFETY: terminate without running inherited Rust destructors.
        unsafe {
            libc::_exit(0);
        }
    }
    assert!(pid > 0, "fork failed: {}", std::io::Error::last_os_error());
    let _child = Child(pid);
    drop(lock);
    let reopened = DirectFileSystem.lock_file(&path);
    assert!(
        reopened.is_ok(),
        "closed owner left its lock held by an inherited descriptor"
    );
}
