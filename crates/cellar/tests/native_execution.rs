//! Execution admission remains joined when executors reject or cancel work.
#![allow(clippy::disallowed_methods)] // Real runtime shutdown and thread probes.

use anyhow::{ensure, Result};
use cellar::sdk::{ExecutionDomain, HostServices};
use std::sync::{
    atomic::{AtomicBool, AtomicUsize, Ordering},
    Arc,
};
use std::time::{Duration, Instant};

struct DropProbe {
    owner: Arc<HostServices>,
    released: Arc<AtomicUsize>,
    wrong_owner: Arc<AtomicBool>,
}
impl Drop for DropProbe {
    fn drop(&mut self) {
        if !Arc::ptr_eq(&self.owner, &cellar::asyncrt::services()) {
            self.wrong_owner.store(true, Ordering::SeqCst);
        }
        self.released.fetch_add(1, Ordering::SeqCst);
    }
}

#[test]
fn stopped_executor_dispatch_releases_work_and_allows_domain_join() -> Result<()> {
    const CASE: &str = "CELLAR_EXECUTOR_REJECTION_CASE";
    if let Some(case) = std::env::var_os(CASE) {
        let executor = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()?;
        let domain = ExecutionDomain::new(executor.handle().clone());
        drop(executor);
        let runner = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()?;
        return runner.block_on(async {
            let released = Arc::new(AtomicUsize::new(0));
            let wrong_owner = Arc::new(AtomicBool::new(false));
            let ran = Arc::new(AtomicBool::new(false));
            let probe = DropProbe {
                owner: domain.services(),
                released: released.clone(),
                wrong_owner: wrong_owner.clone(),
            };
            let invoked = ran.clone();
            let result = if case == "blocking" {
                domain
                    .blocking(move || {
                        let _probe = probe;
                        invoked.store(true, Ordering::SeqCst);
                        42
                    })?
                    .await
            } else {
                domain
                    .spawn(async move {
                        let _probe = probe;
                        invoked.store(true, Ordering::SeqCst);
                        42
                    })?
                    .await
            };
            ensure!(result.is_err(), "a stopped executor accepted work");
            ensure!(!ran.load(Ordering::SeqCst));
            ensure!(
                released.load(Ordering::SeqCst) == 1,
                "rejected captures were retained"
            );
            ensure!(
                !wrong_owner.load(Ordering::SeqCst),
                "rejected captures lost their host"
            );
            tokio::time::timeout(Duration::from_secs(1), domain.shutdown()).await?;
            ensure!(runner.handle().spawn(async { 42 }).await? == 42);
            Ok(())
        });
    }
    for case in ["async", "blocking"] {
        let mut child = std::process::Command::new(std::env::current_exe()?)
            .args([
                "--exact",
                "stopped_executor_dispatch_releases_work_and_allows_domain_join",
                "--nocapture",
            ])
            .env(CASE, case)
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::piped())
            .spawn()?;
        let deadline = Instant::now() + Duration::from_secs(5);
        loop {
            if child.try_wait()?.is_some() {
                break;
            }
            if Instant::now() >= deadline {
                child.kill()?;
                let output = child.wait_with_output()?;
                anyhow::bail!(
                    "{case} dispatch or domain join deadlocked: {}",
                    String::from_utf8_lossy(&output.stderr)
                );
            }
            std::thread::sleep(Duration::from_millis(5));
        }
        let output = child.wait_with_output()?;
        ensure!(
            output.status.success(),
            "{case} executor rejection failed: {}",
            String::from_utf8_lossy(&output.stderr)
        );
    }
    Ok(())
}

#[test]
fn queued_blocking_cancellation_keeps_destructors_in_their_host() -> Result<()> {
    let executor = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(1)
        .max_blocking_threads(1)
        .enable_all()
        .build()?;
    let runner = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?;
    let domain = ExecutionDomain::new(executor.handle().clone());
    let released = Arc::new(AtomicUsize::new(0));
    let wrong_owner = Arc::new(AtomicBool::new(false));
    let ran = Arc::new(AtomicBool::new(false));
    let (entered, ready) = std::sync::mpsc::channel();
    let (release, gate) = std::sync::mpsc::channel();
    let blocker = executor.handle().spawn_blocking(move || {
        entered.send(()).unwrap();
        gate.recv().unwrap();
    });
    ready.recv_timeout(Duration::from_secs(5))?;
    let probe = DropProbe {
        owner: domain.services(),
        released: released.clone(),
        wrong_owner: wrong_owner.clone(),
    };
    let invoked = ran.clone();
    let queued = domain.blocking(move || {
        let _probe = probe;
        invoked.store(true, Ordering::SeqCst);
        42
    })?;
    domain.close();
    release.send(())?;
    runner.block_on(async {
        blocker.await?;
        ensure!(queued.await.is_err(), "queued file work ran after closure");
        tokio::time::timeout(Duration::from_secs(5), domain.shutdown()).await?;
        Ok::<_, anyhow::Error>(())
    })?;
    ensure!(!ran.load(Ordering::SeqCst));
    ensure!(released.load(Ordering::SeqCst) == 1);
    ensure!(!wrong_owner.load(Ordering::SeqCst));
    Ok(())
}
