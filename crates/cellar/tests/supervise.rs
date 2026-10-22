//! The shipped supervisor must own and reap real processes under failure.
#![cfg(unix)]
#![allow(clippy::disallowed_methods)]
use anyhow::{ensure, Context as _, Result};
use serde_json::Value;
use std::{
    fs::File,
    io::{Read as _, Write as _},
    net::{SocketAddr, TcpListener},
    path::{Path, PathBuf},
    process::{Child, Command, ExitStatus},
    sync::atomic::{AtomicBool, Ordering},
    time::{Duration, Instant},
};

static STOP: AtomicBool = AtomicBool::new(false);

extern "C" fn terminate(_: libc::c_int) {
    STOP.store(true, Ordering::Release);
}

#[test]
#[ignore = "disposable process fixture, invoked explicitly by supervisor tests"]
fn service_fixture() -> Result<()> {
    let root = PathBuf::from(std::env::var("CELLAR_SUPERVISE_TEST_ROOT")?);
    let mode = std::env::var("CELLAR_SUPERVISE_TEST_MODE")?;
    let pid = std::process::id();
    if mode == "exit" {
        std::process::exit(7);
    }
    // SAFETY: this disposable process installs a static C handler before
    // serving. It only writes a lock-free atomic, without allocation or I/O.
    unsafe {
        libc::signal(
            libc::SIGTERM,
            if matches!(mode.as_str(), "ignore-term" | "second-signal") {
                libc::SIG_IGN
            } else {
                terminate as *const () as libc::sighandler_t
            },
        );
    }
    std::fs::write(root.join(format!("signal-ready-{pid}")), "true")?;
    if matches!(mode.as_str(), "no-health" | "ignore-term" | "second-signal") {
        while !STOP.load(Ordering::Acquire) {
            std::thread::sleep(Duration::from_millis(5));
        }
        return Ok(());
    }
    let _ = std::fs::remove_file(root.join("unhealthy"));
    let listener = TcpListener::bind(std::env::var("CELLAR_SUPERVISE_TEST_ADDR")?)?;
    listener.set_nonblocking(true)?;
    let instance = std::env::var("CELLAR_SUPERVISOR_INSTANCE")?;
    let mut probes = 0;
    while !STOP.load(Ordering::Acquire) {
        let (mut stream, _) = match listener.accept() {
            Ok(connection) => connection,
            Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                std::thread::sleep(Duration::from_millis(5));
                continue;
            }
            Err(error) => return Err(error.into()),
        };
        // macOS inherits the listener's nonblocking flag on accepted sockets.
        // Read one bounded request without treating an early EAGAIN as death.
        stream.set_nonblocking(false)?;
        stream.set_read_timeout(Some(Duration::from_secs(1)))?;
        let mut request = [0u8; 4096];
        match stream.read(&mut request) {
            Ok(0) => continue,
            Ok(_) => {}
            Err(_) if STOP.load(Ordering::Acquire) => break,
            Err(error)
                if matches!(
                    error.kind(),
                    std::io::ErrorKind::WouldBlock
                        | std::io::ErrorKind::TimedOut
                        | std::io::ErrorKind::Interrupted
                        | std::io::ErrorKind::ConnectionReset
                ) =>
            {
                continue
            }
            Err(error) => return Err(error.into()),
        }
        probes += 1;
        let failed = root.join("unhealthy").exists() || root.join("fail-once").exists();
        let _ = std::fs::remove_file(root.join("fail-once"));
        let token = if mode == "wrong-instance" {
            "another-process"
        } else {
            &instance
        };
        let status = if failed { "503 Unavailable" } else { "200 OK" };
        let reply = format!("HTTP/1.1 {status}\r\nx-cellar-supervisor-instance: {token}\r\ncontent-length: 2\r\nconnection: close\r\n\r\n{{}}");
        let next = root.join(format!("next-probes-{pid}"));
        std::fs::write(&next, probes.to_string())?;
        std::fs::rename(next, root.join("probes"))?;
        // A timed-out probe may disconnect before its reply. That does not
        // terminate the fixture or substitute for a supervisor decision.
        let _ = stream.write_all(reply.as_bytes());
    }
    std::fs::write(root.join(format!("joined-{pid}")), "true")?;
    Ok(())
}

struct Supervisor {
    child: Child,
    stdout: PathBuf,
    stderr: PathBuf,
}

impl Supervisor {
    fn start(root: &Path, mode: &str, restarts: u32) -> Result<Self> {
        let reserve = TcpListener::bind("127.0.0.1:0")?;
        let address: SocketAddr = reserve.local_addr()?;
        drop(reserve);
        let stdout = root.join("stdout");
        let stderr = root.join("stderr");
        let child = Command::new(env!("CARGO_BIN_EXE_cellar"))
            .args([
                "supervise",
                "--json",
                "--health-url",
                &format!("http://{address}/health"),
                "--startup-ms",
                if mode == "second-signal" {
                    "10000"
                } else {
                    "2000"
                },
                "--probe-ms",
                "200",
                "--interval-ms",
                "25",
                "--failures",
                "2",
                "--shutdown-ms",
                if mode == "second-signal" {
                    "5000"
                } else {
                    "500"
                },
                "--restart-ms",
                "25",
                "--max-restarts",
                &restarts.to_string(),
                "--",
            ])
            .arg(std::env::current_exe()?)
            .args(["--exact", "service_fixture", "--ignored", "--nocapture"])
            .env("CELLAR_SUPERVISE_TEST_ROOT", root)
            .env("CELLAR_SUPERVISE_TEST_MODE", mode)
            .env("CELLAR_SUPERVISE_TEST_ADDR", address.to_string())
            .stdout(File::create(&stdout)?)
            .stderr(File::create(&stderr)?)
            .spawn()?;
        Ok(Self {
            child,
            stdout,
            stderr,
        })
    }

    fn events(&self) -> Result<Vec<Value>> {
        std::fs::read_to_string(&self.stdout)?
            .split_inclusive('\n')
            .filter(|line| line.ends_with('\n'))
            .map(|line| Ok(serde_json::from_str(line)?))
            .collect()
    }

    fn wait_event(&mut self, event: &str, attempt: u32) -> Result<()> {
        let deadline = Instant::now() + Duration::from_secs(10);
        loop {
            if self
                .events()?
                .iter()
                .any(|record| record["event"] == event && record["attempt"] == attempt)
            {
                return Ok(());
            }
            ensure!(
                self.child.try_wait()?.is_none(),
                "supervisor exited before {event}: {}",
                std::fs::read_to_string(&self.stderr)?
            );
            ensure!(
                Instant::now() < deadline,
                "supervisor did not emit {event} attempt {attempt}: events={:?}; log={}",
                self.events()?,
                std::fs::read_to_string(&self.stderr)?
            );
            std::thread::sleep(Duration::from_millis(10));
        }
    }

    fn wait(&mut self) -> Result<ExitStatus> {
        let deadline = Instant::now() + Duration::from_secs(12);
        loop {
            if let Some(status) = self.child.try_wait()? {
                return Ok(status);
            }
            ensure!(
                Instant::now() < deadline,
                "supervisor did not stop: {}",
                std::fs::read_to_string(&self.stderr)?
            );
            std::thread::sleep(Duration::from_millis(10));
        }
    }

    fn terminate(&mut self) -> Result<()> {
        // SAFETY: the PID is this test's live, owned supervisor child.
        ensure!(unsafe { libc::kill(self.child.id() as libc::pid_t, libc::SIGTERM) } == 0);
        Ok(())
    }

    fn reaped(&self) -> Result<()> {
        for event in self
            .events()?
            .iter()
            .filter(|record| record["event"] == "supervisor_started")
        {
            let pid = event["pid"].as_u64().context("child PID")? as libc::pid_t;
            // SAFETY: signal 0 only queries the child recorded by this fixture.
            ensure!(
                unsafe { libc::kill(pid, 0) } == -1
                    && std::io::Error::last_os_error().raw_os_error() == Some(libc::ESRCH),
                "supervisor left child {pid} alive/unreaped"
            );
        }
        Ok(())
    }
}

impl Drop for Supervisor {
    fn drop(&mut self) {
        if self.child.try_wait().ok().flatten().is_none() {
            // SAFETY: only our live supervisor PID is signalled.
            unsafe {
                libc::kill(self.child.id() as libc::pid_t, libc::SIGTERM);
            }
            for _ in 0..300 {
                if self.child.try_wait().ok().flatten().is_some() {
                    break;
                }
                std::thread::sleep(Duration::from_millis(10));
            }
            let _ = self.child.kill();
        }
        let _ = self.child.wait();
        // Failure cleanup covers only active groups in our private records,
        // including macOS where parent-death SIGKILL is unavailable.
        if let Ok(events) = self.events() {
            let mut active = std::collections::BTreeSet::new();
            for row in events {
                if let Some(pid) = row["pid"]
                    .as_u64()
                    .and_then(|pid| libc::pid_t::try_from(pid).ok())
                {
                    if row["event"] == "supervisor_started" {
                        active.insert(pid);
                    }
                    if row["event"] == "supervisor_stopped" {
                        active.remove(&pid);
                    }
                }
            }
            for pid in active {
                // SAFETY: these are separately grouped children owned by this test.
                unsafe {
                    libc::kill(-pid, libc::SIGKILL);
                }
            }
        }
    }
}

#[test]
fn failed_startup_and_wrong_instance_never_announce_readiness() -> Result<()> {
    for mode in ["no-health", "wrong-instance"] {
        let directory = tempfile::tempdir()?;
        let mut supervisor = Supervisor::start(directory.path(), mode, 1)?;
        ensure!(!supervisor.wait()?.success());
        let events = supervisor.events()?;
        ensure!(
            events
                .iter()
                .filter(|row| row["event"] == "supervisor_started")
                .count()
                == 2
        );
        ensure!(!events.iter().any(|row| row["event"] == "supervisor_ready"));
        let stops = events
            .iter()
            .filter(|row| row["event"] == "supervisor_stopped")
            .collect::<Vec<_>>();
        ensure!(
            stops.len() == 2
                && stops
                    .iter()
                    .all(|row| row["reason"] == "startup_timeout" && row["forced"] == false),
            "unexpected startup lifecycle: {events:?}"
        );
        supervisor.reaped()?;
    }
    Ok(())
}

#[test]
fn unresponsive_process_requires_kill_and_never_reports_a_clean_stop() -> Result<()> {
    let directory = tempfile::tempdir()?;
    let mut supervisor = Supervisor::start(directory.path(), "ignore-term", 0)?;
    ensure!(!supervisor.wait()?.success());
    let events = supervisor.events()?;
    let stop = events
        .iter()
        .find(|row| row["event"] == "supervisor_stopped")
        .context("missing forced stop")?;
    ensure!(
        stop["reason"] == "startup_timeout"
            && stop["forced"] == true
            && stop["exit_signal"] == libc::SIGKILL
    );
    ensure!(!events.iter().any(|row| row.get("drain_complete").is_some()));
    supervisor.reaped()
}

#[test]
fn unexpected_exit_is_joined_and_restart_count_is_global() -> Result<()> {
    let directory = tempfile::tempdir()?;
    let mut supervisor = Supervisor::start(directory.path(), "exit", 1)?;
    ensure!(!supervisor.wait()?.success());
    let events = supervisor.events()?;
    let stops = events
        .iter()
        .filter(|row| row["event"] == "supervisor_stopped")
        .collect::<Vec<_>>();
    ensure!(
        stops.len() == 2
            && stops.iter().all(|row| row["reason"] == "child_exit"
                && row["exit_code"] == 7
                && row["forced"] == false),
        "unexpected restart accounting: {events:?}"
    );
    supervisor.reaped()
}

#[test]
fn second_signal_escalates_without_waiting_for_the_remaining_grace() -> Result<()> {
    let directory = tempfile::tempdir()?;
    let mut supervisor = Supervisor::start(directory.path(), "second-signal", 0)?;
    supervisor.wait_event("supervisor_started", 0)?;
    let pid = supervisor
        .events()?
        .iter()
        .find(|row| row["event"] == "supervisor_started")
        .and_then(|row| row["pid"].as_u64())
        .context("missing signal fixture PID")?;
    let deadline = Instant::now() + Duration::from_secs(3);
    while !directory
        .path()
        .join(format!("signal-ready-{pid}"))
        .exists()
    {
        ensure!(
            Instant::now() < deadline,
            "signal fixture did not initialize"
        );
        std::thread::sleep(Duration::from_millis(10));
    }
    supervisor.terminate()?;
    supervisor.wait_event("supervisor_stopping", 0)?;
    let started = Instant::now();
    supervisor.terminate()?;
    ensure!(!supervisor.wait()?.success());
    ensure!(
        started.elapsed() < Duration::from_secs(4),
        "second signal waited for five-second grace"
    );
    let events = supervisor.events()?;
    ensure!(
        events.iter().any(|row| row["event"] == "supervisor_stopped"
            && row["reason"] == "signal"
            && row["forced"] == true
            && row["exit_signal"] == libc::SIGKILL),
        "second signal did not force termination: {events:?}"
    );
    supervisor.reaped()
}

#[test]
fn invalid_policy_and_credentials_in_probe_urls_are_refused_before_spawn() -> Result<()> {
    for arguments in [
        vec![
            "--health-url",
            "http://127.0.0.1/health",
            "--startup-ms",
            "0",
        ],
        vec![
            "--health-url",
            "http://127.0.0.1/health",
            "--max-restarts",
            "1001",
        ],
        vec![
            "--health-url",
            "http://user:PRIVATE-PROBE-PASSWORD@127.0.0.1/health",
        ],
        vec!["--health-url", "ftp://127.0.0.1/health"],
    ] {
        let result = Command::new(env!("CARGO_BIN_EXE_cellar"))
            .arg("supervise")
            .args(arguments)
            .args(["--", "/unreachable-supervise-fixture"])
            .output()?;
        ensure!(!result.status.success() && result.stdout.is_empty());
        let error = String::from_utf8(result.stderr)?;
        ensure!(
            !error.contains("PRIVATE-PROBE-PASSWORD")
                && !error.contains("start supervised process"),
            "invalid policy reached spawn or leaked URL credentials"
        );
    }
    Ok(())
}

#[test]
fn health_failure_restarts_only_after_join_and_resets_consecutive_failures() -> Result<()> {
    let directory = tempfile::tempdir()?;
    let root = directory.path();
    let mut supervisor = Supervisor::start(root, "healthy", 1)?;
    supervisor.wait_event("supervisor_ready", 0)?;
    let before = std::fs::read_to_string(root.join("probes"))?.parse::<u32>()?;
    std::fs::write(root.join("fail-once"), "true")?;
    let deadline = Instant::now() + Duration::from_secs(3);
    while std::fs::read_to_string(root.join("probes"))?.parse::<u32>()? < before + 4 {
        ensure!(
            Instant::now() < deadline,
            "fixture probes stopped: events={:?}; log={}",
            supervisor.events()?,
            std::fs::read_to_string(&supervisor.stderr)?
        );
        std::thread::sleep(Duration::from_millis(10));
    }
    ensure!(
        supervisor
            .events()?
            .iter()
            .filter(|row| row["event"] == "supervisor_started")
            .count()
            == 1
    );
    std::fs::write(root.join("unhealthy"), "true")?;
    supervisor.wait_event("supervisor_ready", 1)?;
    supervisor.terminate()?;
    ensure!(supervisor.wait()?.success());
    let events = supervisor.events()?;
    let stops = events
        .iter()
        .filter(|row| row["event"] == "supervisor_stopped")
        .collect::<Vec<_>>();
    ensure!(
        stops.len() == 2
            && stops[0]["reason"] == "health_failed"
            && stops[1]["reason"] == "signal"
            && stops
                .iter()
                .all(|row| row["forced"] == false && row["exit_code"] == 0)
    );
    for stop in stops {
        ensure!(
            root.join(format!("joined-{}", stop["pid"])).exists(),
            "child was not joined before restart"
        );
    }
    supervisor.reaped()
}
