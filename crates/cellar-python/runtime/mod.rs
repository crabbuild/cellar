//! The executable bootstrap owns Python and the joined Rust host thread.
// Dependency filesystem inspection happens before any node execution domain.
#![allow(clippy::disallowed_methods)]

use crate::application::{PythonApplication, Source};
use anyhow::{ensure, Context as _, Result};
use pyo3::{prelude::*, types::PyModule};
use std::{
    ffi::CString,
    sync::{
        atomic::{AtomicBool, AtomicU64, Ordering},
        mpsc, Arc, Mutex,
    },
    thread,
    time::Duration,
};
use tokio::sync::{mpsc as async_mpsc, watch, Semaphore};

mod bridge;
mod budget;
pub use budget::BridgeReport;
pub(crate) use budget::{Budget, Reservation};
mod environment;
mod initialization;
mod inventory;
pub(crate) use bridge::{Event, InvocationBridge, Life, Start};
pub use environment::Environment;
pub use initialization::Interpreter;
pub use inventory::{is_verified_build, verified_inventory_sha256};

static INITIALIZED: AtomicBool = AtomicBool::new(false);
const FACADE: &str = include_str!("../python/cellar.py");
const RUNNER: &str = include_str!("../python/runner.py");

/// Finite bridge and body budgets, captured before starting the node.
#[derive(Clone, Debug)]
pub struct Options {
    pub interpreter: Interpreter,
    pub environment: Environment,
    pub module_paths: Vec<std::path::PathBuf>,
    pub max_invocations: usize,
    pub max_body_bytes: usize,
    pub max_value_bytes: usize,
    pub max_bridge_bytes: usize,
    pub max_operations: usize,
    pub max_batch_operations: usize,
    pub max_tasks: usize,
    pub max_objects: usize,
    pub shutdown_timeout: Duration,
    pub poll_interval: Duration,
}

impl Default for Options {
    fn default() -> Self {
        Self {
            interpreter: Interpreter::default(),
            environment: Environment::default(),
            module_paths: initialization::bound_module_paths(),
            max_invocations: 128,
            max_body_bytes: 1024 * 1024,
            max_value_bytes: 2 * 1024 * 1024,
            max_bridge_bytes: 32 * 1024 * 1024,
            max_operations: 1024,
            max_batch_operations: 128,
            max_tasks: 1024,
            max_objects: 4096,
            shutdown_timeout: Duration::from_secs(5),
            poll_interval: Duration::from_millis(1),
        }
    }
}

impl Options {
    fn validate(&self) -> Result<()> {
        self.interpreter.validate(&self.module_paths)?;
        ensure!(
            (1..=4096).contains(&self.max_invocations),
            "max_invocations must be 1..4096"
        );
        ensure!(
            self.max_body_bytes > 0
                && self.max_value_bytes > 0
                && self.max_bridge_bytes >= self.max_value_bytes,
            "byte budgets must be positive"
        );
        ensure!(
            self.max_operations > 0 && self.max_batch_operations > 0,
            "operation budgets must be positive"
        );
        ensure!(
            self.max_tasks > self.max_invocations && self.max_objects > 0,
            "Python task/object budgets are too small"
        );
        ensure!(
            !self.shutdown_timeout.is_zero() && !self.poll_interval.is_zero(),
            "runtime durations must be positive"
        );
        Ok(())
    }
}

/// Measurements from the managed interpreter loop. CPU excludes executor/native
/// worker threads; RSS covers the whole process, not an isolated Python heap.
#[derive(Clone, Debug, Default, serde::Deserialize, serde::Serialize)]
pub struct InterpreterReport {
    pub thread_cpu_ns: u64,
    pub wall_ns: u64,
    pub process_peak_rss_bytes: u64,
    pub allocated_blocks_initial: usize,
    pub allocated_blocks_final: usize,
    pub handlers_started: usize,
    pub handlers_completed: usize,
    pub handlers_failed: usize,
    pub handlers_cancelled: usize,
    /// Sum and maximum include host-await time; concurrent handlers overlap.
    pub handler_wall_ns_sum: u64,
    pub handler_wall_ns_max: u64,
}

/// Python work remaining after the Rust host has stopped.
#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
pub struct Report {
    pub pending_tasks: usize,
    pub pending_objects: usize,
    pub pending_threads: usize,
    pub objects_created: usize,
    pub objects_retired: usize,
    pub peak_tasks: usize,
    pub budget_exhaustions: usize,
    pub runtime_failed: bool,
    #[serde(default)]
    pub unraisable_errors: usize,
    #[serde(default)]
    pub thread_errors: usize,
    #[serde(default)]
    pub interpreter: InterpreterReport,
    #[serde(default)]
    pub host_failed: bool,
    #[serde(default)]
    pub native_outcomes: Vec<serde_json::Value>,
    #[serde(default)]
    pub bridge: BridgeReport,
}

impl Report {
    pub fn complete(&self) -> bool {
        self.pending_tasks == 0
            && self.pending_objects == 0
            && self.pending_threads == 0
            && self.objects_created == self.objects_retired
            && !self.runtime_failed
            && self.unraisable_errors == 0
            && self.thread_errors == 0
            && !self.host_failed
            && self.bridge.queued_bytes == 0
            && self.bridge.pending_reservations == 0
            && self
                .native_outcomes
                .iter()
                .all(|outcome| outcome["complete"] == true)
    }
}

/// A joined runtime failure retains its structured cleanup evidence.
#[derive(Debug)]
pub struct Incomplete {
    pub report: Report,
    pub host_error: Option<anyhow::Error>,
}

impl std::fmt::Display for Incomplete {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Python shutdown incomplete: {} tasks, {} objects, {} threads; runtime_failed={}, host_failed={}, incomplete_native_outcomes={}, queued_bridge_bytes={}, bridge_reservations={}",
            self.report.pending_tasks, self.report.pending_objects, self.report.pending_threads, self.report.runtime_failed, self.report.host_failed, self.report.native_outcomes.iter().filter(|outcome| outcome["complete"] != true).count(), self.report.bridge.queued_bytes, self.report.bridge.pending_reservations)
    }
}

impl std::error::Error for Incomplete {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        self.host_error.as_ref().map(|error| error.as_ref())
    }
}

pub(crate) struct Runtime {
    starts: mpsc::SyncSender<Start>,
    replies: mpsc::SyncSender<Reply>,
    pub budget: Arc<Budget>,
    pub options: Options,
    pub permits: Arc<Semaphore>,
    next: AtomicU64,
    pub closed: AtomicBool,
    pub stopped: watch::Sender<bool>,
}

impl Runtime {
    pub fn id(&self) -> u64 {
        self.next.fetch_add(1, Ordering::Relaxed)
    }

    pub fn start(&self, start: Start) -> Result<()> {
        ensure!(
            !self.closed.load(Ordering::Acquire),
            "Python runtime is closed"
        );
        self.starts
            .try_send(start)
            .context("Python invocation admission is full or closed")
    }

    pub fn reply(&self, invocation: u64, operation: u64, value: String) -> Result<()> {
        self.replies
            .try_send(Reply {
                invocation,
                operation,
                reservation: self.budget.reserve(value.len())?,
                value,
            })
            .context("Python result channel is full or closed")
    }
}

struct Reply {
    invocation: u64,
    operation: u64,
    value: String,
    reservation: Reservation,
}

#[pyclass]
struct Inbox {
    starts: Mutex<mpsc::Receiver<Start>>,
    replies: Mutex<mpsc::Receiver<Reply>>,
    done: Arc<AtomicBool>,
    runtime: Arc<Runtime>,
}

#[pymethods]
impl Inbox {
    fn stop(&self) {
        self.runtime.closed.store(true, Ordering::Release);
        self.runtime.stopped.send_replace(true);
        self.runtime.permits.close();
    }

    fn take(&self, py: Python<'_>) -> PyResult<Option<(u64, String, Py<InvocationBridge>)>> {
        let next = self
            .starts
            .lock()
            .map_err(|_| pyo3::exceptions::PyRuntimeError::new_err("start channel poisoned"))?
            .try_recv()
            .ok();
        next.map(|mut start| {
            let _reservation = start.reservation;
            start.bridge.event_loop = Some(
                py.import("asyncio")?
                    .call_method0("get_running_loop")?
                    .unbind(),
            );
            Ok((start.id, start.payload, Py::new(py, start.bridge)?))
        })
        .transpose()
    }

    fn replies(&self) -> PyResult<Vec<(u64, u64, String)>> {
        Ok(self
            .replies
            .lock()
            .map_err(|_| pyo3::exceptions::PyRuntimeError::new_err("result channel poisoned"))?
            .try_iter()
            .map(|reply| {
                let _reservation = reply.reservation;
                (reply.invocation, reply.operation, reply.value)
            })
            .collect())
    }

    fn done(&self) -> bool {
        self.done.load(Ordering::Acquire)
    }
}

/// Cooperative stop signal. Hosts must stop admission and join when signalled.
#[derive(Clone)]
pub struct Stop {
    receiver: watch::Receiver<bool>,
    reports: Arc<Mutex<Vec<serde_json::Value>>>,
}

impl Stop {
    /// Retain a native node's real outcome in the final combined report.
    /// Call for every node owned by the supervisor, after its join completes.
    pub fn record_node(&self, outcome: cellar::node_lifecycle::NodeOutcome) {
        use cellar::node_lifecycle::{NodeOutcome, ShutdownMode};
        use serde_json::json;
        let value = match outcome {
            NodeOutcome::Stopped(r) => json!({
                "outcome":"stopped", "mode": match r.mode { ShutdownMode::Preserve => "preserve", ShutdownMode::Handoff => "handoff" },
                "ingress_drained":r.ingress_drained, "application_work_drained":r.application_work_drained,
                "pending_application_events":r.pending_application_events, "cells_drained":r.cells_drained,
                "pending_dispatches":r.pending_dispatches, "log_quiesced":r.log_quiesced,
                "local_durability_closed":r.local_durability_closed, "clean_reload_prepared":r.clean_reload_prepared,
                "drain_timed_out":r.drain_timed_out, "deadline_elapsed":r.deadline_elapsed,
                "complete":r.ingress_drained && r.application_work_drained && r.pending_application_events == 0
                    && r.cells_drained && r.pending_dispatches == 0 && r.log_quiesced && r.local_durability_closed
                    && !r.drain_timed_out && !r.deadline_elapsed,
            }),
            NodeOutcome::Cancelled => json!({"outcome":"cancelled", "complete":false}),
            NodeOutcome::Fenced { code, reason } => {
                json!({"outcome":"fenced","code":code,"reason":format!("{reason:?}"),"complete":false})
            }
            NodeOutcome::CoreFailed { .. } => json!({"outcome":"core_failed","complete":false}),
            NodeOutcome::ReplicationFailed { .. } => {
                json!({"outcome":"replication_failed","complete":false})
            }
        };
        self.reports
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .push(value);
    }

    pub async fn cancelled(mut self) {
        if !*self.receiver.borrow() {
            let _ = self.receiver.changed().await;
        }
    }
}

/// Run one Python application and a synchronous Rust host supervisor.
///
/// Call on the process main thread. The host closure runs on a joined thread
/// and may own Tokio runtimes or call `Host::run_until`. Python is initialized once;
/// interpreter reinitialization and concurrent application generations are refused.
pub struct PythonRuntime;

impl PythonRuntime {
    /// # Safety
    /// This bootstrap owns process-wide CPython initialization. No other thread
    /// may initialize/finalize Python concurrently, and no Python API may run
    /// until initialization completes. Start it before spawning Python users.
    /// The process main-thread requirement is checked before invoking CPython.
    pub unsafe fn run<F>(options: Options, source: Source, host: F) -> Result<Report>
    where
        F: FnOnce(Arc<PythonApplication>, Stop) -> Result<()> + Send + 'static,
    {
        options.validate()?;
        source.validate()?;
        initialization::ensure_main_thread()?;
        inventory::verify(&options, &source)?;
        ensure!(
            !INITIALIZED.swap(true, Ordering::AcqRel),
            "embedded Python may be started only once per process"
        );
        // SAFETY: exclusive process bootstrap is required by this function's
        // public contract; initialization validates the actual main thread.
        unsafe {
            options.interpreter.initialize(&options.module_paths)?;
        }
        let (starts_tx, starts_rx) = mpsc::sync_channel(options.max_invocations);
        let (replies_tx, replies_rx) = mpsc::sync_channel(options.max_invocations * 2);
        let done = Arc::new(AtomicBool::new(false));
        let (stopped_tx, stopped_rx) = watch::channel(false);
        let reports = Arc::new(Mutex::new(Vec::new()));
        let native_reports = reports.clone();
        let runtime = Arc::new(Runtime {
            starts: starts_tx,
            replies: replies_tx,
            permits: Arc::new(Semaphore::new(options.max_invocations)),
            stopped: stopped_tx,
            budget: Budget::new(options.max_bridge_bytes),
            options: options.clone(),
            next: AtomicU64::new(1),
            closed: AtomicBool::new(false),
        });
        Python::attach(|py| -> Result<Report> {
            let info = py.import("sys")?.getattr("version_info")?;
            let version: (u8, u8) = (info.get_item(0)?.extract()?, info.get_item(1)?.extract()?);
            ensure!(
                version == (3, 14),
                "embedded Python requires CPython 3.14, got {}.{}",
                version.0,
                version.1
            );
            // sysconfig can execute an environment-selected config-data file
            // even in isolated mode. ABI facts come from builtin sys instead.
            let system = py.import("sys")?;
            let abi_flags: String = system.getattr("abiflags")?.extract()?;
            ensure!(
                !abi_flags.contains('t'),
                "free-threaded CPython is not supported"
            );
            let multiarch: String = system
                .getattr("implementation")?
                .getattr("_multiarch")?
                .extract()?;
            let soabi = format!(
                "cpython-{}{}{}-{multiarch}",
                version.0, version.1, abi_flags
            );
            ensure!(
                soabi == options.interpreter.soabi,
                "CPython SOABI mismatch: expected {}, got {soabi}",
                options.interpreter.soabi
            );
            options.environment.install(py)?;
            for (name, expected) in &source.distributions {
                let actual: String = py
                    .import("importlib.metadata")?
                    .call_method1("version", (name.as_str(),))?
                    .extract()?;
                ensure!(
                    actual == *expected,
                    "Python distribution {name}: expected {expected}, got {actual}"
                );
            }
            let modules = py.import("sys")?.getattr("modules")?;
            let facade = PyModule::from_code(py, &CString::new(FACADE)?, c"cellar.py", c"cellar")?;
            modules.set_item("cellar", &facade)?;
            let runner = PyModule::from_code(
                py,
                &CString::new(RUNNER)?,
                c"cellar_runner.py",
                c"_cellar_runner",
            )?;
            runner.getattr("install_error_hooks")?.call0()?;
            let app = PyModule::from_code(
                py,
                &CString::new(source.code.as_str())?,
                &CString::new(source.filename.as_str())?,
                c"_cellar_application",
            )?;
            modules.set_item("_cellar_application", &app)?;
            for class in &source.classes {
                ensure!(
                    app.getattr(class.as_str())?.is_callable(),
                    "Python class {class} is not callable"
                );
            }
            runner
                .getattr("validate")?
                .call1((app.clone(), serde_json::to_string(&source.descriptor())?))?;
            let inbox = Py::new(
                py,
                Inbox {
                    starts: Mutex::new(starts_rx),
                    replies: Mutex::new(replies_rx),
                    done: done.clone(),
                    runtime: runtime.clone(),
                },
            )?;
            let application = Arc::new(PythonApplication::new(runtime.clone(), source));
            let run = runner.getattr("run")?;
            let finished = done.clone();
            let worker = thread::Builder::new()
                .name("cellar-python-host".into())
                .spawn(move || {
                    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                        host(
                            application,
                            Stop {
                                receiver: stopped_rx,
                                reports,
                            },
                        )
                    }))
                    .unwrap_or_else(|_| {
                        Err(anyhow::anyhow!("Rust Python host supervisor panicked"))
                    });
                    finished.store(true, Ordering::Release);
                    result
                })?;
            let outcome = run.call1((
                app,
                inbox.clone_ref(py),
                options.poll_interval.as_secs_f64(),
                options.shutdown_timeout.as_secs_f64(),
                options.max_tasks,
                options.max_objects,
            ));
            runtime.closed.store(true, Ordering::Release);
            runtime.stopped.send_replace(true);
            runtime.permits.close();
            // The Python runner has stopped accepting work. Never join while
            // attached: a final Rust cleanup path could otherwise wait for the GIL.
            let host_result = py
                .detach(move || worker.join())
                .map_err(|_| anyhow::anyhow!("Python host thread join failed"))?;
            let encoded: String = py
                .import("json")?
                .call_method1("dumps", (outcome?,))?
                .extract()?;
            let mut report: Report = serde_json::from_str(&encoded)?;
            // The host is joined; no sender can authorize more work. Drain
            // unused channel entries and their reservations before observing.
            let bound = inbox.borrow(py);
            bound
                .starts
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .try_iter()
                .for_each(drop);
            bound
                .replies
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .try_iter()
                .for_each(drop);
            report.bridge = runtime.budget.report();
            report.host_failed = host_result.is_err();
            report.native_outcomes = native_reports
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .clone();
            if !report.complete() {
                return Err(Incomplete {
                    report,
                    host_error: host_result.err(),
                }
                .into());
            }
            Ok(report)
        })
    }
}

pub(crate) fn channel() -> (async_mpsc::Sender<Event>, async_mpsc::Receiver<Event>) {
    async_mpsc::channel(32)
}
