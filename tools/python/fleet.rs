//! Python application on the same S3 fleet host as the native counter benchmark.
#![allow(clippy::disallowed_methods)]
use anyhow::{ensure, Result};
use cellar_python::{EventKind, Incomplete, Options, PythonRuntime, Source};
use serde_json::json;
use std::time::Duration;

#[path = "../perf/deployment.rs"]
mod deployment;
#[path = "../perf/embedded.rs"]
#[allow(dead_code)]
mod embedded;

#[global_allocator]
static ALLOC: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

fn emit(report: &cellar_python::Report) -> Result<()> {
    cellar::cli_output::Output::new(cellar::cli_output::Format::Text).line(format_args!(
        "{}",
        json!({"event":"embedded_python_stopped", "complete":report.complete(), "report":report})
    ))
}

fn main() -> Result<()> {
    let node = std::env::var("CELLAR_PERF_NODE")?;
    let requests = embedded::setting("CELLAR_PERF_MAX_REQUESTS", 512usize)?;
    ensure!(
        (1..=2048).contains(&requests),
        "Python fleet max requests must be 1..2048"
    );
    let mut options = Options::default();
    // Root and cell handlers share the Python admission budget. Keep the bound
    // explicit and report it; the adapter's normal defaults remain unchanged.
    options.max_invocations = requests * 2;
    options.max_tasks = options.max_tasks.max(options.max_invocations * 2 + 1);
    let shutdown_ms = embedded::setting("CELLAR_PERF_SHUTDOWN_MS", 30_000u64)?;
    ensure!(shutdown_ms > 0, "shutdown budget must be positive");
    options.shutdown_timeout = Duration::from_millis(shutdown_ms);
    let limits = json!({"max_requests":requests, "max_invocations":options.max_invocations,
        "max_tasks":options.max_tasks, "max_objects":options.max_objects,
        "max_bridge_bytes":options.max_bridge_bytes, "shutdown_timeout_ms":shutdown_ms});
    // JSON string/integer literals are also valid Python literals. Configuration
    // is captured at this executable boundary, never inside Cellar's library.
    let source = format!(
        "NODE = {}\nLIMITS = {}\n{}",
        serde_json::to_string(&node)?,
        limits,
        include_str!("fleet_app.py")
    );
    // SAFETY: main owns CPython initialization before creating any host workers
    // or foreign Python callers. PythonRuntime joins the owned supervisor.
    let result = unsafe {
        PythonRuntime::run(
            options,
            Source::new(source, "fleet_app.py")
                .events([EventKind::Fetch])
                .cell("Entity"),
            |application, monitor| {
                let cancellation = monitor.clone();
                embedded::run_observed(
                    deployment::graph(),
                    move |_| Ok(application.clone()),
                    cancellation.cancelled(),
                    move |outcome| monitor.record_node(outcome),
                )
            },
        )
    };
    match result {
        Ok(report) => {
            emit(&report)?;
            ensure!(
                report.complete() && report.native_outcomes.len() == 1,
                "Python fleet host did not completely join"
            );
            Ok(())
        }
        Err(error) => {
            if let Some(incomplete) = error.downcast_ref::<Incomplete>() {
                emit(&incomplete.report)?;
            }
            Err(error)
        }
    }
}
