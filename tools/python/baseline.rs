//! Disposable native/Python measurement hosts; the caller owns all resources.
#![allow(clippy::disallowed_methods)]
use anyhow::{ensure, Context as _, Result};
use cellar::sdk::{Application, Context};
use serde_json::{json, Value};
use std::{
    path::PathBuf,
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};

#[path = "../perf/deployment.rs"]
mod deployment;
#[path = "baseline_host.rs"]
mod host;
#[path = "baseline_native.rs"]
mod native;

#[global_allocator]
static ALLOC: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

type Observation = Arc<dyn Fn() -> Value + Send + Sync>;

struct Arguments {
    mode: String,
    directory: PathBuf,
    role: String,
    before_runtime: Value,
}

impl Arguments {
    fn parse() -> Result<Self> {
        let args = std::env::args().skip(1).collect::<Vec<_>>();
        ensure!(
            args.len() == 3,
            "usage: python-baseline MODE DIRECTORY primary|recovered"
        );
        ensure!(
            matches!(
                args[0].as_str(),
                "native"
                    | "python"
                    | "python-defaults"
                    | "native-resident"
                    | "python-resident"
                    | "native-direct-resident"
                    | "python-direct-resident"
                    | "native-direct"
                    | "python-direct"
            ),
            "invalid host mode"
        );
        ensure!(
            matches!(args[2].as_str(), "primary" | "recovered"),
            "invalid host role"
        );
        let directory = std::fs::canonicalize(&args[1])?;
        ensure!(directory.is_dir(), "private directory required");
        Ok(Self {
            mode: args[0].clone(),
            directory,
            role: args[2].clone(),
            before_runtime: memory(),
        })
    }

    fn residency(&self) -> bool {
        self.mode.ends_with("-resident")
    }
}

fn memory() -> Value {
    let rss = cellar::memory::resident_bytes();
    json!({
        "rss_bytes":(rss > 0).then_some(rss),
        "jemalloc":cellar::memory::allocator_stats(),
        "libc":cellar::memory::libc_malloc_stats(),
    })
}

fn main() -> Result<()> {
    let arguments = Arguments::parse()?;
    tracing_subscriber::fmt()
        .with_env_filter("cellar::actor=warn,cellar::daemon::embedded=warn")
        .with_ansi(false)
        .with_writer(std::io::stderr)
        .try_init()
        .map_err(|error| anyhow::anyhow!("baseline tracing: {error}"))?;
    let before = arguments.before_runtime.clone();
    if arguments.mode.starts_with("native") {
        ensure!(
            !cfg!(feature = "embed"),
            "build the native reference without embed/libpython"
        );
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(4)
            .enable_all()
            .build()?;
        let report = runtime.block_on(host::serve(
            &arguments,
            native::application()?,
            Arc::new(|| json!({"kind":"native", "bridge":null, "interpreter":null})),
        ))?;
        drop(runtime);
        std::fs::write(
            arguments.directory.join("complete.json"),
            serde_json::to_vec(&json!({
                "complete":true, "native":report, "before_runtime":before, "after_join":memory(), "python":null,
            }))?,
        )?;
        return Ok(());
    }
    python(arguments, before)
}

#[cfg(not(feature = "embed"))]
fn python(_arguments: Arguments, _before: Value) -> Result<()> {
    anyhow::bail!("build Python mode with the embed feature")
}

#[cfg(feature = "embed")]
fn python(arguments: Arguments, before: Value) -> Result<()> {
    use cellar_python::{EventKind, Options, PythonRuntime, Source};
    let directory = arguments.directory.clone();
    let options = Options::default();
    let limits = json!({
        "max_invocations": options.max_invocations, "max_body_bytes": options.max_body_bytes,
        "max_value_bytes": options.max_value_bytes, "max_bridge_bytes": options.max_bridge_bytes,
        "max_operations": options.max_operations, "max_pending_operations": options.max_pending_operations,
        "max_batch_operations": options.max_batch_operations, "max_tasks": options.max_tasks,
        "max_objects": options.max_objects, "shutdown_timeout_ms": options.shutdown_timeout.as_millis(),
        "poll_interval_ns": options.poll_interval.as_nanos(), "telemetry_interval_ms": options.telemetry_interval.as_millis(),
    });
    // SAFETY: this executable initializes once on its main thread, before any
    // node or foreign Python caller exists. The owned supervisor joins below.
    let report = unsafe {
        PythonRuntime::run(
            options,
            Source::new(include_str!("baseline_app.py"), "baseline_app.py")
                .events([EventKind::Fetch])
                .cell("Entity"),
            move |application, monitor| {
                let observed = application.clone();
                let observation: Observation = Arc::new(move || {
                    json!({
                        "kind":"python", "bridge":observed.bridge_report(), "interpreter":observed.interpreter_snapshot(), "limits":limits,
                    })
                });
                let runtime = tokio::runtime::Builder::new_multi_thread()
                    .worker_threads(4)
                    .enable_all()
                    .build()?;
                let (outcome, report) =
                    runtime.block_on(host::drive(&arguments, application, observation))?;
                monitor.record_node(outcome);
                std::fs::write(
                    arguments.directory.join("native-complete.json"),
                    serde_json::to_vec(&report)?,
                )?;
                Ok(())
            },
        )
    }?;
    ensure!(
        report.complete(),
        "Python baseline did not completely join: {report:?}"
    );
    std::fs::write(
        directory.join("complete.json"),
        serde_json::to_vec(&json!({
            "complete":true, "before_runtime":before, "after_join":memory(), "python":report,
            "native":serde_json::from_slice::<Value>(&std::fs::read(directory.join("native-complete.json"))?)?,
        }))?,
    )?;
    Ok(())
}
