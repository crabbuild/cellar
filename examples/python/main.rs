//! Compiled host: Python source participates in the native executable digest.
use anyhow::Result;
use cellar::sdk::Host;
use cellar_python::{EventKind, Options, PythonRuntime, Source};

fn main() -> Result<()> {
    let mut options = Options::default();
    if let Some(paths) = std::env::var_os("CELLAR_PYTHON_SITE_PACKAGES") {
        options.module_paths = std::env::split_paths(&paths).collect();
    }
    // SAFETY: main owns CPython initialization; no Python APIs or threads exist
    // before this call. The Rust host starts only after initialization succeeds.
    let result = unsafe {
        PythonRuntime::run(
            options,
            Source::new(include_str!("app.py"), "app.py")
                .events([EventKind::Fetch, EventKind::Queue, EventKind::Scheduled])
                .cell_events("Counter", [EventKind::Fetch, EventKind::Alarm])
                .require_distribution("pydantic", "2.12.3")
                .require_distribution("pydantic-core", "2.41.4")
                .require_distribution("annotated-types", "0.7.0")
                .require_distribution("typing-extensions", "4.15.0")
                .require_distribution("typing-inspection", "0.4.2")
                .require_distribution("numpy", "2.3.4"),
            |application, stop| {
                let monitor = stop.clone();
                Host::fleet()
                    .register("python-example", "v1", move || Ok(application.clone()))?
                    .run_until_observed(stop.cancelled(), move |outcome| {
                        monitor.record_node(outcome)
                    })
            },
        )
    };
    if matches!(
        std::env::var("CELLAR_SHUTDOWN_REPORT_JSON").as_deref(),
        Ok("1" | "true")
    ) {
        let report = result.as_ref().ok().or_else(|| {
            result
                .as_ref()
                .err()?
                .downcast_ref::<cellar_python::Incomplete>()
                .map(|failure| &failure.report)
        });
        if let Some(report) = report {
            cellar::cli_output::Output::new(cellar::cli_output::Format::Json).line(
                format_args!(
                    "{}",
                    serde_json::json!({"event":"embedded_python_shutdown", "report":report})
                ),
            )?;
        }
    }
    result?;
    Ok(())
}
