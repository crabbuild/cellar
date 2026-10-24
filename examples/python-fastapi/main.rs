//! Compiled host: Python source participates in the native executable digest.
use anyhow::{ensure, Result};
use cellar::sdk::Host;
use cellar_python::{Environment, EventKind, Options, PythonRuntime, Source};

const APPLICATION_VERSION: &str = match option_env!("CELLAR_APPLICATION_VERSION") {
    Some(version) => version,
    None => "v1",
};

fn main() -> Result<()> {
    let mut options = Options::default();
    if let Some(paths) = std::env::var_os("CELLAR_PYTHON_SITE_PACKAGES") {
        options.module_paths = std::env::split_paths(&paths).collect();
    }
    if let Some(instance) = std::env::var_os("CELLAR_SUPERVISOR_INSTANCE") {
        let instance = instance
            .into_string()
            .map_err(|_| anyhow::anyhow!("supervisor instance must be UTF-8"))?;
        ensure!(
            instance.len() == 32
                && instance
                    .bytes()
                    .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte)),
            "supervisor instance must be 32 lowercase hexadecimal characters"
        );
        options.environment = Environment::new([("CELLAR_SUPERVISOR_INSTANCE".into(), instance)])?;
    }
    // SAFETY: main owns CPython initialization; no Python APIs or threads exist
    // before this call. The Rust host starts only after initialization succeeds.
    let result = unsafe {
        PythonRuntime::run(
            options,
            Source::new(include_str!("app.py"), "app.py")
                .events([EventKind::Fetch])
                .cell("Order")
                .cell_initial_reads("Order", EventKind::Fetch, ["order"])
                .require_distribution("annotated-doc", "0.0.5")
                .require_distribution("annotated-types", "0.8.0")
                .require_distribution("anyio", "4.15.1")
                .require_distribution("fastapi", "0.157.0")
                .require_distribution("idna", "3.20")
                .require_distribution("opentelemetry-api", "1.45.1")
                .require_distribution("pydantic", "2.14.1")
                .require_distribution("pydantic-core", "2.50.1")
                .require_distribution("starlette", "1.9.0")
                .require_distribution("typing-extensions", "4.16.0")
                .require_distribution("typing-inspection", "0.4.4"),
            |application, stop| {
                let monitor = stop.clone();
                let mut host = Host::fleet();
                if let Some(inventory) = cellar_python::verified_inventory_sha256() {
                    host = host.runtime_inventory(
                        cellar::protocol::FEATURE_EMBEDDED_PYTHON_V1,
                        inventory,
                    )?;
                }
                host.register("python-fastapi", APPLICATION_VERSION, move || {
                    Ok(application.clone())
                })?
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
