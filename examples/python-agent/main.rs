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
    let mut environment = vec![("PYDANTIC_AI_NO_BANNER".to_owned(), "1".to_owned())];
    for name in ["CELLAR_AGENT_MODEL", "OPENAI_API_KEY"] {
        if let Ok(value) = std::env::var(name) {
            environment.push((name.to_owned(), value));
        }
    }
    let live = environment
        .iter()
        .any(|(key, value)| key == "CELLAR_AGENT_MODEL" && value != "demo");
    ensure!(
        !live
            || environment
                .iter()
                .any(|(key, value)| key == "OPENAI_API_KEY" && !value.is_empty()),
        "OPENAI_API_KEY is required when CELLAR_AGENT_MODEL selects a live model"
    );
    options.environment = Environment::new(environment)?;
    // SAFETY: main owns CPython initialization; no Python APIs or threads exist
    // before this call. The Rust host starts only after initialization succeeds.
    let result = unsafe {
        PythonRuntime::run(
            options,
            Source::new(include_str!("app.py"), "app.py")
                .events([EventKind::Fetch])
                .cell_events("DurableAgent", [EventKind::Fetch, EventKind::Alarm])
                .require_distribution("annotated-types", "0.8.0")
                .require_distribution("anyio", "4.15.1")
                .require_distribution("certifi", "2026.10.19")
                .require_distribution("charset-normalizer", "3.5.2")
                .require_distribution("genai-prices", "0.1.13")
                .require_distribution("griffelib", "2.4.1")
                .require_distribution("h11", "0.16.0")
                .require_distribution("httpcore2", "2.16.0")
                .require_distribution("httpx2", "2.16.0")
                .require_distribution("idna", "3.20")
                .require_distribution("jiter", "0.17.0")
                .require_distribution("logfire-api", "5.2.0")
                .require_distribution("openai", "3.37.0")
                .require_distribution("opentelemetry-api", "1.45.1")
                .require_distribution("pydantic", "2.14.1")
                .require_distribution("pydantic-ai-slim", "2.60.0")
                .require_distribution("pydantic-core", "2.50.1")
                .require_distribution("pydantic-graph", "2.60.0")
                .require_distribution("regex", "2026.9.29")
                .require_distribution("requests", "2.34.2")
                .require_distribution("sniffio", "1.3.1")
                .require_distribution("tiktoken", "0.14.0")
                .require_distribution("truststore", "0.10.5")
                .require_distribution("typing-extensions", "4.16.0")
                .require_distribution("typing-inspection", "0.4.4")
                .require_distribution("urllib3", "2.8.0"),
            |application, stop| {
                let monitor = stop.clone();
                let mut host = Host::fleet();
                if let Some(inventory) = cellar_python::verified_inventory_sha256() {
                    host = host.runtime_inventory(
                        cellar::protocol::FEATURE_EMBEDDED_PYTHON_V1,
                        inventory,
                    )?;
                }
                host.register("python-agent", APPLICATION_VERSION, move || {
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
