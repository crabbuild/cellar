use super::*;

/// Entry point for the Cellar CLI. Compiled application executables host nodes;
/// this executable builds and manages their artifacts and invokes fleet tools.
pub fn main() -> Result<()> {
    let arguments: Vec<String> = std::env::args().skip(1).collect();
    if arguments.first().is_some_and(|command| {
        matches!(
            command.as_str(),
            "cell"
                | "sql"
                | "kv"
                | "queue"
                | "outbox"
                | "stream"
                | "subscription"
                | "blob"
                | "diagnose"
                | "connect"
                | "credentials"
                | "token"
                | "disconnect"
        )
    }) {
        return crate::daemon::NativeDaemon::fleet().run();
    }
    rustls::crypto::ring::default_provider()
        .install_default()
        .ok();
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()?;
    runtime.block_on(async {
        crate::asyncrt::set_host_handle(tokio::runtime::Handle::current());
        run(arguments).await
    })
}

pub async fn run(arguments: Vec<String>) -> Result<()> {
    let mut arguments = arguments.into_iter();
    let command = arguments.next().unwrap_or_else(|| "help".into());
    if command == "fleet" {
        return super::fleet_format::run(arguments.collect()).await;
    }
    if matches!(command.as_str(), "--version" | "-V") {
        crate::cli_output::Output::new(crate::cli_output::Format::Text)
            .line(format_args!("cellar {}", env!("CARGO_PKG_VERSION")))?;
        return Ok(());
    }
    if command == "dev" {
        return super::dev::run(arguments.collect()).await;
    }
    if matches!(command.as_str(), "help" | "--help" | "-h") {
        crate::cli_output::Output::new(crate::cli_output::Format::Text).line(format_args!(
            "cellar build PROJECT.json --output DIRECTORY\n\
             cellar dev PROJECT.json [--host IP] [--port PORT] [--logs] [--no-watch]\n\
             cellar deploy DIRECTORY --bucket NAME [--endpoint URL] [--region REGION]\n\
             cellar install DIRECTORY --bucket NAME --output EXECUTABLE [--target TRIPLE]\n\n\
             Build compiles Rust without JavaScript-engine dependencies. Deploy publishes\n\
             verified executables, assets and configuration. Install verifies downloaded code and\n\
             writes a new executable path; use your process supervisor to activate it.\n\n\
             cellar cell|sql|kv|queue|outbox|stream|subscription|blob COMMAND [OPTIONS]\n\
             cellar fleet upgrade --operation UUID --reason TEXT --confirm-stopped-writers [OPTIONS]\n\
             cellar diagnose|connect|credentials|token|disconnect [OPTIONS]\n\
             Run a compiled application executable to start a Cellar node."
        ))?;
        return Ok(());
    }
    let input = PathBuf::from(
        arguments
            .next()
            .context("native command requires a project file or package directory")?,
    );
    let mut output = None;
    let mut bucket = None;
    let mut endpoint = None;
    let mut region = std::env::var("AWS_REGION").unwrap_or_else(|_| "us-east-1".into());
    let mut target = current_target().to_string();
    while let Some(flag) = arguments.next() {
        let value = arguments
            .next()
            .with_context(|| format!("{flag} requires a value"))?;
        match flag.as_str() {
            "--output" => output = Some(PathBuf::from(value)),
            "--bucket" => bucket = Some(value),
            "--endpoint" => endpoint = Some(value),
            "--region" => region = value,
            "--target" => target = value,
            _ => bail!("unknown native command option {flag}"),
        }
    }
    if command == "watch-roots" {
        crate::cli_output::Output::new(crate::cli_output::Format::Json).line(format_args!(
            "{}",
            serde_json::to_string(&Project::watch_roots(&input)?)?
        ))?;
        return Ok(());
    }
    if matches!(command.as_str(), "build" | "build-dev") {
        let output = output.context("native build requires --output")?;
        let package = if command == "build-dev" {
            Project::build_dev(&input, &output)?
        } else {
            Project::build(&input, &output)?
        };
        crate::cli_output::Output::new(crate::cli_output::Format::Json).line(format_args!(
            "{}",
            serde_json::json!({
                "script_name": package.primary.script_name, "version": package.primary.version,
            })
        ))?;
        return Ok(());
    }
    let package: Package = serde_json::from_slice(&std::fs::read(input.join("package.json"))?)?;
    package.validate()?;
    let bucket = crate::fleet::bucket_client(
        &bucket.context("native command requires --bucket")?,
        endpoint.as_deref(),
        &region,
    )?;
    match command.as_str() {
        "deploy" => {
            package.publish_staged(&bucket, &input).await?;
        }
        "install" => {
            package
                .install(
                    &bucket,
                    &target,
                    &output.context("native install requires --output")?,
                )
                .await?
        }
        _ => bail!("unknown native command {command}"),
    }
    Ok(())
}
