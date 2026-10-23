use anyhow::{Context as _, Result};
use cellar::sdk::Host;
use cellar_javascript::{EventKind, NodeOptions, NodeRuntime, Options, Source};

const APPLICATION_VERSION: &str = match option_env!("CELLAR_APPLICATION_VERSION") {
    Some(version) => version,
    None => "v1",
};

fn main() -> Result<()> {
    // Configuration is explicit at the executable boundary, never read by SDK operations.
    let entry = std::env::var_os("CELLAR_NODE_ENTRY")
        .context("set CELLAR_NODE_ENTRY to the absolute application module path")?;
    let mut node = NodeOptions::new(std::path::PathBuf::from(entry));
    node.working_directory = node
        .entry
        .parent()
        .and_then(|path| path.parent())
        .map(|path| path.to_path_buf());
    if let Some(executable) = std::env::var_os("CELLAR_NODE_EXECUTABLE") {
        node.executable = executable.into();
    }
    NodeRuntime::run(
        Options::default(),
        node,
        Source::module().events([EventKind::Fetch]).cell("Counter"),
        |application| {
            let stop = application.clone();
            Host::local(graph())
                .register("node-example", APPLICATION_VERSION, move || {
                    Ok(application.clone())
                })?
                .run_until(async move { stop.stopped().await })
        },
    )
}

fn graph() -> cellar::sdk::DeploymentGraph {
    use cellar::{
        engine_api::{Compat, WorkerConfigOptions},
        fleet::LoadedDeployment,
        sdk::DeploymentGraph,
    };
    DeploymentGraph::single(LoadedDeployment {
        event_streams: Vec::new(),
        native: None,
        options: WorkerConfigOptions {
            stream_bindings: Vec::new(),
            src: String::new(),
            script_name: "node-example".into(),
            do_classes: vec!["Counter".into()],
            bindings: vec![("COUNTERS".into(), "Counter".into())],
            blob_bindings: vec![],
            sql_bindings: vec![],
            kv_bindings: vec![],
            queue_bindings: vec![],
            queue_consumers: vec![],
            workflow_bindings: vec![],
            vars: vec![],
            node: String::new(),
            modules: vec![],
            compat: Compat::default(),
        },
        script_name: "node-example".into(),
        version: APPLICATION_VERSION.into(),
        prefix: "node-example/v1".into(),
        asset_binding: None,
        loader_bindings: vec![],
        assets: None,
        services: vec![],
        crons: vec![],
        cron_revision: None,
        containers: vec![],
        fence_image: None,
        container_images: vec![],
    })
}
