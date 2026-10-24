// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Standalone HTTP benchmark host; configuration is resolved at this CLI boundary.
use anyhow::{Context as _, Result};
use cellar::sdk::Host;
use cellar_javascript::{EventKind, JavaScriptRuntime, NodeOptions, NodeRuntime, Options, Source};

const APPLICATION_VERSION: &str = "v1";

// Bootstrap reads the benchmark input before entering the execution domain.
#[allow(clippy::disallowed_methods)]
fn main() -> Result<()> {
    let backend = std::env::var("CELLAR_PERF_BACKEND")?;
    let entry = std::path::PathBuf::from(
        std::env::var_os("CELLAR_PERF_ENTRY").context("set CELLAR_PERF_ENTRY")?,
    );
    let serve = |application: std::sync::Arc<cellar_javascript::JavaScriptApplication>| {
        let stop = application.clone();
        Host::local(graph())
            .register("javascript-perf", APPLICATION_VERSION, move || {
                Ok(application.clone())
            })?
            .run_until(async move { stop.stopped().await })
    };
    match backend.as_str() {
        "v8" => JavaScriptRuntime::run(
            Options::default(),
            Source::new(std::fs::read_to_string(entry)?, "perf.ts")
                .events([EventKind::Fetch])
                .cell("Counter"),
            serve,
        ),
        "node" => {
            let mut node = NodeOptions::new(entry);
            node.executable = std::env::var_os("CELLAR_NODE_EXECUTABLE")
                .context("set CELLAR_NODE_EXECUTABLE")?
                .into();
            NodeRuntime::run(
                Options::default(),
                node,
                Source::module().events([EventKind::Fetch]).cell("Counter"),
                serve,
            )
        }
        _ => anyhow::bail!("CELLAR_PERF_BACKEND must be v8 or node"),
    }
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
            script_name: "javascript-perf".into(),
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
        script_name: "javascript-perf".into(),
        version: APPLICATION_VERSION.into(),
        prefix: "javascript-perf/v1".into(),
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
