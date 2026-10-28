// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

use anyhow::{Context as _, Result};
use cellar::sdk::Host;
use cellar_javascript::{EventKind, NodeOptions, NodeRuntime, Options, Source};

const APPLICATION_VERSION: &str = match option_env!("CELLAR_APPLICATION_VERSION") {
    Some(version) => version,
    None => "v1",
};

fn main() -> Result<()> {
    let (node, inventory) = match NodeOptions::packaged()? {
        Some((node, sha)) => (node, Some(sha)),
        None => {
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
            (node, None)
        }
    };
    NodeRuntime::run(
        // Each turn can have ingress, cell, alarm and directory invocations in
        // flight together. Keep the 16-turn example bounded with room for that
        // nested call graph rather than exhausting the default 64 slots.
        Options {
            max_invocations: 128,
            ..Options::default()
        },
        node,
        Source::module()
            .events([EventKind::Fetch])
            .cell_events("DurableAgent", [EventKind::Fetch, EventKind::Alarm])
            .filesystem("DurableAgent", cellar::sdk::filesystem::Policy::default())?
            .cell_events("ConversationDirectory", [EventKind::Fetch]),
        |application| {
            let stop = application.clone();
            let host = if let Some(sha) = inventory {
                Host::fleet().runtime_inventory(cellar::protocol::FEATURE_NODE_V1, sha)?
            } else {
                Host::local(graph())
            };
            host.register("vercel-agent-example", APPLICATION_VERSION, move || {
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
            script_name: "vercel-agent-example".into(),
            do_classes: vec!["DurableAgent".into(), "ConversationDirectory".into()],
            bindings: vec![
                ("AGENTS".into(), "DurableAgent".into()),
                ("CONVERSATIONS".into(), "ConversationDirectory".into()),
            ],
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
        script_name: "vercel-agent-example".into(),
        version: APPLICATION_VERSION.into(),
        prefix: "vercel-agent-example/v1".into(),
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
