// Copyright 2026 Deno Land Inc. Apache-2.0 license.
use cellar::{
    engine_api::{Compat, RuntimeOptions, WorkerConfigOptions},
    fleet::LoadedDeployment,
    generation::{DeploymentGraph, GenerationOptions},
};
pub fn graph(script: &str, source: &str, classes: Vec<String>) -> DeploymentGraph {
    DeploymentGraph::single(LoadedDeployment {
        native: None,
        options: WorkerConfigOptions {
            src: source.into(),
            script_name: script.into(),
            do_classes: classes,
            bindings: Vec::new(),
            blob_bindings: Vec::new(),
            sql_bindings: Vec::new(),
            kv_bindings: Vec::new(),
            queue_bindings: Vec::new(),
            queue_consumers: Vec::new(),
            workflow_bindings: Vec::new(),
            vars: Vec::new(),
            node: "test".into(),
            modules: Vec::new(),
            compat: Compat::default(),
        },
        script_name: script.into(),
        version: "test".into(),
        prefix: "test/".into(),
        asset_binding: None,
        loader_bindings: Vec::new(),
        assets: None,
        services: Vec::new(),
        crons: Vec::new(),
        containers: Vec::new(),
        fence_image: None,
        container_images: Vec::new(),
    })
}
pub fn generation_options() -> GenerationOptions {
    GenerationOptions {
        node: "test".into(),
        region: "test".into(),
    }
}
pub fn runtime_options(path: std::path::PathBuf) -> RuntimeOptions {
    RuntimeOptions {
        data_dir: path,
        replication: None,
        wake: None,
        alarm_observer: std::sync::Arc::new(|_, _| {}),
        node: "test".into(),
        region: "test".into(),
        bucket: None,
    }
}
