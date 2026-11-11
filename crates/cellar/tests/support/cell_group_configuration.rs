// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Standard deployment options for shared native execution qualification.

pub fn options(script: &str) -> cellar::engine_api::WorkerConfigOptions {
    cellar::engine_api::WorkerConfigOptions {
        src: String::new(),
        script_name: script.into(),
        do_classes: Vec::new(),
        bindings: Vec::new(),
        blob_bindings: Vec::new(),
        sql_bindings: Vec::new(),
        kv_bindings: Vec::new(),
        queue_bindings: Vec::new(),
        stream_bindings: Vec::new(),
        queue_consumers: Vec::new(),
        workflow_bindings: Vec::new(),
        vars: Vec::new(),
        node: String::new(),
        modules: Vec::new(),
        compat: Default::default(),
    }
}

pub fn deployment(
    options: cellar::engine_api::WorkerConfigOptions,
) -> cellar::fleet::LoadedDeployment {
    cellar::fleet::LoadedDeployment {
        script_name: options.script_name.clone(),
        options,
        event_streams: Vec::new(),
        native: None,
        version: "test".into(),
        prefix: "configuration-test".into(),
        asset_binding: None,
        loader_bindings: Vec::new(),
        assets: None,
        services: Vec::new(),
        crons: Vec::new(),
        cron_revision: None,
        containers: Vec::new(),
        fence_image: None,
        container_images: Vec::new(),
    }
}
