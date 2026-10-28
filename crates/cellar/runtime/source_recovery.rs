// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Framework-only execution for retained sources and provider bindings whose
//! application class is gone.

use crate::application::{
    Application, Cell, CellIdentity, Context, HandlerFuture, Invocation, Output,
};
use crate::engine_api::{Compat, WorkerConfigOptions};
use crate::worker_config::WorkerConfig;
use anyhow::Result;
use std::sync::Arc;

pub(crate) const SCRIPT: &str = "__cellar_source_recovery";

struct Recovery(Option<Arc<crate::application::containers::Supervision>>);

impl Application for Recovery {
    fn container_supervision(&self) -> Option<Arc<crate::application::containers::Supervision>> {
        self.0.clone()
    }

    fn invoke(self: Arc<Self>, _: Context, _: Invocation) -> HandlerFuture<Output> {
        Box::pin(async { Err(unavailable()) })
    }

    fn supports_cell(&self, _: &str) -> bool {
        false
    }

    fn create_cell(&self, _: Context, _: &CellIdentity) -> Result<Arc<dyn Cell>> {
        Err(unavailable())
    }
}

pub(crate) fn unavailable() -> anyhow::Error {
    crate::application::event_stream::DeliveryError::new(crate::application::event_stream::ErrorCode::Unavailable,"source application class is not deployed; only framework recovery and source controls are available").into()
}

pub(crate) fn configuration(
    generation: super::GenerationId,
    node: &str,
    supervision: Option<Arc<crate::application::containers::Supervision>>,
) -> Arc<WorkerConfig> {
    let mut config = WorkerConfig::new(WorkerConfigOptions {
        src: String::new(),
        script_name: SCRIPT.into(),
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
        node: node.into(),
        modules: Vec::new(),
        compat: Compat::default(),
    })
    .with_native_application(Arc::new(Recovery(supervision)));
    config.generation = generation;
    config.source_recovery = true;
    Arc::new(config)
}
