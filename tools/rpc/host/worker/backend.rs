// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

use crate::application::Composite;
use anyhow::Result;
use cellar_javascript::{
    EventKind, JavaScriptApplication, JavaScriptRuntime, NodeOptions, NodeRuntime, Options, Source,
};
use std::{path::Path, sync::Arc};

/// The provider always owns a different engine from every surviving caller.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum Backend {
    Node,
    V8,
}

impl Backend {
    pub fn label(self) -> &'static str {
        match self {
            Self::Node => "node",
            Self::V8 => "v8",
        }
    }

    pub fn class(self) -> &'static str {
        match self {
            Self::Node => "NodeCounter",
            Self::V8 => "V8Counter",
        }
    }

    /// Returning from either bootstrap joins its owned engine. V8 also joins
    /// its watchdog; Node additionally reaps its owned worker process.
    pub fn run<T>(
        self,
        root: &Path,
        base: &Composite,
        options: Options,
        recovery: bool,
        host: impl FnOnce(Arc<JavaScriptApplication>) -> Result<T>,
    ) -> Result<T> {
        let source = match self {
            Self::Node => Source::module(),
            Self::V8 => {
                let name = if recovery {
                    "app.v8.js"
                } else {
                    "worker.v8.js"
                };
                let path = root.join("tools/rpc/dist").join(name);
                Source::new(std::fs::read_to_string(path)?, name)
            }
        }
        .events([EventKind::Fetch])
        .cell_rpc(
            self.class(),
            "example.counter.v1.Counter",
            base.contract.clone(),
        )?;
        match self {
            Self::Node => {
                let name = if recovery { "node.mjs" } else { "worker.mjs" };
                let entry = root.join("tools/rpc/dist/node").join(name).canonicalize()?;
                NodeRuntime::run(options, NodeOptions::new(entry), source, host)
            }
            Self::V8 => JavaScriptRuntime::run(options, source, host),
        }
    }
}
