// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! The receiving host declares stricter limits than the generated callers.
use anyhow::Result;
use cellar::sdk::{rpc, *};
use std::sync::Arc;

use crate::{application::Composite, fleet};

pub(super) const BYTE_LIMIT: usize = 128;
pub(super) const ELEMENT_LIMIT: usize = 4;

pub(super) struct Receiver {
    inner: Arc<Composite>,
    catalog: rpc::Catalog,
}

impl Receiver {
    pub fn new(inner: Arc<Composite>) -> Result<Self> {
        let contract = Arc::new(rpc::Contract::from_descriptor(
            inner.contract.descriptor(),
            rpc::Limits {
                max_message_bytes: BYTE_LIMIT,
                max_elements: ELEMENT_LIMIT,
                ..Default::default()
            },
        )?);
        let mut catalog = rpc::Catalog::new();
        for (_, _, class) in fleet::CLASSES {
            catalog.register(class, "example.counter.v1.Counter", contract.clone())?;
        }
        Ok(Self { inner, catalog })
    }
}

impl Application for Receiver {
    fn rpc_catalog(&self) -> Option<&rpc::Catalog> {
        Some(&self.catalog)
    }

    fn supports_cell(&self, class: &str) -> bool {
        self.inner.supports_cell(class)
    }

    fn validate_configuration(
        &self,
        options: &cellar::engine_api::WorkerConfigOptions,
    ) -> Result<()> {
        self.inner.validate_configuration(options)
    }

    fn create_cell(&self, context: Context, identity: &CellIdentity) -> Result<Arc<dyn Cell>> {
        self.inner.create_cell(context, identity)
    }

    fn invoke(self: Arc<Self>, context: Context, invocation: Invocation) -> HandlerFuture<Output> {
        self.inner.clone().invoke(context, invocation)
    }
}
