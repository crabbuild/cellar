// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

use super::trace::{Role, Trace};
use crate::application::Composite;
use anyhow::Result;
use cellar::sdk::*;
use cellar_javascript::JavaScriptApplication;
use std::sync::{atomic::Ordering, Arc};

pub(super) struct Fixture {
    pub base: Arc<Composite>,
    pub worker: Arc<JavaScriptApplication>,
    pub trace: Arc<Trace>,
}

struct Observed {
    inner: Arc<dyn Cell>,
    trace: Arc<Trace>,
    control: bool,
    provider: bool,
}

impl Cell for Observed {
    fn invoke(self: Arc<Self>, context: Context, event: Invocation) -> HandlerFuture<Output> {
        Box::pin(async move {
            let method = match &event {
                Invocation::CellRpc { method, .. } => method.as_str(),
                _ => "",
            };
            let _frame = if self.provider
                && method.ends_with("/Increment")
                && self.trace.armed.load(Ordering::Acquire)
            {
                Some(self.trace.enter(Role::Provider, &context)?)
            } else {
                None
            };
            if self.control
                && method.ends_with("/GetCount")
                && self.trace.armed.load(Ordering::Acquire)
            {
                self.trace.hold(&context).await?;
            }
            let observing = self.provider && self.trace.armed.load(Ordering::Acquire);
            let mut inner = self.inner.clone().invoke(context.clone(), event);
            std::future::poll_fn(|task| {
                let outcome = std::future::Future::poll(inner.as_mut(), task);
                if observing && self.trace.armed.load(Ordering::Acquire) && outcome.is_pending() {
                    if let Err(error) = self.trace.observe_provider(&context) {
                        // Observation must never complete or cancel user work.
                        // The coordinator fails its audit independently.
                        self.trace.observation_failed(error);
                    }
                }
                outcome
            })
            .await
        })
    }
}

impl Application for Fixture {
    fn rpc_catalog(&self) -> Option<&rpc::Catalog> {
        self.base.rpc_catalog()
    }

    fn supports_cell(&self, class: &str) -> bool {
        self.base.supports_cell(class)
    }

    fn validate_configuration(
        &self,
        options: &cellar::engine_api::WorkerConfigOptions,
    ) -> Result<()> {
        self.base.validate_configuration(options)?;
        let mut selected = options.clone();
        selected.do_classes.retain(|class| class == "NodeCounter");
        self.worker.validate_configuration(&selected)
    }

    fn create_cell(&self, context: Context, identity: &CellIdentity) -> Result<Arc<dyn Cell>> {
        let provider = identity.class == "NodeCounter";
        let control =
            identity.class == "RustCounter" && identity.name.as_deref() == Some("worker-control");
        let inner = if provider {
            self.worker.create_cell(context, identity)?
        } else {
            self.base.create_cell(context, identity)?
        };
        Ok(Arc::new(Observed {
            inner,
            trace: self.trace.clone(),
            control,
            provider,
        }))
    }

    fn invoke(self: Arc<Self>, context: Context, event: Invocation) -> HandlerFuture<Output> {
        let event = match event {
            Invocation::Fetch(mut request)
                if request
                    .url
                    .split('?')
                    .next()
                    .is_some_and(|path| path.ends_with("/worker-pid")) =>
            {
                request.url = "http://worker/node/process".into();
                return self
                    .worker
                    .clone()
                    .invoke(context, Invocation::Fetch(request));
            }
            event => event,
        };
        if self.trace.armed.load(Ordering::Acquire)
            && matches!(&event, Invocation::Fetch(request) if request.url.contains("/increment?"))
        {
            if let Err(error) = self.trace.caller(&context) {
                return Box::pin(async { Err(error) });
            }
        }
        self.base.clone().invoke(context, event)
    }
}
