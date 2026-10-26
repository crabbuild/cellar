// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Hold an admitted native driver before the original business handler enters.
use anyhow::{ensure, Context as _, Result};
use cellar::sdk::{rpc, *};
use rpc::prost::Message;
use serde_json::{json, Value};
use std::sync::{Arc, Mutex};
use tokio::sync::oneshot;

use crate::application::Composite;

struct Arm {
    class: String,
    operation: String,
    entered: oneshot::Sender<Value>,
    release: oneshot::Receiver<()>,
}

#[derive(Default)]
pub(super) struct Gate {
    arm: Mutex<Option<Arm>>,
    services: Mutex<Option<Arc<cellar::host_services::HostServices>>>,
}

impl Gate {
    pub fn arm(
        &self,
        class: &str,
        operation: &str,
    ) -> Result<(oneshot::Receiver<Value>, oneshot::Sender<()>)> {
        let (entered, waiting) = oneshot::channel();
        let (release, released) = oneshot::channel();
        let mut arm = self.arm.lock().unwrap();
        ensure!(arm.is_none(), "saturation barrier already armed");
        *arm = Some(Arm {
            class: class.into(),
            operation: operation.into(),
            entered,
            release: released,
        });
        Ok((waiting, release))
    }

    pub fn services(&self) -> Result<Arc<cellar::host_services::HostServices>> {
        self.services
            .lock()
            .unwrap()
            .clone()
            .context("source services were not observed")
    }

    fn take(&self, context: &Context, invocation: &Invocation) -> Result<Option<Arm>> {
        let Invocation::CellRpc { method, args } = invocation else {
            return Ok(None);
        };
        if method != "example.counter.v1.Counter/Increment" {
            return Ok(None);
        }
        let (_, bytes) = rpc::decode_envelope(args, rpc::Limits::default().max_message_bytes)?;
        let request = crate::generated::IncrementRequest::decode(bytes.as_slice())?;
        let identity = context
            .identity()
            .context("held call has no cell identity")?;
        let mut arm = self.arm.lock().unwrap();
        Ok(
            if arm
                .as_ref()
                .is_some_and(|a| a.class == identity.class && a.operation == request.operation_id)
            {
                arm.take()
            } else {
                None
            },
        )
    }
}

pub(super) struct Application {
    pub inner: Arc<Composite>,
    pub gate: Arc<Gate>,
}

impl cellar::sdk::Application for Application {
    fn rpc_catalog(&self) -> Option<&rpc::Catalog> {
        self.inner.rpc_catalog()
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
        Ok(Arc::new(HeldCell {
            inner: self.inner.create_cell(context, identity)?,
            gate: self.gate.clone(),
        }))
    }

    fn invoke(self: Arc<Self>, context: Context, invocation: Invocation) -> HandlerFuture<Output> {
        if context.var("DOMAIN_MARKER") == Some(super::SOURCE) {
            let mut services = self.gate.services.lock().unwrap();
            let current = cellar::asyncrt::services();
            if let Some(original) = services.as_ref() {
                assert!(
                    Arc::ptr_eq(original, &current),
                    "source changed execution domain"
                );
            } else {
                *services = Some(current);
            }
        }
        self.inner.clone().invoke(context, invocation)
    }
}

struct HeldCell {
    inner: Arc<dyn Cell>,
    gate: Arc<Gate>,
}

impl Cell for HeldCell {
    fn invoke(self: Arc<Self>, context: Context, invocation: Invocation) -> HandlerFuture<Output> {
        Box::pin(async move {
            if let Some(arm) = self.gate.take(&context, &invocation)? {
                let identity = context.identity().context("barrier lost cell identity")?;
                let sample = cellar::storage::sync_sample(&identity.scope)?
                    .context("barrier has no storage")?;
                ensure!(
                    !sample.in_transaction && sample.epoch == identity.epoch,
                    "barrier is stale or inside a transaction"
                );
                let count = context
                    .storage()?
                    .get_json("count")?
                    .unwrap_or_else(|| json!("0"));
                arm.entered
                    .send(
                        json!({"node": context.var("DOMAIN_MARKER"), "class": identity.class,
                    "scope":identity.scope,"epoch":identity.epoch,"count":count,
                    "transaction_open":sample.in_transaction}),
                    )
                    .map_err(|_| anyhow::anyhow!("barrier observer dropped"))?;
                arm.release.await.context("barrier coordinator dropped")?;
            }
            // Both the generated provider and its existing audit run unchanged.
            self.inner.clone().invoke(context, invocation).await
        })
    }
}
