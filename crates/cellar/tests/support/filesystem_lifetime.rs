// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Observe the cell's actual native future, independently of its HTTP caller.
use anyhow::Result;
use cellar::sdk::*;
use std::{
    future::Future,
    pin::Pin,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    task::{Context as TaskContext, Poll},
};

#[derive(Default)]
pub(crate) struct Cancellation {
    finished: AtomicBool,
    wake: tokio::sync::Notify,
}

impl Cancellation {
    #[allow(clippy::disallowed_methods)] // Supervisor observation, outside SDK work.
    pub(crate) async fn wait(&self) -> Result<()> {
        tokio::time::timeout(std::time::Duration::from_secs(5), async {
            loop {
                let notified = self.wake.notified();
                if self.finished.load(Ordering::Acquire) {
                    return;
                }
                notified.await;
            }
        })
        .await?;
        Ok(())
    }
}

pub(crate) fn observe(
    application: Arc<dyn Application>,
) -> (Arc<dyn Application>, Arc<Cancellation>) {
    let cancellation = Arc::new(Cancellation::default());
    (
        Arc::new(ObservedApplication(application, cancellation.clone())),
        cancellation,
    )
}

struct ObservedApplication(Arc<dyn Application>, Arc<Cancellation>);

impl Application for ObservedApplication {
    fn container_supervision(&self) -> Option<Arc<containers::Supervision>> {
        self.0.container_supervision()
    }

    fn rpc_catalog(&self) -> Option<&rpc::Catalog> {
        self.0.rpc_catalog()
    }

    fn invoke(self: Arc<Self>, context: Context, event: Invocation) -> HandlerFuture<Output> {
        self.0.clone().invoke(context, event)
    }

    fn validate_configuration(
        &self,
        options: &cellar::engine_api::WorkerConfigOptions,
    ) -> Result<()> {
        self.0.validate_configuration(options)
    }

    fn validate_deployment(&self, deployment: &cellar::fleet::LoadedDeployment) -> Result<()> {
        self.0.validate_deployment(deployment)
    }

    fn supports_cell(&self, class: &str) -> bool {
        self.0.supports_cell(class)
    }

    fn filesystem_policy(&self, class: &str) -> Option<filesystem::Policy> {
        self.0.filesystem_policy(class)
    }

    fn create_cell(&self, context: Context, identity: &CellIdentity) -> Result<Arc<dyn Cell>> {
        Ok(Arc::new(ObservedCell(
            self.0.create_cell(context, identity)?,
            self.1.clone(),
        )))
    }
}

struct ObservedCell(Arc<dyn Cell>, Arc<Cancellation>);

impl Cell for ObservedCell {
    fn invoke(self: Arc<Self>, context: Context, event: Invocation) -> HandlerFuture<Output> {
        let observed =
            matches!(&event, Invocation::Fetch(request) if request.url.ends_with("/cancel-start"));
        let inner = self.0.clone().invoke(context, event);
        if observed {
            self.1.finished.store(false, Ordering::Release);
            Box::pin(ObservedFuture {
                inner: Some(inner),
                cancellation: self.1.clone(),
            })
        } else {
            inner
        }
    }
}

struct ObservedFuture {
    inner: Option<HandlerFuture<Output>>,
    cancellation: Arc<Cancellation>,
}

impl Future for ObservedFuture {
    type Output = Result<Output>;

    fn poll(mut self: Pin<&mut Self>, context: &mut TaskContext<'_>) -> Poll<Self::Output> {
        self.inner
            .as_mut()
            .expect("native future exists")
            .as_mut()
            .poll(context)
    }
}

impl Drop for ObservedFuture {
    fn drop(&mut self) {
        // Drop the original future under its native caller before reporting
        // completion. An HTTP response timeout does not establish cell expiry.
        drop(self.inner.take());
        self.cancellation.finished.store(true, Ordering::Release);
        self.cancellation.wake.notify_one();
    }
}
