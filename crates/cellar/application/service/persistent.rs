// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! A run may retain a deployment route; each request obtains fresh authority.

use super::{Fetcher, Request};
use crate::{
    actor::GateTicket,
    application::Context,
    engine_api::HttpResponse,
    host_channels::{
        begin_call_cancellation, request_gate, GateReq, SvcCallReq, GATE_TX, SVC_CALL_TX,
    },
    host_services::HostServices,
    http_streams::RequestBodyGuard,
    worker_config::OutboundBroker,
};
use anyhow::{ensure, Context as _, Result};
use std::sync::{Arc, Weak};
use tokio::sync::{mpsc, oneshot};

const MAX_ROUTE_BYTES: usize = 64 * 1024;

pub(crate) trait HttpHandler: Send + Sync {
    fn dispatch(
        &self,
        request: Request,
        body_guard: RequestBodyGuard,
        valid_run: Arc<dyn Fn() -> Result<()> + Send + Sync>,
    ) -> crate::application::HandlerFuture<HttpResponse>;
}

/// Host-minted, immutable provenance. Callers never provide a script,
/// deployment generation, epoch or channel as application request data.
#[derive(Clone)]
pub(crate) struct Persistent {
    host: Weak<HostServices>,
    scope: String,
    epoch: u64,
    route: OutboundBroker,
    gate: mpsc::UnboundedSender<GateReq>,
    calls: mpsc::UnboundedSender<SvcCallReq>,
}

impl Persistent {
    pub(super) fn capture(fetcher: &Fetcher, context: &Context) -> Result<Self> {
        fetcher.check_context(context)?;
        context.container_provider_authority()?.check(context)?;
        ensure!(
            fetcher.loaded.is_none(),
            "persistent container routes require a deployed Worker"
        );
        ensure!(
            fetcher.generation != 0 && fetcher.generation == context.generation,
            "persistent container routes require the current explicit deployment generation"
        );
        let identity = context
            .identity()
            .context("persistent route requires a root cell")?;
        let host = context.io.host_services();
        let gate = GATE_TX
            .get_in(host)
            .context("persistent route has no ownership gate")?;
        let calls = SVC_CALL_TX
            .get_in(host)
            .context("persistent route has no service channel")?;
        let mut remaining = MAX_ROUTE_BYTES;
        let route = fetcher.route(&mut remaining)?;
        Ok(Self {
            host: Arc::downgrade(host),
            scope: identity.scope.clone(),
            epoch: identity.epoch,
            route: OutboundBroker {
                generation: fetcher.generation,
                script: route.script,
                entrypoint: route.entrypoint.map(|name| crate::WorkerFetchEntrypoint {
                    name,
                    props: route.props,
                }),
            },
            gate,
            calls,
        })
    }

    fn check(&self) -> Result<Arc<HostServices>> {
        let host = self
            .host
            .upgrade()
            .context("persistent route host is closed")?;
        ensure!(
            host.accepts_work() && Arc::ptr_eq(&host, &crate::asyncrt::services()),
            "persistent route belongs to another or closed execution domain"
        );
        Ok(host)
    }

    pub(crate) async fn fetch(
        &self,
        mut request: Request,
        body_guard: RequestBodyGuard,
        valid_run: impl Fn() -> Result<()>,
    ) -> Result<HttpResponse> {
        let _host = self.check()?;
        valid_run()?;
        request.method = crate::application::http::normalize_method(&request.method)?;
        request_gate(
            Some(&self.gate),
            self.scope.clone(),
            GateTicket {
                channel: cellar_logic::Channel::Service,
                position: None,
                observed: None,
                epoch: Some(self.epoch),
            },
        )
        .await
        .map_err(|error| anyhow::anyhow!("{error}"))?;
        self.check()?;
        valid_run()?;
        let (_, cancel, mut cancellation) = begin_call_cancellation();
        let (reply, receive) = oneshot::channel();
        self.calls
            .send(SvcCallReq {
                generation: self.route.generation,
                script: self.route.script.clone(),
                entrypoint: self.route.entrypoint.clone(),
                url: request.url,
                method: request.method,
                body: request.body,
                body_guard,
                headers: request.headers,
                cancel: Some(cancel),
                reply,
            })
            .map_err(|_| anyhow::anyhow!("persistent route service channel is closed"))?;
        let response = receive.await.context("persistent route service dropped")?;
        cancellation.disarm();
        self.check()?;
        valid_run()?;
        response
    }
}

impl HttpHandler for Persistent {
    fn dispatch(
        &self,
        request: Request,
        guard: RequestBodyGuard,
        valid_run: Arc<dyn Fn() -> Result<()> + Send + Sync>,
    ) -> crate::application::HandlerFuture<HttpResponse> {
        let handler = self.clone();
        Box::pin(async move { handler.fetch(request, guard, || valid_run()).await })
    }
}

#[cfg(test)]
#[path = "persistent_tests.rs"]
mod tests;
