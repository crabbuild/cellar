// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Public HTTP admission for application-owned transports.
//!
//! Callers transfer bodies, not execution authority. Dispatch is polled by the
//! production driver inside its domain. Permits cover the queue through body
//! completion so shutdown cannot observe an empty interval between phases.

use super::*;
use crate::asyncrt::ExecutionDomain;
use std::future::Future;
use std::pin::Pin;
use std::sync::Weak;
use std::task::{Context, Poll};
use tokio::sync::{OwnedSemaphorePermit, Semaphore};

/// Owned transport body. This type does not grant application event authority.
pub type IngressBody = http_body_util::combinators::UnsyncBoxBody<Bytes, std::io::Error>;
pub type IngressResponse = Response<IngressBody>;

/// Cloneable, bounded admission to one node's public application surface.
/// Internal peer and operator endpoints are never dispatched by this handle.
#[derive(Clone)]
pub struct IngressHandle {
    app: AppHandle,
    domain: Weak<ExecutionDomain>,
    sender: mpsc::UnboundedSender<IngressCall>,
    capacity: Arc<Semaphore>,
}

impl IngressHandle {
    pub(super) fn new(
        app: AppHandle,
        domain: Weak<ExecutionDomain>,
        sender: mpsc::UnboundedSender<IngressCall>,
        capacity: usize,
    ) -> Self {
        Self {
            app,
            domain,
            sender,
            capacity: Arc::new(Semaphore::new(capacity)),
        }
    }

    /// Close public admission before draining an application-owned server.
    /// Already accepted calls remain owned by the node's normal shutdown path.
    pub fn close_admission(&self) {
        self.app
            .draining
            .store(true, std::sync::atomic::Ordering::SeqCst);
    }

    pub fn is_closed(&self) -> bool {
        self.sender.is_closed()
            || self
                .domain
                .upgrade()
                .is_none_or(|domain| domain.is_closed())
            || self.app.public_admission_closed()
    }

    /// Admit synchronously, then await the owning driver's response. Dropping
    /// the future cancels the call, including when it has never been polled.
    /// Saturation and closure return HTTP 503; mutating calls are never retried.
    pub fn call<B>(
        &self,
        request: Request<B>,
    ) -> impl Future<Output = IngressResponse> + Send + 'static
    where
        B: hyper::body::Body<Data = Bytes> + Send + 'static,
        B::Error: std::error::Error + Send + Sync + 'static,
    {
        let closed = self.sender.is_closed()
            || self
                .domain
                .upgrade()
                .is_none_or(|domain| domain.is_closed());
        let health = request.uri().path() == "/.well-known/cellar/health";
        let admitted = if closed {
            None
        } else if health {
            self.capacity
                .clone()
                .try_acquire_owned()
                .ok()
                .map(|permit| (permit, None))
        } else {
            self.capacity
                .clone()
                .try_acquire_owned()
                .ok()
                .and_then(|permit| {
                    self.app
                        .admit_public()
                        .map(|admission| (permit, Some(admission)))
                })
        };
        let receive = admitted.and_then(|(permit, admission)| {
            let (reply, receive) = oneshot::channel();
            let request = request.map(|body| body.map_err(std::io::Error::other).boxed_unsync());
            let call = IngressCall {
                request,
                reply,
                lifetime: CallLifetime {
                    connection: ConnectionWorkerRequests::default(),
                    _admission: admission,
                    _permit: permit,
                },
            };
            self.sender.send(call).ok().map(|_| receive)
        });
        async move {
            match receive {
                Some(receive) => receive.await.unwrap_or_else(|_| unavailable()),
                None => unavailable(),
            }
        }
    }
}

fn unavailable() -> IngressResponse {
    let mut response = response(
        StatusCode::SERVICE_UNAVAILABLE,
        "{\"error\":\"node ingress unavailable\"}",
    );
    response.headers_mut().insert(
        hyper::header::RETRY_AFTER,
        hyper::header::HeaderValue::from_static("1"),
    );
    response
}

struct CallLifetime {
    connection: ConnectionWorkerRequests,
    _admission: Option<PublicRequestGuard>,
    _permit: OwnedSemaphorePermit,
}

impl Drop for CallLifetime {
    fn drop(&mut self) {
        self.connection.abort_all();
    }
}

pub(super) struct IngressCall {
    request: Request<IngressBody>,
    reply: oneshot::Sender<IngressResponse>,
    lifetime: CallLifetime,
}

impl IngressCall {
    pub(super) async fn dispatch(self, app: AppHandle) {
        let Self {
            request,
            mut reply,
            lifetime,
        } = self;
        if reply.is_closed() {
            return;
        }
        // Admission was acquired before enqueueing. Do not reject accepted work
        // merely because the driver's shutdown barrier is now closed.
        let dispatch = super::handle_admitted_public(request, app, lifetime.connection.clone());
        crate::asyncrt::select! {
            response = dispatch => {
                let response = response.unwrap_or_else(|never| match never {});
                let response = response.map(|body| GuardedBody { body, lifetime: Some(lifetime) }.boxed_unsync());
                let _ = reply.send(response);
            },
            _ = reply.closed() => {},
        }
    }
}

struct GuardedBody {
    body: IngressBody,
    lifetime: Option<CallLifetime>,
}

impl hyper::body::Body for GuardedBody {
    type Data = Bytes;
    type Error = std::io::Error;

    fn poll_frame(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<Frame<Bytes>, Self::Error>>> {
        let polled = Pin::new(&mut self.body).poll_frame(cx);
        if matches!(polled, Poll::Ready(None | Some(Err(_)))) || self.body.is_end_stream() {
            self.lifetime.take();
        }
        polled
    }

    fn is_end_stream(&self) -> bool {
        self.body.is_end_stream()
    }

    fn size_hint(&self) -> hyper::body::SizeHint {
        self.body.size_hint()
    }
}
