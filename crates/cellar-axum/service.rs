use axum::{body::Body, extract::Request, response::Response};
use cellar::sdk::IngressHandle;
use std::{
    convert::Infallible,
    future::Future,
    pin::Pin,
    task::{Context, Poll},
};
use tower::Service;

/// Mount a ready embedded node into an application-owned Axum router.
#[derive(Clone)]
pub struct CellarService {
    ingress: IngressHandle,
}

impl CellarService {
    pub fn new(ingress: IngressHandle) -> Self {
        Self { ingress }
    }

    pub fn ingress(&self) -> &IngressHandle {
        &self.ingress
    }
}

impl Service<Request> for CellarService {
    type Response = Response;
    type Error = Infallible;
    type Future = Pin<Box<dyn Future<Output = Result<Response, Infallible>> + Send>>;

    // Admission is an immediate bounded HTTP refusal, matching native ingress;
    // poll_ready does not hide an unbounded wait queue in an outer Axum router.
    fn poll_ready(&mut self, _: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, request: Request) -> Self::Future {
        let response = self.ingress.call(request);
        Box::pin(async move { Ok(response.await.map(Body::new)) })
    }
}
