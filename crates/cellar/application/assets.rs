//! Static assets through the generation-scoped host resolver.

use super::{Context, HandlerFuture, Request};
use crate::engine_api::{HttpResponse, RequestBody};
use std::sync::Arc;

#[derive(Clone, Debug)]
pub struct Fetcher {
    script: Arc<str>,
}
impl Fetcher {
    pub(crate) fn new(script: &str) -> Self {
        Self {
            script: script.into(),
        }
    }
    pub fn fetch_url(
        &self,
        context: &Context,
        url: impl Into<String>,
    ) -> HandlerFuture<HttpResponse> {
        self.fetch(
            context,
            Request {
                url: url.into(),
                method: "GET".into(),
                headers: Vec::new(),
                body: RequestBody::Bytes(Vec::new().into()),
                entrypoint: None,
            },
        )
    }
    pub fn fetch(&self, context: &Context, request: Request) -> HandlerFuture<HttpResponse> {
        let prepared: anyhow::Result<_> = (|| {
            context.check_turn()?;
            url::Url::parse(&request.url)?;
            let method = super::http::normalize_method(&request.method)?;
            // Assets ignore request bodies. Release an owned producer without
            // pulling it, including non-GET requests that receive a 405.
            if request.body.stream_id().is_some() {
                drop(context.take_body(request.body)?);
            }
            Ok(crate::assets::fetch_binding(
                context.generation,
                self.script.to_string(),
                request.url,
                method,
                request.headers,
            ))
        })();
        let context = context.clone();
        Box::pin(async move {
            let response = prepared?.await.map_err(anyhow::Error::msg)?;
            context.check_turn()?;
            Ok(response)
        })
    }
}
