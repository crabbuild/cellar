// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

use crate::support;
use anyhow::Result;
use cellar::sdk::{http::Response, Context, Output, Request};

pub async fn fetch(context: &Context, request: Request) -> Result<Output> {
    support::method(&request, &["POST"])?;
    let url = url::Url::parse(&request.url)?;
    if url.path() == "/body/stream" {
        // No collection: preserve transport backpressure and cancellation.
        return Ok(Output::Http(
            Response::streaming(
                200,
                context.take_body(request.body)?,
                vec![("content-type".into(), "application/octet-stream".into())],
            )
            .into_http(context)?,
        ));
    }
    let bytes = support::bytes(context, request).await?;
    Ok(Output::Http(
        Response::new(
            200,
            bytes,
            vec![("content-type".into(), "application/octet-stream".into())],
        )
        .into_http(context)?,
    ))
}
