// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! HTTP conversion with owned body producers and native response metadata.

use anyhow::{anyhow, Context as _, Result};
use axum::body::Body;
use axum::response::Response;
use cellar::engine_api::{HttpResponse, HttpResponseWebSocket};
use cellar::sdk::{streams::IdentityTransformStream, Context, Request};
use futures_util::TryStreamExt;
use http_body_util::BodyExt;
use std::sync::{Arc, Mutex};

#[derive(Clone)]
struct NativeMetadata(Arc<Mutex<Option<Metadata>>>);

#[derive(Clone)]
struct InvocationFailure(Arc<Mutex<Option<anyhow::Error>>>);

struct Metadata {
    websocket: Option<HttpResponseWebSocket>,
    write_position: Option<u64>,
    observed_position: Option<u64>,
}

pub(crate) fn request(context: &Context, request: Request) -> Result<axum::extract::Request> {
    let mut result = axum::http::Request::builder()
        .method(request.method.as_str())
        .uri(request.url.as_str());
    for (name, value) in request.headers {
        result = result.header(name, value);
    }
    let stream = context.take_body(request.body)?.map_ok(bytes::Bytes::from);
    let mut result = result
        .body(Body::from_stream(stream))
        .context("invalid invocation HTTP request")?;
    result.extensions_mut().insert(context.clone());
    Ok(result)
}

/// Preserve native WebSocket targets through Axum response middleware. Proof
/// positions remain informational; only Cellar's event driver authorizes output.
pub fn response(response: HttpResponse) -> Result<Response> {
    let metadata = NativeMetadata(Arc::new(Mutex::new(Some(Metadata {
        websocket: response.websocket,
        write_position: response.write_position,
        observed_position: response.observed_position,
    }))));
    let body = match response.stream {
        Some(stream) => Body::from_stream(stream),
        None => Body::from(response.body),
    };
    let mut builder = Response::builder().status(response.status);
    for (name, value) in response.headers {
        builder = builder.header(name, value);
    }
    let mut result = builder.body(body).context("invalid native HTTP response")?;
    result.extensions_mut().insert(metadata);
    Ok(result)
}

/// Execute body polling under the invocation, including SSE producers. Each
/// chunk trails the existing response egress gate before entering the bounded
/// pipe. Transport consumers need no event authority and drop cancels producers.
pub(crate) fn output(context: &Context, response: Response) -> Result<HttpResponse> {
    let (mut parts, body) = response.into_parts();
    if let Some(failure) = parts.extensions.remove::<InvocationFailure>() {
        return Err(failure
            .0
            .lock()
            .map_err(|_| anyhow!("invocation failure poisoned"))?
            .take()
            .ok_or_else(|| anyhow!("invocation failure already consumed"))?);
    }
    let metadata = parts
        .extensions
        .remove::<NativeMetadata>()
        .map(|metadata| {
            metadata
                .0
                .lock()
                .map_err(|_| anyhow!("response metadata poisoned"))?
                .take()
                .ok_or_else(|| anyhow!("native response metadata already consumed"))
        })
        .transpose()?;
    let Metadata {
        websocket,
        write_position,
        observed_position,
    } = metadata.unwrap_or(Metadata {
        websocket: None,
        write_position: None,
        observed_position: None,
    });
    let headers = parts
        .headers
        .iter()
        .map(|(name, value)| {
            Ok((
                name.to_string(),
                value
                    .to_str()
                    .context("non-text response header is unsupported")?
                    .to_owned(),
            ))
        })
        .collect::<Result<Vec<_>>>()?;
    let readable = owned_body(context, body)?;
    Ok(HttpResponse {
        status: parts.status.as_u16(),
        headers,
        body: Vec::new(),
        stream: Some(cellar::sdk::streams::http_byte_stream(&readable)?),
        websocket,
        write_position,
        observed_position,
    })
}

pub(crate) fn failure(error: anyhow::Error) -> Response {
    use axum::response::IntoResponse;
    let mut response = (
        axum::http::StatusCode::INTERNAL_SERVER_ERROR,
        error.to_string(),
    )
        .into_response();
    response
        .extensions_mut()
        .insert(InvocationFailure(Arc::new(Mutex::new(Some(error)))));
    response
}

pub(crate) fn forwarded_request(
    context: &Context,
    request: axum::extract::Request,
) -> Result<Request> {
    let (parts, body) = request.into_parts();
    let headers = cellar::sdk::http::Headers::new();
    let mut wire_headers = Vec::with_capacity(parts.headers.len());
    for (name, value) in &parts.headers {
        let value = value
            .to_str()
            .context("non-text request header is unsupported")?;
        headers.append(name.as_str(), value)?;
        wire_headers.push((name.to_string(), value.to_owned()));
    }
    let readable = owned_body(context, body)?;
    let mut request = Request::with_byte_stream(
        context,
        &parts.uri.to_string(),
        parts.method.as_str(),
        &headers,
        &readable,
    )?;
    // The web Headers view combines duplicate values. The invocation protocol
    // can retain each validated wire entry for the cell's Axum extractors,
    // together with framing headers supplied by the stream constructor.
    wire_headers.extend(
        std::mem::take(&mut request.headers)
            .into_iter()
            .filter(|(name, _)| !parts.headers.contains_key(name.as_str())),
    );
    request.headers = wire_headers;
    Ok(request)
}

fn owned_body(context: &Context, mut body: Body) -> Result<cellar::sdk::streams::ByteReadable> {
    let pipe = IdentityTransformStream::new();
    let writer = pipe.writable.get_writer()?;
    let producer = context.clone();
    context.wait_until_byte_stream(&pipe.readable, async move {
        while let Some(frame) = body.frame().await {
            let frame = frame?;
            match frame.into_data() {
                Ok(bytes) => producer.write_stream_chunk(&writer, bytes.to_vec()).await?,
                Err(_) => return Err(anyhow!("HTTP trailers are unsupported")),
            }
        }
        writer.close().await?;
        Ok(())
    })?;
    Ok(pipe.readable)
}
