// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Original-invocation SDK HTTP, with bounded bodies and no redirects or retries.
use anyhow::{ensure, Result};
use cellar::{
    engine_api::HttpResponse,
    sdk::{
        http::{OutboundRequest, Redirect},
        Context,
    },
};
use futures_util::StreamExt;
use std::time::Duration;

pub(super) const CONTROL_BYTES: usize = 64 * 1024;

pub(super) struct Reply {
    pub status: u16,
    pub headers: Vec<(String, String)>,
    pub body: Vec<u8>,
}

pub(super) async fn open(
    context: &Context,
    url: String,
    method: &str,
    headers: Vec<(String, String)>,
    body: Option<Vec<u8>>,
    timeout_ms: u64,
) -> Result<HttpResponse> {
    let request = OutboundRequest {
        url,
        method: method.into(),
        headers,
        body: body.map(Into::into),
        redirect: Redirect::Manual,
    };
    cellar::asyncrt::timeout(Duration::from_millis(timeout_ms), context.fetch(request))
        .await
        .map_err(|_| anyhow::anyhow!("E2B request timed out; outcome may be unknown"))?
}

pub(super) async fn collect(
    context: &Context,
    mut response: HttpResponse,
    maximum: usize,
    timeout_ms: u64,
) -> Result<Reply> {
    let authority = context.invocation_authority()?;
    let read = async {
        ensure!(
            response.websocket.is_none(),
            "unexpected E2B transport upgrade"
        );
        ensure!(
            response.body.len() <= maximum,
            "E2B response exceeds byte limit"
        );
        if let Some(stream) = response.stream.as_mut() {
            while let Some(chunk) = stream.next().await {
                authority.check(context)?;
                let chunk = chunk.map_err(anyhow::Error::msg)?;
                ensure!(
                    chunk.len() <= maximum.saturating_sub(response.body.len()),
                    "E2B response exceeds byte limit"
                );
                response.body.extend_from_slice(&chunk);
            }
        }
        authority.check(context)?;
        Ok(Reply {
            status: response.status,
            headers: response.headers,
            body: response.body,
        })
    };
    cellar::asyncrt::timeout(Duration::from_millis(timeout_ms), read)
        .await
        .map_err(|_| anyhow::anyhow!("E2B response timed out; outcome may be unknown"))?
}

pub(super) async fn request(
    context: &Context,
    url: String,
    method: &str,
    headers: Vec<(String, String)>,
    body: Option<Vec<u8>>,
    maximum: usize,
    timeout_ms: u64,
) -> Result<Reply> {
    collect(
        context,
        open(context, url, method, headers, body, timeout_ms).await?,
        maximum,
        timeout_ms,
    )
    .await
}
