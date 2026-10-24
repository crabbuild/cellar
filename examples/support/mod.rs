// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Only HTTP plumbing is shared; each recipe owns its business behavior.

use anyhow::Result;
use cellar::sdk::{http::Response, Context, Output, Request};
use futures_util::StreamExt;
use serde_json::Value;

#[derive(Debug)]
pub struct ClientError {
    pub status: u16,
    pub message: String,
}

impl std::fmt::Display for ClientError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.message)
    }
}

impl std::error::Error for ClientError {}

pub fn reject(status: u16, message: impl Into<String>) -> anyhow::Error {
    ClientError {
        status,
        message: message.into(),
    }
    .into()
}

pub fn json(context: &Context, value: Value) -> Result<Output> {
    json_status(context, 200, value)
}

pub fn json_status(context: &Context, status: u16, value: Value) -> Result<Output> {
    Ok(Output::Http(
        Response::new(
            status,
            serde_json::to_vec(&value)?,
            vec![("content-type".into(), "application/json".into())],
        )
        .into_http(context)?,
    ))
}

pub fn text(context: &Context, status: u16, value: &str) -> Result<Output> {
    Ok(Output::Http(
        Response::new(status, value, Vec::new()).into_http(context)?,
    ))
}

pub fn name(value: &str) -> Result<&str> {
    if value.is_empty()
        || value.len() > 128
        || !value
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b"-_".contains(&b))
    {
        return Err(reject(
            400,
            "names must be 1..128 ASCII letters, digits, '-' or '_'",
        ));
    }
    Ok(value)
}

pub fn method(request: &Request, allowed: &[&str]) -> Result<()> {
    if !allowed.contains(&request.method.as_str()) {
        return Err(reject(405, format!("use {}", allowed.join(" or "))));
    }
    Ok(())
}

pub async fn bytes(context: &Context, request: Request) -> Result<Vec<u8>> {
    let mut stream = context.take_body(request.body)?;
    let mut bytes = Vec::new();
    while let Some(chunk) = stream.next().await {
        let chunk = chunk.map_err(anyhow::Error::msg)?;
        if chunk.len() > 64 * 1024 - bytes.len() {
            return Err(reject(413, "example request limit is 64 KiB"));
        }
        bytes.extend(chunk);
    }
    Ok(bytes)
}

pub async fn payload(context: &Context, request: Request) -> Result<Value> {
    serde_json::from_slice(&bytes(context, request).await?)
        .map_err(|_| reject(400, "expected a JSON body"))
}

pub async fn route(context: &Context, request: Request, binding: &str) -> Result<Output> {
    let url = url::Url::parse(&request.url)?;
    let entity = name(url.path().split('/').nth(2).unwrap_or_default())?;
    Ok(Output::Http(
        context
            .namespace(binding)?
            .get_by_name(entity)
            .fetch(context, request)
            .await?,
    ))
}

pub fn now_ms() -> i64 {
    cellar::asyncrt::wall_ms()
}

/// Preserve user input status codes across a cell's HTTP routing boundary.
pub struct HttpErrors<T>(pub std::sync::Arc<T>);

impl<T: cellar::sdk::Cell> cellar::sdk::Cell for HttpErrors<T> {
    fn invoke(
        self: std::sync::Arc<Self>,
        context: Context,
        event: cellar::sdk::Invocation,
    ) -> cellar::sdk::HandlerFuture<Output> {
        let is_http = matches!(&event, cellar::sdk::Invocation::Fetch(_));
        Box::pin(async move {
            match self.0.clone().invoke(context.clone(), event).await {
                Err(error) if is_http => match error.downcast_ref::<ClientError>() {
                    Some(client) => json_status(
                        &context,
                        client.status,
                        serde_json::json!({ "error": client.message }),
                    ),
                    None => Err(error),
                },
                result => result,
            }
        })
    }
}

pub fn cell<T: cellar::sdk::Cell>(value: T) -> std::sync::Arc<dyn cellar::sdk::Cell> {
    std::sync::Arc::new(HttpErrors(std::sync::Arc::new(value)))
}
