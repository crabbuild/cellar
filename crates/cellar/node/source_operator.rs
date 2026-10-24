// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Authenticated source administration enters the existing RPC owner route.

use super::*;

pub(super) async fn handle<B>(app: &AppHandle, request: Request<B>) -> HttpReply
where
    B: hyper::body::Body<Data = Bytes> + Send + 'static,
    B::Error: std::error::Error + Send + Sync + 'static,
{
    checked(app, request).await.unwrap_or_else(|error| {
        peer_response(response(
            StatusCode::SERVICE_UNAVAILABLE,
            serde_json::json!({"error":error.to_string()}).to_string(),
        ))
    })
}

async fn checked<B>(app: &AppHandle, request: Request<B>) -> anyhow::Result<HttpReply>
where
    B: hyper::body::Body<Data = Bytes> + Send + 'static,
    B::Error: std::error::Error + Send + Sync + 'static,
{
    let scope = request
        .uri()
        .path()
        .strip_prefix("/outbox/")
        .unwrap_or_default()
        .to_owned();
    if !cellar_logic::cell::valid_cell_scope(&scope)
        || crate::application::event_stream::is_event_scope(&scope)
    {
        return Ok(peer_response(malformed_scope()));
    }
    if request.method() != hyper::Method::POST || request.uri().query().is_some() {
        return Ok(peer_response(response(
            StatusCode::BAD_REQUEST,
            "invalid source operator request",
        )));
    }
    let method = request.method().clone();
    let path = request.uri().path().to_owned();
    let headers = request.headers().clone();
    let body = match collect_limited_body(
        request.into_body(),
        crate::runtime::source_control::MAX_REQUEST_BYTES,
    )
    .await
    {
        Ok(body) => body,
        Err(error) => return Ok(peer_response(body_read_error("source operator", error))),
    };
    if let Err(error) =
        app.peer_auth
            .verify(&method, &path, &headers, &body, app.peer_auth.source())
    {
        return Ok(peer_response(response(error.status(), error.message())));
    }
    let command: crate::runtime::source_control::Request = match serde_json::from_slice(&body) {
        Ok(command) => command,
        Err(_) => {
            return Ok(peer_response(response(
                StatusCode::BAD_REQUEST,
                "invalid source operator command",
            )))
        }
    };
    if let Err(error) = command.validate() {
        return Ok(peer_response(response(
            StatusCode::BAD_REQUEST,
            error.to_string(),
        )));
    }
    if let crate::runtime::source_control::Request::Retry { change } = &command {
        if change.expected.source != scope {
            return Ok(peer_response(response(
                StatusCode::BAD_REQUEST,
                "retry names another source",
            )));
        }
    }
    let operator = crate::peer_auth::verified_source(&headers)?.to_owned();
    let authority = crate::runtime::SourceAuthority::mint(scope.clone(), operator)?;
    let (reply, receiver) = oneshot::channel();
    let call = RpcCallReq {
        authority: Some(crate::runtime::RpcAuthority::Source(authority)),
        request_id: Some(crate::engine_api::next_request_id()),
        cancel: None,
        scope: scope.clone(),
        name: None,
        method: crate::runtime::source_control::METHOD.into(),
        args: crate::application::encode_rpc(&cellar_native::clone::StructuredValue::from_json(
            serde_json::to_value(&command)?,
        )?)?,
        reply,
    };
    let (_, result) =
        futures_util::future::join(super::dispatch_rpc_call(app.clone(), call), receiver).await;
    let result = result.context("source operator response channel closed")?;
    match result {
        Ok(data) => {
            let value = crate::application::decode_rpc_result(&data)?.to_json()?;
            if let crate::runtime::source_control::Request::Inspect { limit, cursor } = &command {
                if let Some(result) = value.get("result") {
                    let page: crate::application::OutboxPage =
                        serde_json::from_value(result.clone())?;
                    page.validate(
                        &scope,
                        &crate::application::OutboxOptions {
                            limit: *limit,
                            cursor: cursor.clone(),
                        },
                    )?;
                }
            }
            Ok(peer_response(
                Response::builder()
                    .status(StatusCode::OK)
                    .header("content-type", "application/json")
                    .body(
                        Full::new(Bytes::from(serde_json::to_vec(&value)?))
                            .map_err(|never| match never {})
                            .boxed_unsync(),
                    )?,
            ))
        }
        Err(error) => Ok(peer_response(response(
            StatusCode::SERVICE_UNAVAILABLE,
            serde_json::json!({"error":error.to_string()}).to_string(),
        ))),
    }
}
