// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Fleet-authenticated stream commands retain receiver grants and owner gates.

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
        .strip_prefix("/streams/")
        .unwrap_or_default()
        .to_owned();
    if !cellar_logic::cell::valid_cell_scope(&scope)
        || !crate::application::event_stream::is_event_scope(&scope)
    {
        return Ok(peer_response(malformed_scope()));
    }
    if request.method() != hyper::Method::POST || request.uri().query().is_some() {
        return Ok(peer_response(response(
            StatusCode::BAD_REQUEST,
            "invalid stream operator request",
        )));
    }
    let method = request.method().clone();
    let path = request.uri().path().to_owned();
    let headers = request.headers().clone();
    let body = match collect_limited_body(
        request.into_body(),
        crate::runtime::stream_control::MAX_REQUEST_BYTES,
    )
    .await
    {
        Ok(body) => body,
        Err(error) => return Ok(peer_response(body_read_error("stream operator", error))),
    };
    if let Err(error) =
        app.peer_auth
            .verify(&method, &path, &headers, &body, app.peer_auth.source())
    {
        return Ok(peer_response(response(error.status(), error.message())));
    }
    let command: crate::runtime::stream_control::Request = match serde_json::from_slice(&body) {
        Ok(command) => command,
        Err(_) => {
            return Ok(peer_response(response(
                StatusCode::BAD_REQUEST,
                "invalid stream operator command",
            )))
        }
    };
    if !command.scope().is_ok_and(|target| target == scope) {
        return Ok(peer_response(response(
            StatusCode::BAD_REQUEST,
            "command names another stream",
        )));
    }
    let operator = crate::peer_auth::verified_source(&headers)?.to_owned();
    let (authority, method, args) = match command.prepare(operator) {
        Ok(prepared) => prepared,
        Err(error) => {
            return Ok(peer_response(response(
                StatusCode::BAD_REQUEST,
                error.to_string(),
            )))
        }
    };
    let (reply, receiver) = oneshot::channel();
    let call = RpcCallReq {
        reservation: None,
        parent_cancellation: None,
        signal: None,
        deadline_ms: None,
        authority: Some(crate::runtime::RpcAuthority::Event(authority)),
        request_id: Some(crate::engine_api::next_request_id()),
        cancel: None,
        scope,
        name: Some(command.name()?),
        method: method.into(),
        args: crate::application::encode_rpc(&cellar_native::clone::StructuredValue::from_json(
            args,
        )?)?,
        reply,
    };
    let (_, result) =
        futures_util::future::join(super::dispatch_rpc_call(app.clone(), call), receiver).await;
    let result = result.context("stream operator response channel closed")?;
    match result {
        Ok(data) => {
            let value =
                command.project(crate::application::decode_rpc_result(&data)?.to_json()?)?;
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
