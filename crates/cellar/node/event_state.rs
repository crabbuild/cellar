//! Signed platform state samples enter the ordinary owning-cell RPC path.
use super::*;

pub(super) async fn handle(app: &AppHandle, request: Request<Incoming>) -> HttpReply {
    checked(app, request).await.unwrap_or_else(|_| {
        peer_response(response(
            StatusCode::SERVICE_UNAVAILABLE,
            "event state observation unavailable",
        ))
    })
}

async fn checked(app: &AppHandle, request: Request<Incoming>) -> anyhow::Result<HttpReply> {
    let scope = request
        .uri()
        .path()
        .strip_prefix("/event-state/")
        .unwrap_or_default()
        .to_owned();
    if request.method() != hyper::Method::POST
        || request.uri().query().is_some()
        || !cellar_logic::cell::valid_cell_scope(&scope)
        || !scope.contains(':')
    {
        return Ok(peer_response(response(
            StatusCode::BAD_REQUEST,
            "invalid event state request",
        )));
    }
    let path = request.uri().path().to_owned();
    let headers = request.headers().clone();
    let body = match collect_limited_body(request.into_body(), 2).await {
        Ok(body) => body,
        Err(error) => return Ok(peer_response(body_read_error("event state", error))),
    };
    if let Err(error) = app.peer_auth.verify(
        &hyper::Method::POST,
        &path,
        &headers,
        &body,
        app.peer_auth.source(),
    ) {
        return Ok(peer_response(response(error.status(), error.message())));
    }
    if body.as_ref() != b"{}" {
        return Ok(peer_response(response(
            StatusCode::BAD_REQUEST,
            "event state body must be empty object",
        )));
    }
    let authority = crate::runtime::rpc_authority::StateAuthority::mint(scope.clone())?;
    let (reply, receiver) = oneshot::channel();
    let call = RpcCallReq {
        parent_cancellation: None,
        signal: None,
        deadline_ms: None,
        authority: Some(crate::runtime::RpcAuthority::State(authority)),
        request_id: Some(crate::engine_api::next_request_id()),
        cancel: None,
        scope: scope.clone(),
        name: None,
        method: crate::runtime::event_state::METHOD.into(),
        args: crate::application::encode_rpc(&cellar_native::clone::StructuredValue::from_json(
            serde_json::json!({}),
        )?)?,
        reply,
    };
    let (_, result) =
        futures_util::future::join(super::dispatch_rpc_call(app.clone(), call), receiver).await;
    let data = result??;
    let size = match &data {
        crate::engine_api::RpcData::Json(s) => s.len(),
        crate::engine_api::RpcData::Structured(b) => b.len(),
    };
    anyhow::ensure!(size <= 64 << 10, "encoded event state reply exceeds bounds");
    let value = crate::application::decode_rpc_result(&data)?.to_json()?;
    let sample: crate::event_state::StateSample = serde_json::from_value(
        value
            .get("result")
            .ok_or_else(|| anyhow::anyhow!("owner observation unavailable"))?
            .clone(),
    )?;
    sample.values(&scope)?;
    let bytes = serde_json::to_vec(&serde_json::json!({"result":sample}))?;
    anyhow::ensure!(
        bytes.len() <= crate::runtime::event_state::MAX_REPLY_BYTES,
        "event state reply exceeds bounds"
    );
    Ok(peer_response(response(StatusCode::OK, bytes)))
}
