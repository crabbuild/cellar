//! Fleet-authenticated metadata inventory uses the ordinary owner RPC route.
use super::*;

pub(super) async fn handle(app: &AppHandle, request: Request<Incoming>) -> HttpReply {
    checked(app, request).await.unwrap_or_else(|_| {
        peer_response(response(
            StatusCode::SERVICE_UNAVAILABLE,
            serde_json::json!({"error":"event inventory unavailable"}).to_string(),
        ))
    })
}

async fn checked(app: &AppHandle, request: Request<Incoming>) -> anyhow::Result<HttpReply> {
    let scope = request
        .uri()
        .path()
        .strip_prefix("/event-inventory/")
        .unwrap_or_default()
        .to_owned();
    if request.method() != hyper::Method::POST
        || request.uri().query().is_some()
        || !cellar_logic::cell::valid_cell_scope(&scope)
        || !scope.contains(':')
    {
        return Ok(peer_response(response(
            StatusCode::BAD_REQUEST,
            "invalid event inventory request",
        )));
    }
    let path = request.uri().path().to_owned();
    let headers = request.headers().clone();
    let body = match collect_limited_body(
        request.into_body(),
        crate::runtime::event_inventory::MAX_REQUEST_BYTES,
    )
    .await
    {
        Ok(body) => body,
        Err(error) => return Ok(peer_response(body_read_error("event inventory", error))),
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
    let command: crate::event_inventory::InventoryRequest = match serde_json::from_slice(&body) {
        Ok(command) => command,
        Err(_) => {
            return Ok(peer_response(response(
                StatusCode::BAD_REQUEST,
                "invalid inventory command",
            )))
        }
    };
    if command.validate(&scope).is_err() {
        return Ok(peer_response(response(
            StatusCode::BAD_REQUEST,
            "invalid inventory bounds or scope",
        )));
    }
    let authority = crate::runtime::rpc_authority::InventoryAuthority::mint(scope.clone())?;
    let (reply, receiver) = oneshot::channel();
    let call = RpcCallReq {
        parent_cancellation: None,
        signal: None,
        deadline_ms: None,
        authority: Some(crate::runtime::RpcAuthority::Inventory(authority)),
        request_id: Some(crate::engine_api::next_request_id()),
        cancel: None,
        scope: scope.clone(),
        name: None,
        method: crate::runtime::event_inventory::METHOD.into(),
        args: crate::application::encode_rpc(&cellar_native::clone::StructuredValue::from_json(
            serde_json::to_value(&command)?,
        )?)?,
        reply,
    };
    let (_, result) =
        futures_util::future::join(super::dispatch_rpc_call(app.clone(), call), receiver).await;
    let data = result??;
    let bytes = match &data {
        crate::engine_api::RpcData::Json(s) => s.len(),
        crate::engine_api::RpcData::Structured(b) => b.len(),
    };
    anyhow::ensure!(
        bytes <= 4 * crate::runtime::event_inventory::MAX_REPLY_BYTES,
        "encoded inventory reply exceeds bounds"
    );
    let value = crate::application::decode_rpc_result(&data)?.to_json()?;
    let reply: crate::event_inventory::InventoryReply = serde_json::from_value(
        value
            .get("result")
            .ok_or_else(|| anyhow::anyhow!("owner inventory unavailable"))?
            .clone(),
    )?;
    reply.validate(&scope, &command)?;
    let bytes = serde_json::to_vec(&serde_json::json!({"result":reply}))?;
    anyhow::ensure!(
        bytes.len() <= crate::runtime::event_inventory::MAX_REPLY_BYTES,
        "inventory reply exceeds bounds"
    );
    Ok(peer_response(response(StatusCode::OK, bytes)))
}
