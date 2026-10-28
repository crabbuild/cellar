// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Signed platform state samples enter the ordinary owning-cell RPC path.
use super::*;

#[derive(Clone, Copy)]
pub(super) enum SampleKind {
    Events,
    Filesystem,
}

impl SampleKind {
    fn prefix(self) -> &'static str {
        match self {
            Self::Events => "/event-state/",
            Self::Filesystem => "/filesystem-state/",
        }
    }

    fn limit(self) -> usize {
        match self {
            Self::Events => crate::runtime::event_state::MAX_REPLY_BYTES,
            Self::Filesystem => crate::runtime::filesystem_state::MAX_REPLY_BYTES,
        }
    }
}

pub(super) async fn handle(
    app: &AppHandle,
    request: Request<Incoming>,
    kind: SampleKind,
) -> HttpReply {
    checked(app, request, kind).await.unwrap_or_else(|_| {
        peer_response(response(
            StatusCode::SERVICE_UNAVAILABLE,
            "owner state observation unavailable",
        ))
    })
}

async fn checked(
    app: &AppHandle,
    request: Request<Incoming>,
    kind: SampleKind,
) -> anyhow::Result<HttpReply> {
    let scope = request
        .uri()
        .path()
        .strip_prefix(kind.prefix())
        .unwrap_or_default()
        .to_owned();
    if request.method() != hyper::Method::POST
        || request.uri().query().is_some()
        || !cellar_logic::cell::valid_cell_scope(&scope)
        || !scope.contains(':')
    {
        return Ok(peer_response(response(
            StatusCode::BAD_REQUEST,
            "invalid state sample request",
        )));
    }
    let path = request.uri().path().to_owned();
    let headers = request.headers().clone();
    let body = match collect_limited_body(request.into_body(), 2).await {
        Ok(body) => body,
        Err(error) => return Ok(peer_response(body_read_error("state sample", error))),
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
            "state sample body must be empty object",
        )));
    }
    let authority = match kind {
        SampleKind::Events => crate::runtime::rpc_authority::StateAuthority::mint(scope.clone())?,
        SampleKind::Filesystem => {
            crate::runtime::rpc_authority::StateAuthority::filesystem(scope.clone())?
        }
    };
    let method = authority.method();
    let (reply, receiver) = oneshot::channel();
    let call = RpcCallReq {
        reservation: None,
        parent_cancellation: None,
        signal: None,
        deadline_ms: None,
        authority: Some(crate::runtime::RpcAuthority::State(authority)),
        request_id: Some(crate::engine_api::next_request_id()),
        cancel: None,
        scope: scope.clone(),
        name: None,
        method: method.into(),
        args: crate::application::encode_rpc(&cellar_native::clone::StructuredValue::from_json(
            serde_json::json!({}),
        )?)?,
        reply,
    };
    let (_, result) =
        futures_util::future::join(super::dispatch_rpc_call(app.clone(), call), receiver).await;
    let data = result??;
    let size = data.payload_bytes();
    anyhow::ensure!(
        size <= 64 << 10,
        "encoded state sample reply exceeds bounds"
    );
    let value = crate::application::decode_rpc_result(&data)?.to_json()?;
    let sample = value
        .get("result")
        .ok_or_else(|| anyhow::anyhow!("owner observation unavailable"))?
        .clone();
    let sample = match kind {
        SampleKind::Events => {
            let sample: crate::event_state::StateSample = serde_json::from_value(sample)?;
            sample.values(&scope)?;
            serde_json::to_value(sample)?
        }
        SampleKind::Filesystem => {
            let sample: crate::filesystem_state::FilesystemState = serde_json::from_value(sample)?;
            sample.validate(&scope)?;
            serde_json::to_value(sample)?
        }
    };
    let bytes = serde_json::to_vec(&serde_json::json!({"result":sample}))?;
    anyhow::ensure!(
        bytes.len() <= kind.limit(),
        "state sample reply exceeds bounds"
    );
    Ok(peer_response(response(StatusCode::OK, bytes)))
}
