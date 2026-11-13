// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Peer-authenticated Queue uses the same admitted group owner as SDK ingress.

use super::{response, AppHandle, HttpReply, StatusCode};
use crate::application::{decode_rpc_result, encode_rpc, queue};

/// This is called after the private entrance verifies the exact method, path
/// and body. An unavailable Raft owner never selects ordinary Fleet execution.
pub(super) async fn dispatch_authenticated(
    app: &AppHandle,
    method: &hyper::Method,
    scope: &str,
    name: Option<&str>,
    body: &[u8],
    deadline: u64,
) -> Option<HttpReply> {
    let generation = app.runtime.as_ref()?.generation();
    let mut raft = false;
    for application in generation.native_applications.values() {
        use crate::application::consensus::RegistrationError;
        match application.resolve_raft_class(crate::deploy::QUEUE_CLASS) {
            Ok(_) => raft = true,
            Err(RegistrationError::UnknownClass(_) | RegistrationError::UnsupportedFactory(_)) => {}
            Err(error) => {
                return Some(response(
                    StatusCode::SERVICE_UNAVAILABLE,
                    serde_json::json!({"error":error.to_string(),"outcome":"unknown"}).to_string(),
                ))
            }
        }
    }
    if !raft {
        return None;
    }
    if method != hyper::Method::POST {
        return Some(response(
            StatusCode::METHOD_NOT_ALLOWED,
            "Queue operator requires POST",
        ));
    }
    let input = (|| -> anyhow::Result<_> {
        anyhow::ensure!(
            name.is_some_and(|name| crate::engine_api::queue_cell_scope(name) == scope),
            "Queue resource name differs from its scope"
        );
        let body: serde_json::Value = serde_json::from_slice(body)
            .map_err(|_| anyhow::anyhow!("QUEUE_ERROR: invalid request body"))?;
        encode_rpc(&cellar_native::clone::StructuredValue::from_json(
            serde_json::json!([body]),
        )?)
    })();
    let args = match input {
        Ok(args) => args,
        Err(error) => {
            return Some(response(
                StatusCode::BAD_REQUEST,
                serde_json::json!({"error":error.to_string()}).to_string(),
            ))
        }
    };
    let prepared = (|| -> anyhow::Result<_> {
        let services = crate::asyncrt::services();
        let owner = services.raft_lifecycle().application_service()?;
        let admitted = services
            .admit_outbound_rpc(scope, name, queue::OPERATOR, &args)
            .ok_or(crate::application::consensus::CommandFailure::Overloaded)?;
        drop(args);
        let call_scope = crate::host_rpc::CallScope::default()
            .with_reply_limit(super::MAX_PEER_PROTOCOL_BODY_BYTES);
        Ok(owner.queue_operator(admitted, call_scope, deadline))
    })();
    let result = match prepared {
        Ok(call) => call.await,
        Err(error) => Err(error),
    };
    Some(match result {
        Ok(result) => match decode_rpc_result(&result).and_then(|value| value.to_json()) {
            Ok(value) => response(StatusCode::OK, value.to_string()),
            Err(error) => response(
                StatusCode::BAD_REQUEST,
                serde_json::json!({"error":error.to_string()}).to_string(),
            ),
        },
        Err(error) => response(
            StatusCode::SERVICE_UNAVAILABLE,
            serde_json::json!({"error":format!("{error:#}"),"outcome":"unknown"}).to_string(),
        ),
    })
}
