// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Peer-authenticated KV uses the same admitted group owner as SDK ingress.

use super::{response, AppHandle, HttpReply, StatusCode};
use crate::application::{encode_rpc, kv};

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
        match application.resolve_raft_class(crate::deploy::KV_CLASS) {
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
            "KV operator requires POST",
        ));
    }
    let input = (|| -> anyhow::Result<_> {
        crate::application::consensus::native_rpc::validate_identity(
            "cellar-kv-service",
            crate::deploy::KV_CLASS,
            scope,
            name,
        )?;
        let body: serde_json::Value = serde_json::from_slice(body)
            .map_err(|_| anyhow::anyhow!("KV_ERROR: invalid request body"))?;
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
    let prepared = async {
        let services = crate::asyncrt::services();
        let owner = services.raft_lifecycle().application_service()?;
        let args = kv::raft_wire::stage_admin(&owner, scope, name, args, deadline).await?;
        let admitted = services
            .admit_outbound_rpc(scope, name, kv::OPERATOR, &args)
            .ok_or(crate::application::consensus::CommandFailure::Overloaded)?;
        drop(args);
        let call_scope = crate::host_rpc::CallScope::default()
            .with_reply_limit(super::MAX_PEER_PROTOCOL_BODY_BYTES);
        let reply = owner.kv_operator(admitted, call_scope, deadline).await?;
        Ok::<_, anyhow::Error>((owner, reply))
    }
    .await;
    let result = match prepared {
        Ok((owner, reply)) => kv::raft_wire::expand_admin(&owner, scope, reply, deadline).await,
        Err(error) => Err(error),
    };
    Some(match result {
        Ok(value) => response(
            StatusCode::OK,
            serde_json::json!({"result":value}).to_string(),
        ),
        Err(error) if error.is::<cellar_native::rpc::RemoteError>() => response(
            StatusCode::BAD_REQUEST,
            serde_json::json!({"error":error.to_string()}).to_string(),
        ),
        Err(error) => response(
            StatusCode::SERVICE_UNAVAILABLE,
            serde_json::json!({"error":format!("{error:#}"),"outcome":"unknown"}).to_string(),
        ),
    })
}
