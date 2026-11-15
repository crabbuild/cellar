// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

use super::*;
use crate::application::event_stream::{self, DeliveryError, ErrorCode};
use crate::application::Output;

fn reply(result: anyhow::Result<serde_json::Value>) -> anyhow::Result<RpcData> {
    let Output::Rpc(response) = event_stream::broker::reply(result)? else {
        anyhow::bail!("expected RPC response");
    };
    Ok(response)
}

#[test]
fn configuration_join_falls_back_only_on_typed_operation_absence() -> anyhow::Result<()> {
    assert!(!configuration_operation_absent(&reply(Ok(
        serde_json::json!({
            "state":"creating", "operation":"a".repeat(32),
        })
    ))?)?);
    for (code, missing) in [
        (ErrorCode::OperationUnknown, true),
        (ErrorCode::NotInitialized, true),
        (ErrorCode::Forbidden, false),
        (ErrorCode::ConfigConflict, false),
        (ErrorCode::DestinationDeleted, false),
        (ErrorCode::OwnershipLost, false),
        (ErrorCode::Ambiguous, false),
        (ErrorCode::Unavailable, false),
        (ErrorCode::StaleReceipt, false),
        (ErrorCode::UnsupportedProtocol, false),
        (ErrorCode::InvalidRequest, false),
    ] {
        let response = reply(Err(DeliveryError::new(code, "typed catalog result").into()))?;
        assert_eq!(
            configuration_operation_absent(&response)?,
            missing,
            "{code:?}"
        );
    }
    Ok(())
}

#[test]
fn malformed_configuration_probe_cannot_authorize_a_command() -> anyhow::Result<()> {
    assert!(configuration_operation_absent(&RpcData::Json("not-json".into())).is_err());
    let response = crate::application::encode_rpc(
        &cellar_native::clone::StructuredValue::from_json(serde_json::json!({
            "error":{"code":"UnknownFutureResult","message":"not an absence proof"},
        }))?,
    )?;
    assert!(configuration_operation_absent(&response).is_err());
    let response = crate::application::encode_rpc(
        &cellar_native::clone::StructuredValue::from_json(serde_json::json!({
            "ok":{"state":"active"},
            "error":{"code":"OperationUnknown","message":"contradictory response"},
        }))?,
    )?;
    assert!(configuration_operation_absent(&response).is_err());
    Ok(())
}
