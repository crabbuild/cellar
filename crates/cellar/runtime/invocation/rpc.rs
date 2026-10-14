//! Cell RPC waits own their cancellation across gates, routing and execution.
use crate::engine_api::RpcData;
use crate::host_channels::{begin_call_cancellation, RpcCallReq, RPC_CALL_TX};
use crate::host_gates::{gated_channel_send, EgressGate};
use anyhow::Result;

pub(crate) async fn call(
    gate: EgressGate,
    scope: String,
    name: Option<String>,
    method: String,
    args: RpcData,
    authority: Option<crate::runtime::RpcAuthority>,
) -> Result<RpcData> {
    let event_call = authority.is_some();
    let (request_id, cancel, mut cancellation) = begin_call_cancellation();
    let (reply, receive) = tokio::sync::oneshot::channel();
    let call = RpcCallReq {
        authority,
        request_id: Some(request_id),
        cancel: Some(cancel),
        scope,
        name,
        method,
        args,
        reply,
    };
    if event_call {
        use crate::application::event_stream::{DeliveryError, ErrorCode};
        use crate::host_channels::GateRefusal;
        crate::host_gates::egress_gate_verdict(gate)
            .await
            .map_err(|refusal| {
                let code = if matches!(
                    refusal,
                    GateRefusal::Unproven(cellar_logic::RequestError::NodeFenced)
                ) {
                    ErrorCode::OwnershipLost
                } else {
                    ErrorCode::Unavailable
                };
                DeliveryError::new(code, "event RPC was refused before submission")
            })?;
        let sender = RPC_CALL_TX.get().ok_or_else(|| {
            DeliveryError::new(ErrorCode::Unavailable, "event RPC channel is unavailable")
        })?;
        sender.send(call).map_err(|_| {
            DeliveryError::new(
                ErrorCode::Unavailable,
                "event RPC channel refused submission",
            )
        })?;
    } else {
        gated_channel_send(gate, &RPC_CALL_TX, call, "no RPC channel")
            .await
            .map_err(anyhow::Error::msg)?;
    }
    let result = receive.await.map_err(|failure| {
        if event_call {
            anyhow::Error::new(failure).context(
                crate::application::event_stream::DeliveryError::new(
                    crate::application::event_stream::ErrorCode::Ambiguous,
                    "event RPC result was lost after submission",
                ),
            )
        } else {
            anyhow::Error::new(failure).context("RPC proxy dropped")
        }
    })?;
    cancellation.disarm();
    result
}

#[cfg(test)]
#[path = "rpc_tests.rs"]
mod tests;
