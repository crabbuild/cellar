//! Cell RPC waits own their cancellation across gates, routing and execution.
use crate::engine_api::RpcData;
use crate::host_channels::{begin_call_cancellation, RpcCallReq, RPC_CALL_TX};
use crate::host_gates::{gated_channel_send, EgressGate};
use anyhow::{Context as _, Result};

pub(crate) async fn call(
    gate: EgressGate,
    scope: String,
    name: Option<String>,
    method: String,
    args: RpcData,
) -> Result<RpcData> {
    let (request_id, cancel, mut cancellation) = begin_call_cancellation();
    let (reply, receive) = tokio::sync::oneshot::channel();
    let call = RpcCallReq {
        request_id: Some(request_id),
        cancel: Some(cancel),
        scope,
        name,
        method,
        args,
        reply,
    };
    gated_channel_send(gate, &RPC_CALL_TX, call, "no RPC channel")
        .await
        .map_err(anyhow::Error::msg)?;
    let result = receive.await.context("RPC proxy dropped")?;
    cancellation.disarm();
    result
}
