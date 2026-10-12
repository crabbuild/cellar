// Copyright 2026 Deno Land Inc. Apache-2.0 license.
//! Generation-scoped service calls over existing host routing.
use crate::engine_api::HttpResponse;
use crate::host_channels::{SvcCallReq, SvcRpcReq, SVC_CALL_TX, SVC_RPC_TX};
use crate::host_gates::{gated_channel_send, EgressGate};
use tokio::sync::oneshot;

#[doc(hidden)]
pub async fn dispatch_fetch(runtime: Option<&crate::runtime::RuntimeManager>, call: SvcCallReq) {
    let mut body_guard = call.body_guard;
    let response = match runtime {
        Some(runtime) => {
            runtime
                .fetch_service(crate::engine_api::ServiceFetch {
                    generation: call.generation,
                    script: call.script,
                    entrypoint: call.entrypoint,
                    url: call.url,
                    method: call.method,
                    body: call.body,
                    headers: call.headers,
                    cancel: call.cancel,
                })
                .await
        }
        None => Err(anyhow::anyhow!("no Worker runtime")),
    };
    if response.is_ok() {
        body_guard.disarm();
    }
    let _ = call.reply.send(response);
}
#[doc(hidden)]
pub async fn dispatch_rpc(runtime: Option<&crate::runtime::RuntimeManager>, call: SvcRpcReq) {
    let response = match runtime {
        Some(runtime) => {
            runtime
                .rpc_service_cancellable(
                    call.generation,
                    &call.script,
                    call.entrypoint,
                    call.props,
                    call.operation,
                    call.cancel,
                )
                .await
        }
        None => Err(anyhow::anyhow!("no Worker runtime")),
    };
    let _ = call.reply.send(response);
}

pub(crate) async fn fetch(
    gate: EgressGate,
    request: SvcCallReq,
    receive: oneshot::Receiver<anyhow::Result<HttpResponse>>,
) -> Result<HttpResponse, String> {
    gated_channel_send(gate, &SVC_CALL_TX, request, "no service binding channel").await?;
    receive
        .await
        .map_err(|error| format!("service dropped: {error}"))?
        .map_err(|error| error.to_string())
}
pub(crate) async fn rpc(
    gate: EgressGate,
    mut request: SvcRpcReq,
    receive: oneshot::Receiver<anyhow::Result<Vec<u8>>>,
) -> Result<Vec<u8>, String> {
    let (_, cancel, mut cancellation) = crate::host_channels::begin_call_cancellation();
    request.cancel = Some(cancel);
    gated_channel_send(gate, &SVC_RPC_TX, request, "no service binding channel").await?;
    let result = receive
        .await
        .map_err(|error| format!("service dropped: {error}"))?;
    cancellation.disarm();
    result.map_err(|error| error.to_string())
}
