// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Cell RPC waits own their cancellation across gates, routing and execution.
use crate::engine_api::RpcData;
use crate::host_channels::{begin_call_cancellation, RpcCallReq, RPC_CALL_TX};
use crate::host_gates::{gated_channel_send, EgressGate};
use anyhow::Result;
use std::sync::atomic::{AtomicBool, Ordering};

#[path = "rpc_budget.rs"]
mod budget;

#[derive(Default)]
pub(crate) struct CallScope {
    pub deadline_ms: Option<u64>,
    pub parent_cancellation: Option<std::sync::Arc<crate::runtime::InvocationCancellation>>,
    pub signal: Option<cellar_native::web::AbortSignal>,
}

impl CallScope {
    pub(crate) fn in_context(
        context: &crate::application::Context,
        deadline_ms: Option<u64>,
        signal: Option<cellar_native::web::AbortSignal>,
    ) -> Self {
        Self {
            deadline_ms,
            parent_cancellation: context.io.rpc_cancellation.get().cloned(),
            signal,
        }
    }
}

pub(crate) fn event_budget_ms(method: &str, args: &RpcData, transport_ms: u64) -> u64 {
    budget::duration(
        method,
        args,
        transport_ms,
        crate::asyncrt::services().cell_limits().events.max_poll_ms,
    )
}

#[cfg(test)]
pub(crate) async fn call(
    gate: EgressGate,
    scope: String,
    name: Option<String>,
    method: String,
    args: RpcData,
    authority: Option<crate::runtime::RpcAuthority>,
) -> Result<RpcData> {
    call_scoped(
        gate,
        scope,
        name,
        method,
        args,
        authority,
        CallScope::default(),
    )
    .await
}

/// Typed calls retain the originating deadline through local admission. The
/// source client owns its timer and cancellation registration throughout.
pub(crate) async fn call_scoped(
    gate: EgressGate,
    scope: String,
    name: Option<String>,
    method: String,
    args: RpcData,
    authority: Option<crate::runtime::RpcAuthority>,
    call_scope: CallScope,
) -> Result<RpcData> {
    let event_call = authority.is_some();
    let deadline_ms = if event_call {
        Some(crate::asyncrt::mono_ms().saturating_add(event_budget_ms(
            &method,
            &args,
            crate::asyncrt::services().operation_deadline_ms()?,
        )))
    } else {
        None
    };
    let submitted = AtomicBool::new(false);
    let operation = async {
        let (request_id, cancel, mut cancellation) = begin_call_cancellation();
        let (reply, receive) = tokio::sync::oneshot::channel();
        let call = RpcCallReq {
            parent_cancellation: call_scope.parent_cancellation,
            signal: call_scope.signal,
            deadline_ms: call_scope.deadline_ms,
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
            submitted.store(true, Ordering::SeqCst);
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
    };
    if let Some(deadline_ms) = deadline_ms {
        let remaining_ms = deadline_ms.saturating_sub(crate::asyncrt::mono_ms());
        if remaining_ms == 0 {
            return Err(source_timeout(&submitted));
        }
        crate::asyncrt::select_biased! {
            "the original SDK event RPC deadline wins an undelivered result";
            _ = crate::asyncrt::sleep(std::time::Duration::from_millis(remaining_ms)) => Err(source_timeout(&submitted)),
            result = operation => result,
        }
    } else {
        operation.await
    }
}

fn source_timeout(submitted: &AtomicBool) -> anyhow::Error {
    crate::application::event_stream::DeliveryError::new(
        if submitted.load(Ordering::SeqCst) {
            crate::application::event_stream::ErrorCode::Ambiguous
        } else {
            crate::application::event_stream::ErrorCode::Unavailable
        },
        "event RPC source deadline elapsed",
    )
    .into()
}

#[cfg(test)]
#[path = "rpc_tests.rs"]
mod tests;
