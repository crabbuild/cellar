// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Signed Queue observations avoid entering the mutation lane after bootstrap.

use super::super::{native_rpc, CommandFailure};
use super::*;
use crate::application::HandlerFuture;
use crate::engine_api::RpcData;
use crate::raft::completion::RuntimeError;

impl RaftCells {
    /// Only the node's peer-authenticated Queue operator entrance calls this.
    pub(crate) fn queue_operator_read(
        &self,
        admitted: crate::outbound_rpc::Admitted,
        call_scope: crate::host_rpc::CallScope,
        deadline: u64,
    ) -> HandlerFuture<RpcData> {
        let prepared = (|| -> anyhow::Result<_> {
            self.check_open()?;
            anyhow::ensure!(
                admitted.method == crate::application::queue::OPERATOR,
                CommandFailure::UnsupportedCapability
            );
            let route = self
                .handle
                .core
                .host
                .queue_operator_read_route(&admitted.scope, &self.handle.core.log)?;
            // Discovery cannot initialize a cell. Keep cold enrollment on the
            // existing operator command without first attempting any mutation.
            if route.history_hint.is_none() {
                return Ok(None);
            }
            let bytes = route
                .max_input_bytes
                .checked_mul(2)
                .and_then(|bytes| {
                    route
                        .max_result_bytes
                        .checked_mul(4)
                        .and_then(|result| bytes.checked_add(result))
                })
                .and_then(|bytes| bytes.checked_add(8192))
                .ok_or(CommandFailure::Overloaded)?;
            let timeout = Duration::from_millis(
                deadline
                    .checked_sub(self.handle.domain.mono_ms()?)
                    .filter(|remaining| *remaining != 0)
                    .ok_or(RuntimeError::DeadlineExceeded)?,
            );
            let (_, memory) = self.metadata_admission(timeout, bytes)?;
            let input = native_rpc::encode_request(
                admitted.name.as_deref(),
                &admitted.args,
                route.max_input_bytes,
            )?;
            Ok(Some((route, input, timeout, memory)))
        })();
        let owner = self.internal_observer();
        Box::pin(async move {
            let Some((route, input, timeout, memory)) = prepared? else {
                return owner.queue_operator(admitted, call_scope, deadline).await;
            };
            let operation = owner.query_resource(route, input, timeout, false);
            let operation = crate::runtime::cancellation::receive(
                call_scope.parent_cancellation.as_ref(),
                call_scope.signal.as_ref(),
                operation,
            );
            let timer = owner.handle.domain.sleep_until(deadline)?;
            let response = crate::asyncrt::select_biased! { "original Queue observation deadline";
                _=timer=>Err(anyhow::Error::from(RuntimeError::DeadlineExceeded)),
                result=operation=>result,
            }?;
            match response {
                // The registered query emits this exact private sentinel only
                // for absent schema/statistics. Arbitrary read errors never
                // cause a command retry or establish mutation non-commit.
                RpcData::Json(value) if value == "null" => {
                    drop(memory);
                    owner.queue_operator(admitted, call_scope, deadline).await
                }
                RpcData::Structured(bytes) => {
                    if let Some(limit) = call_scope.reply_limit() {
                        anyhow::ensure!(
                            bytes.len() <= limit,
                            crate::application::rpc::Failure::new(
                                crate::application::rpc::Code::InvalidResponse
                            )
                        );
                    }
                    let held = crate::rpc_reservation::Reservation::shared(Arc::new((
                        admitted.reservation,
                        memory,
                    )));
                    Ok(RpcData::Structured(held.retain_bytes(bytes)))
                }
                _ => anyhow::bail!(CommandFailure::CorruptState),
            }
        })
    }
}
