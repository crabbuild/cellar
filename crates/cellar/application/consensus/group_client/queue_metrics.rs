// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! SDK metrics use the same immutable image/proof owner as Queue inspection.

use super::super::{native_queue_metrics, CommandFailure};
use super::queue_read::retained_bytes;
use super::*;
use crate::application::{cell_bindings::Route, HandlerFuture};
use crate::engine_api::RpcData;
use crate::raft::completion::RuntimeError;

impl RaftCells {
    pub(crate) fn queue_metrics(
        &self,
        admitted: crate::outbound_rpc::Admitted,
        call_scope: crate::host_rpc::CallScope,
        deadline: u64,
        configuration: Option<[u8; 32]>,
    ) -> HandlerFuture<RpcData> {
        let prepared = (|| -> anyhow::Result<_> {
            self.check_open()?;
            anyhow::ensure!(
                native_queue_metrics::is_method(&admitted.scope, &admitted.method),
                CommandFailure::UnsupportedCapability
            );
            let route = self.handle.core.host.queue_metrics_route(
                None,
                configuration,
                &admitted.scope,
                &self.handle.core.log,
            )?;
            let remaining = deadline
                .checked_sub(self.handle.domain.mono_ms()?)
                .filter(|n| *n > 0)
                .ok_or(RuntimeError::DeadlineExceeded)?;
            let (_, memory) =
                self.metadata_admission(Duration::from_millis(remaining), retained_bytes(&route)?)?;
            native_queue_metrics::encode(
                &admitted.scope,
                admitted.name.as_deref(),
                &admitted.args,
                route.max_input_bytes,
            )?;
            // No input copy or enrollment output may retain this shared bank
            // across discovery/genesis. The outbound owner still holds args.
            drop(memory);
            Ok(route)
        })();
        let owner = self.internal_observer();
        Box::pin(async move {
            let mut route = prepared?;
            let parent = call_scope.parent_cancellation.clone();
            let signal = call_scope.signal.clone();
            let timer = owner.handle.domain.sleep_until(deadline)?;
            let operation = async move {
                owner
                    .resolve_queue_metadata(&mut route, deadline, false)
                    .await?;
                let remaining = deadline
                    .checked_sub(owner.handle.domain.mono_ms()?)
                    .filter(|n| *n > 0)
                    .ok_or(RuntimeError::DeadlineExceeded)?;
                let (_, memory) = owner.metadata_admission(
                    Duration::from_millis(remaining),
                    retained_bytes(&route)?,
                )?;
                let input = native_queue_metrics::encode(
                    &admitted.scope,
                    admitted.name.as_deref(),
                    &admitted.args,
                    route.max_input_bytes,
                )?;
                let (reply, _) = owner
                    .query_resource_after(route, input, deadline, false, None)
                    .await?;
                let RpcData::Structured(bytes) = reply else {
                    anyhow::bail!(CommandFailure::CorruptState)
                };
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
            };
            let timed = async move {
                crate::asyncrt::select_biased! { "original SDK Queue metrics deadline";
                    _ = timer => Err(anyhow::Error::from(RuntimeError::DeadlineExceeded)),
                    result = operation => result,
                }
            };
            crate::runtime::cancellation::receive(parent.as_ref(), signal.as_ref(), timed).await
        })
    }

    /// Only the source-proof driver's framework permit can enter this adapter.
    /// The source's existing byte lease retains input and physical read owners.
    pub(super) fn query_queue_metrics(
        &self,
        mut route: Route,
        input: Vec<u8>,
        timeout: Duration,
    ) -> HandlerFuture<RpcData> {
        let prepared = (|| -> anyhow::Result<_> {
            super::super::require_framework_dispatch()?;
            self.check_open()?;
            anyhow::ensure!(
                route.contract == native_queue_metrics::contract(),
                CommandFailure::UnsupportedCapability
            );
            anyhow::ensure!(
                input.len() <= route.max_input_bytes,
                CommandFailure::Overloaded
            );
            native_queue_metrics::validate(route.cell.as_str(), &input)?;
            self.handle
                .domain
                .mono_ms()?
                .checked_add(u64::try_from(timeout.as_millis())?)
                .ok_or_else(|| RuntimeError::DeadlineExceeded.into())
        })();
        let owner = self.internal_observer();
        Box::pin(async move {
            let deadline = prepared?;
            let timer = owner.handle.domain.sleep_until(deadline)?;
            let operation = async move {
                // Source writes are already proven before lazy schema genesis;
                // no target business operation or child sequence is allocated.
                owner
                    .resolve_queue_metadata(&mut route, deadline, true)
                    .await?;
                owner
                    .query_resource_after(route, input, deadline, true, None)
                    .await
                    .map(|(data, _)| data)
            };
            crate::asyncrt::select_biased! { "source-proven SDK Queue metrics deadline";
                _ = timer => Err(anyhow::Error::from(RuntimeError::DeadlineExceeded)),
                result = operation => result,
            }
        })
    }
}
