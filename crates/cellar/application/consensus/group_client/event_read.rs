// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Event metadata uses immutable strong reads without a command sequence.
//! Provisioning may join schema genesis after a full source checkpoint.
//! All discovery, archive proof and output share one deadline.

use super::super::{native_event_rpc, CommandFailure};
use super::*;
use crate::application::{cell_bindings::Route, HandlerFuture};
use crate::engine_api::RpcData;
use crate::raft::completion::RuntimeError;

impl RaftCells {
    /// Join an existing exact provisioning intent before considering a new
    /// command. Only a verified absence permits the ordinary update path;
    /// discovery, authorization and proof failures cannot authorize mutation.
    pub(super) async fn joined_event_configuration(
        &self,
        history: &CellHistory,
        input: &[u8],
        minimum: Durability,
        deadline: u64,
    ) -> anyhow::Result<Option<RpcData>> {
        let (route, query) = {
            let scope = history.cell.as_str();
            let (authority, query) =
                native_event_rpc::configuration_receipt_query(scope, input, input.len())?;
            let mut route = self.handle.core.host.event_query_route(
                scope,
                "__catalogConfigureReceipt",
                &authority,
                &self.handle.core.log,
            )?;
            route.durability = route.durability.max(minimum);
            // Discovery supplies only a locator. The query must obtain current
            // linearizable authority and the independent target minimum proof.
            route.history_hint = Some(history.clone());
            anyhow::ensure!(
                query.len() <= route.max_input_bytes,
                CommandFailure::Overloaded
            );
            (route, query)
        };
        let response = self
            .query_event_resource(route, query, deadline, false)
            .await?;
        if configuration_operation_absent(&response)? {
            // Drop the selected image and archive proof before an update needs
            // another cut. The caller still owns its untouched command wire.
            return Ok(None);
        }
        Ok(Some(response))
    }

    pub(crate) fn event_read(
        &self,
        admitted: crate::outbound_rpc::Admitted,
        call_scope: crate::host_rpc::CallScope,
        deadline: u64,
        authority: native_event_rpc::Authority,
    ) -> HandlerFuture<RpcData> {
        let read = self.event_read_observed(admitted, call_scope, deadline, authority, None);
        Box::pin(async move { read.await.map(|(value, _)| value) })
    }

    pub(crate) fn event_read_observed(
        &self,
        admitted: crate::outbound_rpc::Admitted,
        call_scope: crate::host_rpc::CallScope,
        deadline: u64,
        authority: native_event_rpc::Authority,
        after: Option<crate::raft::version::CellVersionToken>,
    ) -> HandlerFuture<(RpcData, crate::raft::version::CellVersionToken)> {
        let prepared = (|| -> anyhow::Result<_> {
            self.check_open()?;
            let route = self.handle.core.host.event_query_route(
                &admitted.scope,
                &admitted.method,
                &authority,
                &self.handle.core.log,
            )?;
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
                    .filter(|n| *n > 0)
                    .ok_or(RuntimeError::DeadlineExceeded)?,
            );
            let (_, memory) = self.metadata_admission(timeout, bytes)?;
            let input = native_event_rpc::encode(
                &admitted.scope,
                admitted.name.as_deref(),
                &admitted.method,
                authority,
                &admitted.args,
                route.max_input_bytes,
            )?;
            Ok((route, input, admitted.reservation, memory))
        })();
        let owner = self.internal_observer();
        Box::pin(async move {
            let (route, input, reservation, memory) = prepared?;
            let operation = owner.query_resource_after(route, input, deadline, false, after);
            let (result, token) = crate::runtime::cancellation::receive(
                call_scope.parent_cancellation.as_ref(),
                call_scope.signal.as_ref(),
                operation,
            )
            .await?;
            let RpcData::Structured(bytes) = result else {
                anyhow::bail!(CommandFailure::CorruptState);
            };
            if let Some(limit) = call_scope.reply_limit() {
                anyhow::ensure!(
                    bytes.len() <= limit,
                    crate::application::rpc::Failure::new(
                        crate::application::rpc::Code::InvalidResponse
                    )
                );
            }
            let retained =
                crate::rpc_reservation::Reservation::shared(Arc::new((reservation, memory)));
            Ok((RpcData::Structured(retained.retain_bytes(bytes)), token))
        })
    }

    /// Source callers enter through their owned read dispatch, after proving
    /// preceding writes. Neither source nor target acquires a mutation identity.
    pub(super) fn query_event_resource(
        &self,
        route: Route,
        input: Vec<u8>,
        deadline: u64,
        source_proven: bool,
    ) -> HandlerFuture<RpcData> {
        let prepared = (|| -> anyhow::Result<_> {
            if !source_proven || route.history_hint.is_some() {
                return Ok(false);
            }
            super::super::require_framework_dispatch()?;
            anyhow::ensure!(
                route.admission == super::super::CommandAdmission::Event
                    && input.len() <= route.max_input_bytes,
                CommandFailure::UnsupportedCapability
            );
            super::super::native_event_query::broker_genesis(route.cell.as_str(), &input)
        })();
        let owner = self.internal_observer();
        Box::pin(async move {
            let mut route = route;
            if prepared? {
                // Genesis owns only a creation identity. The exact configuration
                // is read afterwards with fresh quorum and target-minimum proof;
                // no unused child business sequence can be left behind.
                owner
                    .resolve_resource_metadata(&mut route, deadline, true)
                    .await?;
            }
            owner
                .query_resource_after(route, input, deadline, source_proven, None)
                .await
                .map(|(value, _)| value)
        })
    }
}

fn configuration_operation_absent(response: &RpcData) -> anyhow::Result<bool> {
    super::super::native_event_query::configuration_operation_absent(response)
}

#[cfg(test)]
#[path = "event_read_tests.rs"]
mod tests;
