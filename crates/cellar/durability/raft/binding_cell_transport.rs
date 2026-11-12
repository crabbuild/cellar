// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

use super::*;
use crate::raft::execution::binding_seal;

impl RaftTransport {
    pub(crate) async fn retire_cell_binding(
        &self,
        route: &cellar_logic::cell_group::GroupRoute,
        cell: &CellHistory,
        member: MemberId,
        request: binding_seal::Request,
        keys: VersionKeys,
        deadline: u64,
    ) -> Result<(), TransportError> {
        let remaining = deadline
            .checked_sub(
                self.handle
                    .0
                    .domain
                    .mono_ms()
                    .map_err(|_| TransportError::Closed)?,
            )
            .filter(|n| *n != 0)
            .ok_or(TransportError::Timeout)?;
        let response = self
            .write_cell(
                route,
                cell,
                member,
                WriteCall::Binding(wire::binding::Call {
                    request,
                    timeout_ms: remaining,
                    route_epoch: route.epoch.get(),
                    birth_digest: route.birth_digest,
                }),
                Some(keys),
                deadline,
            )
            .await?;
        if !matches!(response.value, WriteValue::Binding) {
            return Err(TransportError::Protocol);
        }
        Ok(())
    }
}

impl State {
    pub(in crate::raft::transport) async fn execute_binding_retirement(
        &self,
        route: &QueryRoute,
        call: wire::binding::Call,
        deadline: u64,
    ) -> Result<super::super::query_replies::Reply, TransportError> {
        let (_, target, cell) = route.decode_shared()?;
        self.authorize_cell_command(route)?;
        let gateway = self
            .cell_command_gateways
            .lock()
            .map_err(|_| TransportError::Closed)?
            .get(&(cell.cluster, cell.namespace))
            .and_then(|g| g.upgrade())
            .ok_or(TransportError::Refused)?;
        if !gateway.accepting.load(Ordering::Acquire) {
            return Err(TransportError::Refused);
        }
        let result = async {
            let cut = self
                .runtime
                .shared()
                .map_err(|_| CommandFailure::Unavailable)?
                .quorum_membership(target.scope, deadline)
                .await
                .map_err(|_| CommandFailure::Unavailable)?;
            let selected = cut.root().route().clone();
            if selected.schema_digest != route.schema()
                || selected.epoch.get() != call.route_epoch
                || selected.birth_digest != call.birth_digest
            {
                return Err(CommandFailure::Unavailable);
            }
            self.authorize_cell_command(route)
                .map_err(|_| CommandFailure::Unavailable)?;
            gateway
                .controller
                .binding_retire_local(
                    &gateway.dispatcher,
                    gateway.storage.for_cell(&cell),
                    cell,
                    (selected, deadline),
                    call.request.clone(),
                )
                .await
                .map_err(|error| command_failure(&error))
        }
        .await;
        super::super::query_replies::Reply::binding_retirement(
            result,
            &call.request,
            self.options.max_query_response_bytes,
        )
    }
}
