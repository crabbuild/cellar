// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Weak node ownership behind ordinary binding dispatch.

use super::super::require_framework_dispatch;
use super::*;
use crate::application::{
    cell_bindings::{Host, Route},
    HandlerFuture,
};
use crate::raft::completion::RuntimeError;

pub(super) struct BindingHost(pub std::sync::Weak<Handle>);

impl BindingHost {
    fn owner(&self) -> anyhow::Result<RaftCells> {
        let handle = self.0.upgrade().ok_or(RuntimeError::Closed)?;
        let owner = RaftCells::from_internal_handle(handle);
        owner.check_open()?;
        Ok(owner)
    }
}

impl Host for BindingHost {
    fn admit(
        &self,
        group: GroupHistory,
        bytes: usize,
    ) -> anyhow::Result<crate::application::cell_bindings::BufferLease> {
        Ok(self
            .owner()?
            .handle
            .core
            .controller
            .retain_binding_bytes(group, bytes)?)
    }

    fn route(
        &self,
        source: &CellHistory,
        configuration: [u8; 32],
        scope: &str,
        method: &str,
    ) -> anyhow::Result<Route> {
        Ok(self
            .owner()?
            .handle
            .core
            .host
            .binding_route(source, configuration, scope, method)?)
    }

    fn resolve(&self, route: Route, timeout: Duration) -> HandlerFuture<Option<CellHistory>> {
        let prepared = require_framework_dispatch()
            .and_then(|()| self.owner())
            .map(|owner| {
                owner.resolve_cell_history_direct(
                    route.cluster,
                    route.namespace,
                    route.cell,
                    QueryOptions {
                        timeout,
                        ..QueryOptions::default()
                    },
                )
            });
        Box::pin(async move { prepared?.await.map_err(anyhow::Error::from) })
    }

    fn enroll(
        &self,
        history: CellHistory,
        creation: super::super::ProposalId,
        durability: Durability,
        timeout: Duration,
    ) -> HandlerFuture<()> {
        let prepared = require_framework_dispatch()
            .and_then(|()| self.owner())
            .and_then(|owner| {
                Ok(owner.cell(history)?.enroll_direct(
                    CellEnrollmentRequest {
                        creation,
                        durability,
                    },
                    timeout,
                ))
            });
        Box::pin(async move {
            prepared?.await.map_err(anyhow::Error::from)?;
            Ok(())
        })
    }

    fn command(
        &self,
        history: CellHistory,
        contract: OperationContract,
        input: Vec<u8>,
        options: CommandOptions,
    ) -> HandlerFuture<CommandReceipt> {
        let prepared = require_framework_dispatch()
            .and_then(|()| self.owner())
            .and_then(|owner| {
                Ok(owner
                    .cell(history)?
                    .command_direct(&contract, input, options))
            });
        Box::pin(async move { prepared?.await.map_err(anyhow::Error::from) })
    }
}
