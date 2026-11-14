// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Weak node ownership behind ordinary binding dispatch.

use super::super::require_framework_dispatch;
use super::*;
use crate::application::{
    cell_bindings::{Host, Route},
    HandlerFuture,
};
use crate::engine_api::RpcData;
use crate::raft::completion::RuntimeError;
use cellar_logic::{
    cell_group::CellId,
    consensus::{ClusterId, IncarnationId, NamespaceId, ProposalId},
};

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
    fn prepare_kv_payload(
        &self,
        source: &CellHistory,
        configuration: [u8; 32],
        scope: &str,
        digest: [u8; 32],
        length: u64,
    ) -> anyhow::Result<
        Option<(
            CellHistory,
            crate::raft::cell_groups::CellGroupPayloadReference,
        )>,
    > {
        let owner = self.owner()?;
        Ok(owner.handle.core.host.prepare_binding_kv_payload(
            source,
            configuration,
            scope,
            digest,
            length,
            &owner.handle.core.log,
        )?)
    }

    fn kv_binding_payload_store(
        &self,
        source: &CellHistory,
        configuration: [u8; 32],
        target: &CellHistory,
    ) -> anyhow::Result<crate::raft::cell_groups::CellGroupPayloadStore> {
        let owner = self.owner()?;
        Ok(owner.handle.core.host.kv_binding_payload_store(
            source,
            configuration,
            target,
            &owner.handle.core.log,
        )?)
    }

    fn kv_payload_store(
        &self,
        source: &CellHistory,
        configuration: [u8; 32],
    ) -> anyhow::Result<crate::raft::cell_groups::CellGroupPayloadStore> {
        Ok(self
            .owner()?
            .handle
            .core
            .host
            .kv_payload_store(source, configuration)?)
    }

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
        let owner = self.owner()?;
        let route = owner.handle.core.host.binding_route(
            source,
            configuration,
            scope,
            method,
            &owner.handle.core.log,
        )?;
        if route.history_hint.is_some() {
            owner.handle.core.log.performance().increment(
                crate::raft::performance::Counter::CellBindingAppliedHistoryHints,
                1,
            );
        }
        Ok(route)
    }

    fn read_route(
        &self,
        source: &CellHistory,
        configuration: [u8; 32],
        scope: &str,
        method: &str,
    ) -> anyhow::Result<Route> {
        let owner = self.owner()?;
        Ok(owner.handle.core.host.workflow_read_route(
            Some(source),
            configuration,
            scope,
            method,
            &owner.handle.core.log,
        )?)
    }

    fn query(&self, route: Route, input: Vec<u8>, timeout: Duration) -> HandlerFuture<RpcData> {
        let prepared = require_framework_dispatch()
            .and_then(|()| self.owner())
            .map(|owner| owner.query_workflow_resource(route, input, timeout, true));
        Box::pin(async move { prepared?.await })
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

    fn retire(
        &self,
        pending: crate::storage::raft_binding_recovery::Pending,
        timeout: Duration,
    ) -> HandlerFuture<bool> {
        let owner = self.owner();
        Box::pin(async move {
            let owner = owner?;
            let body = &pending.body;
            let history = CellHistory {
                cluster: ClusterId::new(body.cluster)?,
                namespace: NamespaceId::new(body.namespace)?,
                cell: CellId::new(&body.cell)?,
                incarnation: IncarnationId::new(pending.selected.unwrap_or(body.candidate))?,
            };
            let deadline = owner
                .handle
                .core
                .log
                .domain()
                .mono_ms()?
                .checked_add(u64::try_from(timeout.as_millis())?)
                .ok_or(RuntimeError::DeadlineExceeded)?;
            let remaining = || -> anyhow::Result<Duration> {
                Ok(Duration::from_millis(
                    deadline
                        .checked_sub(owner.handle.core.log.domain().mono_ms()?)
                        .filter(|n| *n != 0)
                        .ok_or(RuntimeError::DeadlineExceeded)?,
                ))
            };
            let minimum = crate::storage::raft_binding_recovery::durability(body.durability)?;
            let target = owner.handle.core.host.cell(history.clone())?;
            target.verify_binding_recovery_schema(body.schema)?;
            if body.enroll && pending.selected.is_none() {
                // Enrollment is an independently idempotent, durable creation
                // obligation. Recovery uses its original creation and policy,
                // never the original RPC input or a fresh candidate history.
                let enrolled = owner
                    .cell(history.clone())?
                    .enroll_direct(
                        CellEnrollmentRequest {
                            creation: ProposalId::new(body.creation)?,
                            durability: minimum,
                        },
                        remaining()?,
                    )
                    .await;
                if let Err(error) = enrolled {
                    let observed = owner
                        .resolve_cell_history_direct(
                            history.cluster,
                            history.namespace,
                            history.cell.clone(),
                            QueryOptions {
                                timeout: remaining()?,
                                ..QueryOptions::default()
                            },
                        )
                        .await?;
                    if observed.as_ref().is_some_and(|current| current != &history)
                        && body.child_sequence == 1
                    {
                        return Ok(true);
                    }
                    return Err(error.into());
                }
            }
            target
                .retire_binding(
                    &owner.handle.core.transport,
                    body.schema,
                    crate::raft::execution::binding_seal::Request {
                        format: 1,
                        client: body.child_client,
                        sequence: body.child_sequence,
                        operation: body.operation.clone(),
                        contract: body.contract,
                        request_digest: body.request_digest,
                        durability: body.durability,
                        retry_until_ms: body.retry_until_ms,
                    },
                    remaining()?,
                )
                .await?;
            Ok(false)
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
